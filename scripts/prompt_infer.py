"""
Prompt-only inference for Northwind (MySQL): builds context with system prompt + extracted schema JSON
and asks gpt-4.1-mini to generate a SQL query.

Usage:
  python scripts/prompt_infer.py "Domanda in italiano" --k 0 --dry-run
  python scripts/prompt_infer.py "Domanda in italiano"
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from datetime import datetime
import time
import re

from dotenv import load_dotenv
import mysql.connector
from openai import OpenAI


ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT / "dataset"
SYSTEM_PROMPT_FILE = DATASET_DIR / "northwind_system_prompt.txt"
SCHEMA_FILE = DATASET_DIR / "northwind_schema_canonical.json"

# Stima prezzi per modelli noti (USD per 1M token). Sovrascrivibile da CLI/env.
# Default ora impostato su gpt-4.1-mini
DEFAULT_PRICING = {
    "gpt-4.1-mini": {"input": 0.40, "output": 1.60},  # valori indicativi
    # Prezzi standard (modalità chat) per gpt-4.1: usare override CLI/env se diverso
    "gpt-4.1": {"input": 3.00, "output": 12.00},
}


def build_messages(question: str, include_context: bool = True):
    """Return messages for the chat API. If include_context is False, only the user question is returned.
    """
    if not include_context:
        return [{"role": "user", "content": question}]

    system = SYSTEM_PROMPT_FILE.read_text(encoding="utf-8").strip()
    schema = {}
    if SCHEMA_FILE.exists():
        try:
            schema = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
        except Exception:
            schema = {}
    # Attach schema JSON into the same system message, as expected by the prompt
    if schema:
        system = f"{system}\n\nSchema (JSON):\n" + json.dumps(schema, ensure_ascii=False, indent=2)
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": question},
    ]


def _normalize_single_sql(text: str) -> str | None:
    """Return the raw text as SQL if it already starts with WITH or SELECT.
    Removes code fences; ensures a trailing semicolon for consistency.
    Skips regex-based extraction because the model is instructed to output ONLY the final query."""
    if not text:
        return None
    cleaned = re.sub(r"```(?:sql)?\n", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"```", "", cleaned).strip()
    low = cleaned.lower()
    if not (low.startswith("with") or low.startswith("select")):
        return None
    if not cleaned.rstrip().endswith(";"):
        cleaned = cleaned.rstrip() + ";"
    return cleaned


def _build_count_sql(stmt: str) -> str:
    body = stmt.rstrip().rstrip(";")
    low = body.lower()
    if low.startswith("with"):
        idx = low.rfind("select")
        if idx == -1:
            return "SELECT 0"
        cte_part = body[:idx]
        main_select = body[idx:]
        return f"{cte_part} SELECT COUNT(*) FROM ( {main_select} ) AS _t"
    return f"SELECT COUNT(*) FROM ( {body} ) AS _t"


def _run_query(sql: str, preview_rows: int = 10):
    sql_clean = sql.strip()
    parts = [p.strip() for p in sql_clean.split(';') if p.strip()]
    if len(parts) == 2 and parts[0].lower().startswith('with') and parts[1].lower().startswith('select'):
        sql_clean = parts[0] + '\n' + parts[1]
    if not sql_clean.rstrip().endswith(';'):
        sql_clean += ';'
    head = sql_clean.strip().lower()
    if not (head.startswith('select') or head.startswith('with')):
        raise ValueError("Sono permesse solo query SELECT (anche con CTE WITH) in modalità verifica.")
    if ';' in sql_clean.strip()[:-1]:
        raise ValueError("La query non deve contenere più statement.")

    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DB", "Northwind")
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
    try:
        # Usa cursori "buffered" per evitare l'errore "Unread result found"
        cur = conn.cursor(buffered=True)
        cur.execute(sql_clean)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(preview_rows)
        # Conta righe totali con wrapper CTE-aware
        count_sql = _build_count_sql(sql_clean)
        cur2 = conn.cursor(buffered=True)
        cur2.execute(count_sql)
        total = cur2.fetchone()[0]
        cur2.close()
        cur.close()
        return {"columns": cols, "preview": rows, "total": total}
    finally:
        conn.close()


def _resolve_pricing(model: str, in_cli: float | None, out_cli: float | None):
    # Ordine di priorità: CLI > env > default map > 0.0
    if in_cli is not None and out_cli is not None:
        return in_cli, out_cli
    in_env = os.getenv("OPENAI_INPUT_COST_PER_MT")
    out_env = os.getenv("OPENAI_OUTPUT_COST_PER_MT")
    if in_env and out_env:
        try:
            return float(in_env), float(out_env)
        except Exception:
            pass
    # Normalize model id: handle 'ft:' prefixes and versioned slugs
    base = (model or "").strip()
    if base.startswith("ft:"):
        # ft:<model-slug> or ft:<base-model>:... -> remove leading 'ft:'
        base = base[len("ft:"):]
    # Check known keys inside the model slug (handles versioned names)
    for key in DEFAULT_PRICING:
        if key in base:
            return DEFAULT_PRICING[key]["input"], DEFAULT_PRICING[key]["output"]
    return 0.0, 0.0


def _log_cost(row: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp",
                "model",
                "question",
                "sql",
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "input_cost_per_mt",
                "output_cost_per_mt",
                "input_cost",
                "output_cost",
                "total_cost",
                "latency_s",
            ],
        )
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("question", type=str, help="Domanda in linguaggio naturale")
    parser.add_argument("--model", type=str, default="gpt-4.1-2025-04-14", help="Modello da usare")
    parser.add_argument("--no-context", action="store_true", help="Non includere system prompt e schema nel messaggio; usa solo la domanda (utile con modelli FT)")
    parser.add_argument("--dry-run", action="store_true", help="Stampa i messaggi senza chiamare l'API")
    parser.add_argument("--exec", action="store_true", help="Esegue la SQL generata su MySQL e mostra un'anteprima dei risultati")
    parser.add_argument("--validate", action="store_true", help="Valida la sintassi su MySQL con EXPLAIN senza eseguire la query")
    parser.add_argument("--input", type=float, default=None, help="Costo input per 1M token (USD). Se omesso usa env o default modello")
    parser.add_argument("--output", type=float, default=None, help="Costo output per 1M token (USD). Se omesso usa env o default modello")
    parser.add_argument("--cost-log", type=str, default=str(ROOT / "results" / "qa_cost_log.csv"), help="Percorso CSV per tracciare domanda, risposta e costi")
    args = parser.parse_args()

    messages = build_messages(args.question, include_context=not args.no_context)
    if args.dry_run:
        print("=== Dry-run: messaggi ===")
        for m in messages:
            print(f"[{m['role']}]\n{m['content'][:800]}\n---")
        return

    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY mancante. Definirlo in .env o nell'ambiente.")

    client = OpenAI()
    t0 = time.perf_counter()
    resp = client.chat.completions.create(
        model=args.model,
        messages=messages,
        temperature=0.0,
    )
    latency = time.perf_counter() - t0
    raw = resp.choices[0].message.content.strip()
    sql = _normalize_single_sql(raw)
    print("=== Output modello (completo) ===")
    print(raw)
    if sql:
        print("\n=== SQL estratta ===")
        print(sql)

    # Calcolo e log costi
    usage = getattr(resp, "usage", None)
    prompt_toks = getattr(usage, "prompt_tokens", None) if usage else None
    completion_toks = getattr(usage, "completion_tokens", None) if usage else None
    total_toks = getattr(usage, "total_tokens", None) if usage else None
    in_cost_mt, out_cost_mt = _resolve_pricing(args.model, args.input, args.output)
    input_cost = output_cost = total_cost = None
    if prompt_toks is not None and completion_toks is not None:
        input_cost = (prompt_toks * in_cost_mt) / 1_000_000
        output_cost = (completion_toks * out_cost_mt) / 1_000_000
        total_cost = (input_cost or 0) + (output_cost or 0)
        print("\n[USAGE]")
        print(f"Prompt tokens: {prompt_toks}")
        print(f"Completion tokens: {completion_toks}")
        print(f"Total tokens: {total_toks}")
        # Print with higher precision to avoid showing 0 for very small costs
        print(f"Costo stimato: input=${input_cost:.9f} output=${output_cost:.9f} totale=${total_cost:.9f}")
    # Log sempre una riga (anche se usage mancante) con domanda+risposta+costi (se disponibili)
    try:
        _log_cost(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "model": args.model,
                "question": args.question,
                "sql": sql,
                "prompt_tokens": prompt_toks,
                "completion_tokens": completion_toks,
                "total_tokens": total_toks,
                "input_cost_per_mt": in_cost_mt,
                "output_cost_per_mt": out_cost_mt,
                "input_cost": float(f"{input_cost:.12f}") if input_cost is not None else None,
                "output_cost": float(f"{output_cost:.12f}") if output_cost is not None else None,
                "total_cost": float(f"{total_cost:.12f}") if total_cost is not None else None,
                "latency_s": round(latency, 3),
            },
            Path(args.cost_log),
        )
    except Exception:
        pass

    if args.exec:
        # Carica anche eventuali credenziali DB
        load_dotenv()
        try:
            if not sql:
                print("\n[ESECUZIONE SQL] Nessuna SQL valida da eseguire (attesa WITH/SELECT).")
                return
            result = _run_query(sql)
        except Exception as e:
            print(f"\n[ESECUZIONE SQL] Errore: {e}")
            return
        print("\n[ESECUZIONE SQL] Anteprima risultati (max 10 righe):")
        if result["columns"]:
            print(" | ".join(result["columns"]))
        for r in result["preview"]:
            print(" | ".join(str(x) for x in r))
        print(f"\nTotale righe: {result['total']}")
    elif args.validate:
        # Valida sintassi con EXPLAIN senza eseguire la query
        load_dotenv()
        try:
            if not sql:
                print("\n[VALIDAZIONE] Nessuna SQL valida da spiegare (attesa WITH/SELECT).")
                return
            host = os.getenv("MYSQL_HOST", "localhost")
            user = os.getenv("MYSQL_USER", "root")
            password = os.getenv("MYSQL_PASSWORD", "")
            database = os.getenv("MYSQL_DB", "Northwind")
            conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
            cur = conn.cursor()
            sql_clean = sql.strip()
            parts = [p.strip() for p in sql_clean.split(';') if p.strip()]
            if len(parts) == 2 and parts[0].lower().startswith('with') and parts[1].lower().startswith('select'):
                sql_clean = parts[0] + '\n' + parts[1]
            if not sql_clean.rstrip().endswith(';'):
                sql_clean += ';'
            cur.execute(f"EXPLAIN {sql_clean}")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            print("\n[VALIDAZIONE] EXPLAIN output:")
            if cols:
                print(" | ".join(cols))
            for r in rows:
                print(" | ".join(str(x) for x in r))
            cur.close()
            conn.close()
        except Exception as e:
            print(f"\n[VALIDAZIONE] Errore sintassi: {e}")


if __name__ == "__main__":
    main()
