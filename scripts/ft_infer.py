"""
Inference helper for a fine-tuned Northwind SQL model.

Features:
- Reads fine_tuned_model from results/sft_state.json with --use-ft (preferred)
- Allows overriding model via --model
- Sends only the user question to the fine-tuned model (the FT model is expected to encode the prompting rules)
- Extracts the first SQL SELECT from the model response (removes code fences and leading text)
- Computes token usage costs (default prices: gpt-4.1-mini input=0.4, output=1.6 USD per 1M tokens)
- Optionally executes the SQL on MySQL and shows an preview + total rows (--exec)

Usage example:
  python .\scripts\ft_infer.py "Elenca i 10 clienti con più ordini negli ultimi 12 mesi." --use-ft --exec
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import mysql.connector
from openai import OpenAI


ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT / "dataset"
SYSTEM_PROMPT_FILE = DATASET_DIR / "northwind_system_prompt.txt"
SCHEMA_FILE = DATASET_DIR / "northwind_schema_canonical.json"
STATE_FILE = ROOT / "results" / "sft_state.json"

# Default pricing for gpt-4.1-mini (as requested)
DEFAULT_PRICING = {
    "gpt-4.1-mini": {"input": 0.4, "output": 1.6},
}


def build_messages(question: str) -> List[Dict[str, str]]:
    """Return only the user message for FT inference (FT model contains the prompting rules)."""
    return [{"role": "user", "content": question}]


def extract_first_select(text: str) -> Optional[str]:
    """Try to extract the first SELECT statement from the model output.
    Handles code fences and leading explanation text.
    Returns the SQL string including a trailing ';' if found, else None.
    """
    if not text:
        return None
    # Remove common code fences
    text = re.sub(r"```(?:sql)?\n", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```", "", text)
    # Normalize whitespace
    txt = text.strip()
    # Find the first occurrence of 'select' (case-insensitive)
    m = re.search(r"\bselect\b", txt, flags=re.IGNORECASE)
    if not m:
        return None
    start = m.start()
    candidate = txt[start:]
    # Truncate at the first standalone semicolon that likely ends the statement
    semi = candidate.find(";")
    if semi != -1:
        candidate = candidate[: semi + 1]
    # As a last step, ensure it starts with SELECT
    candidate = candidate.strip()
    if not candidate.lower().startswith("select"):
        return None
    return candidate


def _run_query(sql: str, preview_rows: int = 10) -> Dict[str, Any]:
    if not sql.strip().lower().startswith("select"):
        raise ValueError("Sono permesse solo query SELECT in modalità verifica.")
    if ";" in sql.strip()[:-1]:
        # allow trailing semicolon but disallow multiple statements
        raise ValueError("La query non deve contenere più statement.")

    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    # Never hardcode secrets: require env or use empty default
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DB", "Northwind")
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
    try:
        cur = conn.cursor(buffered=True)
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(preview_rows)
        count_sql = f"SELECT COUNT(*) FROM ( {sql.rstrip(';')} ) AS _t"
        cur2 = conn.cursor(buffered=True)
        cur2.execute(count_sql)
        total = cur2.fetchone()[0]
        cur2.close()
        cur.close()
        return {"columns": cols, "preview": rows, "total": total}
    finally:
        conn.close()


def _resolve_pricing(model: str, in_cli: float | None, out_cli: float | None):
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
        base = base[len("ft:"):]
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
    parser.add_argument("--model", type=str, default=None, help="Model id da usare (se non specificato usa --use-ft)")
    parser.add_argument("--use-ft", action="store_true", help="Usa il modello fine-tuned registrato in results/sft_state.json")
    parser.add_argument("--exec", action="store_true", help="Esegue la SQL generata su MySQL e mostra un'anteprima dei risultati")
    parser.add_argument("--input", type=float, default=None, help="Costo input per 1M token (USD). Se omesso usa env o default modello")
    parser.add_argument("--output", type=float, default=None, help="Costo output per 1M token (USD). Se omesso usa env o default modello")
    parser.add_argument("--cost-log", type=str, default=str(ROOT / "results" / "qa_cost_log.csv"), help="Percorso CSV per tracciare domanda, risposta e costi")
    args = parser.parse_args()

    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY mancante. Definirlo in .env o nell'ambiente.")

    model = args.model
    if args.use_ft:
        state_path = Path(STATE_FILE)
        if not state_path.exists():
            raise RuntimeError(f"State file non trovato: {state_path}")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        ft_model = state.get("fine_tuned_model") or state.get("result_model")
        if not ft_model:
            raise RuntimeError("Nessun 'fine_tuned_model' presente in state. Esegui sft_status.py --update-state prima.")
        model = ft_model

    if not model:
        raise RuntimeError("Nessun modello specificato. Usa --model o --use-ft.")

    messages = build_messages(args.question)

    client = OpenAI()
    t0 = time.perf_counter()
    resp = client.chat.completions.create(model=model, messages=messages, temperature=0.0)
    latency = time.perf_counter() - t0

    # Extract SQL
    raw = resp.choices[0].message.content.strip()
    sql = extract_first_select(raw)
    if not sql:
        # Fallback: use whole response (but don't execute)
        print("Output modello (nessuna SELECT trovata):\n")
        print(raw)
        return

    print(sql)

    # Cost calculation
    usage = getattr(resp, "usage", None)
    prompt_toks = getattr(usage, "prompt_tokens", None) if usage else None
    completion_toks = getattr(usage, "completion_tokens", None) if usage else None
    total_toks = getattr(usage, "total_tokens", None) if usage else None
    in_cost_mt, out_cost_mt = _resolve_pricing(model, args.input, args.output)
    input_cost = output_cost = total_cost = None
    if prompt_toks is not None and completion_toks is not None:
        input_cost = (prompt_toks * in_cost_mt) / 1_000_000
        output_cost = (completion_toks * out_cost_mt) / 1_000_000
        total_cost = (input_cost or 0) + (output_cost or 0)
        print("\n[USAGE]")
        print(f"Prompt tokens: {prompt_toks}")
        print(f"Completion tokens: {completion_toks}")
        print(f"Total tokens: {total_toks}")
        print(f"Costo stimato: input=${input_cost:.9f} output=${output_cost:.9f} totale=${total_cost:.9f}")

    try:
        _log_cost(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "model": model,
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
        try:
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


if __name__ == "__main__":
    main()
