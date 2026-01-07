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

# Default pricing map (USD per 1M tokens). Overridable via CLI/env
DEFAULT_PRICING = {
    "gpt-4.1-mini": {"input": 0.4, "output": 1.6},
    # Standard pricing for gpt-4.1 (chat): adjust via --input/--output or env if needed
    "gpt-4.1": {"input": 3.0, "output": 12.0},
}


def build_messages(question: str, prompt_file: Optional[str] = None) -> List[Dict[str, str]]:
    """Costruisce i messaggi per l'inferenza.
    - Se `prompt_file` è fornito, lo usa come messaggio di sistema (regole/prompt) concatenando il testo del file.
    - In ogni caso include la domanda dell'utente come messaggio `user`.
    """
    msgs: List[Dict[str, str]] = []
    if prompt_file:
        p = Path(prompt_file)
        if not p.exists():
            raise RuntimeError(f"Prompt file non trovato: {prompt_file}")
        system_text = p.read_text(encoding="utf-8")
        msgs.append({"role": "system", "content": system_text})
    msgs.append({"role": "user", "content": question})
    return msgs


def load_messages_from_file(path: str) -> List[Dict[str, str]]:
    """Load chat messages from a JSON/JSONL file.
    Accepted formats:
      - {"messages": [{"role": "system"|"user"|"assistant", "content": "..."}, ...]}
      - [{"role": ..., "content": ...}, ...]
      - JSONL with a single line containing one of the above objects

    For inference, assistant messages are ignored; only system and user are sent.
    """
    p = Path(path)
    text = p.read_text(encoding="utf-8").strip()
    obj: Any
    if p.suffix.lower() == ".jsonl":
        # take the first non-empty jsonl line
        line = next((ln for ln in text.splitlines() if ln.strip()), "")
        if not line:
            raise ValueError(f"File {path} è vuoto")
        obj = json.loads(line)
    else:
        obj = json.loads(text)

    if isinstance(obj, dict) and "messages" in obj:
        msgs = obj["messages"]
    elif isinstance(obj, list):
        msgs = obj
    else:
        raise ValueError("Formato file non valido: atteso dict con 'messages' o lista di messaggi")

    # keep only system and user messages for inference
    cleaned: List[Dict[str, str]] = []
    for m in msgs:
        role = m.get("role")
        content = m.get("content")
        if role in {"system", "user"} and isinstance(content, str):
            cleaned.append({"role": role, "content": content})
    if not cleaned:
        raise ValueError("Nessun messaggio valido trovato (system/user)")
    return cleaned


def _normalize_single_sql(text: str) -> Optional[str]:
    """Attempt to extract a single executable SELECT from model output.
    - Removes code fences
    - Accepts outputs starting with SELECT or WITH directly
    - If output is a DDL like CREATE VIEW ... AS SELECT ..., extracts the SELECT body after AS
    - If multiple statements are present, attempts to pick the first SELECT statement
    Returns None if no SELECT could be found.
    """
    if not text:
        return None
    cleaned = re.sub(r"```(?:sql)?\n", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"```", "", cleaned).strip()
    low = cleaned.lower()

    # Direct SELECT / WITH
    if low.startswith("select") or low.startswith("with"):
        out = cleaned.rstrip()
        if not out.endswith(";"):
            out += ";"
        return out

    # Handle CREATE VIEW ... AS SELECT ...
    if low.startswith("create view") or low.startswith("create or replace view"):
        # Find the last occurrence of ' as ' and take the substring after it
        # to be robust against view names with spaces/backticks
        idx = low.rfind(" as ")
        if idx != -1:
            after = cleaned[idx + 4 :].strip()
            # If the remaining starts with SELECT/WITH, normalize it
            alow = after.lower()
            if alow.startswith("select") or alow.startswith("with"):
                if not after.endswith(";"):
                    after += ";"
                return after

    # If multiple statements, try to pick the first SELECT
    stmts = [s.strip() for s in re.split(r";\s*", cleaned) if s.strip()]
    for s in stmts:
        sl = s.lower()
        if sl.startswith("select") or sl.startswith("with"):
            if not s.endswith(";"):
                s += ";"
            return s
    return None


def _build_count_sql(stmt: str) -> str:
    body = stmt.rstrip().rstrip(";")
    low = body.lower()
    # Rimuovi ORDER BY finale dalla SELECT principale quando si fa il COUNT,
    # poiché MySQL non consente ORDER BY in subquery senza LIMIT.
    def _strip_order_by(sql_text: str) -> str:
        # cerca un ORDER BY fuori da parentesi e rimuovilo
        # semplice euristica: taglia l'ultimo ' order by ' non dentro WITH/CTE.
        m = re.search(r"\border\s+by\b", sql_text, flags=re.IGNORECASE)
        if not m:
            return sql_text
        # Se esiste, prendi solo la parte prima di ORDER BY
        return sql_text[:m.start()].strip()
    if low.startswith("with"):
        # Separate CTE part from main SELECT for counting
        idx = low.rfind("select")
        if idx == -1:
            return f"SELECT 0"  # malformed
        cte_part = body[:idx]
        main_select = body[idx:]
        main_select_no_order = _strip_order_by(main_select)
        return f"{cte_part} SELECT COUNT(*) FROM ( {main_select_no_order} ) AS _t"
    # Plain SELECT
    return f"SELECT COUNT(*) FROM ( {_strip_order_by(body)} ) AS _t"


def _run_query(sql: str, preview_rows: int = 10) -> Dict[str, Any]:
    # Tolleranza: unisci pattern WITH ...; SELECT ...; in unico statement
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
        cur = conn.cursor(buffered=True)
        cur.execute(sql_clean)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(preview_rows)
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
    parser.add_argument("question", type=str, nargs="?", default=None, help="Domanda in linguaggio naturale (alternativa a --msg-file)")
    parser.add_argument("--msg-file", type=str, default=None, help="Percorso file JSON/JSONL con messaggi (system+user) in formato simile al dataset; se presente, ignora 'question'")
    parser.add_argument("--prompt-file", type=str, default=None, help="Percorso file testo con regole prompt da inviare come messaggio di sistema (es. dataset/new_prompt.txt)")
    parser.add_argument("--model", type=str, default=None, help="Model id da usare (se non specificato usa --use-ft)")
    parser.add_argument("--use-ft", action="store_true", help="Usa il modello fine-tuned registrato in results/sft_state.json")
    parser.add_argument("--exec", action="store_true", help="Esegue la SQL generata su MySQL e mostra un'anteprima dei risultati")
    parser.add_argument("--validate", action="store_true", help="Valida la sintassi su MySQL con EXPLAIN senza eseguire la query")
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

    if args.msg_file:
        messages = load_messages_from_file(args.msg_file)
        # Se è stato fornito anche --prompt-file, aggiungilo come messaggio USER
        # prima della domanda presente nel file, così il modello lo considererà parte dell'input utente.
        if args.prompt_file:
            p = Path(args.prompt_file)
            if not p.exists():
                raise RuntimeError(f"Prompt file non trovato: {args.prompt_file}")
            user_text = p.read_text(encoding="utf-8")
            messages = [{"role": "user", "content": user_text}] + messages
    else:
        if not args.question:
            raise RuntimeError("Specificare una domanda oppure --msg-file con i messaggi da inviare")
        messages = build_messages(args.question, args.prompt_file)

    client = OpenAI()
    t0 = time.perf_counter()
    resp = client.chat.completions.create(model=model, messages=messages, temperature=0.0)
    latency = time.perf_counter() - t0

    # Extract SQL
    raw = resp.choices[0].message.content.strip()
    sql = _normalize_single_sql(raw)

    # Always show full model output first
    print("=== Output modello (completo) ===")
    print(raw)

    if not sql:
        # Nessuna SELECT trovata: mostra suggerimenti e termina
        print("\n[INFO] Nessuna SELECT trovata nell'output.\n- Se l'output è DDL (es. CREATE VIEW), verrà mostrato solo il testo completo.\n- Per eseguire, il runner richiede una SELECT singola.\n- Suggerimento: il modello può generare direttamente una SELECT equivalente oppure usare --msg-file con un prompt che richiede una SELECT.")
        return

    # Then show the extracted SQL (used for execution and logging)
    print("\n=== SQL estratta ===")
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
    elif args.validate:
        # Usa EXPLAIN per verificare che la query sia sintatticamente corretta senza esecuzione completa
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        database = os.getenv("MYSQL_DB", "Northwind")
        try:
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
        except Exception as e:
            print(f"\n[VALIDAZIONE] Errore sintassi: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
