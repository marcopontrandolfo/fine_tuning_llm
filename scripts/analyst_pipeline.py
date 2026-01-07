"""
Agentic pipeline: SQL generation + Data Analysis

Pipeline:
  1. Utente pone una domanda in linguaggio naturale
  2. Modello fine-tuned genera la query SQL
  3. Query eseguita su MySQL Northwind
  4. Risultati passati a gpt-4.1-mini per analisi statistica
  5. Output: query SQL + dati + insights dell'analista

Uso:
  python scripts/analyst_pipeline.py "Quali sono le vendite mensili per categoria nel 2006?"
  python scripts/analyst_pipeline.py "Top 10 clienti per fatturato" --analysis-depth detailed
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import mysql.connector
from openai import OpenAI


# ─────────────────────────────────────────────────────────────────────────────
# PATHS & CONFIG
# ─────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
STATE_FILE = ROOT / "results" / "sft_state.json"

# Pricing (USD per 1M tokens)
PRICING = {
    "gpt-4.1-mini": {"input": 0.4, "output": 1.6},
    "gpt-4.1": {"input": 3.0, "output": 12.0},
}

# Analyst system prompt
ANALYST_SYSTEM_PROMPT = """\
Sei un Data Analyst esperto. Ricevi i risultati di una query SQL eseguita su un database e devi fornire un'analisi statistica e insights.

Linee guida:
- Analizza la distribuzione dei dati (min, max, media, mediana se applicabile)
- Identifica pattern, trend, outlier o anomalie
- Fornisci interpretazioni business-oriented
- Sii conciso ma completo
- Usa numeri e percentuali per supportare le tue osservazioni
- Se i dati sono pochi, nota i limiti dell'analisi
- Formatta l'output in modo leggibile (usa elenchi puntati, sezioni)

Rispondi in italiano.
"""

ANALYST_SYSTEM_PROMPT_DETAILED = """\
Sei un Senior Data Analyst. Ricevi i risultati di una query SQL e devi fornire un'analisi statistica approfondita.

Analisi richiesta:
1. **Statistiche descrittive**: media, mediana, deviazione standard, min/max, range, quartili (se applicabile)
2. **Distribuzione**: forma della distribuzione, skewness, presenza di outlier
3. **Pattern e Trend**: tendenze temporali, stagionalità, correlazioni tra variabili
4. **Anomalie**: valori insoliti, gap nei dati, incongruenze
5. **Segmentazione**: raggruppamenti naturali nei dati
6. **Insights di business**: interpretazioni actionable, raccomandazioni
7. **Limitazioni**: cosa non si può concludere dai dati disponibili

Formatta l'output in sezioni chiare. Usa numeri e percentuali.
Rispondi in italiano.
"""


# ─────────────────────────────────────────────────────────────────────────────
# SQL GENERATION (using fine-tuned model)
# ─────────────────────────────────────────────────────────────────────────────
def get_finetuned_model() -> str:
    """Legge il modello fine-tuned da sft_state.json"""
    if not STATE_FILE.exists():
        raise RuntimeError(f"State file non trovato: {STATE_FILE}. Esegui prima il fine-tuning.")
    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    model = state.get("fine_tuned_model") or state.get("result_model")
    if not model:
        raise RuntimeError("Nessun modello fine-tuned trovato in sft_state.json")
    return model


def generate_sql(client: OpenAI, model: str, question: str, prompt_file: Optional[Path] = None) -> tuple[str, dict]:
    """
    Genera SQL usando il modello fine-tuned.
    Returns: (sql_query, usage_info)
    """
    messages: List[Dict[str, str]] = []
    
    # System message fisso
    messages.append({"role": "system", "content": "Sei un assistente SQL esperto per Northwind su MySQL 8."})
    
    # Costruisci il contenuto user: prompt + domanda
    user_content = question
    if prompt_file and prompt_file.exists():
        prompt_text = prompt_file.read_text(encoding="utf-8")
        user_content = f"{prompt_text}\n\n{question}"
    
    messages.append({"role": "user", "content": user_content})
    
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.1,
        max_tokens=2000,
    )
    
    raw_output = response.choices[0].message.content or ""
    sql = _normalize_sql(raw_output)
    
    usage = {
        "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
        "completion_tokens": response.usage.completion_tokens if response.usage else 0,
        "model": model,
    }
    
    return sql, usage


def _normalize_sql(text: str) -> str:
    """Pulisce l'output del modello estraendo la query SQL."""
    if not text:
        return ""
    # Rimuovi code fences
    cleaned = re.sub(r"```(?:sql)?\n?", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"```", "", cleaned).strip()
    
    # Assicurati che termini con ;
    if cleaned and not cleaned.endswith(";"):
        cleaned += ";"
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# SQL EXECUTION
# ─────────────────────────────────────────────────────────────────────────────
def execute_query(sql: str, max_rows: int = 500) -> Dict[str, Any]:
    """
    Esegue la query su MySQL e ritorna i risultati.
    Returns: {"columns": [...], "rows": [...], "total": int, "error": str|None}
    """
    load_dotenv()
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DB", "Northwind")
    
    try:
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cur = conn.cursor(buffered=True)
        cur.execute(sql)
        
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(max_rows)
        
        # Count totale
        total = len(rows)
        if len(rows) == max_rows:
            # Potrebbe esserci di più, fai count
            count_sql = _build_count_sql(sql)
            cur2 = conn.cursor(buffered=True)
            try:
                cur2.execute(count_sql)
                total = cur2.fetchone()[0]
            except:
                pass  # fallback al numero di righe fetchate
            finally:
                cur2.close()
        
        cur.close()
        conn.close()
        
        return {
            "columns": columns,
            "rows": [list(r) for r in rows],  # converti tuple in liste per JSON
            "total": total,
            "error": None,
        }
    except Exception as e:
        return {
            "columns": [],
            "rows": [],
            "total": 0,
            "error": str(e),
        }


def _build_count_sql(stmt: str) -> str:
    """Costruisce una query COUNT(*) per contare i risultati."""
    body = stmt.rstrip().rstrip(";")
    # Rimuovi ORDER BY per evitare errori in subquery
    body_no_order = re.sub(r"\s+ORDER\s+BY\s+.*$", "", body, flags=re.IGNORECASE)
    return f"SELECT COUNT(*) FROM ( {body_no_order} ) AS _cnt"


# ─────────────────────────────────────────────────────────────────────────────
# DATA ANALYSIS (using gpt-4.1-mini)
# ─────────────────────────────────────────────────────────────────────────────
def analyze_results(
    client: OpenAI,
    question: str,
    sql: str,
    data: Dict[str, Any],
    analysis_depth: str = "standard",
    analyst_model: str = "gpt-4.1-mini",
) -> tuple[str, dict]:
    """
    Analizza i risultati della query usando gpt-4.1-mini.
    Returns: (analysis_text, usage_info)
    """
    if data.get("error"):
        return f"❌ Impossibile analizzare: errore nell'esecuzione della query.\n{data['error']}", {}
    
    if not data.get("rows"):
        return "📊 Nessun dato da analizzare: la query non ha restituito risultati.", {}
    
    # Prepara i dati per l'analista
    columns = data["columns"]
    rows = data["rows"]
    total = data["total"]
    
    # Formatta i dati come tabella testuale (max 100 righe per il prompt)
    preview_rows = rows[:100]
    data_text = _format_data_as_text(columns, preview_rows, total)
    
    # Scegli il prompt in base alla profondità
    system_prompt = ANALYST_SYSTEM_PROMPT_DETAILED if analysis_depth == "detailed" else ANALYST_SYSTEM_PROMPT
    
    user_prompt = f"""Domanda originale dell'utente:
"{question}"

Query SQL eseguita:
```sql
{sql}
```

Risultati ({len(preview_rows)} righe mostrate su {total} totali):
{data_text}

Fornisci la tua analisi dei dati."""

    response = client.chat.completions.create(
        model=analyst_model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
        max_tokens=1500,
    )
    
    analysis = response.choices[0].message.content or ""
    
    usage = {
        "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
        "completion_tokens": response.usage.completion_tokens if response.usage else 0,
        "model": analyst_model,
    }
    
    return analysis, usage


def _format_data_as_text(columns: List[str], rows: List[List], total: int) -> str:
    """Formatta i dati come tabella testuale per l'LLM."""
    if not rows:
        return "(nessun dato)"
    
    lines = []
    # Header
    lines.append(" | ".join(columns))
    lines.append("-" * len(lines[0]))
    
    # Rows
    for row in rows:
        lines.append(" | ".join(str(v) if v is not None else "NULL" for v in row))
    
    if len(rows) < total:
        lines.append(f"... (altre {total - len(rows)} righe non mostrate)")
    
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# COST CALCULATION
# ─────────────────────────────────────────────────────────────────────────────
def calculate_cost(usage: dict) -> dict:
    """Calcola il costo in USD per un'invocazione."""
    model = usage.get("model", "")
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    
    # Trova pricing (gestisce prefisso ft:)
    base_model = model
    if model.startswith("ft:"):
        base_model = model[3:]
    
    pricing = {"input": 0.0, "output": 0.0}
    for key in PRICING:
        if key in base_model:
            pricing = PRICING[key]
            break
    
    input_cost = (prompt_tokens / 1_000_000) * pricing["input"]
    output_cost = (completion_tokens / 1_000_000) * pricing["output"]
    
    return {
        "input_cost": input_cost,
        "output_cost": output_cost,
        "total_cost": input_cost + output_cost,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(
    question: str,
    prompt_file: Optional[Path] = None,
    sql_model: Optional[str] = None,
    analyst_model: str = "gpt-4.1-mini",
    analysis_depth: str = "standard",
    max_rows: int = 500,
    show_data: bool = True,
    data_preview: int = 20,
) -> Dict[str, Any]:
    """
    Esegue l'intera pipeline: SQL generation → Execution → Analysis
    """
    load_dotenv()
    client = OpenAI()
    
    # 1. Determina il modello SQL
    if not sql_model:
        sql_model = get_finetuned_model()
    
    result = {
        "question": question,
        "sql_model": sql_model,
        "analyst_model": analyst_model,
        "timestamp": datetime.now().isoformat(),
    }
    
    # 2. Genera SQL
    print("🔄 Generazione query SQL...")
    try:
        sql, sql_usage = generate_sql(client, sql_model, question, prompt_file)
        result["sql"] = sql
        result["sql_usage"] = sql_usage
        result["sql_cost"] = calculate_cost(sql_usage)
        print(f"✅ Query generata")
    except Exception as e:
        result["error"] = f"Errore generazione SQL: {e}"
        return result
    
    # 3. Esegui query
    print("🔄 Esecuzione query su MySQL...")
    data = execute_query(sql, max_rows=max_rows)
    result["data"] = {
        "columns": data["columns"],
        "total_rows": data["total"],
        "error": data.get("error"),
    }
    
    if data.get("error"):
        print(f"❌ Errore SQL: {data['error']}")
        result["analysis"] = f"Query non eseguibile: {data['error']}"
        return result
    
    print(f"✅ Query eseguita: {data['total']} righe")
    
    # 4. Analisi dati
    print("🔄 Analisi dati con LLM...")
    analysis, analyst_usage = analyze_results(
        client, question, sql, data, 
        analysis_depth=analysis_depth,
        analyst_model=analyst_model,
    )
    result["analysis"] = analysis
    result["analyst_usage"] = analyst_usage
    result["analyst_cost"] = calculate_cost(analyst_usage)
    print("✅ Analisi completata")
    
    # 5. Costo totale
    sql_cost = result.get("sql_cost", {}).get("total_cost", 0)
    analyst_cost = result.get("analyst_cost", {}).get("total_cost", 0)
    result["total_cost"] = sql_cost + analyst_cost
    
    # 6. Preview dati (opzionale)
    if show_data:
        result["data_preview"] = data["rows"][:data_preview]
    
    return result


def print_result(result: Dict[str, Any], show_data: bool = True, data_preview: int = 10):
    """Stampa i risultati della pipeline in modo formattato."""
    print("\n" + "=" * 80)
    print("📋 PIPELINE RISULTATI")
    print("=" * 80)
    
    print(f"\n📝 Domanda: {result.get('question', 'N/A')}")
    
    print(f"\n🗄️ Query SQL ({result.get('sql_model', 'N/A')}):")
    print("-" * 40)
    print(result.get("sql", "N/A"))
    
    if result.get("data", {}).get("error"):
        print(f"\n❌ Errore esecuzione: {result['data']['error']}")
    else:
        data_info = result.get("data", {})
        print(f"\n📊 Risultati: {data_info.get('total_rows', 0)} righe")
        
        if show_data and result.get("data_preview"):
            cols = data_info.get("columns", [])
            preview = result["data_preview"][:data_preview]
            if cols and preview:
                print("-" * 40)
                print(" | ".join(cols))
                print("-" * 40)
                for row in preview:
                    print(" | ".join(str(v) if v is not None else "NULL" for v in row))
                if data_info.get("total_rows", 0) > data_preview:
                    print(f"... (altre {data_info['total_rows'] - data_preview} righe)")
    
    print(f"\n🔍 Analisi ({result.get('analyst_model', 'N/A')}):")
    print("-" * 40)
    print(result.get("analysis", "N/A"))
    
    # Costi
    print("\n" + "-" * 40)
    print("💰 COSTI:")
    sql_cost = result.get("sql_cost", {})
    analyst_cost = result.get("analyst_cost", {})
    print(f"   SQL Generator: ${sql_cost.get('total_cost', 0):.6f}")
    print(f"   Data Analyst:  ${analyst_cost.get('total_cost', 0):.6f}")
    print(f"   TOTALE:        ${result.get('total_cost', 0):.6f}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline: SQL generation + Data Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  python scripts/analyst_pipeline.py "Vendite mensili per categoria nel 2006"
  python scripts/analyst_pipeline.py "Top 10 clienti" --analysis-depth detailed
  python scripts/analyst_pipeline.py "Ordini per paese" --no-data --json
        """,
    )
    parser.add_argument("question", type=str, help="Domanda in linguaggio naturale")
    parser.add_argument(
        "--prompt-file", type=str, default=None,
        help="File con system prompt per SQL generator (es. dataset/new_prompt.txt)"
    )
    parser.add_argument(
        "--sql-model", type=str, default=None,
        help="Modello per SQL (default: fine-tuned da sft_state.json)"
    )
    parser.add_argument(
        "--analyst-model", type=str, default="gpt-4.1-mini",
        help="Modello per analisi (default: gpt-4.1-mini)"
    )
    parser.add_argument(
        "--analysis-depth", type=str, choices=["standard", "detailed"], default="standard",
        help="Profondità analisi: standard o detailed"
    )
    parser.add_argument(
        "--max-rows", type=int, default=500,
        help="Max righe da fetchare dal DB (default: 500)"
    )
    parser.add_argument(
        "--data-preview", type=int, default=10,
        help="Righe di anteprima dati da mostrare (default: 10)"
    )
    parser.add_argument(
        "--no-data", action="store_true",
        help="Non mostrare anteprima dati nell'output"
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output in formato JSON"
    )
    args = parser.parse_args()
    
    prompt_file = Path(args.prompt_file) if args.prompt_file else None
    
    result = run_pipeline(
        question=args.question,
        prompt_file=prompt_file,
        sql_model=args.sql_model,
        analyst_model=args.analyst_model,
        analysis_depth=args.analysis_depth,
        max_rows=args.max_rows,
        show_data=not args.no_data,
        data_preview=args.data_preview,
    )
    
    if args.json:
        # Rimuovi data_preview se troppo grande per JSON leggibile
        output = result.copy()
        if output.get("data_preview") and len(output["data_preview"]) > 20:
            output["data_preview"] = output["data_preview"][:20]
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, show_data=not args.no_data, data_preview=args.data_preview)


if __name__ == "__main__":
    main()
