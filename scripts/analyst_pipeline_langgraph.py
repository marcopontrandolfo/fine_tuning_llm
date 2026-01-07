"""
Agentic pipeline con LangGraph: SQL generation + Data Analysis

Pipeline (grafo):
  START → generate_sql → execute_sql → analyze_results → END
                              ↓ (errore)
                             END

Uso:
  python scripts/analyst_pipeline_langgraph.py "Quali sono le vendite mensili per categoria nel 2006?"
  python scripts/analyst_pipeline_langgraph.py "Top 10 clienti per fatturato" --analysis-depth detailed
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict, Annotated

from dotenv import load_dotenv
import mysql.connector
from openai import OpenAI

# LangGraph imports
from langgraph.graph import StateGraph, START, END


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

# Analyst prompts
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
# STATE DEFINITION (LangGraph)
# ─────────────────────────────────────────────────────────────────────────────
class PipelineState(TypedDict):
    """Stato condiviso tra i nodi del grafo."""
    # Input
    question: str
    prompt_file: Optional[str]
    sql_model: str
    analyst_model: str
    analysis_depth: str
    max_rows: int
    data_preview_count: int
    
    # Output SQL generation
    sql: Optional[str]
    sql_usage: Optional[Dict[str, Any]]
    sql_cost: Optional[Dict[str, float]]
    sql_error: Optional[str]
    
    # Output SQL execution
    columns: Optional[List[str]]
    rows: Optional[List[List[Any]]]
    total_rows: Optional[int]
    exec_error: Optional[str]
    
    # Output analysis
    analysis: Optional[str]
    analyst_usage: Optional[Dict[str, Any]]
    analyst_cost: Optional[Dict[str, float]]
    
    # Metadata
    timestamp: str
    total_cost: float


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
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


def normalize_sql(text: str) -> str:
    """Pulisce l'output del modello estraendo la query SQL."""
    if not text:
        return ""
    cleaned = re.sub(r"```(?:sql)?\n?", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"```", "", cleaned).strip()
    if cleaned and not cleaned.endswith(";"):
        cleaned += ";"
    return cleaned


def calculate_cost(usage: Dict[str, Any]) -> Dict[str, float]:
    """Calcola il costo in USD per un'invocazione."""
    model = usage.get("model", "")
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    
    base_model = model[3:] if model.startswith("ft:") else model
    
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


def build_count_sql(stmt: str) -> str:
    """Costruisce una query COUNT(*) per contare i risultati."""
    body = stmt.rstrip().rstrip(";")
    body_no_order = re.sub(r"\s+ORDER\s+BY\s+.*$", "", body, flags=re.IGNORECASE)
    return f"SELECT COUNT(*) FROM ( {body_no_order} ) AS _cnt"


def format_data_as_text(columns: List[str], rows: List[List], total: int) -> str:
    """Formatta i dati come tabella testuale per l'LLM."""
    if not rows:
        return "(nessun dato)"
    
    lines = []
    lines.append(" | ".join(columns))
    lines.append("-" * len(lines[0]))
    
    for row in rows:
        lines.append(" | ".join(str(v) if v is not None else "NULL" for v in row))
    
    if len(rows) < total:
        lines.append(f"... (altre {total - len(rows)} righe non mostrate)")
    
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# GRAPH NODES
# ─────────────────────────────────────────────────────────────────────────────
def node_generate_sql(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 1: Genera la query SQL usando il modello fine-tuned.
    """
    print("🔄 [Node: generate_sql] Generazione query SQL...")
    
    try:
        client = OpenAI()
        
        messages: List[Dict[str, str]] = []
        messages.append({"role": "system", "content": "Sei un assistente SQL esperto per Northwind su MySQL 8."})
        
        user_content = state["question"]
        if state.get("prompt_file"):
            prompt_path = Path(state["prompt_file"])
            if prompt_path.exists():
                prompt_text = prompt_path.read_text(encoding="utf-8")
                user_content = f"{prompt_text}\n\n{state['question']}"
        
        messages.append({"role": "user", "content": user_content})
        
        response = client.chat.completions.create(
            model=state["sql_model"],
            messages=messages,
            temperature=0.1,
            max_tokens=2000,
        )
        
        raw_output = response.choices[0].message.content or ""
        sql = normalize_sql(raw_output)
        
        usage = {
            "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
            "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            "model": state["sql_model"],
        }
        
        cost = calculate_cost(usage)
        
        print(f"✅ [Node: generate_sql] Query generata ({usage['completion_tokens']} tokens)")
        
        return {
            "sql": sql,
            "sql_usage": usage,
            "sql_cost": cost,
            "sql_error": None,
        }
        
    except Exception as e:
        print(f"❌ [Node: generate_sql] Errore: {e}")
        return {
            "sql": None,
            "sql_error": str(e),
        }


def node_execute_sql(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 2: Esegue la query SQL su MySQL.
    """
    print("🔄 [Node: execute_sql] Esecuzione query su MySQL...")
    
    if state.get("sql_error") or not state.get("sql"):
        return {
            "exec_error": state.get("sql_error") or "Nessuna query da eseguire",
            "columns": [],
            "rows": [],
            "total_rows": 0,
        }
    
    try:
        load_dotenv()
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        database = os.getenv("MYSQL_DB", "Northwind")
        
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cur = conn.cursor(buffered=True)
        cur.execute(state["sql"])
        
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(state["max_rows"])
        
        total = len(rows)
        if len(rows) == state["max_rows"]:
            count_sql = build_count_sql(state["sql"])
            cur2 = conn.cursor(buffered=True)
            try:
                cur2.execute(count_sql)
                total = cur2.fetchone()[0]
            except:
                pass
            finally:
                cur2.close()
        
        cur.close()
        conn.close()
        
        print(f"✅ [Node: execute_sql] Query eseguita: {total} righe")
        
        return {
            "columns": columns,
            "rows": [list(r) for r in rows],
            "total_rows": total,
            "exec_error": None,
        }
        
    except Exception as e:
        print(f"❌ [Node: execute_sql] Errore: {e}")
        return {
            "columns": [],
            "rows": [],
            "total_rows": 0,
            "exec_error": str(e),
        }


def node_analyze_results(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 3: Analizza i risultati usando gpt-4.1-mini.
    """
    print("🔄 [Node: analyze_results] Analisi dati con LLM...")
    
    if state.get("exec_error"):
        return {
            "analysis": f"❌ Query non eseguibile: {state['exec_error']}",
            "analyst_usage": {},
            "analyst_cost": {"input_cost": 0, "output_cost": 0, "total_cost": 0},
        }
    
    if not state.get("rows"):
        return {
            "analysis": "📊 Nessun dato da analizzare: la query non ha restituito risultati.",
            "analyst_usage": {},
            "analyst_cost": {"input_cost": 0, "output_cost": 0, "total_cost": 0},
        }
    
    try:
        client = OpenAI()
        
        columns = state["columns"]
        rows = state["rows"]
        total = state["total_rows"]
        
        preview_rows = rows[:100]
        data_text = format_data_as_text(columns, preview_rows, total)
        
        system_prompt = ANALYST_SYSTEM_PROMPT_DETAILED if state["analysis_depth"] == "detailed" else ANALYST_SYSTEM_PROMPT
        
        user_prompt = f"""Domanda originale dell'utente:
"{state['question']}"

Query SQL eseguita:
```sql
{state['sql']}
```

Risultati ({len(preview_rows)} righe mostrate su {total} totali):
{data_text}

Fornisci la tua analisi dei dati."""

        response = client.chat.completions.create(
            model=state["analyst_model"],
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
            "model": state["analyst_model"],
        }
        
        cost = calculate_cost(usage)
        
        print(f"✅ [Node: analyze_results] Analisi completata ({usage['completion_tokens']} tokens)")
        
        return {
            "analysis": analysis,
            "analyst_usage": usage,
            "analyst_cost": cost,
        }
        
    except Exception as e:
        print(f"❌ [Node: analyze_results] Errore: {e}")
        return {
            "analysis": f"❌ Errore durante l'analisi: {e}",
            "analyst_usage": {},
            "analyst_cost": {"input_cost": 0, "output_cost": 0, "total_cost": 0},
        }


def node_finalize(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo finale: Calcola costi totali.
    """
    sql_cost = state.get("sql_cost", {}).get("total_cost", 0)
    analyst_cost = state.get("analyst_cost", {}).get("total_cost", 0)
    
    return {
        "total_cost": sql_cost + analyst_cost,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CONDITIONAL EDGES
# ─────────────────────────────────────────────────────────────────────────────
def should_continue_after_sql(state: PipelineState) -> str:
    """Decide se continuare dopo l'esecuzione SQL."""
    if state.get("sql_error"):
        return "finalize"  # Salta l'analisi se c'è errore SQL
    return "execute_sql"


def should_continue_after_exec(state: PipelineState) -> str:
    """Decide se continuare dopo l'esecuzione."""
    # Sempre vai all'analisi (anche per mostrare errori)
    return "analyze_results"


# ─────────────────────────────────────────────────────────────────────────────
# BUILD GRAPH
# ─────────────────────────────────────────────────────────────────────────────
def build_pipeline_graph() -> StateGraph:
    """
    Costruisce il grafo LangGraph:
    
    START → generate_sql → execute_sql → analyze_results → finalize → END
                 ↓ (errore)
              finalize → END
    """
    # Crea il grafo con lo stato tipizzato
    graph = StateGraph(PipelineState)
    
    # Aggiungi i nodi
    graph.add_node("generate_sql", node_generate_sql)
    graph.add_node("execute_sql", node_execute_sql)
    graph.add_node("analyze_results", node_analyze_results)
    graph.add_node("finalize", node_finalize)
    
    # Aggiungi gli edge
    graph.add_edge(START, "generate_sql")
    graph.add_conditional_edges(
        "generate_sql",
        should_continue_after_sql,
        {
            "execute_sql": "execute_sql",
            "finalize": "finalize",
        }
    )
    graph.add_edge("execute_sql", "analyze_results")
    graph.add_edge("analyze_results", "finalize")
    graph.add_edge("finalize", END)
    
    return graph


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
    data_preview_count: int = 20,
) -> Dict[str, Any]:
    """
    Esegue la pipeline LangGraph.
    """
    load_dotenv()
    
    # Determina il modello SQL
    if not sql_model:
        sql_model = get_finetuned_model()
    
    # Stato iniziale
    initial_state: PipelineState = {
        "question": question,
        "prompt_file": str(prompt_file) if prompt_file else None,
        "sql_model": sql_model,
        "analyst_model": analyst_model,
        "analysis_depth": analysis_depth,
        "max_rows": max_rows,
        "data_preview_count": data_preview_count,
        "timestamp": datetime.now().isoformat(),
        # Output fields (inizializzati a None/default)
        "sql": None,
        "sql_usage": None,
        "sql_cost": None,
        "sql_error": None,
        "columns": None,
        "rows": None,
        "total_rows": None,
        "exec_error": None,
        "analysis": None,
        "analyst_usage": None,
        "analyst_cost": None,
        "total_cost": 0.0,
    }
    
    # Costruisci e compila il grafo
    graph = build_pipeline_graph()
    app = graph.compile()
    
    # Esegui il grafo
    print("\n" + "=" * 60)
    print("🚀 LANGGRAPH PIPELINE - START")
    print("=" * 60)
    
    final_state = app.invoke(initial_state)
    
    print("=" * 60)
    print("🏁 LANGGRAPH PIPELINE - END")
    print("=" * 60 + "\n")
    
    return final_state


def print_result(result: Dict[str, Any], show_data: bool = True, data_preview: int = 10):
    """Stampa i risultati della pipeline in modo formattato."""
    print("\n" + "=" * 80)
    print("📋 PIPELINE RISULTATI")
    print("=" * 80)
    
    print(f"\n📝 Domanda: {result.get('question', 'N/A')}")
    
    print(f"\n🗄️ Query SQL ({result.get('sql_model', 'N/A')}):")
    print("-" * 40)
    print(result.get("sql", "N/A"))
    
    if result.get("exec_error"):
        print(f"\n❌ Errore esecuzione: {result['exec_error']}")
    else:
        print(f"\n📊 Risultati: {result.get('total_rows', 0)} righe")
        
        if show_data and result.get("rows"):
            cols = result.get("columns", [])
            preview = result["rows"][:data_preview]
            if cols and preview:
                print("-" * 40)
                print(" | ".join(cols))
                print("-" * 40)
                for row in preview:
                    print(" | ".join(str(v) if v is not None else "NULL" for v in row))
                if result.get("total_rows", 0) > data_preview:
                    print(f"... (altre {result['total_rows'] - data_preview} righe)")
    
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
        description="Pipeline LangGraph: SQL generation + Data Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  python scripts/analyst_pipeline_langgraph.py "Vendite mensili per categoria nel 2006"
  python scripts/analyst_pipeline_langgraph.py "Top 10 clienti" --analysis-depth detailed
  python scripts/analyst_pipeline_langgraph.py "Ordini per paese" --no-data --json
        """,
    )
    parser.add_argument("question", type=str, help="Domanda in linguaggio naturale")
    parser.add_argument(
        "--prompt-file", type=str, default=None,
        help="File con prompt per SQL generator (es. dataset/new_prompt.txt)"
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
        data_preview_count=args.data_preview,
    )
    
    if args.json:
        output = dict(result)
        if output.get("rows") and len(output["rows"]) > 20:
            output["rows"] = output["rows"][:20]
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, show_data=not args.no_data, data_preview=args.data_preview)


if __name__ == "__main__":
    main()
