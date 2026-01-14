"""Agentic pipeline Orchestrator-Worker-Orchestrator

Pipeline (grafo):
  START → orchestrate (decompose) → execute_subtasks (parallelo) → orchestrate (analyze) → END

L'orchestratore ha due fasi:
  1. DECOMPOSIZIONE: scompone la richiesta complessa in N sotto-query indipendenti
  2. ANALISI: sintetizza tutti i risultati delle sotto-query in un report integrato

Uso:
  python scripts/analyst_pipeline_orchestrated.py "Analizza le performance di vendita del 2006"
  python scripts/analyst_pipeline_orchestrated.py "Quali sono i trend e le anomalie nelle vendite?" --verbose
"""

from __future__ import annotations

import argparse
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

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

# ─────────────────────────────────────────────────────────────────────────────
# PROMPTS
# ─────────────────────────────────────────────────────────────────────────────
ORCHESTRATOR_SYSTEM_PROMPT = """\
Sei un Data Analyst Orchestrator esperto. Il tuo compito è scomporre una richiesta di analisi complessa in sotto-query SQL più semplici e indipendenti.

Database: Northwind (MySQL 8)

⚠️ REGOLA CRITICA - OUTPUT SINTETICO:
Questo sistema è progettato per database con MILIONI di righe.
L'output di tutte le sotto-query sarà letto da un LLM per l'analisi finale, quindi ogni query DEVE essere progettata per restituire POCHE RIGHE (idealmente 1-30).

COME garantire output ridotto:
- USA sempre aggregazioni: SUM(), COUNT(), AVG(), MAX(), MIN()
- USA sempre GROUP BY su poche categorie (es. 8 categorie, 4 trimestri, 12 mesi)
- USA sempre TOP N / LIMIT per classifiche (Top 10 clienti, Top 5 prodotti)
- MAI query "lista tutti" o "mostra ogni" - sempre "i migliori N" o "il totale per X"

✅ TIPI DI QUERY CONSENTITI (output ridotto):
- Aggregazioni: "Calcola il ricavo TOTALE...", "Conta il NUMERO di..."
- Top/Bottom N: "Top 10 clienti...", "I 5 prodotti meno venduti..."
- Medie/Percentuali: "Media ordini per cliente", "Percentuale crescita YoY"
- Raggruppamenti LIMITATI: "Ricavo PER categoria (8 categorie)", "Vendite PER mese (12 mesi)"
- Conteggi: "Quanti ordini...", "Numero di clienti attivi"
- Confronti aggregati: "Confronta il ricavo Q1 vs Q2"
- Valori singoli: "Mese con il ricavo massimo", "Cliente più fedele"

❌ TIPI DI QUERY VIETATI (troppi dati):
- "Per ogni prodotto..." → Centinaia di righe! Usa invece "Top 10 prodotti..."
- "Per ogni cliente..." → Troppi clienti! Usa invece "Top 20 clienti..."
- "Mostra tutti gli ordini..." → Milioni di righe!
- "Lista completa..." → Mai liste complete!
- Query senza aggregazione E senza LIMIT

⚠️ TRASFORMAZIONI OBBLIGATORIE:
Se la domanda dell'utente chiede "per ogni X", DEVI trasformarla:
- "Per ogni prodotto" → "Top 10/20 prodotti per [metrica]"
- "Per ogni cliente" → "Top 10/20 clienti per [metrica]"  
- "Per ogni mese degli ultimi 24 mesi" → "Trend trimestrale (8 righe)" oppure "Top 6 mesi per [metrica]"
- "Proiezioni per ogni prodotto" → "Proiezioni per i Top 10 prodotti"

Linee guida:
1. Analizza la richiesta dell'utente
2. Identifica le diverse dimensioni/aspetti da analizzare
3. TRASFORMA richieste "per ogni X" in "Top N di X" per limitare output
4. Genera sotto-domande INDIPENDENTI (non devono dipendere l'una dall'altra)
5. Ogni sotto-domanda deve essere specifica e focalizzata su UN aspetto
6. OGNI query deve contenere: aggregazioni (SUM, COUNT, AVG) E/O limiti espliciti (TOP N, LIMIT)
7. Non generare più di 8 sotto-query (ottimizza per copertura)
8. Specifica sempre il numero atteso di righe tra parentesi: "(max 10 righe)", "(12 mesi)"

Rispondi SOLO con un JSON array di oggetti, ogni oggetto ha:
- "id": numero progressivo (1, 2, 3...)
- "aspect": breve descrizione dell'aspetto analizzato (es. "trend_mensile", "top_clienti")
- "question": la sotto-domanda in linguaggio naturale da passare al generatore SQL

Esempio di output:
[
  {"id": 1, "aspect": "total_revenue", "question": "Calcola il ricavo TOTALE del 2006 (1 riga)"},
  {"id": 2, "aspect": "revenue_by_category", "question": "Calcola il ricavo totale PER categoria nel 2006 (max 8 righe)"},
  {"id": 3, "aspect": "quarterly_trend", "question": "Calcola il ricavo TOTALE per trimestre del 2006 (4 righe)"},
  {"id": 4, "aspect": "top_customers", "question": "Top 10 clienti per ricavo nel 2006 (10 righe)"},
  {"id": 5, "aspect": "top_products", "question": "Top 10 prodotti per ricavo nel 2006 (10 righe)"},
  {"id": 6, "aspect": "yoy_growth", "question": "Calcola la percentuale di crescita del ricavo 2006 vs 2005 (1 riga)"}
]
"""

ORCHESTRATOR_ANALYZE_PROMPT = """\
Sei lo stesso Data Analyst Orchestrator che ha scomposto la richiesta iniziale.
Ora ricevi i risultati delle sotto-query che hai generato e devi fornire un'analisi completa e integrata.

Database: Northwind (MySQL 8)

Il tuo compito:
1. **Sintesi esecutiva**: riassumi i punti chiave in 2-3 frasi
2. **Analisi per aspetto**: per ogni sotto-analisi che hai richiesto, estrai gli insight principali
3. **Pattern identificati**: tendenze, correlazioni tra i diversi aspetti
4. **Anomalie e outlier**: valori insoliti, picchi, cali significativi
5. **Confronti**: metti in relazione i diversi aspetti analizzati
6. **Valutazione Followup** (se presenti query di approfondimento): 
   Struttura questa sezione in modo SPECIFICO:
   
   a) **PROBLEMA RILEVATO**: Descrivi ESATTAMENTE quale anomalia, lacuna informativa o aspetto critico 
      è emerso dall'analisi iniziale che ha richiesto approfondimento.
      Esempio: "L'analisi iniziale ha mostrato un calo del 40% in Q3, ma non identificava le cause"
      Esempio: "I dati iniziali coprivano solo il periodo corrente, mancava il confronto storico"
      Esempio: "Era emersa una concentrazione anomala su pochi prodotti, ma non era chiaro se fosse strutturale"
   
   b) **STRATEGIA DI APPROFONDIMENTO**: Spiega quale approccio l'orchestratore ha adottato per 
      investigare il problema (quali dimensioni ha esplorato: temporale, per categoria, per prodotto, etc.)
   
   c) **RISOLUZIONE**: Descrivi COME i followup hanno risolto o chiarito il problema iniziale.
      Quali risposte concrete sono emerse? Il problema è stato spiegato? Confermato? Ridimensionato?
   
   d) **LIMITI**: Se alcuni followup non hanno prodotto dati o non hanno risposto al problema, indicalo.
   
   NON elencare ogni query singolarmente. Scrivi un RACCONTO LOGICO del processo di approfondimento.
7. **Raccomandazioni**: suggerimenti actionable basati sui dati
8. **Limitazioni**: cosa non si può concludere, dati mancanti

Formatta l'output in modo chiaro con sezioni e bullet point.
Usa numeri e percentuali per supportare le osservazioni.
Rispondi in italiano.
"""

ORCHESTRATOR_CHECK_COMPLETENESS_PROMPT = """\
Sei un Data Analyst Orchestrator esperto. Hai ricevuto i risultati delle sotto-query eseguite.
Ora devi valutare se hai TUTTI i dati necessari per un'analisi completa e accurata.

Database: Northwind (MySQL 8)

Analizza criticamente i risultati e chiediti:
1. **Anomalie da approfondire**: Ci sono valori anomali (picchi, cali, outlier) che richiedono investigazione?
   - Es: Un calo del 40% in Q3 → Serve capire QUALI categorie/prodotti hanno causato il calo
2. **Aspetti mancanti**: La domanda dell'utente copre aspetti non ancora analizzati?
   - Es: L'utente chiede "analisi vendite" ma non abbiamo dati sui prodotti
3. **Correlazioni da verificare**: I dati suggeriscono correlazioni che andrebbero confermate?
   - Es: Top clienti sono tutti dalla stessa regione → Serve conferma geografica
4. **Contesto temporale**: Servono confronti con periodi precedenti per dare contesto?

⚠️ REGOLE PER LE NUOVE QUERY:
- Max 3 nuove query per iterazione
- Ogni query DEVE produrre poche righe (usa aggregazioni, TOP N, LIMIT)
- Le query devono essere INDIPENDENTI e focalizzate su UN aspetto specifico
- Specifica sempre il numero atteso di righe tra parentesi

Rispondi con un JSON object:
{
  "analysis_complete": true/false,
  "reasoning": "Breve spiegazione del perché servono o non servono altri dati",
  "followup_queries": [
    {"id": N, "aspect": "nome_aspetto", "question": "Domanda specifica (max X righe)"}
  ]
}

Se analysis_complete=true, followup_queries deve essere un array vuoto [].
Se analysis_complete=false, fornisci da 1 a 3 query di followup.
"""


# ─────────────────────────────────────────────────────────────────────────────
# TYPE DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────
class SubTask(TypedDict):
    """Singola sotto-query con i suoi risultati."""
    id: int
    aspect: str
    question: str
    sql: Optional[str]
    columns: Optional[List[str]]
    rows: Optional[List[List[Any]]]
    total_rows: Optional[int]
    error: Optional[str]
    sql_usage: Optional[Dict[str, Any]]
    is_followup: Optional[bool]  # True se generata durante cicli di followup


class PipelineState(TypedDict):
    """Stato condiviso tra i nodi del grafo."""
    # Input
    question: str
    prompt_file: Optional[str]
    sql_model: str
    orchestrator_model: str
    max_rows_per_query: int
    max_retries: int
    max_followups: int
    verbose: bool
    
    # Orchestration Phase 1: Decomposition
    subtasks: List[SubTask]
    orchestrator_decompose_usage: Optional[Dict[str, Any]]
    orchestrator_decompose_cost: Optional[Dict[str, float]]
    orchestrator_error: Optional[str]
    
    # Execution stats
    successful_subtasks: int
    failed_subtasks: int
    retried_subtasks: int
    total_sql_cost: float
    
    # Followup cycle tracking
    current_cycle: int
    analysis_complete: bool
    followup_reasoning: Optional[str]
    total_followup_queries: int
    orchestrator_check_usage: Optional[Dict[str, Any]]
    orchestrator_check_cost: Optional[Dict[str, float]]
    
    # Orchestration Phase 2: Analysis
    analysis: Optional[str]
    orchestrator_analyze_usage: Optional[Dict[str, Any]]
    orchestrator_analyze_cost: Optional[Dict[str, float]]
    
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


def is_aggregate_query(sql: str) -> bool:
    """
    Verifica se una query SQL è aggregata (produce poche righe).
    Ritorna True se la query contiene pattern che indicano output ridotto.
    """
    if not sql:
        return False
    
    sql_upper = sql.upper()
    
    # Pattern che indicano aggregazione/output ridotto
    aggregate_patterns = [
        r'\bSUM\s*\(',
        r'\bCOUNT\s*\(',
        r'\bAVG\s*\(',
        r'\bMAX\s*\(',
        r'\bMIN\s*\(',
        r'\bGROUP\s+BY\b',
        r'\bLIMIT\s+\d+',
        r'\bTOP\s+\d+',
        r'\bDISTINCT\b',
        r'\bHAVING\b',
    ]
    
    for pattern in aggregate_patterns:
        if re.search(pattern, sql_upper):
            return True
    
    return False


def validate_query_output_size(sql: str, rows_count: int, max_expected: int = 50) -> Dict[str, Any]:
    """
    Valida che una query produca un output di dimensioni ragionevoli.
    Ritorna warning se la query sembra non aggregata o produce troppe righe.
    """
    warnings = []
    
    if not is_aggregate_query(sql):
        warnings.append("Query non contiene aggregazioni (SUM, COUNT, AVG, GROUP BY, LIMIT)")
    
    if rows_count > max_expected:
        warnings.append(f"Query restituisce {rows_count} righe (max atteso: {max_expected})")
    
    return {
        "is_valid": len(warnings) == 0,
        "warnings": warnings,
        "is_aggregate": is_aggregate_query(sql),
        "rows_count": rows_count,
    }


def format_data_as_text(columns: List[str], rows: List[List], total: int, max_display: int = 20) -> str:
    """Formatta i dati come tabella testuale per l'LLM."""
    if not rows:
        return "(nessun dato)"
    
    lines = []
    lines.append(" | ".join(str(c) for c in columns))
    lines.append("-" * len(lines[0]))
    
    display_rows = rows[:max_display]
    for row in display_rows:
        lines.append(" | ".join(str(v) if v is not None else "NULL" for v in row))
    
    if len(rows) < total:
        lines.append(f"... ({total} righe totali)")
    elif len(display_rows) < len(rows):
        lines.append(f"... (mostrate {max_display} di {len(rows)} righe)")
    
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE SUBTASK EXECUTION (for parallel processing)
# ─────────────────────────────────────────────────────────────────────────────
def execute_single_subtask(
    subtask: SubTask,
    sql_model: str,
    prompt_file: Optional[str],
    max_rows: int,
    verbose: bool = False,
) -> SubTask:
    """
    Esegue una singola sotto-query: genera SQL → esegue su MySQL.
    Ritorna il subtask aggiornato con risultati o errore.
    """
    load_dotenv()
    result = dict(subtask)  # copia
    
    if verbose:
        print(f"   📝 [{subtask['id']}] {subtask['aspect']}: Generazione SQL...")
    
    # 1. Genera SQL
    try:
        client = OpenAI()
        
        messages: List[Dict[str, str]] = []
        messages.append({"role": "system", "content": "Sei un assistente SQL esperto per Northwind su MySQL 8."})
        
        user_content = subtask["question"]
        if prompt_file:
            prompt_path = Path(prompt_file)
            if prompt_path.exists():
                prompt_text = prompt_path.read_text(encoding="utf-8")
                user_content = f"{prompt_text}\n\n{subtask['question']}"
        
        messages.append({"role": "user", "content": user_content})
        
        response = client.chat.completions.create(
            model=sql_model,
            messages=messages,
            temperature=0.1,
            max_tokens=2000,
        )
        
        raw_output = response.choices[0].message.content or ""
        sql = normalize_sql(raw_output)
        
        result["sql"] = sql
        result["sql_usage"] = {
            "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
            "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            "model": sql_model,
        }
        
    except Exception as e:
        result["error"] = f"SQL generation error: {e}"
        return result
    
    if verbose:
        print(f"   ⚡ [{subtask['id']}] {subtask['aspect']}: Esecuzione query...")
    
    # 2. Esegui SQL
    try:
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        database = os.getenv("MYSQL_DB", "Northwind")
        
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cur = conn.cursor(buffered=True)
        cur.execute(result["sql"])
        
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(max_rows)
        total = len(rows)
        
        # Se abbiamo fetchato il max, prova a contare il totale
        if len(rows) == max_rows:
            try:
                body = result["sql"].rstrip().rstrip(";")
                body_no_order = re.sub(r"\s+ORDER\s+BY\s+.*$", "", body, flags=re.IGNORECASE)
                count_sql = f"SELECT COUNT(*) FROM ( {body_no_order} ) AS _cnt"
                cur2 = conn.cursor(buffered=True)
                cur2.execute(count_sql)
                total = cur2.fetchone()[0]
                cur2.close()
            except:
                pass
        
        cur.close()
        conn.close()
        
        result["columns"] = columns
        result["rows"] = [list(r) for r in rows]
        result["total_rows"] = total
        result["error"] = None
        
        # Validazione output size (guardrail)
        validation = validate_query_output_size(result["sql"], total)
        if not validation["is_valid"]:
            result["warnings"] = validation["warnings"]
            if verbose:
                for w in validation["warnings"]:
                    print(f"   ⚠️  [{subtask['id']}] {subtask['aspect']}: {w}")
        
        if verbose:
            print(f"   ✅ [{subtask['id']}] {subtask['aspect']}: {total} righe")
        
    except Exception as e:
        result["error"] = f"SQL execution error: {e}"
        result["columns"] = []
        result["rows"] = []
        result["total_rows"] = 0
        
        if verbose:
            print(f"   ❌ [{subtask['id']}] {subtask['aspect']}: {e}")
    
    return result


# ─────────────────────────────────────────────────────────────────────────────
# RETRY FAILED SUBTASK (with error context as guardrail)
# ─────────────────────────────────────────────────────────────────────────────
def retry_single_subtask(
    subtask: SubTask,
    sql_model: str,
    prompt_file: Optional[str],
    max_rows: int,
    verbose: bool = False,
) -> SubTask:
    """
    Ritenta una sotto-query fallita, passando l'errore precedente come contesto.
    Questo funge da guardrail per guidare il modello verso una query corretta.
    """
    load_dotenv()
    result = dict(subtask)  # copia
    
    previous_error = subtask.get("error", "")
    previous_sql = subtask.get("sql", "")
    
    if verbose:
        print(f"   🔄 [{subtask['id']}] {subtask['aspect']}: Retry con guardrail...")
    
    # 1. Genera SQL corretto usando l'errore come feedback
    try:
        client = OpenAI()
        
        # Costruisci prompt con contesto dell'errore (guardrail)
        guardrail_prompt = f"""La query SQL precedente ha generato un errore. Correggi la query.

DOMANDA ORIGINALE:
{subtask["question"]}

QUERY SQL PRECEDENTE (ERRATA):
```sql
{previous_sql}
```

ERRORE RICEVUTO:
{previous_error}

ISTRUZIONI PER LA CORREZIONE:
1. Analizza l'errore e identifica la causa
2. Verifica i nomi di tabelle e colonne (usa lo schema Northwind MySQL 8)
3. Controlla la sintassi SQL
4. Genera SOLO la query SQL corretta, senza spiegazioni

Tabelle principali Northwind: Customer, SalesOrder, OrderDetail, Product, Category, Supplier, Employee, Shipper, Region, Territory

Rispondi SOLO con la query SQL corretta:"""

        user_content = guardrail_prompt
        if prompt_file:
            prompt_path = Path(prompt_file)
            if prompt_path.exists():
                prompt_text = prompt_path.read_text(encoding="utf-8")
                user_content = f"{prompt_text}\n\n{guardrail_prompt}"
        
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": "Sei un assistente SQL esperto per Northwind su MySQL 8. Correggi le query errate."},
            {"role": "user", "content": user_content},
        ]
        
        response = client.chat.completions.create(
            model=sql_model,
            messages=messages,
            temperature=0.1,
            max_tokens=2000,
        )
        
        raw_output = response.choices[0].message.content or ""
        sql = normalize_sql(raw_output)
        
        result["sql"] = sql
        
        # Aggiorna usage (accumula)
        prev_usage = subtask.get("sql_usage") or {}
        new_prompt_tokens = response.usage.prompt_tokens if response.usage else 0
        new_completion_tokens = response.usage.completion_tokens if response.usage else 0
        
        result["sql_usage"] = {
            "prompt_tokens": prev_usage.get("prompt_tokens", 0) + new_prompt_tokens,
            "completion_tokens": prev_usage.get("completion_tokens", 0) + new_completion_tokens,
            "model": sql_model,
        }
        
    except Exception as e:
        result["error"] = f"Retry SQL generation error: {e}"
        return result
    
    if verbose:
        print(f"   ⚡ [{subtask['id']}] {subtask['aspect']}: Esecuzione query corretta...")
    
    # 2. Esegui la query corretta
    try:
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        database = os.getenv("MYSQL_DB", "Northwind")
        
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cur = conn.cursor(buffered=True)
        cur.execute(result["sql"])
        
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(max_rows)
        total = len(rows)
        
        if len(rows) == max_rows:
            try:
                body = result["sql"].rstrip().rstrip(";")
                body_no_order = re.sub(r"\s+ORDER\s+BY\s+.*$", "", body, flags=re.IGNORECASE)
                count_sql = f"SELECT COUNT(*) FROM ( {body_no_order} ) AS _cnt"
                cur2 = conn.cursor(buffered=True)
                cur2.execute(count_sql)
                total = cur2.fetchone()[0]
                cur2.close()
            except:
                pass
        
        cur.close()
        conn.close()
        
        result["columns"] = columns
        result["rows"] = [list(r) for r in rows]
        result["total_rows"] = total
        result["error"] = None  # Risolto!
        
        if verbose:
            print(f"   ✅ [{subtask['id']}] {subtask['aspect']}: Retry riuscito! {total} righe")
        
    except Exception as e:
        result["error"] = f"Retry SQL execution error: {e}"
        result["columns"] = []
        result["rows"] = []
        result["total_rows"] = 0
        
        if verbose:
            print(f"   ❌ [{subtask['id']}] {subtask['aspect']}: Retry fallito: {e}")
    
    return result


# ─────────────────────────────────────────────────────────────────────────────
# GRAPH NODES
# ─────────────────────────────────────────────────────────────────────────────
def node_orchestrate(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 1: L'orchestratore scompone la richiesta in sotto-query.
    """
    print("🎯 [Orchestrator] Analisi della richiesta e decomposizione...")
    
    try:
        client = OpenAI()
        
        user_prompt = f"""Richiesta dell'utente:
"{state['question']}"

Scomponi questa richiesta in sotto-query SQL indipendenti per analizzare i diversi aspetti.
Rispondi SOLO con il JSON array, senza altri commenti."""

        response = client.chat.completions.create(
            model=state["orchestrator_model"],
            messages=[
                {"role": "system", "content": ORCHESTRATOR_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=2000,
        )
        
        raw_output = response.choices[0].message.content or ""
        
        # Parse JSON (rimuovi eventuali markdown)
        json_str = re.sub(r"```(?:json)?\n?", "", raw_output, flags=re.IGNORECASE)
        json_str = re.sub(r"```", "", json_str).strip()
        
        subtasks_raw = json.loads(json_str)
        
        # Converti in SubTask
        subtasks: List[SubTask] = []
        for st in subtasks_raw:
            subtasks.append({
                "id": st.get("id", len(subtasks) + 1),
                "aspect": st.get("aspect", f"aspect_{len(subtasks) + 1}"),
                "question": st.get("question", ""),
                "sql": None,
                "columns": None,
                "rows": None,
                "total_rows": None,
                "error": None,
                "sql_usage": None,
                "is_followup": False,  # Query iniziale
            })
        
        usage = {
            "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
            "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            "model": state["orchestrator_model"],
        }
        cost = calculate_cost(usage)
        
        print(f"✅ [Orchestrator Phase 1] Generati {len(subtasks)} sotto-task:")
        for st in subtasks:
            print(f"   {st['id']}. [{st['aspect']}] {st['question']}")
        
        return {
            "subtasks": subtasks,
            "orchestrator_decompose_usage": usage,
            "orchestrator_decompose_cost": cost,
            "orchestrator_error": None,
        }
        
    except Exception as e:
        print(f"❌ [Orchestrator] Errore: {e}")
        return {
            "subtasks": [],
            "orchestrator_error": str(e),
        }


def node_execute_subtasks(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 2: Esegue tutte le sotto-query in parallelo.
    """
    if state.get("orchestrator_error") or not state.get("subtasks"):
        return {
            "successful_subtasks": 0,
            "failed_subtasks": 0,
            "total_sql_cost": 0.0,
        }
    
    subtasks = state["subtasks"]
    print(f"\n⚡ [Executor] Esecuzione parallela di {len(subtasks)} sotto-query...")
    
    # Esegui in parallelo con ThreadPoolExecutor
    results: List[SubTask] = []
    total_sql_cost = 0.0
    
    with ThreadPoolExecutor(max_workers=min(len(subtasks), 5)) as executor:
        futures = {
            executor.submit(
                execute_single_subtask,
                st,
                state["sql_model"],
                state.get("prompt_file"),
                state["max_rows_per_query"],
                state.get("verbose", False),
            ): st["id"]
            for st in subtasks
        }
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            # Accumula costi SQL
            if result.get("sql_usage"):
                cost = calculate_cost(result["sql_usage"])
                total_sql_cost += cost.get("total_cost", 0)
    
    # Ordina per ID
    results.sort(key=lambda x: x["id"])
    
    successful = sum(1 for r in results if r.get("error") is None and r.get("rows"))
    failed = sum(1 for r in results if r.get("error"))
    
    # Conta query con warning (non aggregate o troppe righe)
    with_warnings = [r for r in results if r.get("warnings")]
    
    print(f"✅ [Executor] Completato: {successful} successi, {failed} errori")
    
    if with_warnings:
        print(f"⚠️  [Executor] {len(with_warnings)} query con warning (output potenzialmente troppo grande):")
        for r in with_warnings:
            print(f"   [{r['id']}] {r['aspect']}: {r['total_rows']} righe")
    
    return {
        "subtasks": results,
        "successful_subtasks": successful,
        "failed_subtasks": failed,
        "total_sql_cost": total_sql_cost,
    }


def node_retry_failed(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 2.5: Mostra le query fallite e tenta di correggerle con guardrails.
    """
    subtasks = state.get("subtasks", [])
    max_retries = state.get("max_retries", 1)
    
    # Identifica i subtask falliti
    failed_subtasks = [st for st in subtasks if st.get("error")]
    
    if not failed_subtasks:
        return {
            "retried_subtasks": 0,
        }
    
    # Mostra riepilogo errori
    print(f"\n⚠️  [Guardrails] Rilevati {len(failed_subtasks)} errori:")
    print("-" * 60)
    for st in failed_subtasks:
        print(f"\n   ❌ [{st['id']}] {st['aspect']}")
        print(f"      Domanda: {st['question'][:60]}...")
        if st.get("sql"):
            print(f"      SQL: {st['sql'][:80]}...")
        print(f"      Errore: {st['error']}")
    print("-" * 60)
    
    if max_retries == 0:
        print("   ℹ️  Retry disabilitato (--max-retries 0)")
        return {"retried_subtasks": 0}
    
    print(f"\n🔄 [Guardrails] Tentativo di correzione automatica (max {max_retries} retry)...")
    
    # Tenta il retry per ogni subtask fallito
    updated_subtasks = list(subtasks)  # copia
    total_retried = 0
    additional_cost = 0.0
    
    for retry_round in range(max_retries):
        # Trova i subtask ancora falliti
        still_failed = [st for st in updated_subtasks if st.get("error")]
        
        if not still_failed:
            print(f"   ✅ Tutti gli errori risolti al round {retry_round + 1}!")
            break
        
        if retry_round > 0:
            print(f"\n   🔄 Round {retry_round + 1}: {len(still_failed)} query ancora fallite")
        
        # Retry in parallelo
        with ThreadPoolExecutor(max_workers=min(len(still_failed), 5)) as executor:
            futures = {
                executor.submit(
                    retry_single_subtask,
                    st,
                    state["sql_model"],
                    state.get("prompt_file"),
                    state["max_rows_per_query"],
                    state.get("verbose", False),
                ): st["id"]
                for st in still_failed
            }
            
            for future in as_completed(futures):
                result = future.result()
                total_retried += 1
                
                # Aggiorna il subtask nella lista
                for i, st in enumerate(updated_subtasks):
                    if st["id"] == result["id"]:
                        updated_subtasks[i] = result
                        break
                
                # Accumula costi
                if result.get("sql_usage"):
                    cost = calculate_cost(result["sql_usage"])
                    additional_cost += cost.get("total_cost", 0)
    
    # Ricalcola statistiche
    successful = sum(1 for r in updated_subtasks if r.get("error") is None and r.get("rows"))
    failed = sum(1 for r in updated_subtasks if r.get("error"))
    recovered = len(failed_subtasks) - failed
    
    print(f"\n✅ [Guardrails] Completato:")
    print(f"   - Query recuperate: {recovered}/{len(failed_subtasks)}")
    print(f"   - Query ancora fallite: {failed}")
    
    if failed > 0:
        print(f"\n   ⚠️  Errori non risolti:")
        for st in updated_subtasks:
            if st.get("error"):
                print(f"      [{st['id']}] {st['aspect']}: {st['error'][:60]}...")
    
    return {
        "subtasks": updated_subtasks,
        "successful_subtasks": successful,
        "failed_subtasks": failed,
        "retried_subtasks": total_retried,
        "total_sql_cost": state.get("total_sql_cost", 0) + additional_cost,
    }


def node_check_completeness(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 3: Verifica se l'analisi è completa o servono dati aggiuntivi.
    Se servono, genera nuove query di followup (max 3).
    """
    current_cycle = state.get("current_cycle", 0)
    max_followups = state.get("max_followups", 3)
    
    # Se abbiamo raggiunto il limite di cicli, procedi all'analisi
    if current_cycle >= max_followups:
        print(f"\n🔄 [Check] Raggiunto limite massimo di {max_followups} cicli di followup")
        return {
            "analysis_complete": True,
            "followup_reasoning": f"Limite di {max_followups} cicli raggiunto",
        }
    
    subtasks = state.get("subtasks", [])
    successful = [st for st in subtasks if st.get("error") is None and st.get("rows")]
    
    if not successful:
        return {
            "analysis_complete": True,
            "followup_reasoning": "Nessun dato disponibile per valutare completezza",
        }
    
    print(f"\n🔍 [Check Completeness] Ciclo {current_cycle + 1}/{max_followups} - Valutazione dati...")
    
    try:
        client = OpenAI()
        
        # Costruisci contesto con i risultati attuali
        results_text = []
        results_text.append(f"DOMANDA ORIGINALE: \"{state['question']}\"\n")
        results_text.append(f"SOTTO-ANALISI COMPLETATE ({len(successful)} query):\n")
        
        for st in successful:
            results_text.append(f"\n### {st['id']}. {st['aspect'].upper()}")
            results_text.append(f"Domanda: {st['question']}")
            if st.get("rows"):
                data_text = format_data_as_text(
                    st["columns"], st["rows"], st["total_rows"], max_display=15
                )
                results_text.append(f"Risultati ({st['total_rows']} righe):\n{data_text}")
            results_text.append("-" * 40)
        
        # Aggiungi info sulle query fallite
        failed = [st for st in subtasks if st.get("error")]
        if failed:
            results_text.append(f"\n⚠️ Query fallite: {len(failed)}")
            for st in failed:
                results_text.append(f"  - [{st['aspect']}]: {st['error'][:50]}...")
        
        full_context = "\n".join(results_text)
        
        # Calcola prossimo ID disponibile
        max_id = max((st["id"] for st in subtasks), default=0)
        
        user_prompt = f"""{full_context}

Ciclo attuale: {current_cycle + 1} di {max_followups}
Prossimo ID disponibile per nuove query: {max_id + 1}

Valuta se hai tutti i dati necessari per un'analisi completa.
Se servono approfondimenti, genera max 3 query di followup."""

        response = client.chat.completions.create(
            model=state["orchestrator_model"],
            messages=[
                {"role": "system", "content": ORCHESTRATOR_CHECK_COMPLETENESS_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=1500,
        )
        
        raw_output = response.choices[0].message.content or ""
        
        # Parse JSON
        json_str = re.sub(r"```(?:json)?\n?", "", raw_output, flags=re.IGNORECASE)
        json_str = re.sub(r"```", "", json_str).strip()
        
        result = json.loads(json_str)
        
        usage = {
            "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
            "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            "model": state["orchestrator_model"],
        }
        
        # Accumula costi check (gestisce None)
        prev_check_cost = (state.get("orchestrator_check_cost") or {}).get("total_cost", 0)
        new_cost = calculate_cost(usage)
        accumulated_cost = {
            "input_cost": new_cost["input_cost"],
            "output_cost": new_cost["output_cost"],
            "total_cost": prev_check_cost + new_cost["total_cost"],
        }
        
        is_complete = result.get("analysis_complete", True)
        reasoning = result.get("reasoning", "")
        followup_queries = result.get("followup_queries", [])
        
        if is_complete:
            print(f"✅ [Check] Analisi completa: {reasoning[:60]}...")
            return {
                "analysis_complete": True,
                "followup_reasoning": reasoning,
                "current_cycle": current_cycle,
                "orchestrator_check_usage": usage,
                "orchestrator_check_cost": accumulated_cost,
            }
        else:
            # Limita a max 3 query
            followup_queries = followup_queries[:3]
            
            print(f"🔄 [Check] Servono approfondimenti: {reasoning[:60]}...")
            print(f"   Nuove query da eseguire: {len(followup_queries)}")
            
            # Converti in SubTask e aggiungi ai subtasks esistenti
            new_subtasks: List[SubTask] = []
            for i, fq in enumerate(followup_queries):
                new_id = max_id + i + 1
                new_subtasks.append({
                    "id": new_id,
                    "aspect": fq.get("aspect", f"followup_{new_id}"),
                    "question": fq.get("question", ""),
                    "sql": None,
                    "columns": None,
                    "rows": None,
                    "total_rows": None,
                    "error": None,
                    "sql_usage": None,
                    "is_followup": True,  # Query di approfondimento
                })
                print(f"   {new_id}. [{new_subtasks[-1]['aspect']}] {new_subtasks[-1]['question']}")
            
            # Aggiungi le nuove query ai subtasks esistenti
            updated_subtasks = list(subtasks) + new_subtasks
            
            return {
                "analysis_complete": False,
                "followup_reasoning": reasoning,
                "subtasks": updated_subtasks,
                "current_cycle": current_cycle + 1,
                "total_followup_queries": state.get("total_followup_queries", 0) + len(new_subtasks),
                "orchestrator_check_usage": usage,
                "orchestrator_check_cost": accumulated_cost,
            }
        
    except Exception as e:
        print(f"❌ [Check] Errore: {e}")
        return {
            "analysis_complete": True,
            "followup_reasoning": f"Errore durante check: {e}",
        }


def node_execute_followup(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 3.5: Esegue SOLO le nuove query di followup (quelle senza risultati).
    """
    subtasks = state.get("subtasks", [])
    
    # Trova i subtask senza risultati (nuove query)
    pending = [st for st in subtasks if st.get("sql") is None and st.get("error") is None]
    
    if not pending:
        return {}
    
    print(f"\n⚡ [Executor] Esecuzione {len(pending)} query di followup...")
    
    results: List[SubTask] = []
    additional_cost = 0.0
    
    with ThreadPoolExecutor(max_workers=min(len(pending), 5)) as executor:
        futures = {
            executor.submit(
                execute_single_subtask,
                st,
                state["sql_model"],
                state.get("prompt_file"),
                state["max_rows_per_query"],
                state.get("verbose", False),
            ): st["id"]
            for st in pending
        }
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            if result.get("sql_usage"):
                cost = calculate_cost(result["sql_usage"])
                additional_cost += cost.get("total_cost", 0)
    
    # Aggiorna i subtask con i risultati
    updated_subtasks = list(subtasks)
    for result in results:
        for i, st in enumerate(updated_subtasks):
            if st["id"] == result["id"]:
                updated_subtasks[i] = result
                break
    
    successful = sum(1 for r in results if r.get("error") is None and r.get("rows"))
    failed = sum(1 for r in results if r.get("error"))
    
    print(f"✅ [Executor] Followup completato: {successful} successi, {failed} errori")
    
    return {
        "subtasks": updated_subtasks,
        "successful_subtasks": state.get("successful_subtasks", 0) + successful,
        "failed_subtasks": state.get("failed_subtasks", 0) + failed,
        "total_sql_cost": state.get("total_sql_cost", 0) + additional_cost,
    }


def node_analyze_all(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo 4: L'orchestratore (Phase 2) sintetizza tutti i risultati.
    """
    print("\n📊 [Orchestrator Phase 2] Sintesi dei risultati...")
    
    subtasks = state.get("subtasks", [])
    successful = [st for st in subtasks if st.get("error") is None and st.get("rows")]
    
    if not successful:
        error_msg = "Nessuna sotto-query ha prodotto risultati."
        if state.get("orchestrator_error"):
            error_msg = f"Errore orchestratore: {state['orchestrator_error']}"
        elif all(st.get("error") for st in subtasks):
            errors = [f"[{st['aspect']}]: {st['error']}" for st in subtasks if st.get("error")]
            error_msg = "Tutte le query sono fallite:\n" + "\n".join(errors)
        
        return {
            "analysis": f"❌ {error_msg}",
            "orchestrator_analyze_usage": {},
            "orchestrator_analyze_cost": {"input_cost": 0, "output_cost": 0, "total_cost": 0},
        }
    
    try:
        client = OpenAI()
        
        # Separa query iniziali da followup
        initial_tasks = [st for st in subtasks if not st.get("is_followup")]
        followup_tasks = [st for st in subtasks if st.get("is_followup")]
        
        # Costruisci il prompt con tutti i risultati
        results_text = []
        results_text.append(f"DOMANDA ORIGINALE DELL'UTENTE:\n\"{state['question']}\"\n")
        results_text.append("=" * 60)
        
        # Sezione query iniziali
        initial_successful = [st for st in initial_tasks if st.get("error") is None and st.get("rows")]
        results_text.append(f"\n📋 QUERY INIZIALI (decomposizione) - {len(initial_successful)} di {len(initial_tasks)} riuscite:\n")
        
        for st in initial_tasks:
            results_text.append(f"\n### {st['id']}. {st['aspect'].upper()}")
            results_text.append(f"Domanda: {st['question']}")
            
            if st.get("error"):
                results_text.append(f"❌ Errore: {st['error']}")
            elif st.get("sql"):
                results_text.append(f"\nQuery SQL:\n```sql\n{st['sql']}\n```")
                
                if st.get("rows"):
                    data_text = format_data_as_text(
                        st["columns"], st["rows"], st["total_rows"], max_display=30
                    )
                    results_text.append(f"\nRisultati ({st['total_rows']} righe):\n{data_text}")
                else:
                    results_text.append("\n(Nessun dato restituito)")
            
            results_text.append("-" * 40)
        
        # Sezione query di followup (se presenti)
        if followup_tasks:
            followup_successful = [st for st in followup_tasks if st.get("error") is None and st.get("rows")]
            results_text.append("\n" + "=" * 60)
            results_text.append(f"\n🔍 QUERY DI FOLLOWUP (approfondimenti) - {len(followup_successful)} di {len(followup_tasks)} riuscite:")
            results_text.append(f"\n💭 Motivazione followup: {state.get('followup_reasoning', 'N/A')}\n")
            
            for st in followup_tasks:
                results_text.append(f"\n### {st['id']}. {st['aspect'].upper()} [FOLLOWUP]")
                results_text.append(f"Domanda di approfondimento: {st['question']}")
                
                if st.get("error"):
                    results_text.append(f"❌ Errore: {st['error']}")
                elif st.get("sql"):
                    results_text.append(f"\nQuery SQL:\n```sql\n{st['sql']}\n```")
                    
                    if st.get("rows"):
                        data_text = format_data_as_text(
                            st["columns"], st["rows"], st["total_rows"], max_display=30
                        )
                        results_text.append(f"\nRisultati ({st['total_rows']} righe):\n{data_text}")
                    else:
                        results_text.append("\n(Nessun dato restituito)")
                
                results_text.append("-" * 40)
        
        full_context = "\n".join(results_text)
        
        followup_instruction = ""
        if followup_tasks:
            # Recupera la motivazione del check completeness per dare contesto
            followup_reasoning = state.get('followup_reasoning', 'Non specificato')
            
            followup_instruction = f"""

⚠️ IMPORTANTE - SEZIONE VALUTAZIONE FOLLOWUP (struttura obbligatoria):

MOTIVAZIONE ORIGINALE per i followup: "{followup_reasoning}"

Sono state eseguite {len(followup_tasks)} query di followup. La sezione DEVE seguire questa struttura:

1. PROBLEMA RILEVATO: Qual era l'anomalia/lacuna/aspetto critico emerso dall'analisi iniziale?
   (Usa la motivazione sopra come riferimento)

2. STRATEGIA: Come l'orchestratore ha deciso di investigare? Quali dimensioni ha esplorato?

3. RISOLUZIONE: I followup hanno risolto/chiarito il problema? Quali risposte concrete sono emerse?

4. LIMITI: Ci sono stati followup senza dati o che non hanno risposto al problema?

NON elencare ogni query. Scrivi un racconto logico: problema → indagine → conclusione."""
        
        user_prompt = f"""{full_context}
{followup_instruction}

Basandoti su tutti i risultati sopra, fornisci un'analisi completa e integrata.
Identifica pattern, anomalie, correlazioni tra i diversi aspetti, e fornisci raccomandazioni."""

        response = client.chat.completions.create(
            model=state["orchestrator_model"],  # Stesso modello dell'orchestratore
            messages=[
                {"role": "system", "content": ORCHESTRATOR_ANALYZE_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=3000,
        )
        
        analysis = response.choices[0].message.content or ""
        
        usage = {
            "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
            "completion_tokens": response.usage.completion_tokens if response.usage else 0,
            "model": state["orchestrator_model"],
        }
        cost = calculate_cost(usage)
        
        print(f"✅ [Orchestrator Phase 2] Analisi completata ({usage['completion_tokens']} tokens)")
        
        return {
            "analysis": analysis,
            "orchestrator_analyze_usage": usage,
            "orchestrator_analyze_cost": cost,
        }
        
    except Exception as e:
        print(f"❌ [Orchestrator Phase 2] Errore: {e}")
        return {
            "analysis": f"❌ Errore durante l'analisi: {e}",
            "orchestrator_analyze_usage": {},
            "orchestrator_analyze_cost": {"input_cost": 0, "output_cost": 0, "total_cost": 0},
        }


def node_finalize(state: PipelineState) -> Dict[str, Any]:
    """
    Nodo finale: Calcola costi totali.
    """
    decompose_cost = (state.get("orchestrator_decompose_cost") or {}).get("total_cost", 0)
    sql_cost = state.get("total_sql_cost", 0) or 0
    check_cost = (state.get("orchestrator_check_cost") or {}).get("total_cost", 0)
    analyze_cost = (state.get("orchestrator_analyze_cost") or {}).get("total_cost", 0)
    
    total = decompose_cost + sql_cost + check_cost + analyze_cost
    
    return {
        "total_cost": total,
    }


# ─────────────────────────────────────────────────────────────────────────────
# BUILD GRAPH
# ─────────────────────────────────────────────────────────────────────────────
def should_continue_loop(state: PipelineState) -> str:
    """
    Determina se continuare il loop di followup o procedere all'analisi.
    """
    if state.get("analysis_complete", False):
        return "analyze"
    else:
        return "execute_followup"


def build_pipeline_graph() -> StateGraph:
    """
    Costruisce il grafo LangGraph con ciclo di followup:
    
    START → orchestrate → execute_subtasks → retry_failed → check_completeness
                                                                    ↓
                                              ┌─────────[need_more]─┴─[complete]─────┐
                                              ↓                                       ↓
                                      execute_followup                          analyze_all
                                              ↓                                       ↓
                                      check_completeness                          finalize
                                                                                      ↓
                                                                                     END
    """
    graph = StateGraph(PipelineState)
    
    # Aggiungi i nodi
    graph.add_node("orchestrate", node_orchestrate)
    graph.add_node("execute_subtasks", node_execute_subtasks)
    graph.add_node("retry_failed", node_retry_failed)
    graph.add_node("check_completeness", node_check_completeness)
    graph.add_node("execute_followup", node_execute_followup)
    graph.add_node("analyze_all", node_analyze_all)
    graph.add_node("finalize", node_finalize)
    
    # Flusso principale
    graph.add_edge(START, "orchestrate")
    graph.add_edge("orchestrate", "execute_subtasks")
    graph.add_edge("execute_subtasks", "retry_failed")
    graph.add_edge("retry_failed", "check_completeness")
    
    # Branch condizionale dopo check_completeness
    graph.add_conditional_edges(
        "check_completeness",
        should_continue_loop,
        {
            "execute_followup": "execute_followup",
            "analyze": "analyze_all",
        }
    )
    
    # Loop: execute_followup → retry_failed → check_completeness
    # (retry anche per le query di followup!)
    graph.add_edge("execute_followup", "retry_failed")
    graph.add_edge("retry_failed", "check_completeness")
    
    # Flusso finale
    graph.add_edge("analyze_all", "finalize")
    graph.add_edge("finalize", END)
    
    return graph


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(
    question: str,
    prompt_file: Optional[Path] = None,
    sql_model: Optional[str] = None,
    orchestrator_model: str = "gpt-4.1-mini",
    max_rows_per_query: int = 100,
    max_retries: int = 1,
    max_followups: int = 3,
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Esegue la pipeline Orchestrator-Worker-Orchestrator con Guardrails e Followup ciclico.
    
    L'orchestratore viene usato in tre fasi:
      Phase 1: Decompone la richiesta in sotto-query
      Phase 1.5: Verifica completezza e genera followup (max 3 cicli)
      Phase 2: Sintetizza i risultati in un'analisi integrata
    
    Guardrails: Le query fallite vengono mostrate e ritentate automaticamente
    passando l'errore come contesto per guidare la correzione.
    
    Followup: L'orchestratore può richiedere dati aggiuntivi per approfondire
    anomalie o coprire aspetti mancanti (max 3 query per ciclo, max 3 cicli).
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
        "orchestrator_model": orchestrator_model,
        "max_rows_per_query": max_rows_per_query,
        "max_retries": max_retries,
        "max_followups": max_followups,
        "verbose": verbose,
        "timestamp": datetime.now().isoformat(),
        # Phase 1: Decomposition
        "subtasks": [],
        "orchestrator_decompose_usage": None,
        "orchestrator_decompose_cost": None,
        "orchestrator_error": None,
        # Execution
        "successful_subtasks": 0,
        "failed_subtasks": 0,
        "retried_subtasks": 0,
        "total_sql_cost": 0.0,
        # Followup cycle
        "current_cycle": 0,
        "analysis_complete": False,
        "followup_reasoning": None,
        "total_followup_queries": 0,
        "orchestrator_check_usage": None,
        "orchestrator_check_cost": None,
        # Phase 2: Analysis
        "analysis": None,
        "orchestrator_analyze_usage": None,
        "orchestrator_analyze_cost": None,
        "total_cost": 0.0,
    }
    
    # Costruisci e compila il grafo
    graph = build_pipeline_graph()
    app = graph.compile()
    
    # Esegui il grafo
    print("\n" + "=" * 70)
    print("🚀 ORCHESTRATOR-WORKER-ORCHESTRATOR PIPELINE (CYCLIC) - START")
    print(f"   Orchestrator Model: {orchestrator_model}")
    print(f"   SQL Model: {sql_model}")
    print(f"   Guardrails: max {max_retries} retry per errori")
    print(f"   Followup: max {max_followups} cicli, max 3 query/ciclo")
    print("=" * 70)
    
    final_state = app.invoke(initial_state)
    
    print("=" * 70)
    print("🏁 ORCHESTRATOR-WORKER-ORCHESTRATOR PIPELINE - END")
    print("=" * 70 + "\n")
    
    return final_state


def print_result(result: Dict[str, Any], show_subtasks: bool = True, show_data: bool = False):
    """Stampa i risultati della pipeline in modo formattato."""
    print("\n" + "=" * 80)
    print("📋 RISULTATI PIPELINE ORCHESTRATOR-WORKER-ORCHESTRATOR")
    print("=" * 80)
    
    print(f"\n📝 Domanda originale: {result.get('question', 'N/A')}")
    print(f"🕐 Timestamp: {result.get('timestamp', 'N/A')}")
    
    # Sotto-task
    subtasks = result.get("subtasks", [])
    successful = result.get("successful_subtasks", 0)
    failed = result.get("failed_subtasks", 0)
    
    print(f"\n📊 Sotto-analisi: {len(subtasks)} totali ({successful} ✅, {failed} ❌)")
    
    # Info retry e followup
    retried = result.get("retried_subtasks", 0)
    if retried > 0:
        print(f"   🔄 Retry effettuati: {retried}")
    
    followup_queries = result.get("total_followup_queries", 0)
    cycles = result.get("current_cycle", 0)
    if followup_queries > 0:
        print(f"   🔍 Followup: {followup_queries} query aggiuntive in {cycles} cicli")
        if result.get("followup_reasoning"):
            print(f"   📝 Motivo: {result['followup_reasoning']}")
    
    if show_subtasks:
        # Separa query iniziali da followup
        initial_tasks = [st for st in subtasks if not st.get("is_followup")]
        followup_tasks = [st for st in subtasks if st.get("is_followup")]
        
        print("-" * 40)
        print("📋 QUERY INIZIALI (decomposizione):")
        for st in initial_tasks:
            status = "✅" if st.get("error") is None and st.get("rows") else "❌"
            error_msg = st.get("error") or "nessun dato"
            rows_info = f"{st.get('total_rows', 0)} righe" if st.get("rows") else error_msg[:50]
            print(f"  {st['id']}. [{st['aspect']}] {status} {rows_info}")
            print(f"      ❓ Domanda: {st['question']}")
            if show_data and st.get("rows"):
                print(f"      SQL: {st['sql'][:80]}...")
        
        if followup_tasks:
            print()
            print("🔍 QUERY DI FOLLOWUP (approfondimenti orchestratore):")
            if result.get("followup_reasoning"):
                print(f"   💭 Motivazione: {result['followup_reasoning']}")
            print()
            for st in followup_tasks:
                status = "✅" if st.get("error") is None and st.get("rows") else "❌"
                error_msg = st.get("error") or "nessun dato"
                rows_info = f"{st.get('total_rows', 0)} righe" if st.get("rows") else error_msg[:50]
                print(f"  {st['id']}. [{st['aspect']}] {status} {rows_info}")
                print(f"      ❓ Domanda: {st['question']}")
                if show_data and st.get("rows"):
                    print(f"      SQL: {st['sql'][:80]}...")
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SEZIONE ERRORI - Query fallite per aggiungere guardrails al prompt
    # ═══════════════════════════════════════════════════════════════════════════
    failed_tasks = [st for st in subtasks if st.get("error")]
    if failed_tasks:
        print("\n" + "=" * 80)
        print("🚨 QUERY FALLITE - Aggiungi guardrails al prompt file per correggere")
        print("=" * 80)
        
        for st in failed_tasks:
            print(f"\n┌{'─' * 78}┐")
            print(f"│ [{st['id']}] {st['aspect']:<70} │")
            print(f"├{'─' * 78}┤")
            print(f"│ DOMANDA:                                                                     │")
            # Wrap question
            question = st.get('question', '')
            for i in range(0, len(question), 74):
                line = question[i:i+74]
                print(f"│   {line:<74} │")
            
            print(f"├{'─' * 78}┤")
            print(f"│ SQL GENERATO:                                                                │")
            sql = st.get('sql', 'N/A') or 'N/A'
            # Mostra SQL su più righe
            sql_lines = sql.split('\n')
            for line in sql_lines[:10]:  # Max 10 righe
                line = line[:74]
                print(f"│   {line:<74} │")
            if len(sql_lines) > 10:
                print(f"│   ... (troncato)                                                            │")
            
            print(f"├{'─' * 78}┤")
            print(f"│ ❌ ERRORE:                                                                   │")
            error = st.get('error', 'N/A')
            # Wrap error message
            for i in range(0, len(error), 74):
                line = error[i:i+74]
                print(f"│   {line:<74} │")
            
            print(f"└{'─' * 78}┘")
        
        print("\n" + "-" * 80)
        print("💡 SUGGERIMENTO: Aggiungi al tuo prompt file (--prompt-file) istruzioni come:")
        print("-" * 80)
        print("""
   Esempio di guardrails da aggiungere:
   
   - NON usare la tabella 'Orders', usa 'SalesOrder'
   - Il campo data si chiama 'OrderDate', non 'Date'
   - Per il ricavo usa: SUM(od.UnitPrice * od.Quantity * (1 - od.Discount))
   - Le colonne di SalesOrder sono: Id, OrderDate, CustomerId, ...
   - Usa sempre alias per le tabelle (es. SalesOrder so, OrderDetail od)
""")
        print("=" * 80)
    
    # Analisi
    print(f"\n🔍 Analisi finale (Orchestrator Phase 2):")
    print("-" * 40)
    print(result.get("analysis", "N/A"))
    
    # Costi
    print("\n" + "-" * 40)
    print("💰 COSTI (Orchestrator-Worker-Orchestrator):")
    decompose_cost = (result.get("orchestrator_decompose_cost") or {}).get("total_cost", 0)
    sql_cost = result.get("total_sql_cost", 0) or 0
    check_cost = (result.get("orchestrator_check_cost") or {}).get("total_cost", 0)
    analyze_cost = (result.get("orchestrator_analyze_cost") or {}).get("total_cost", 0)
    orchestrator_total = decompose_cost + check_cost + analyze_cost
    
    print(f"   🎯 Orchestrator Phase 1 (Decompose):   ${decompose_cost:.6f}")
    print(f"   ⚡ SQL Generator (Workers):            ${sql_cost:.6f} ({len(subtasks)} query)")
    if check_cost > 0:
        print(f"   🔍 Orchestrator Check (Followup):      ${check_cost:.6f}")
    print(f"   📊 Orchestrator Phase 2 (Analyze):     ${analyze_cost:.6f}")
    print(f"   ─────────────────────────────────────")
    print(f"   Orchestrator Totale:                   ${orchestrator_total:.6f}")
    print(f"   TOTALE PIPELINE:                       ${result.get('total_cost', 0):.6f}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Orchestrator-Worker-Orchestrator: L'orchestratore decompone e analizza",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Architettura:
  Orchestrator (Phase 1) → Workers (SQL) → Guardrails (Retry) → Orchestrator (Phase 2)
  
Esempi:
  python scripts/analyst_pipeline_orchestrated.py "Analizza le performance di vendita del 2006"
  python scripts/analyst_pipeline_orchestrated.py "Quali sono i trend nelle vendite?" --verbose
  python scripts/analyst_pipeline_orchestrated.py "Identifica anomalie e pattern" --max-retries 2
        """,
    )
    parser.add_argument("question", type=str, help="Richiesta di analisi in linguaggio naturale")
    parser.add_argument(
        "--prompt-file", type=str, default=None,
        help="File con prompt per SQL generator (es. dataset/new_prompt.txt)"
    )
    parser.add_argument(
        "--sql-model", type=str, default=None,
        help="Modello per SQL (default: fine-tuned da sft_state.json)"
    )
    parser.add_argument(
        "--orchestrator-model", type=str, default="gpt-4.1-mini",
        help="Modello per l'orchestratore - usato sia per decomposizione che analisi (default: gpt-4.1-mini)"
    )
    parser.add_argument(
        "--max-rows", type=int, default=100,
        help="Max righe per query (default: 100)"
    )
    parser.add_argument(
        "--max-retries", type=int, default=1,
        help="Max tentativi di correzione per query fallite (default: 1, 0=disabilita)"
    )
    parser.add_argument(
        "--max-followups", type=int, default=3,
        help="Max cicli di followup per approfondimenti (default: 3, 0=disabilita)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Output dettagliato durante l'esecuzione"
    )
    parser.add_argument(
        "--show-data", action="store_true",
        help="Mostra le query SQL nell'output"
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
        orchestrator_model=args.orchestrator_model,
        max_rows_per_query=args.max_rows,
        max_retries=args.max_retries,
        max_followups=args.max_followups,
        verbose=args.verbose,
    )
    
    if args.json:
        output = dict(result)
        # Tronca rows per output leggibile
        for st in output.get("subtasks", []):
            if st.get("rows") and len(st["rows"]) > 10:
                st["rows"] = st["rows"][:10]
                st["rows_truncated"] = True
        print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, show_subtasks=True, show_data=args.show_data)


if __name__ == "__main__":
    main()
