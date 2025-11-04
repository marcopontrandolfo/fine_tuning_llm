# Fine-tuning GPT-4.1 mini per SQL su Northwind

Questo progetto supporta due modalità:
- Prompting ("context FT"): semplice prompting con regole + schema nel system prompt e (opzionale) K-shot.
 Fine-tuning: addestra un modello derivato da `gpt-4.1-mini` sui tuoi esempi.
  - `fine_tune.py`: carica il dataset e avvia il job di fine-tuning su `gpt-4.1-mini`
  - `prompt_infer.py`: inference solo prompting (system+schema+K-shot) su `gpt-4.1-mini`
- `dataset/`
  - `northwind_incontext_examples.jsonl`: esempi utente → SQL (uno per riga)
 crea un job di fine-tuning su `gpt-4.1-mini`
If `.ft_state.json` non è presente o non contiene `fine_tuned_model`, lo script usa il modello base `gpt-4.1-mini`.
- `scripts/`
  - `prepare_ft_dataset.py`: genera `dataset/northwind_ft_train.jsonl` nel formato richiesto dal fine-tuning chat
  - `fine_tune.py`: carica il dataset e avvia il job di fine-tuning su `gpt-4.1-mini`
  - `inference.py`: interroga il modello fine-tunato (o il base) e stampa la SQL
  - `extract_schema_from_sql.py`: estrae tabelle/colonne/foreign keys da `dataset/northwind.sql` e genera `dataset/northwind_schema_canonical.json`
  - `prompt_utils.py`: funzioni condivise per costruire i messaggi e normalizzare SQL
  - `prompt_infer.py`: inference solo prompting (system+schema+K-shot) su `gpt-4.1-mini`
  - `evaluate_prompting.py`: valuta la qualità del prompting sugli esempi (accuracy exact-match)
- `requirements.txt`: dipendenze Python

## Requisiti
- Python 3.9+
- Chiave OpenAI valida (`OPENAI_API_KEY`)

Crea un file `.env` nella root con:
```
OPENAI_API_KEY=sk-...
```

Installa le dipendenze:
```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
```

## 1) Modalità Prompting (consigliata per partire)

Estrai lo schema (opzionale ma consigliato):
```powershell
python .\scripts\extract_schema_from_sql.py
```

Esegui inferenza con K-shot opzionale:
```powershell
python .\scripts\prompt_infer.py "Elenca i 10 clienti con più ordini" --k 3
```

Valuta sugli esempi (senza allenare):
```powershell
python .\scripts\evaluate_prompting.py --k 3 --limit 10
```
Usa `--dry-run` per vedere la finestra di contesto senza chiamare l'API.

## 2) Prepara il dataset di fine-tuning
Opzionale ma consigliato: genera lo schema canonico dal dump SQL (se hai aggiunto `dataset/northwind.sql`):
```powershell
python .\scripts\extract_schema_from_sql.py
```

Poi genera il file `dataset/northwind_ft_train.jsonl` (formato chat FT) a partire dagli esempi esistenti; se presente, incorpora lo schema canonico:
```powershell
python .\scripts\prepare_ft_dataset.py
```
Output atteso: un messaggio con il numero di esempi convertiti.

## 3) Avvia il fine-tuning su gpt-4.1-mini
```powershell
python .\scripts\fine_tune.py
```
Lo script:
- carica il file di training su OpenAI
- crea un job di fine-tuning su `gpt-4.1-mini`
- fa polling finché termina
- salva il model id risultante in `.ft_state.json`

Nota: il fine-tuning può richiedere tempo e crediti. Verifica i limiti e i costi del tuo account.

## 4) Esegui inferenza con il modello FT
Dopo il completamento, puoi porre domande in italiano e ottenere la SQL:
```powershell
python .\scripts\inference.py "Elenca i 10 clienti con più ordini"
```
Se `.ft_state.json` non è presente o non contiene `fine_tuned_model`, lo script usa il modello base `gpt-4.1-mini`.

## Dati e qualità
- Assicurati che `northwind_incontext_examples.jsonl` contenga molti esempi vari e corretti. Più copertura → migliore qualità.
- Il file `northwind_system_prompt.txt` deve imporre le regole chiave (solo SELECT, nessun testo extra, ecc.).
- `northwind_schema.json` è opzionale, ma aiuta il modello a rispettare tabelle e colonne disponibili.

## Troubleshooting
- Errore API Key: verifica `.env` e la variabile `OPENAI_API_KEY`.
- Formato dataset: ogni riga del file FT deve contenere `{ "messages": [ ... ] }`.
- Modello non trovato: assicurati che il job FT sia terminato con stato `succeeded` e che `.ft_state.json` contenga `fine_tuned_model`.

## Licenza
Uso interno/accademico. Verifica le policy OpenAI e i termini d’uso del dataset Northwind.
