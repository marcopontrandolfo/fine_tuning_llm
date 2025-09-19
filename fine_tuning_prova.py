from openai import OpenAI
import os
import json

# SICUREZZA: Leggi la API key da variabile d'ambiente o file separato
# Non mettere mai la API key direttamente nel codice!
api_key = os.getenv('OPENAI_API_KEY')
if not api_key:
    # Se non c'è la variabile d'ambiente, prova a leggere da file
    try:
        with open('.env', 'r') as f:
            for line in f:
                if line.startswith('OPENAI_API_KEY='):
                    api_key = line.split('=', 1)[1].strip()
                    break
    except FileNotFoundError:
        print("ERRORE: API key non trovata!")
        print("Opzioni:")
        print("1. Crea un file .env con: OPENAI_API_KEY=tua_chiave")
        print("2. Imposta variabile d'ambiente: set OPENAI_API_KEY=tua_chiave")
        exit(1)

client = OpenAI(api_key=api_key)

print("=== TEST API CHIAMATA STANDARD ===")
print("Testando una semplice chiamata API...")

# Test di una chiamata API standard
try:
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": "Ciao, dimmi qualcosa sull'intelligenza artificiale in modo breve"}
        ],
        max_tokens=150,
        temperature=0.7
    )
    
    print("✓ Chiamata API riuscita!")
    print(f"Risposta: {response.choices[0].message.content}")
    print(f"Token utilizzati: {response.usage.total_tokens}")
    
except Exception as e:
    print(f"Errore nella chiamata API: {e}")
    print("Verifica i tuoi crediti e la chiave API.")

print("\n" + "="*60)
print("=== SEZIONE FINE-TUNING (COMMENTATA) ===")
print("Il codice seguente è commentato per preservarlo per uso futuro")

# SEZIONE FINE-TUNING - COMMENTATA PER USO FUTURO
"""
# Usa il file corretto
file_path = "dataset_fixed.jsonl"

# Verifica che il file esista
if not os.path.exists(file_path):
    print(f"Errore: Il file {file_path} non esiste.")
    exit(1)

# Valida il formato JSONL
print("Validazione del formato JSONL...")
try:
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    for i, line in enumerate(lines):
        line = line.strip()
        if line:  # Ignora le righe vuote
            try:
                data = json.loads(line)
                if "messages" not in data:
                    print(f"Errore alla riga {i+1}: manca il campo 'messages'")
                    exit(1)
                
                # Verifica che i messaggi abbiano i campi necessari
                for msg in data["messages"]:
                    if "role" not in msg or "content" not in msg:
                        print(f"Errore alla riga {i+1}: messaggio malformato")
                        exit(1)
                        
            except json.JSONDecodeError as e:
                print(f"Errore JSON alla riga {i+1}: {e}")
                exit(1)
    
    print(f"File validato con successo! Trovate {len([l for l in lines if l.strip()])} conversazioni.")
    
except Exception as e:
    print(f"Errore durante la validazione: {e}")
    exit(1)

try:
    # Carica il file per il fine-tuning
    with open(file_path, "rb") as file:
        file_response = client.files.create(
            file=file,
            purpose="fine-tune"
        )
    
    print("File caricato con successo!")
    print(f"File ID: {file_response.id}")
    print(f"Nome file: {file_response.filename}")
    print(f"Dimensione: {file_response.bytes} bytes")
    
    # Crea il job di fine-tuning
    print("\nCreazione del job di fine-tuning...")
    fine_tune_job = client.fine_tuning.jobs.create(
        training_file=file_response.id,
        model="gpt-3.5-turbo",  # Modello base per il fine-tuning
        hyperparameters={
            "n_epochs": 3  # Numero di epoche di training
        }
    )
    
    print("Job di fine-tuning creato con successo!")
    print(f"Job ID: {fine_tune_job.id}")
    print(f"Modello base: {fine_tune_job.model}")
    print(f"Status: {fine_tune_job.status}")
    print(f"Training file: {fine_tune_job.training_file}")
    
    # Salva l'ID del job per monitoraggio futuro
    with open("fine_tune_job_id.txt", "w") as f:
        f.write(fine_tune_job.id)
    print(f"\nID del job salvato in 'fine_tune_job_id.txt' per monitoraggio futuro.")
    
except Exception as e:
    print(f"Errore durante il caricamento del file o creazione del job: {e}")
    print("Assicurati che il file sia in formato JSONL valido per il fine-tuning.")
"""

print("\nPer abilitare il fine-tuning:")
print("1. Assicurati di avere crediti sufficienti nel tuo account OpenAI")
print("2. Rimuovi le triple virgolette per decommentare il codice")
print("3. Esegui nuovamente lo script")