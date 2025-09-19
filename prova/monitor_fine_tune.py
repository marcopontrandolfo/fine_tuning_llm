from openai import OpenAI
import os
import time

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)

def monitor_fine_tune_job(job_id):
    """Monitora lo stato di un job di fine-tuning"""
    try:
        job = client.fine_tuning.jobs.retrieve(job_id)
        
        print(f"Job ID: {job.id}")
        print(f"Status: {job.status}")
        print(f"Modello base: {job.model}")
        print(f"Creato il: {job.created_at}")
        
        if hasattr(job, 'fine_tuned_model') and job.fine_tuned_model:
            print(f"Modello fine-tuned: {job.fine_tuned_model}")
        
        if hasattr(job, 'trained_tokens') and job.trained_tokens:
            print(f"Token addestrati: {job.trained_tokens}")
            
        if hasattr(job, 'error') and job.error:
            print(f"Errore: {job.error}")
            
        return job.status
        
    except Exception as e:
        print(f"Errore nel recupero del job: {e}")
        return None

def list_all_fine_tune_jobs():
    """Lista tutti i job di fine-tuning"""
    try:
        jobs = client.fine_tuning.jobs.list(limit=10)
        
        print("Job di fine-tuning recenti:")
        print("-" * 50)
        
        for job in jobs.data:
            print(f"ID: {job.id}")
            print(f"Status: {job.status}")
            print(f"Modello: {job.model}")
            if hasattr(job, 'fine_tuned_model') and job.fine_tuned_model:
                print(f"Modello fine-tuned: {job.fine_tuned_model}")
            print("-" * 30)
            
    except Exception as e:
        print(f"Errore nel recupero dei job: {e}")

if __name__ == "__main__":
    # Leggi l'ID del job dal file se esiste
    if os.path.exists("fine_tune_job_id.txt"):
        with open("fine_tune_job_id.txt", "r") as f:
            job_id = f.read().strip()
        
        print(f"Monitoraggio del job: {job_id}")
        status = monitor_fine_tune_job(job_id)
        
        print(f"\nStatus attuale: {status}")
        
        if status in ["running", "validating_files"]:
            print("Il job è ancora in corso. Puoi ri-eseguire questo script per controllare lo stato.")
        elif status == "succeeded":
            print("Il fine-tuning è completato con successo!")
        elif status == "failed":
            print("Il fine-tuning è fallito. Controlla i dettagli sopra.")
    else:
        print("Nessun job ID trovato. Esegui prima il script principale per creare un job.")
    
    print("\n" + "="*50)
    print("TUTTI I JOB DI FINE-TUNING:")
    list_all_fine_tune_jobs()
