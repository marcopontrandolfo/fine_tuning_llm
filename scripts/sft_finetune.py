"""
Start a Supervised Fine-Tuning (SFT) job on OpenAI for the Northwind SQL assistant.

Input: a chat-format JSONL (one object per line) with messages: [system, user, assistant]
Default training file: dataset/sft_northwind_50.jsonl

This script will:
- upload the JSONL as a file for fine-tuning
- create a fine-tuning job on the chosen base model (default: gpt-4.1-mini)
- poll until completion and write a state file under results/sft_state.json

Requirements:
- OPENAI_API_KEY in environment or .env

Usage (PowerShell):
    python .\scripts\sft_finetune.py --train .\dataset\sft_northwind_50.jsonl --base-model gpt-4.1-mini --suffix northwind-sql

Notes:
- Fine-tuning availability and pricing depends on your account and the chosen base model.
- Ensure your dataset complies with chat FT format.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from openai import OpenAI


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TRAIN = ROOT / "dataset" / "sft_northwind_50.jsonl"
STATE_FILE = ROOT / "results" / "sft_state.json"


def save_state(data: Dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Start an OpenAI SFT job for Northwind SQL")
    parser.add_argument("--train", type=str, default=str(DEFAULT_TRAIN), help="Path to training JSONL (chat format)")
    parser.add_argument("--base-model", type=str, default="gpt-4.1-mini", help="Base model to fine-tune (e.g., gpt-4.1-mini)")
    parser.add_argument("--suffix", type=str, default="northwind-sql", help="Suffix for the fine-tuned model name")
    parser.add_argument("--poll-seconds", type=int, default=15, help="Polling interval in seconds")
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY mancante. Definirlo in .env o nell'ambiente.")

    train_path = Path(args.train)
    if not train_path.exists():
        raise FileNotFoundError(f"Training file non trovato: {train_path}")

    client = OpenAI()
    print(f"Carico file di training: {train_path}")
    file_obj = client.files.create(file=open(train_path, "rb"), purpose="fine-tune")
    print(f"File caricato: id={file_obj.id}")

    print(f"Creo job di fine-tuning su base '{args.base_model}'...")
    try:
        job = client.fine_tuning.jobs.create(
            training_file=file_obj.id,
            model=args.base_model,
            suffix=args.suffix,
        )
    except Exception as e:
        print("Errore nella creazione del job di SFT.")
        print(str(e))
        print("\nSuggerimenti:")
        print("- Verifica che il modello sia abilitato al fine-tuning per la tua organizzazione e regione.")
        print("- Prova con '--base-model gpt-4.1-mini' o un altro modello esplicitamente supportato per SFT.")
        print("- In alcuni casi è richiesto uno slug/versione specifica del modello.")
        return
    print(f"Job creato: id={job.id}, status={job.status}")

    state = {
        "training_file": str(train_path),
        "file_id": file_obj.id,
        "job_id": job.id,
        "base_model": args.base_model,
        "suffix": args.suffix,
        "status": job.status,
    }
    save_state(state)

    # Poll for completion
    while job.status in ("validating_files", "queued", "running"):
        time.sleep(args.poll_seconds)
        job = client.fine_tuning.jobs.retrieve(job.id)
        print(f"Stato: {job.status}")
        state["status"] = job.status
        save_state(state)

    if job.status == "succeeded":
        # The API typically returns the fine-tuned model name/id on success
        ft_model = getattr(job, "fine_tuned_model", None) or getattr(job, "result_model", None)
        print(f"Completato. Modello FT: {ft_model}")
        state["fine_tuned_model"] = ft_model
        save_state(state)
    else:
        print(f"Job terminato con stato: {job.status}")
        # Optionally print error if available
        error = getattr(job, "error", None)
        if error:
            print(f"Errore: {error}")
        save_state(state)


if __name__ == "__main__":
    main()
