"""
Cancel a running or pending OpenAI fine-tuning job.

Usage (PowerShell):
  # Read job_id from results/sft_state.json
  python .\scripts\sft_cancel.py

  # Or pass a specific job id
  python .\scripts\sft_cancel.py --job-id ftjob-xxxxxxxxxxxxxxxxxxxx

  # Optionally delete the uploaded training file as well (from state)
  python .\scripts\sft_cancel.py --delete-file

Requires OPENAI_API_KEY in environment or .env
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from openai import OpenAI


ROOT = Path(__file__).resolve().parents[1]
STATE_FILE = ROOT / "results" / "sft_state.json"


def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(data: Dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Cancel an OpenAI fine-tuning job")
    parser.add_argument("--job-id", type=str, default=None, help="Fine-tune job id (if omitted, read from results/sft_state.json)")
    parser.add_argument("--delete-file", action="store_true", help="Also delete the uploaded training file (read from state)")
    args = parser.parse_args()

    state = load_state()
    job_id = args.job_id or state.get("job_id")
    if not job_id:
        raise SystemExit("Nessun job_id fornito e nessun results/sft_state.json valido trovato")

    client = OpenAI()
    print(f"Annullamento job: {job_id} ...")
    try:
        job = client.fine_tuning.jobs.cancel(job_id)
        print(f"Esito: id={job.id} status={job.status}")
        state["status"] = getattr(job, "status", state.get("status"))
        save_state(state)
    except Exception as e:
        print("Errore durante la cancellazione del job:")
        print(str(e))
        return

    if args.delete_file:
        file_id = state.get("file_id")
        if not file_id:
            print("Nessun file_id in state; salto la cancellazione del file")
            return
        try:
            print(f"Elimino file di training: {file_id} ...")
            client.files.delete(file_id)
            print("File eliminato")
        except Exception as e:
            print("Errore durante l'eliminazione del file:")
            print(str(e))


if __name__ == "__main__":
    main()
