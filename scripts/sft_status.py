"""
Retrieve status and optional events for an OpenAI fine-tuning job.

Usage (PowerShell):
  # Read job_id from results/sft_state.json
  python .\scripts\sft_status.py

  # Or pass a specific job id
  python .\scripts\sft_status.py --job-id ftjob-xxxxxxxxxxxxxxxxxxxx

  # Also show recent events (last 20 by default)
  python .\scripts\sft_status.py --events --limit 50

  # Update results/sft_state.json with latest status (and fine_tuned_model if present)
  python .\scripts\sft_status.py --update-state

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
    parser = argparse.ArgumentParser(description="Get status for an OpenAI fine-tuning job")
    parser.add_argument("--job-id", type=str, default=None, help="Fine-tune job id (if omitted, read from results/sft_state.json)")
    parser.add_argument("--events", action="store_true", help="Also list recent events for the job")
    parser.add_argument("--limit", type=int, default=20, help="Max number of events to show")
    parser.add_argument("--update-state", action="store_true", help="Update results/sft_state.json with latest status")
    args = parser.parse_args()

    state = load_state()
    job_id = args.job_id or state.get("job_id")
    if not job_id:
        raise SystemExit("Nessun job_id fornito e nessun results/sft_state.json valido trovato")

    client = OpenAI()
    job = client.fine_tuning.jobs.retrieve(job_id)

    # Print a compact summary
    fields = {
        "id": getattr(job, "id", None),
        "status": getattr(job, "status", None),
        "base_model": getattr(job, "model", None),
        "fine_tuned_model": getattr(job, "fine_tuned_model", None) or getattr(job, "result_model", None),
        "created_at": getattr(job, "created_at", None),
        "finished_at": getattr(job, "finished_at", None),
        "training_file": state.get("file_id"),
        "suffix": state.get("suffix"),
    }
    print("Job status:")
    for k, v in fields.items():
        print(f"  {k}: {v}")

    if args.events:
        print("\nRecent events:")
        events = client.fine_tuning.jobs.events.list(job_id, limit=args.limit)
        data = getattr(events, "data", []) or []
        # Events often come newest-first; reverse to show oldest-first
        for ev in reversed(data):
            created = getattr(ev, "created_at", None)
            level = getattr(ev, "level", None)
            msg = getattr(ev, "message", None)
            print(f"  [{level}] {created}: {msg}")

    if args.update_state:
        # Refresh state with latest status and (if present) fine_tuned_model
        state["status"] = getattr(job, "status", state.get("status"))
        ft_model = getattr(job, "fine_tuned_model", None) or getattr(job, "result_model", None)
        if ft_model:
            state["fine_tuned_model"] = ft_model
        save_state(state)
        print("\nState aggiornato su results/sft_state.json")


if __name__ == "__main__":
    main()
