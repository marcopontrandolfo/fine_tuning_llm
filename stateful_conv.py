from openai import OpenAI
from dotenv import load_dotenv
import os
import sys
from typing import List, Dict

# Load environment variables from a .env file if present
load_dotenv()

# Configuration with sensible, low-cost defaults
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # low-cost default
MAX_OUTPUT_TOKENS = int(os.getenv("MAX_OUTPUT_TOKENS", "200"))  # cap tokens to control cost
TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.7"))
TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "30"))  # request timeout in seconds
SECOND_PROMPT = os.getenv("SECOND_PROMPT", "Ora rispondi in modo ancora più breve, riferendoti a quanto detto prima.")
KEEP_LAST_TURNS = int(os.getenv("KEEP_LAST_TURNS", "3"))  # quante coppie user/assistant mantenere

if not OPENAI_API_KEY:
    print("Errore: variabile d'ambiente OPENAI_API_KEY non impostata. Aggiungila al tuo sistema o al file .env.")
    sys.exit(1)

# Initialize client
client = OpenAI(api_key=OPENAI_API_KEY)

# Prompt (can be overridden via env var)
prompt = os.getenv("PROMPT", "Dimmi una barzelletta molto breve in italiano.")

# -----------------------------
# Utilities per conversazione
# -----------------------------
Message = Dict[str, str]

def trim_history(history: List[Message], keep_last_turns: int) -> List[Message]:
    """Mantiene il messaggio di sistema iniziale e le ultime N coppie user/assistant."""
    if not history:
        return history
    system_msgs = [m for m in history if m.get("role") == "system"]
    non_system = [m for m in history if m.get("role") != "system"]
    # Ogni turno è tipicamente user+assistant -> mantieni ultimi 2*keep_last_turns messaggi
    to_keep = max(0, 2 * keep_last_turns)
    trimmed = system_msgs[:1] + non_system[-to_keep:]
    return trimmed

def history_to_string(history: List[Message]) -> str:
    """Converte la history in un input testo semplice per massima compatibilità SDK."""
    lines: List[str] = []
    for m in history:
        role = m.get("role", "user")
        content = m.get("content", "")
        lines.append(f"{role}: {content}")
    return "\n".join(lines)

try:
    # Build conversazione stateful: system -> user -> assistant -> user -> assistant
    history: List[Message] = [
        {"role": "system", "content": "Rispondi in modo conciso e diretto."},
    ]

    # Turno 1: utente fa la prima domanda
    history.append({"role": "user", "content": prompt})

    response_1 = client.responses.create(
        model=MODEL,
        input=history_to_string(history),
        max_output_tokens=MAX_OUTPUT_TOKENS,
        temperature=TEMPERATURE,
        timeout=TIMEOUT,
    )
    text_1 = getattr(response_1, "output_text", None)
    if not text_1:
        text_1 = response_1.output[0].content[0].text  # type: ignore[attr-defined]
    print(text_1)
    history.append({"role": "assistant", "content": text_1})

    # Trimma la history per contenere i costi
    history = trim_history(history, KEEP_LAST_TURNS)

    # Turno 2: nuova domanda basata sulla precedente
    history.append({"role": "user", "content": SECOND_PROMPT})

    response_2 = client.responses.create(
        model=MODEL,
        input=history_to_string(history),
        max_output_tokens=MAX_OUTPUT_TOKENS,
        temperature=TEMPERATURE,
        timeout=TIMEOUT,
    )
    text_2 = getattr(response_2, "output_text", None)
    if not text_2:
        text_2 = response_2.output[0].content[0].text  # type: ignore[attr-defined]
    print(text_2)

except Exception as e:
    print(f"Richiesta fallita: {e}")
