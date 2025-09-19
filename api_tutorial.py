from openai import OpenAI
import os
from dotenv import load_dotenv

# Carica le variabili d'ambiente dal file .env
load_dotenv()

# Inizializza il client con la chiave dal .env
client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY")
    )

# Funzione per testare la connessione usando la nuova API (v1)
def test_openai_connection():
    try:
        # Esempio con Chat Completions (modelli chat)
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Rispondi in modo conciso."},
                {"role": "user", "content": "Ciao, come stai?"}
            ],
            max_tokens=50,
            temperature=0.7,
        )
        print("Risposta ricevuta:")
        print(response.choices[0].message.content.strip())
    except Exception as e:
        msg = str(e)
        print(f"Errore nella chiamata API: {msg}")
        if "insufficient_quota" in msg or "exceeded" in msg:
            print("Suggerimento: hai esaurito la quota. Aggiungi crediti dal pannello Billing della piattaforma OpenAI.")
        elif "invalid" in msg.lower():
            print("Suggerimento: verifica che OPENAI_API_KEY nel file .env sia corretta.")
        elif "You tried to access openai.Completion" in msg:
            print("Stai usando la sintassi vecchia. Ora questo file è già migrato alla nuova API.")

# Esegui il test
if __name__ == "__main__":
    test_openai_connection()
