# OpenAI Fine-Tuning Project

Progetto per sperimentare con l'API di OpenAI e il fine-tuning di modelli.

## 📁 Struttura del Progetto

- `fine_tuning_prova.py` - Script principale per API testing e fine-tuning
- `simulatore_api.py` - Simulatore per testare senza consumare crediti
- `monitor_fine_tune.py` - Script per monitorare job di fine-tuning
- `dataset_fixed.jsonl` - Dataset in formato JSONL per il fine-tuning

## ⚙️ Setup

### 1. Installa le dipendenze
```bash
pip install openai
```

### 2. Configura la API Key
Crea un file `.env` nella root del progetto:
```
OPENAI_API_KEY=tua_chiave_api_qui
```

**⚠️ IMPORTANTE**: Non condividere mai la tua API key! Il file `.env` è ignorato da Git.

### 3. Alternativa: Variabile d'ambiente
```bash
# Windows PowerShell
$env:OPENAI_API_KEY="tua_chiave_api_qui"

# Windows CMD
set OPENAI_API_KEY=tua_chiave_api_qui

# Linux/Mac
export OPENAI_API_KEY="tua_chiave_api_qui"
```

## 🚀 Utilizzo

### Test API (senza consumare crediti)
```bash
python simulatore_api.py
```

### Test API reale (richiede crediti)
```bash
python fine_tuning_prova.py
```

### Monitoraggio Fine-tuning
```bash
python monitor_fine_tune.py
```

## 💰 Costi

- **API standard**: ~$0.002 per 1000 token
- **Fine-tuning**: ~$8 per 1M token di training
- **Dataset di esempio**: ~$0.08 per il training

## 📋 Note

- Il fine-tuning richiede crediti OpenAI
- Il simulatore permette di testare la logica senza costi
- Tutto il codice di fine-tuning è pronto ma commentato per sicurezza

## 🔧 Troubleshooting

- **Quota exceeded**: Aggiungi crediti al tuo account OpenAI
- **API key not found**: Verifica il file `.env` o la variabile d'ambiente
- **File format error**: Il dataset deve essere in formato JSONL valido
