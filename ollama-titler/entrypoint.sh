#!/bin/sh
# Avvia Ollama e pull automatico di qwen2:0.5b solo se manca
ollama serve &
OLLAMA_PID=$!

echo "[titler] Attendo Ollama..."
until curl -s http://localhost:11434/api/tags > /dev/null 2>&1; do
  sleep 1
done

if ! ollama list 2>/dev/null | grep -q "qwen2:0.5b"; then
  echo "[titler] Pull qwen2:0.5b..."
  ollama pull qwen2:0.5b
else
  echo "[titler] qwen2:0.5b già presente, skip download."
fi

echo "[titler] Pronto — modello caricato"
wait $OLLAMA_PID
