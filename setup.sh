#!/bin/bash
# =============================================================
# HyperSpace AGI v1.02 — Setup iniziale
# Copia il modello qwen2 da Ollama nativo nel container titler
# =============================================================
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}[HyperSpace] Setup avviato...${NC}"

# 1. Verifica Ollama nativo
if ! command -v ollama &>/dev/null; then
  echo -e "${RED}[HyperSpace] Ollama non trovato. Installa Ollama da https://ollama.com${NC}"
  exit 1
fi

# 2. Verifica modello qwen2 sull'host
if ! ollama list 2>/dev/null | grep -q "qwen2"; then
  echo -e "${YELLOW}[HyperSpace] qwen2 non trovato. Download in corso (~350MB)...${NC}"
  ollama pull qwen2:0.5b
fi

# 3. Avvia i container (crea i volumi)
echo -e "${GREEN}[HyperSpace] Avvio container...${NC}"
docker compose up -d --build

# 4. Attendi che il volume ollama_titler_data esista
echo -e "${YELLOW}[HyperSpace] Attendo avvio ollama-titler...${NC}"
sleep 5

# 5. Copia modelli dal Mac nel container titler
echo -e "${GREEN}[HyperSpace] Copio modello qwen2 nel titler Docker...${NC}"
docker run --rm \
  -v "$HOME/.ollama":/host_ollama:ro \
  -v hyperspace-agi-102_ollama_titler_data:/root/.ollama \
  alpine sh -c "cp -r /host_ollama/models /root/.ollama/"

# 6. Restart memory-graph per sicurezza
docker compose restart memory-graph

echo -e "${GREEN}"
echo "  ✓ HyperSpace AGI pronto!"
echo "  Dashboard:    http://localhost:8099"
echo "  Control Plane: http://localhost:8085"
echo "  Obsidian:      http://localhost:8091  (password: abc)"
echo "  Memory API:    http://localhost:8090/status"
echo -e "${NC}"
