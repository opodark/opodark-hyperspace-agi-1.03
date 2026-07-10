#!/bin/bash
# =============================================================
# HyperSpace AGI v1.03 — Setup iniziale
# =============================================================
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}[HyperSpace] Setup avviato (v1.03)...${NC}"

# 1. Verifica Ollama nativo
if ! command -v ollama &>/dev/null; then
  echo -e "${RED}[HyperSpace] Ollama non trovato. Installa Ollama da https://ollama.com${NC}"
  exit 1
fi

# 2. Verifica modello qwen2 sull'host (opzionale)
if ! ollama list 2>/dev/null | grep -q "qwen2"; then
  echo -e "${YELLOW}[HyperSpace] qwen2 non trovato. Download in corso...${NC}"
  ollama pull qwen2:0.5b || true
fi

# 3. Avvia tutti i container
echo -e "${GREEN}[HyperSpace] Avvio stack completo...${NC}"
docker compose up -d --build --remove-orphans

# 4. Attendi che i servizi critici siano up
echo -e "${YELLOW}[HyperSpace] Attendo avvio servizi...${NC}"
sleep 8

# 5. (Opzionale) Copia modelli da Ollama host nel titler
# Se non ti serve più questa funzionalità, puoi rimuovere questa sezione
if docker volume ls | grep -q "opodark-hyperspace-agi-103_ollama_titler_data"; then
  echo -e "${GREEN}[HyperSpace] Copio eventuale modello nel titler...${NC}"
  docker run --rm \
    -v "$HOME/.ollama":/host_ollama:ro \
    -v opodark-hyperspace-agi-103_ollama_titler_data:/root/.ollama \
    alpine sh -c "cp -r /host_ollama/models /root/.ollama/ 2>/dev/null || true"
fi

echo -e "${GREEN}"
echo "  ✓ HyperSpace AGI 1.03 pronto!"
echo "  Control Plane: http://localhost:8085"
echo "  Dashboard (Infra UI): http://localhost:8099"
echo "  Open WebUI:   http://localhost:3000"
echo "  Obsidian:      http://localhost:8091"
echo -e "${NC}"
