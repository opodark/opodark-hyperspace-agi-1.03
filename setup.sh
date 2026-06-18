#!/usr/bin/env bash
# =============================================================
# HyperSpace AGI v1.02 — Setup Script (macOS / Linux)
# =============================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${GREEN}[setup]${RESET} $*"; }
warn() { echo -e "${YELLOW}[warn]${RESET}  $*"; }
err()  { echo -e "${RED}[error]${RESET} $*"; exit 1; }
hdr()  { echo -e "\n${BOLD}${CYAN}$*${RESET}"; }

echo -e ""
echo -e "${BOLD}${CYAN}⬢  HyperSpace AGI v1.02 — Setup${RESET}"
echo -e "    Mesh di agenti IA locali su Docker + modelli LLM"
echo -e ""

# ── 1. Dipendenze ─────────────────────────────────────────────────────
hdr "1/4 — Verifica dipendenze"

if ! command -v docker &>/dev/null; then
    err "Docker non trovato. Installa Docker Desktop: https://docs.docker.com/get-docker/"
fi
log "docker ✓"

if ! docker compose version &>/dev/null; then
    err "'docker compose' plugin non trovato: https://docs.docker.com/compose/install/"
fi
log "docker compose ✓"

# ── 2. .env ────────────────────────────────────────────────────────────
hdr "2/4 — Configurazione .env"

if [ ! -f .env ]; then
    cp .env.example .env
    log ".env creato da .env.example"
else
    log ".env già presente — non sovrascritto"
fi

set_env() {
    local key="$1" val="$2"
    if grep -q "^${key}=" .env 2>/dev/null; then
        sed -i.bak "s|^${key}=.*|${key}=${val}|" .env && rm -f .env.bak
    else
        echo "${key}=${val}" >> .env
    fi
}

# ── HELPER: patch Ollama su Linux per ascoltare su 0.0.0.0 ─────────────
_patch_ollama_linux() {
    local port="${1:-11434}"

    # Già ascolta su 0.0.0.0? Non serve nulla.
    if ss -tlnp 2>/dev/null | grep -q "0\.0\.0\.0:${port}"; then
        log "Ollama già in ascolto su 0.0.0.0:${port} — nessuna modifica necessaria."
        return 0
    fi

    warn "Ollama è in ascolto solo su 127.0.0.1 — i container non possono raggiungerlo."
    warn "Riconfigurazione automatica di Ollama per ascoltare su 0.0.0.0:${port}..."

    # --- Caso A: systemd unit esiste ---
    if systemctl list-unit-files ollama.service &>/dev/null 2>&1 | grep -q ollama; then
        local OVERRIDE_DIR="/etc/systemd/system/ollama.service.d"
        sudo mkdir -p "$OVERRIDE_DIR"
        sudo tee "${OVERRIDE_DIR}/override.conf" > /dev/null <<EOF
[Service]
Environment="OLLAMA_HOST=0.0.0.0:${port}"
EOF
        sudo systemctl daemon-reload
        sudo systemctl restart ollama
        log "Ollama systemd riavviato con OLLAMA_HOST=0.0.0.0:${port}"

    # --- Caso B: Ollama installato come binario standalone (no systemd) ---
    elif command -v ollama &>/dev/null; then
        warn "Ollama non è gestito da systemd."
        warn "Fermo il processo ollama corrente e lo riavvio in background con OLLAMA_HOST=0.0.0.0:${port}..."

        # Ferma eventuali istanze in esecuzione
        pkill -x ollama 2>/dev/null || true
        sleep 1

        # Aggiungi OLLAMA_HOST al profilo shell se non già presente
        local PROFILE="${HOME}/.bashrc"
        if ! grep -q 'OLLAMA_HOST' "$PROFILE" 2>/dev/null; then
            echo "export OLLAMA_HOST=0.0.0.0:${port}" >> "$PROFILE"
            log "OLLAMA_HOST aggiunto a $PROFILE"
        fi

        # Riavvia in background
        OLLAMA_HOST="0.0.0.0:${port}" nohup ollama serve > /tmp/ollama.log 2>&1 &
        sleep 2
        log "Ollama riavviato in background (log: /tmp/ollama.log)"

    else
        warn "Impossibile riconfigurare Ollama automaticamente."
        warn "Esegui manualmente: OLLAMA_HOST=0.0.0.0:${port} ollama serve"
        return 1
    fi

    # Apri porta sul bridge docker0 se ufw è attivo
    if command -v ufw &>/dev/null && sudo ufw status 2>/dev/null | grep -q 'Status: active'; then
        sudo ufw allow in on docker0 to any port "${port}" comment 'ollama docker bridge' 2>/dev/null || true
        log "ufw: regola aggiunta per docker0 -> port ${port}"
    fi

    # Verifica finale
    sleep 1
    if ss -tlnp 2>/dev/null | grep -q "0\.0\.0\.0:${port}"; then
        log "✓ Ollama ora in ascolto su 0.0.0.0:${port}"
    else
        warn "Ollama potrebbe non essere ancora pronto — controlla con: ss -tlnp | grep ${port}"
    fi
}

# ── 3. Backend inferenza ────────────────────────────────────────────────────
hdr "3/4 — Backend di inferenza LLM"

echo ""
echo "  Quale backend vuoi usare?"
echo ""
echo "  1) Ollama nativo  (deve essere già installato e in esecuzione sull'host)"
echo "  2) LM Studio      (Local Server attivo sull'app)"
echo "  3) Ollama Docker  (legacy, avvia ollama come container)"
echo ""
read -rp "  Scelta [1/2/3, default 1]: " BACKEND_CHOICE
BACKEND_CHOICE=${BACKEND_CHOICE:-1}

case "$BACKEND_CHOICE" in

1)
    log "Backend: Ollama nativo"
    set_env "INFERENCE_BACKEND" "ollama"
    echo ""
    read -rp "  URL Ollama [default: http://localhost:11434]: " OLLAMA_INPUT
    OLLAMA_INPUT=${OLLAMA_INPUT:-http://localhost:11434}

    # Verifica connessione sull'host (prima del remap)
    if curl -sf "${OLLAMA_INPUT}/api/tags" &>/dev/null; then
        log "Ollama raggiungibile su $OLLAMA_INPUT"
    else
        warn "Ollama non risponde su $OLLAMA_INPUT"
        warn "Assicurati che Ollama sia avviato: ollama serve"
    fi

    # Rimappa per i container Docker
    OS_TYPE=$(uname -s)
    OLLAMA_DOCKER_URL="$OLLAMA_INPUT"
    if echo "$OLLAMA_INPUT" | grep -qE "localhost|127\.0\.0\.1"; then
        OLLAMA_PORT=$(echo "$OLLAMA_INPUT" | grep -oE '[0-9]+$' || echo "11434")
        if [ "$OS_TYPE" = "Darwin" ]; then
            OLLAMA_DOCKER_URL="http://host.docker.internal:${OLLAMA_PORT}"
        else
            # ── FIX LINUX: patch Ollama per ascoltare su 0.0.0.0 ──────────
            _patch_ollama_linux "$OLLAMA_PORT"
            # ────────────────────────────────────────────────────────────────
            if getent hosts host.docker.internal &>/dev/null; then
                OLLAMA_DOCKER_URL="http://host.docker.internal:${OLLAMA_PORT}"
            else
                DOCKER0_IP=$(ip addr show docker0 2>/dev/null | grep 'inet ' | awk '{print $2}' | cut -d/ -f1 || echo "172.17.0.1")
                OLLAMA_DOCKER_URL="http://${DOCKER0_IP}:${OLLAMA_PORT}"
                log "Linux: usando docker0 IP $DOCKER0_IP per raggiungere l'host"
            fi
        fi
    fi
    set_env "OLLAMA_URL" "$OLLAMA_DOCKER_URL"
    log "OLLAMA_URL impostato: $OLLAMA_DOCKER_URL"

    # Verifica finale raggiungibilità dall'URL che useranno i container
    sleep 1
    if curl -sf "${OLLAMA_DOCKER_URL}/api/tags" &>/dev/null; then
        log "✓ Ollama raggiungibile via docker URL: $OLLAMA_DOCKER_URL"
    else
        warn "Ollama non risponde su $OLLAMA_DOCKER_URL — i container potrebbero non riuscire a connettersi."
        warn "Verifica con: curl $OLLAMA_DOCKER_URL/api/tags"
    fi
    COMPOSE_PROFILE=""
    ;;

2)
    log "Backend: LM Studio"
    set_env "INFERENCE_BACKEND" "lmstudio"
    echo ""
    echo "  Assicurati che LM Studio sia aperto con Local Server attivo."
    echo ""
    read -rp "  URL LM Studio [default: http://localhost:1234]: " LMS_INPUT
    LMS_INPUT=${LMS_INPUT:-http://localhost:1234}

    if curl -sf "${LMS_INPUT}/v1/models" &>/dev/null; then
        log "LM Studio raggiungibile su $LMS_INPUT"
    else
        warn "LM Studio non risponde su $LMS_INPUT"
        warn "Apri LM Studio -> Local Server -> Start Server"
    fi

    OS_TYPE=$(uname -s)
    LMS_DOCKER_URL="$LMS_INPUT"
    if echo "$LMS_INPUT" | grep -qE "localhost|127\.0\.0\.1"; then
        LMS_PORT=$(echo "$LMS_INPUT" | grep -oE '[0-9]+$' || echo "1234")
        if [ "$OS_TYPE" = "Darwin" ]; then
            LMS_DOCKER_URL="http://host.docker.internal:${LMS_PORT}"
        else
            DOCKER0_IP=$(ip addr show docker0 2>/dev/null | grep 'inet ' | awk '{print $2}' | cut -d/ -f1 || echo "172.17.0.1")
            LMS_DOCKER_URL="http://${DOCKER0_IP}:${LMS_PORT}"
        fi
    fi
    set_env "OLLAMA_URL" "$LMS_DOCKER_URL"
    set_env "LMS_URL" "$LMS_DOCKER_URL"
    log "OLLAMA_URL (LM Studio): $LMS_DOCKER_URL"
    COMPOSE_PROFILE=""
    ;;

3)
    warn "Modalità Ollama-in-Docker (legacy)."
    set_env "INFERENCE_BACKEND" "ollama-docker"
    set_env "OLLAMA_URL" "http://ollama:11434"
    COMPOSE_PROFILE="--profile with-ollama"
    log "Container ollama sarà avviato."
    ;;

*)
    warn "Scelta non valida. Usando Ollama nativo con URL default."
    set_env "INFERENCE_BACKEND" "ollama"
    set_env "OLLAMA_URL" "http://host.docker.internal:11434"
    COMPOSE_PROFILE=""
    ;;

esac

# ── 4. Avvio container ────────────────────────────────────────────────────────
hdr "4/4 — Avvio HyperSpace AGI"

COMPOSE_FILE="docker-compose.prod.yml"
if [ ! -f "$COMPOSE_FILE" ]; then
    warn "$COMPOSE_FILE non trovato, usando docker-compose.yml"
    COMPOSE_FILE="docker-compose.yml"
fi

log "Build + avvio container..."
# shellcheck disable=SC2086
docker compose -f "$COMPOSE_FILE" $COMPOSE_PROFILE up -d --build

echo ""
log "HyperSpace AGI avviato!"
echo ""
echo -e "  Dashboard:   ${CYAN}http://localhost:8085/dashboard${RESET}"
echo -e "  Node API:    ${CYAN}http://localhost:8084/status${RESET}"
echo -e "  Logs live:   docker compose -f $COMPOSE_FILE logs -f"
echo ""
echo -e "  Per fermare: docker compose -f $COMPOSE_FILE down"
echo ""
