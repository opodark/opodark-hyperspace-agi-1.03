# HyperSpace AGI v0.2

> Framework per agenti IA autonomi basati su SLM (Small Language Models), eseguiti localmente tramite Docker. Il motore di inferenza (Ollama o LM Studio) gira **sull'host**, non in Docker — più veloce, più leggero, accesso diretto alla GPU.

---

## Quick Start

```bash
git clone https://github.com/opodark/hyperspace-agi-1.01.git
cd hyperspace-agi-1.01

# macOS / Linux
chmod +x setup.sh && ./setup.sh

# Windows (PowerShell)
Set-ExecutionPolicy -Scope Process Bypass
.\setup.ps1
```

Il setup guida la configurazione del backend LLM (Ollama o LM Studio) e avvia i container.

**Dashboard:** http://localhost:8085/dashboard 
**Node API:** http://localhost:8084/status

---

## Architettura v0.2

```
  HOST MACHINE
  ┌───────────────────────────────────────────────────────┐
  │  Ollama (nativo) o LM Studio                       │
  │  :11434 / :1234                                    │
  └───────────────────────────────────────────────────────┘
           ↑ host.docker.internal
  DOCKER
  ┌─────────────┐   ┌─────────────────────────┐
  │ control-plane │──┤        node (worker)        │
  │    :8085      │   │ :8084  ECDSA identity  │
  └─────────────┘   │ /status /peers /execute│
                       └─────────────────────────┘
         multi-machine: ogni host ha il suo node
         i nodi si scoprono via BOOT_PEERS + PEX
```

### Principi chiave

- **Mesh-first** — i nodi si scoprono e comunicano direttamente via `/peers` (PEX), senza registry centralizzato
- **Identità crittografica** — ogni nodo genera un keypair ECDSA secp256k1 al primo avvio, `node_id = sha256(pubkey)[:40]`
- **Ollama/LM Studio sull'host** — accesso diretto alla GPU, zero overhead Docker, modelli condivisi tra sessioni
- **Authority legacy** — mantenuta nel codice ma disabilitata di default, nascosta dalla UI

---

## Stack Docker

| Container | Porta | Descrizione |
|---|---|---|
| `node` | 8084 | Worker node — identità ECDSA, PEX, /execute |
| `control-plane` | 8085 | Dashboard mesh + orchestrazione task |
| `ollama` *(opt-in)* | 11434 | Solo con `--profile with-ollama` (legacy) |

> L'authority (`authority:8080`) è mantenuta nel codice per compatibilità ma non viene avviata di default.

---

## Backend LLM supportati

| Backend | Setup | OLLAMA_URL |
|---|---|---|
| **Ollama nativo** | `./setup.sh` opzione 1 | `http://host.docker.internal:11434` |
| **LM Studio** | `./setup.sh` opzione 2 | `http://host.docker.internal:1234` |
| **Ollama Docker** | `./setup.sh` opzione 3 | `http://ollama:11434` |

Modelli consigliati per hardware consumer:

| Modello | Param | VRAM / RAM | Note |
|---|---|---|---|
| `phi3` | 3.8B | ~2.3 GB | Velocissimo, ottimo su CPU |
| `llama3:8b` | 8B | ~5 GB | Bilanciato |
| `mistral:7b` | 7B | ~4.5 GB | Ottima qualità |
| `qwen2:7b` | 7B | ~4.5 GB | Multilingue |
| `llama3:70b` | 70B | ~40 GB | Alta qualità, GPU richiesta |

---

## Struttura del progetto

```
hyperspace-agi-1.01/
├── node/                    # Worker node (FastAPI)
├── worker/                  # Worker legacy (FastAPI)
├──── main.py                # API v0.2: /status /peers /peer/add /execute
├── control-plane/          # Dashboard + orchestrazione (Flask)
├──── main.py                # Dashboard mesh-first, authority nascosta
├── shared/
├──── identity.py            # ECDSA secp256k1: genera node_id, sign, verify
├── authority/              # Registry legacy (mantenuto, non avviato di default)
├── setup.sh                # Setup guidato macOS/Linux
├── setup.ps1               # Setup guidato Windows
├── docker-compose.prod.yml # Compose produzione (senza ollama)
├── docker-compose.yml      # Compose sviluppo
└── .env.example            # Template variabili
```

---

## Variabili d'ambiente principali

```bash
# Node
NODE_HOSTNAME=localhost        # hostname o IP pubblico del nodo
NODE_TIER=leaf                 # leaf | hub | root
VRAM_GB=0.0                    # VRAM GPU disponibile
BOOT_PEERS=                    # peer iniziali: "ip1:8084,ip2:8084"

# Inferenza
INFERENCE_BACKEND=ollama       # ollama | lmstudio | ollama-docker
OLLAMA_URL=http://host.docker.internal:11434
OLLAMA_MODEL=phi3
LMS_URL=                       # URL LM Studio (se INFERENCE_BACKEND=lmstudio)

# Control plane
NODE_ENDPOINTS=node:8084       # nodi da monitorare (separati da virgola)

# Legacy
AUTHORITY_ENABLED=false
```

---

## API Reference

### Node / Worker (porta 8084)

| Method | Path | Descrizione |
|---|---|---|
| GET | `/health` | Ping rapido + uptime |
| GET | `/status` | Schema v0.2 completo (node_id, tier, peers, caps…) |
| GET | `/identity` | Profilo pubblico immutabile |
| GET | `/peers` | Lista peer noti con stato PEX |
| POST | `/peer/add` | Registra un nuovo peer |
| POST | `/execute` | Esegui task LLM |
| POST | `/verify` | Verifica firma ECDSA messaggio peer |
| GET | `/ollama/health` | Stato Ollama/LM Studio |
| GET | `/ollama/models` | Modelli disponibili |

### Control Plane (porta 8085)

| Method | Path | Descrizione |
|---|---|---|
| GET | `/dashboard` | Dashboard HTML |
| GET | `/mesh/nodes` | Stato aggregato nodi mesh |
| GET | `/mesh/node/<ep>/status` | Status singolo nodo |
| GET | `/mesh/node/<ep>/peers` | Peers di un nodo |
| GET | `/tasks` | Lista tasks |
| POST | `/task/create` | Crea task |
| POST | `/task/assign` | Assegna ed esegui task sul nodo più disponibile |
| GET | `/logs` | Stream logs (filtri: type, status, node, q) |
| POST | `/logs/add` | Aggiungi log entry |
| POST | `/logs/clear` | Svuota log |
| GET | `/hb/status` | Stato heartbeat loop |
| GET | `/config/advanced` | Leggi config |
| POST | `/config/advanced` | Salva config |
| POST | `/config/secret/rotate` | Ruota shared secret |
| GET | `/ollama/status` | Stato Ollama/LM Studio dal control-plane |

---

## Deploy multi-macchina

Ogni macchina avvia il proprio `node`. I nodi si scoprono tramite `BOOT_PEERS`:

```bash
# Macchina A (192.168.1.10)
BOOT_PEERS=192.168.1.11:8084
docker compose -f docker-compose.prod.yml up -d --build

# Macchina B (192.168.1.11)
BOOT_PEERS=192.168.1.10:8084
docker compose -f docker-compose.prod.yml up -d --build
```

Dopo il boot i nodi si scambiano la lista peer via `/peers` (PEX leggero). Il control-plane può girare su una sola macchina e monitorare tutti i nodi tramite `NODE_ENDPOINTS`.

---

## Changelog

### v0.2 (Giugno 2026)
- **Mesh-first**: discovery tramite `/peers` + PEX, no registry centralizzato
- **Ollama/LM Studio sull'host**: rimosso container ollama dal default
- **Setup guidato**: `setup.sh` (macOS/Linux) e `setup.ps1` (Windows) con install Ollama, pull modello, supporto LM Studio
- **Dashboard rinnovata**: tab Mesh Nodes con card live per ogni nodo, tier badge, pubkey, uptime, peers
- **Schema `/status` v0.2**: `peers_active`, `peers_known`, `capabilities`, `vram_gb`, `endpoint`
- **Authority legacy**: mantenuta nel codice, nascosta dalla UI, disabilitata di default
- **Identità ECDSA**: `node_id` derivato da keypair secp256k1 persistente

### v1.01
- Dashboard con Log Viewer, Diagnostics, Advanced Setup
- Authority registry + heartbeat
- Task create/assign UI

---

## Roadmap

- [ ] Firma e verifica messaggi inter-nodo in produzione
- [ ] Tier dinamico (leaf → hub) basato su peers_active
- [ ] Persistenza log su SQLite
- [ ] Auth JWT tra nodi
- [ ] UI topologia mesh (grafo nodi)
- [ ] Pull modello automatico da dashboard
