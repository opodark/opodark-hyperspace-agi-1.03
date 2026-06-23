# HyperSpace AGI — Memory Graph (Obsidian)

Questo modulo esporta la memoria del Control Plane in un vault Obsidian navigabile.

## Architettura

```
CP /memory endpoint
      ↓ ogni 60s
  exporter.py (container)
      ↓
  vault/ (volume Docker condiviso)
      ↓
  Obsidian desktop (monta lo stesso volume)
```

## Avvio

Il service `memory-graph` si avvia automaticamente con:

```bash
docker compose up -d memory-graph
```

## Aprire il vault in Obsidian

1. Avvia Obsidian desktop
2. **Open folder as vault**
3. Seleziona il path del volume Docker:
   - Mac: `~/Library/Containers/com.docker.docker/Data/vms/0/data/...` oppure usa il bind mount
   - **Consigliato**: aggiungi `./data/obsidian-vault` in `.env` e Obsidian punta lì

## Endpoint exporter

- `http://localhost:8090/health` — status exporter
- `http://localhost:8090/stats`  — statistiche export

## Struttura vault

```
vault/
├── INDEX.md              ← Map of Content principale
├── prompts/              ← Prompt WebUI
├── responses/            ← Risposte Ollama
├── mesh/                 ← Traffico inter-nodo
├── sync/                 ← Memory sync events
├── vault/                ← Note OMEGA bridge
└── system/               ← System events
```

## Wikilinks automatici

Ogni nota è collegata tramite `[[wikilink]]` alle entry con lo stesso:
- `task_id` (prompt ↔ risposta dello stesso task)
- `node_id` (tutte le attività dello stesso nodo)
- `model` (tutti i prompt sullo stesso modello)

Il grafo relazionale di Obsidian mostra queste connessioni visivamente.
