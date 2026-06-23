# infra-ui — Live Mesh Dashboard

Dashboard 3D in tempo reale per HyperSpace-AGI.

## Struttura

```
infra-ui/
  dashboard.html   ← Single-page app (WebGL canvas + HUD)
  server.py        ← FastAPI SSE bridge + REST proxy
  requirements.txt
```

## Avvio rapido

```bash
cd infra-ui
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8099 --reload
# apri http://localhost:8099
```

## Modalità

| Modalità | Quando | Comportamento |
|---|---|---|
| **SSE live** | Bridge online + CP raggiungibile | Link si accendono su eventi reali |
| **REST poll** | Bridge online, SSE fallisce | Polling ogni 4s su `/memory/stats` e `/nodes/active` |
| **Demo** | Bridge offline | Traffico simulato, nessun dato reale |

## Eventi SSE

| Event type | Payload | Trigger |
|---|---|---|
| `task` | `{from, to, type, label}` | CP `POST /push/task` o poller `/tasks/recent` |
| `memory_sync` | `{from, to, entries}` | CP `POST /push/memory_sync` |
| `heartbeat` | `{node_id, ping, last_seen}` | Poller `/nodes/active` ogni 4s |
| `memory_stats` | `{entries, max_entries, ttl_days, file_size_kb}` | Poller `/memory/stats` ogni 4s |

## Integrare il CP

Nel tuo `control-plane/main.py`, dopo ogni task completato:

```python
import httpx

UI_BRIDGE = os.getenv("UI_BRIDGE_URL", "http://localhost:8099")

async def notify_ui(from_node: str, to_node: str, label: str):
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            await c.post(f"{UI_BRIDGE}/push/task", json={
                "from": from_node, "to": to_node,
                "type": "task", "label": label
            })
    except Exception:
        pass  # UI non bloccante
```

## Variabili env

| Var | Default | Descrizione |
|---|---|---|
| `CP_URL` | `http://localhost:8000` | URL Control Plane |
| `REGISTRY_URL` | `http://localhost:8086` | URL Registry |
| `POLL_INTERVAL` | `4` | Secondi tra i poll REST |
| `UI_BRIDGE_URL` | `http://localhost:8099` | (lato CP) dove pushare gli eventi |
