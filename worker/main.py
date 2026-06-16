# worker/main.py
# HyperSpace AGI v0.2 — Worker Node
# Identità crittografica nativa ECDSA secp256k1

from fastapi import FastAPI
from pydantic import BaseModel
import asyncio
import httpx
import os
import sys
import threading
import time
import uuid
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.identity import generate_or_load_identity, sign_message, verify_message

app = FastAPI()

# ── IDENTITÀ CRITTOGRAFICA ──────────────────────────────────────────────────
_identity    = generate_or_load_identity()
WORKER_ID    = _identity["node_id"]
NODE_PUBKEY  = _identity["public_key"]
_private_key = _identity["_private_key"]

# ── CONFIG ──────────────────────────────────────────────────────────────────
WORKER_HOSTNAME  = os.getenv("WORKER_HOSTNAME", "worker")
WORKER_PORT      = int(os.getenv("WORKER_PORT", 8084))
OLLAMA_URL       = os.getenv("OLLAMA_URL", "http://ollama:11434")
DEFAULT_MODEL    = os.getenv("OLLAMA_MODEL", "phi3")
HEARTBEAT_EVERY  = int(os.getenv("HEARTBEAT_EVERY", 30))
NODE_TIER        = os.getenv("NODE_TIER", "leaf")  # leaf | hub | root
VRAM_GB          = float(os.getenv("VRAM_GB", "0.0"))
NODE_VERSION     = "0.2.0"

# Authority — legacy, usata solo se esplicitamente abilitata
_AUTHORITY_URL     = os.getenv("AUTHORITY_URL", "http://authority:8080")
_AUTHORITY_ENABLED = os.getenv("AUTHORITY_ENABLED", "false").lower() == "true"

_boot_time = time.time()

# ── PEER REGISTRY ───────────────────────────────────────────────────────────
# { endpoint: {node_id, endpoint, pubkey, tier, last_seen, status} }
_peers: dict = {}

# Carica BOOT_PEERS da env (es: "node-2:8084,192.168.1.11:8084")
for _ep in os.getenv("BOOT_PEERS", "").split(","):
    _ep = _ep.strip()
    if _ep:
        _peers[_ep] = {
            "endpoint": _ep,
            "node_id":  "unknown",
            "pubkey":   "",
            "tier":     "leaf",
            "status":   "unknown",
            "last_seen": None,
        }

NODE_PROFILE = {
    "node_id":      WORKER_ID,
    "pubkey":       NODE_PUBKEY,
    "tier":         NODE_TIER,
    "endpoint":     f"{WORKER_HOSTNAME}:{WORKER_PORT}",
    "capabilities": ["ollama", "execute"],
    "vram_gb":      VRAM_GB,
    "version":      NODE_VERSION,
}


# ── HELPERS ─────────────────────────────────────────────────────────────────

def build_signed_payload(data: dict) -> dict:
    payload = {**data, "pubkey": NODE_PUBKEY, "node_id": WORKER_ID}
    return sign_message(payload, _private_key)

def _uptime() -> int:
    return int(time.time() - _boot_time)

def _peers_active() -> int:
    return sum(1 for p in _peers.values() if p.get("status") == "active")


# ── OLLAMA HELPERS ───────────────────────────────────────────────────────────

async def ollama_generate(prompt: str, model: str = DEFAULT_MODEL) -> str:
    payload = {"model": model, "prompt": prompt, "stream": False}
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
            r.raise_for_status()
            return r.json().get("response", "").strip()
    except Exception as e:
        return f"[OLLAMA ERROR] {e}"

async def ollama_health() -> dict:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{OLLAMA_URL}/api/tags")
            models = [m["name"] for m in r.json().get("models", [])]
            return {"ok": True, "models": models}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── PEER EXCHANGE (PEX) ──────────────────────────────────────────────────────

def _ping_peer(ep: str):
    """Interroga /status di un peer, aggiorna _peers."""
    import requests as req
    try:
        r = req.get(f"http://{ep}/status", timeout=3)
        if r.status_code == 200:
            info = r.json()
            _peers[ep] = {
                "endpoint":  ep,
                "node_id":   info.get("node_id", "unknown"),
                "pubkey":    info.get("pubkey", ""),
                "tier":      info.get("tier", "leaf"),
                "version":   info.get("version", "?"),
                "status":    "active",
                "last_seen": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            # PEX: scopri i peer dei peer
            try:
                rp = req.get(f"http://{ep}/peers", timeout=2)
                for p in rp.json().get("peers", []):
                    pep = p.get("endpoint", "")
                    if pep and pep != NODE_PROFILE["endpoint"] and pep not in _peers:
                        _peers[pep] = {
                            "endpoint": pep,
                            "node_id":  p.get("node_id", "unknown"),
                            "pubkey":   p.get("pubkey", ""),
                            "tier":     p.get("tier", "leaf"),
                            "status":   "unknown",
                            "last_seen": None,
                        }
            except Exception:
                pass
    except Exception:
        if ep in _peers:
            _peers[ep]["status"] = "unreachable"


def peer_sync_loop():
    """Ciclo di sincronizzazione peer ogni HEARTBEAT_EVERY secondi."""
    time.sleep(5)
    while True:
        for ep in list(_peers.keys()):
            _ping_peer(ep)
        time.sleep(HEARTBEAT_EVERY)


# ── AUTHORITY HEARTBEAT (legacy, silente) ────────────────────────────────────

def _authority_register():
    if not _AUTHORITY_ENABLED:
        return
    import requests as req
    for attempt in range(10):
        try:
            r = req.post(
                f"{_AUTHORITY_URL}/register",
                json=build_signed_payload({
                    "host":         WORKER_HOSTNAME,
                    "port":         WORKER_PORT,
                    "capabilities": NODE_PROFILE["capabilities"],
                    "version":      NODE_VERSION,
                }),
                timeout=5,
            )
            if r.status_code == 200:
                print(f"[WORKER:{WORKER_ID[:12]}] registered on authority (legacy)")
                return
        except Exception as e:
            print(f"[WORKER:{WORKER_ID[:12]}] authority register attempt {attempt+1}: {e}")
        time.sleep(min(5 + attempt * 3, 30))

def _authority_heartbeat_loop():
    _authority_register()
    if not _AUTHORITY_ENABLED:
        return
    import requests as req
    while True:
        try:
            req.post(
                f"{_AUTHORITY_URL}/heartbeat",
                json=build_signed_payload({"uptime_s": _uptime()}),
                timeout=5,
            )
        except Exception:
            pass
        time.sleep(HEARTBEAT_EVERY)


# ── STARTUP ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    threading.Thread(target=peer_sync_loop, daemon=True).start()
    threading.Thread(target=_authority_heartbeat_loop, daemon=True).start()
    print(f"[WORKER:{WORKER_ID[:12]}] v{NODE_VERSION} up | tier={NODE_TIER} | endpoint={NODE_PROFILE['endpoint']}")
    print(f"[WORKER:{WORKER_ID[:12]}] boot peers: {list(_peers.keys()) or 'none'}")


# ── API v0.2 ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Ping rapido — usato dal control-plane per latency check."""
    return {
        "status":   "ok",
        "node_id":  WORKER_ID,
        "uptime_s": _uptime(),
    }


@app.get("/status")
def status():
    """
    Schema v0.2 completo — consumato dalla dashboard del control-plane.
    Campi richiesti dalla Mesh Nodes card:
      node_id, pubkey, tier, endpoint, version, capabilities,
      vram_gb, peers_active, peers_known, uptime_s, status
    """
    return {
        # Identità
        "node_id":       WORKER_ID,
        "pubkey":        NODE_PUBKEY,
        "tier":          NODE_TIER,
        "version":       NODE_VERSION,
        "endpoint":      NODE_PROFILE["endpoint"],
        # Capacità
        "capabilities":  NODE_PROFILE["capabilities"],
        "vram_gb":       VRAM_GB,
        "default_model": DEFAULT_MODEL,
        "ollama_url":    OLLAMA_URL,
        # Stato mesh
        "peers_active":  _peers_active(),
        "peers_known":   len(_peers),
        "status":        "active",
        "uptime_s":      _uptime(),
        # Diagnostica
        "running":       True,
    }


@app.get("/peers")
def get_peers():
    """
    Lista peer noti con stato corrente.
    Usato dal control-plane per PEX e discovery.
    """
    return {
        "node_id":     WORKER_ID,
        "peers_count": len(_peers),
        "peers": [
            {
                "endpoint":  p["endpoint"],
                "node_id":   p["node_id"],
                "pubkey":    p["pubkey"],
                "tier":      p["tier"],
                "status":    p["status"],
                "last_seen": p["last_seen"],
            }
            for p in _peers.values()
        ],
    }


@app.post("/peer/add")
async def add_peer(data: dict):
    """
    Registra un peer noto (chiamato da altri nodi o dal control-plane).
    Payload: { endpoint, node_id, pubkey, tier }
    """
    ep = data.get("endpoint", "").strip()
    if not ep or ep == NODE_PROFILE["endpoint"]:
        return {"ok": False, "reason": "invalid or self endpoint"}
    if ep not in _peers:
        _peers[ep] = {
            "endpoint":  ep,
            "node_id":   data.get("node_id", "unknown"),
            "pubkey":    data.get("pubkey", ""),
            "tier":      data.get("tier", "leaf"),
            "status":    "unknown",
            "last_seen": None,
        }
        threading.Thread(target=_ping_peer, args=(ep,), daemon=True).start()
    return {"ok": True, "peers_known": len(_peers)}


@app.get("/identity")
def get_identity():
    """Profilo pubblico immutabile del nodo."""
    return {
        "node_id":      WORKER_ID,
        "public_key":   NODE_PUBKEY,
        "tier":         NODE_TIER,
        "version":      NODE_VERSION,
        "capabilities": NODE_PROFILE["capabilities"],
        "endpoint":     NODE_PROFILE["endpoint"],
    }


@app.post("/verify")
async def verify_incoming(message: dict):
    """Verifica firma ECDSA di un messaggio peer."""
    return {
        "valid":   verify_message(message),
        "node_id": message.get("node_id"),
    }


@app.post("/execute")
async def execute_task(task: dict):
    task_id = task.get("task_id", "unknown")
    prompt  = task.get("prompt") or task.get("payload", {}).get("prompt") or f"Esegui task: {task_id}"
    model   = task.get("model") or task.get("payload", {}).get("model") or DEFAULT_MODEL
    print(f"[WORKER:{WORKER_ID[:12]}] execute task={task_id} model={model}")
    response_text = await ollama_generate(prompt, model)
    return {
        "worker":   WORKER_ID,
        "task_id":  task_id,
        "status":   "done",
        "model":    model,
        "response": response_text,
    }


@app.get("/ollama/health")
async def check_ollama():
    return await ollama_health()


@app.get("/ollama/models")
async def list_models():
    h = await ollama_health()
    return {"models": h.get("models", [])} if h["ok"] else {"error": h.get("error")}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=WORKER_PORT)
