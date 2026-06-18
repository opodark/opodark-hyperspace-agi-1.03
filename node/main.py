# node/main.py
# HyperSpace AGI v1.02 — Unified Node
# v1.02: middleware firma inter-nodo, /ollama/pull SSE, version bump

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
import asyncio
import httpx
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.identity import (
    generate_or_load_identity,
    sign_message,
    verify_message,
    make_request_headers,
    verify_request_headers,
)

app = FastAPI()

# ── IDENTITA' ─────────────────────────────────────────────
_identity    = generate_or_load_identity()
NODE_ID      = _identity["node_id"]
NODE_PUBKEY  = _identity["public_key"]
_private_key = _identity["_private_key"]

# ── CONFIG ────────────────────────────────────────────────
NODE_HOSTNAME       = os.getenv("NODE_HOSTNAME", "localhost")
NODE_PORT           = int(os.getenv("NODE_PORT", 8084))
OLLAMA_URL          = os.getenv("OLLAMA_URL", "http://ollama:11434")
DEFAULT_MODEL       = os.getenv("OLLAMA_MODEL", "phi3")
HEARTBEAT_EVERY     = int(os.getenv("HEARTBEAT_EVERY", 15))
PUBLIC_ENDPOINT     = os.getenv("PUBLIC_ENDPOINT", "").strip().rstrip("/")
BOOT_PEERS          = [p.strip().rstrip("/") for p in os.getenv("BOOT_PEERS", "").split(",") if p.strip()]
CONTROL_PLANE_URL   = os.getenv("CONTROL_PLANE_URL", "").strip().rstrip("/")
REGISTRY_URL        = os.getenv("REGISTRY_URL", "http://registry:8086").strip().rstrip("/")
SIGN_REQUESTS       = os.getenv("SIGN_REQUESTS", "true").lower() == "true"

_boot_time = time.time()

# ── TIER ──────────────────────────────────────────────────
def detect_vram_gb() -> float:
    try:
        import subprocess
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits"],
            timeout=3, stderr=subprocess.DEVNULL
        ).decode().strip()
        return round(float(out.split("\n")[0]) / 1024, 1)
    except Exception:
        return 0.0

def calculate_tier(vram_gb: float, uptime_s: float, reputation: float = 0.5) -> str:
    root_score = min(uptime_s / 604800, 1.0) * 25 + 0.5 * 35 + reputation * 40
    if root_score >= 85.0: return "root"
    if vram_gb >= 4.0:     return "hub"
    return "leaf"

VRAM_GB = detect_vram_gb()
NODE_CAPABILITIES = ["execute"]
if VRAM_GB > 0 or os.getenv("OLLAMA_URL"):
    NODE_CAPABILITIES.append("ollama")

_local_endpoint          = f"{NODE_HOSTNAME}:{NODE_PORT}"
NODE_ADVERTISED_ENDPOINT = PUBLIC_ENDPOINT if PUBLIC_ENDPOINT else _local_endpoint

NODE_PROFILE = {
    "node_id":      NODE_ID,
    "pubkey":       NODE_PUBKEY,
    "tier":         calculate_tier(VRAM_GB, 0),
    "endpoint":     NODE_ADVERTISED_ENDPOINT,
    "capabilities": NODE_CAPABILITIES,
    "vram_gb":      VRAM_GB,
    "version":      "1.02.0",
}

# ── PEER REGISTRY ─────────────────────────────────────────
_peers: dict = {}

def register_peer(info: dict):
    nid = info.get("node_id")
    if nid and nid != NODE_ID:
        _peers[nid] = {**info, "last_seen": time.time(), "status": "active"}

def prune_stale_peers(max_age_s: int = 60):
    now = time.time()
    for nid, p in list(_peers.items()):
        if now - p["last_seen"] > max_age_s:
            _peers[nid]["status"] = "stale"

# ── HELPERS ───────────────────────────────────────────────
def build_signed_payload(data: dict) -> dict:
    payload = {**data, "pubkey": NODE_PUBKEY, "node_id": NODE_ID}
    return sign_message(payload, _private_key)

def peer_to_url(endpoint: str) -> str:
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint.rstrip("/")
    return f"http://{endpoint}"

def _signed_headers(body: bytes = b"") -> dict:
    """Genera header di firma per richieste uscenti verso altri nodi."""
    if not SIGN_REQUESTS:
        return {}
    return make_request_headers(NODE_ID, NODE_PUBKEY, _private_key, body)

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
            return {"ok": True, "models": [m["name"] for m in r.json().get("models", [])]}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ── REGISTRAZIONE AL REGISTRY ─────────────────────────────
async def register_to_registry():
    payload = {
        "node_id":        NODE_ID,
        "public_address": NODE_ADVERTISED_ENDPOINT,
        "role":           NODE_PROFILE["tier"],
        "metadata": {
            "version":      NODE_PROFILE["version"],
            "tier":         NODE_PROFILE["tier"],
            "capabilities": ",".join(NODE_CAPABILITIES),
            "vram_gb":      str(VRAM_GB),
            "uptime_s":     str(int(time.time() - _boot_time)),
            "public_key":   NODE_PUBKEY[:32],
        }
    }
    body = str(payload).encode()
    hdrs = _signed_headers(body)
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.post(f"{REGISTRY_URL}/register", json=payload, headers=hdrs)
            if r.status_code in (200, 201):
                print(f"[NODE:{NODE_ID[:10]}] registered to registry OK")
            else:
                print(f"[NODE:{NODE_ID[:10]}] registry /register HTTP {r.status_code}: {r.text[:120]}")
    except Exception as e:
        print(f"[NODE:{NODE_ID[:10]}] registry /register failed: {e}")

# ── REGISTRAZIONE AL CONTROL-PLANE ────────────────────────
async def register_to_control_plane():
    if not CONTROL_PLANE_URL:
        return
    payload = {
        "node_id":      NODE_ID,
        "public_key":   NODE_PUBKEY,
        "tier":         NODE_PROFILE["tier"],
        "endpoint":     NODE_ADVERTISED_ENDPOINT,
        "capabilities": NODE_PROFILE["capabilities"],
        "vram_gb":      VRAM_GB,
        "version":      NODE_PROFILE["version"],
        "uptime_s":     int(time.time() - _boot_time),
        "peers_active": len([p for p in _peers.values() if p["status"] == "active"]),
    }
    body = str(payload).encode()
    hdrs = _signed_headers(body)
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.post(f"{CONTROL_PLANE_URL}/mesh/announce", json=payload, headers=hdrs)
            if r.status_code == 200:
                print(f"[NODE:{NODE_ID[:10]}] registered to control-plane OK")
            else:
                print(f"[NODE:{NODE_ID[:10]}] control-plane announce HTTP {r.status_code}")
    except Exception as e:
        print(f"[NODE:{NODE_ID[:10]}] control-plane announce failed: {e}")

# ── PEER DISCOVERY & HEARTBEAT ────────────────────────────
async def announce_to_peer(endpoint: str):
    base_url = peer_to_url(endpoint)
    try:
        payload = build_signed_payload({
            "type":         "NODE_ANNOUNCE",
            "endpoint":     NODE_ADVERTISED_ENDPOINT,
            "tier":         NODE_PROFILE["tier"],
            "capabilities": NODE_PROFILE["capabilities"],
            "version":      NODE_PROFILE["version"],
            "timestamp":    time.time(),
        })
        import json as _json
        body = _json.dumps(payload, sort_keys=True).encode()
        hdrs = _signed_headers(body)
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(f"{base_url}/announce", json=payload, headers=hdrs)
            if r.status_code == 200:
                data = r.json()
                for peer in data.get("peers", []):
                    register_peer(peer)
                register_peer({
                    "node_id":      data.get("node_id"),
                    "pubkey":       data.get("pubkey", ""),
                    "endpoint":     endpoint,
                    "tier":         data.get("tier", "leaf"),
                    "capabilities": data.get("capabilities", []),
                })
                print(f"[NODE:{NODE_ID[:10]}] announce ok -> {endpoint}")
    except Exception as e:
        print(f"[NODE:{NODE_ID[:10]}] announce failed -> {endpoint}: {e}")

def heartbeat_loop():
    time.sleep(5)

    async def _boot():
        for peer_endpoint in BOOT_PEERS:
            await announce_to_peer(peer_endpoint)
        await register_to_registry()
        await register_to_control_plane()

    asyncio.run(_boot())

    while True:
        time.sleep(HEARTBEAT_EVERY)
        NODE_PROFILE["tier"] = calculate_tier(VRAM_GB, time.time() - _boot_time)
        prune_stale_peers()

        async def _hb():
            active = [p for p in _peers.values() if p["status"] == "active"]
            for peer in active:
                await announce_to_peer(peer["endpoint"])
            await register_to_registry()
            await register_to_control_plane()

        asyncio.run(_hb())

# ── STARTUP ───────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()
    print(f"[NODE:{NODE_ID[:10]}] started v1.02.0")
    print(f"[NODE:{NODE_ID[:10]}] tier={NODE_PROFILE['tier']}")
    print(f"[NODE:{NODE_ID[:10]}] advertised={NODE_ADVERTISED_ENDPOINT}")
    print(f"[NODE:{NODE_ID[:10]}] sign_requests={SIGN_REQUESTS}")
    if BOOT_PEERS:
        print(f"[NODE:{NODE_ID[:10]}] boot_peers={BOOT_PEERS}")

# ── MIDDLEWARE: verifica firma sulle chiamate inter-nodo ───
# Applica solo se SIGN_REQUESTS=true. Le rotte pubbliche (/health, /status, /peers)
# non richiedono firma. Le rotte sensibili (/announce, /execute, /peer/add)
# verificano gli header X-Node-*.
SIGNED_PATHS = {"/announce", "/execute", "/peer/add", "/verify"}

@app.middleware("http")
async def inter_node_auth(request: Request, call_next):
    if SIGN_REQUESTS and request.method == "POST" and request.url.path in SIGNED_PATHS:
        body = await request.body()
        hdrs = dict(request.headers)
        if not verify_request_headers(hdrs, body):
            return Response(
                content='{"error":"invalid or missing node signature"}',
                status_code=401,
                media_type="application/json",
            )
        # Re-inject body for downstream handler
        async def receive():
            return {"type": "http.request", "body": body}
        request._receive = receive
    return await call_next(request)

# ── ENDPOINTS ─────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "node_id": NODE_ID, "tier": NODE_PROFILE["tier"], "version": NODE_PROFILE["version"]}

@app.get("/identity")
def get_identity():
    return {
        "node_id":      NODE_ID,
        "public_key":   NODE_PUBKEY,
        "tier":         NODE_PROFILE["tier"],
        "version":      NODE_PROFILE["version"],
        "capabilities": NODE_PROFILE["capabilities"],
        "endpoint":     NODE_ADVERTISED_ENDPOINT,
        "vram_gb":      VRAM_GB,
    }

@app.get("/status")
def status():
    prune_stale_peers()
    return {
        "node_id":      NODE_ID,
        "public_key":   NODE_PUBKEY,
        "tier":         NODE_PROFILE["tier"],
        "version":      NODE_PROFILE["version"],
        "endpoint":     NODE_ADVERTISED_ENDPOINT,
        "capabilities": NODE_PROFILE["capabilities"],
        "vram_gb":      VRAM_GB,
        "uptime_s":     int(time.time() - _boot_time),
        "peers_active": len([p for p in _peers.values() if p["status"] == "active"]),
        "peers_total":  len(_peers),
        "running":      True,
    }

@app.get("/peers")
def get_peers():
    prune_stale_peers()
    return {
        "node_id": NODE_ID,
        "pubkey":  NODE_PUBKEY,
        "tier":    NODE_PROFILE["tier"],
        "peers": [
            {
                "node_id":      p["node_id"],
                "endpoint":     p["endpoint"],
                "tier":         p.get("tier", "leaf"),
                "capabilities": p.get("capabilities", []),
                "status":       p["status"],
            }
            for p in _peers.values()
        ]
    }

@app.post("/announce")
async def announce(message: dict):
    valid = verify_message(message)
    if not valid:
        return {"error": "invalid signature", "accepted": False}
    register_peer({
        "node_id":      message.get("node_id"),
        "pubkey":       message.get("pubkey", ""),
        "endpoint":     message.get("endpoint", ""),
        "tier":         message.get("tier", "leaf"),
        "capabilities": message.get("capabilities", []),
    })
    prune_stale_peers()
    return {
        "accepted":     True,
        "node_id":      NODE_ID,
        "pubkey":       NODE_PUBKEY,
        "tier":         NODE_PROFILE["tier"],
        "capabilities": NODE_PROFILE["capabilities"],
        "peers": [
            {"node_id": p["node_id"], "endpoint": p["endpoint"], "tier": p.get("tier", "leaf")}
            for p in _peers.values() if p["status"] == "active"
        ]
    }

@app.post("/verify")
async def verify_incoming(message: dict):
    valid = verify_message(message)
    return {"valid": valid, "node_id": message.get("node_id")}

@app.post("/execute")
async def execute_task(task: dict):
    task_id = task.get("task_id", "unknown")
    prompt  = task.get("prompt") or task.get("payload", {}).get("prompt") or f"Esegui task: {task_id}"
    model   = task.get("model") or task.get("payload", {}).get("model") or DEFAULT_MODEL
    print(f"[NODE:{NODE_ID[:10]}] execute task={task_id} model={model}")
    response_text = await ollama_generate(prompt, model)
    return {"node_id": NODE_ID, "task_id": task_id, "status": "done", "model": model, "response": response_text}

@app.get("/ollama/health")
async def check_ollama():
    return await ollama_health()

@app.get("/ollama/models")
async def list_models():
    h = await ollama_health()
    return {"models": h.get("models", [])} if h["ok"] else {"error": h.get("error")}

# ── v1.02: /ollama/pull — stream SSE progress da Ollama ───
@app.post("/ollama/pull")
async def ollama_pull(body: dict):
    """Fa pull di un modello Ollama e streama il progresso come SSE.
    Body: { "model": "phi3" }
    """
    model = body.get("model", DEFAULT_MODEL)

    async def stream_pull():
        try:
            async with httpx.AsyncClient(timeout=600.0) as client:
                async with client.stream(
                    "POST",
                    f"{OLLAMA_URL}/api/pull",
                    json={"name": model, "stream": True},
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.strip():
                            yield f"data: {line}\n\n"
            yield "data: {\"status\":\"done\"}\n\n"
        except Exception as e:
            yield f"data: {{\"error\": \"{e}\"}}\n\n"

    return StreamingResponse(
        stream_pull(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
