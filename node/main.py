# node/main.py
# HyperSpace AGI v1.02 — Unified Node
# v1.02: middleware firma inter-nodo, /ollama/pull SSE, ollama-proxy, /memory
# fix: heartbeat try/except+retry, endpoint normalizzato, peer TTL configurabile
# feat: auto-discovery peer dal registry al boot se BOOT_PEERS vuoto

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

# ── IDENTITA' ──────────────────────────────────────────────
_identity    = generate_or_load_identity()
NODE_ID      = _identity["node_id"]
NODE_PUBKEY  = _identity["public_key"]
_private_key = _identity["_private_key"]

# ── CONFIG ───────────────────────────────────────────────
NODE_HOSTNAME        = os.getenv("NODE_HOSTNAME", "localhost")
NODE_PORT            = int(os.getenv("NODE_PORT", 8084))
OLLAMA_URL           = os.getenv("OLLAMA_URL", "http://ollama:11434")
DEFAULT_MODEL        = os.getenv("OLLAMA_MODEL", "phi3")
HEARTBEAT_EVERY      = int(os.getenv("HEARTBEAT_EVERY", 15))
PUBLIC_ENDPOINT      = os.getenv("PUBLIC_ENDPOINT", "").strip().rstrip("/")
BOOT_PEERS           = [p.strip().rstrip("/") for p in os.getenv("BOOT_PEERS", "").split(",") if p.strip()]
CONTROL_PLANE_URL    = os.getenv("CONTROL_PLANE_URL", "").strip().rstrip("/")
REGISTRY_URL         = os.getenv("REGISTRY_URL", "http://registry:8086").strip().rstrip("/")
REGISTRY_PUBLIC_URL  = os.getenv("REGISTRY_PUBLIC_URL", "https://sanctuary-mower-plated.ngrok-free.dev").strip().rstrip("/")
SIGN_REQUESTS        = os.getenv("SIGN_REQUESTS", "true").lower() == "true"
_FORCED_TIER         = os.getenv("NODE_TIER", "").strip().lower()

# Bug 4 fix: peer TTL configurabile via env (default 120s = 8 cicli heartbeat)
PEER_MAX_AGE_S       = int(os.getenv("PEER_MAX_AGE_S", "120"))

DATA_DIR = os.getenv("DATA_DIR", "/app/data")
os.makedirs(DATA_DIR, exist_ok=True)

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
    if _FORCED_TIER in ("hub", "root", "leaf"):
        return _FORCED_TIER
    root_score = min(uptime_s / 604800, 1.0) * 25 + 0.5 * 35 + reputation * 40
    if root_score >= 85.0: return "root"
    if vram_gb >= 4.0:     return "hub"
    return "leaf"

_vram_env      = float(os.getenv("VRAM_GB", "0.0"))
_vram_detected = detect_vram_gb()
VRAM_GB        = _vram_env if _vram_env > 0.0 else _vram_detected

NODE_CAPABILITIES = ["execute"]
if VRAM_GB > 0 or os.getenv("OLLAMA_URL"):
    NODE_CAPABILITIES.append("ollama")
NODE_CAPABILITIES.append("ollama-proxy")

# Bug 2 fix: normalizza endpoint con schema http:// fin dall'inizio
def _normalize_endpoint(ep: str) -> str:
    """Aggiunge http:// se l'endpoint non ha già uno schema."""
    ep = ep.strip().rstrip("/")
    if not ep:
        return ep
    if ep.startswith("http://") or ep.startswith("https://"):
        return ep
    return f"http://{ep}"

_raw_local = f"{NODE_HOSTNAME}:{NODE_PORT}"
if PUBLIC_ENDPOINT:
    NODE_ADVERTISED_ENDPOINT = _normalize_endpoint(PUBLIC_ENDPOINT)
else:
    NODE_ADVERTISED_ENDPOINT = _normalize_endpoint(_raw_local)

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
    ep  = _normalize_endpoint(info.get("endpoint", ""))
    if nid and nid != NODE_ID:
        _peers[nid] = {**info, "endpoint": ep, "last_seen": time.time(), "status": "active"}

def prune_stale_peers(max_age_s: int = None):
    """Bug 4 fix: usa PEER_MAX_AGE_S configurabile invece di 60s hardcoded."""
    age = max_age_s if max_age_s is not None else PEER_MAX_AGE_S
    now = time.time()
    for nid, p in list(_peers.items()):
        if now - p["last_seen"] > age:
            _peers[nid]["status"] = "stale"

# ── MEMORIA COLLETTIVA (lettura) ───────────────────────────
import json as _json
from pathlib import Path

_MEMORY_FILE = Path(DATA_DIR) / "memory.jsonl"

def _read_memory(limit: int = 50) -> list:
    if not _MEMORY_FILE.exists():
        return []
    lines = _MEMORY_FILE.read_text(encoding="utf-8").strip().splitlines()
    return [_json.loads(l) for l in lines[-limit:] if l.strip()]

# ── HELPERS ───────────────────────────────────────────────
def build_signed_payload(data: dict) -> dict:
    payload = {**data, "pubkey": NODE_PUBKEY, "node_id": NODE_ID}
    return sign_message(payload, _private_key)

def peer_to_url(endpoint: str) -> str:
    """Bug 2 fix: idempotente, usa _normalize_endpoint."""
    return _normalize_endpoint(endpoint)

def _signed_headers(body: bytes = b"") -> dict:
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

# ── REGISTRAZIONE ─────────────────────────────────────────
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
                print(f"[NODE:{NODE_ID[:10]}] registry /register HTTP {r.status_code}")
    except Exception as e:
        print(f"[NODE:{NODE_ID[:10]}] registry /register failed: {e}")

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


# ── AUTO-DISCOVERY DAL REGISTRY ──────────────────────────
async def _discover_peers_from_registry():
    """
    Se BOOT_PEERS è vuoto, scarica i nodi attivi dal registry pubblico
    e si annuncia a ciascuno. Questo permette a un nuovo utente di
    entrare nella mesh senza configurare BOOT_PEERS manualmente.
    """
    # Prova prima il registry locale, poi quello pubblico
    urls_to_try = []
    if REGISTRY_URL:
        urls_to_try.append(REGISTRY_URL)
    if REGISTRY_PUBLIC_URL and REGISTRY_PUBLIC_URL != REGISTRY_URL:
        urls_to_try.append(REGISTRY_PUBLIC_URL)

    discovered = []
    for base_url in urls_to_try:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.get(f"{base_url}/nodes/active")
                if r.status_code == 200:
                    data = r.json()
                    for n in data.get("nodes", []):
                        ep = _normalize_endpoint(n.get("public_address", ""))
                        nid = n.get("node_id", "")
                        if ep and nid and nid != NODE_ID:
                            discovered.append(ep)
                    if discovered:
                        print(f"[NODE:{NODE_ID[:10]}] auto-discovery: found {len(discovered)} peers from {base_url}")
                        break
        except Exception as e:
            print(f"[NODE:{NODE_ID[:10]}] auto-discovery failed from {base_url}: {e}")

    for ep in discovered:
        try:
            await announce_to_peer(ep)
            print(f"[NODE:{NODE_ID[:10]}] auto-discovery: announced to {ep}")
        except Exception as e:
            print(f"[NODE:{NODE_ID[:10]}] auto-discovery announce failed -> {ep}: {e}")


# ── PEER DISCOVERY ────────────────────────────────────────
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
        import json as _j
        body = _j.dumps(payload, sort_keys=True).encode()
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
    except Exception as e:
        print(f"[NODE:{NODE_ID[:10]}] announce failed -> {endpoint}: {e}")

# ── HEARTBEAT — Bug 1 fix ─────────────────────────────────
def _run_async_safe(coro_fn, label: str, *args, **kwargs):
    """Esegue una coroutine in un nuovo event loop con logging degli errori."""
    try:
        asyncio.run(coro_fn(*args, **kwargs))
    except Exception as e:
        print(f"[NODE:{NODE_ID[:10]}] [{label}] ERROR: {e}")

def heartbeat_loop():
    time.sleep(5)

    # Boot con retry (Bug 1 + Bug 3 lato nodo)
    async def _boot():
        for peer_endpoint in BOOT_PEERS:
            try:
                await announce_to_peer(peer_endpoint)
            except Exception as e:
                print(f"[NODE:{NODE_ID[:10]}] boot announce failed -> {peer_endpoint}: {e}")
        await register_to_registry()
        await register_to_control_plane()
        # Auto-discovery: se non ho BOOT_PEERS configurati, li trovo dal registry
        if not BOOT_PEERS:
            await _discover_peers_from_registry()

    boot_ok = False
    for attempt in range(1, 4):
        try:
            asyncio.run(_boot())
            boot_ok = True
            print(f"[NODE:{NODE_ID[:10]}] boot registration OK (attempt {attempt})")
            break
        except Exception as e:
            print(f"[NODE:{NODE_ID[:10]}] boot attempt {attempt}/3 failed: {e}")
            if attempt < 3:
                time.sleep(5)

    if not boot_ok:
        print(f"[NODE:{NODE_ID[:10]}] WARNING: boot registration failed after 3 attempts — will retry in heartbeat")

    while True:
        time.sleep(HEARTBEAT_EVERY)
        NODE_PROFILE["tier"] = calculate_tier(VRAM_GB, time.time() - _boot_time)
        prune_stale_peers()

        async def _hb():
            active = [p for p in _peers.values() if p["status"] == "active"]
            for peer in active:
                try:
                    await announce_to_peer(peer["endpoint"])
                except Exception as e:
                    print(f"[NODE:{NODE_ID[:10]}] hb announce failed -> {peer['endpoint']}: {e}")
            try:
                await register_to_registry()
            except Exception as e:
                print(f"[NODE:{NODE_ID[:10]}] hb registry failed: {e}")
            try:
                await register_to_control_plane()
            except Exception as e:
                print(f"[NODE:{NODE_ID[:10]}] hb control-plane failed: {e}")
            # Se non ho ancora peer attivi, ritenta auto-discovery
            if not active:
                try:
                    await _discover_peers_from_registry()
                except Exception as e:
                    print(f"[NODE:{NODE_ID[:10]}] hb auto-discovery failed: {e}")

        _run_async_safe(_hb, "heartbeat")

# ── STARTUP ───────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    t = threading.Thread(target=heartbeat_loop, daemon=True)
    t.start()
    print(f"[NODE:{NODE_ID[:10]}] started v1.02.0")
    print(f"[NODE:{NODE_ID[:10]}] tier={NODE_PROFILE['tier']} (forced={_FORCED_TIER or 'no'})")
    print(f"[NODE:{NODE_ID[:10]}] advertised={NODE_ADVERTISED_ENDPOINT}")
    print(f"[NODE:{NODE_ID[:10]}] vram_gb={VRAM_GB} (env={_vram_env} detected={_vram_detected})")
    print(f"[NODE:{NODE_ID[:10]}] boot_peers={BOOT_PEERS or 'none — will use registry auto-discovery'}")
    print(f"[NODE:{NODE_ID[:10]}] peer_max_age_s={PEER_MAX_AGE_S}")
    print(f"[NODE:{NODE_ID[:10]}] registry={REGISTRY_URL}")
    print(f"[NODE:{NODE_ID[:10]}] registry_public={REGISTRY_PUBLIC_URL}")
    print(f"[NODE:{NODE_ID[:10]}] ollama -> {OLLAMA_URL}")

# ── MIDDLEWARE firma ───────────────────────────────────────
SIGNED_PATHS = {"/announce", "/execute", "/peer/add", "/verify"}

@app.middleware("http")
async def inter_node_auth(request: Request, call_next):
    if SIGN_REQUESTS and request.method == "POST" and request.url.path in SIGNED_PATHS:
        body = await request.body()
        hdrs = dict(request.headers)
        if not verify_request_headers(hdrs, body):
            return Response(
                content='{"error":"invalid or missing node signature"}',
                status_code=401, media_type="application/json",
            )
        async def receive():
            return {"type": "http.request", "body": body}
        request._receive = receive
    return await call_next(request)

# ── ENDPOINTS ───────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "node_id": NODE_ID, "tier": NODE_PROFILE["tier"], "version": NODE_PROFILE["version"]}

@app.get("/status")
def status():
    prune_stale_peers()
    return {
        "node_id":        NODE_ID,
        "public_key":     NODE_PUBKEY,
        "tier":           NODE_PROFILE["tier"],
        "version":        NODE_PROFILE["version"],
        "endpoint":       NODE_ADVERTISED_ENDPOINT,
        "capabilities":   NODE_CAPABILITIES,
        "vram_gb":        VRAM_GB,
        "uptime_s":       int(time.time() - _boot_time),
        "peers_active":   len([p for p in _peers.values() if p["status"] == "active"]),
        "peers_total":    len(_peers),
        "memory_entries": len(_read_memory(9999)),
        "running":        True,
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

@app.get("/memory")
def get_memory(limit: int = 50):
    return {"node_id": NODE_ID, "entries": _read_memory(limit)}

@app.post("/memory/push")
async def receive_memory(payload: dict):
    entry = payload.get("entry", {})
    if entry and entry.get("node_id") != NODE_ID:
        entry["_received_from"] = payload.get("node_id", "unknown")
        with _MEMORY_FILE.open("a", encoding="utf-8") as f:
            f.write(_json.dumps(entry, ensure_ascii=False) + "\n")
    return {"ok": True}

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
    response_text = await ollama_generate(prompt, model)
    return {"node_id": NODE_ID, "task_id": task_id, "status": "done", "model": model, "response": response_text}

@app.get("/ollama/health")
async def check_ollama():
    return await ollama_health()

@app.get("/ollama/models")
async def list_models():
    h = await ollama_health()
    return {"models": h.get("models", [])} if h["ok"] else {"error": h.get("error")}

@app.post("/ollama/pull")
async def ollama_pull(body: dict):
    model = body.get("model", DEFAULT_MODEL)

    async def stream_pull():
        try:
            async with httpx.AsyncClient(timeout=600.0) as client:
                async with client.stream(
                    "POST", f"{OLLAMA_URL}/api/pull",
                    json={"name": model, "stream": True},
                ) as resp:
                    async for line in resp.aiter_lines():
                        if line.strip():
                            yield f"data: {line}\n\n"
            yield 'data: {"status":"done"}\n\n'
        except Exception as e:
            yield f'data: {{"error": "{e}"}}\n\n'

    return StreamingResponse(
        stream_pull(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
