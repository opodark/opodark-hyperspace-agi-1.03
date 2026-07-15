# node/ollama_proxy.py
# HyperSpace AGI v1.03 — Ollama Proxy
#
# Emula l'API Ollama (e ora anche l'API OpenAI-compatibile) su porta 11435.
# Non e' piu' pensato per essere chiamato direttamente da Open WebUI: il
# punto di ingresso e' node/main.py:/v1/chat/completions (autenticato,
# raggiungibile solo dal control-plane), che rigira qui internamente
# (localhost, stesso container). Questo file resta comunque raggiungibile
# sulla rete Docker per compatibilita' con le sue rotte Ollama-native
# preesistenti (/api/generate, /api/chat, ecc.), ma il percorso "ufficiale"
# per le chat della mesh e' /v1/chat/completions.
#
# Ogni richiesta viene:
#   1. Inoltrata al vero Ollama (OLLAMA_URL)
#   2. Loggata nel control-plane come "webui_interaction"
#   3. Appesa a data/memory.jsonl (memoria collettiva locale)
#   4. Propagata agli hub peer via /memory/push (se PUBLIC_ENDPOINT noto)

import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

OLLAMA_URL        = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434").rstrip("/")
CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "").rstrip("/")
NODE_TIER         = os.getenv("NODE_TIER", "leaf")
PUBLIC_ENDPOINT   = os.getenv("PUBLIC_ENDPOINT", "").rstrip("/")
PROXY_PORT        = int(os.getenv("PROXY_PORT", 11435))

DATA_DIR   = Path(os.getenv("DATA_DIR", "/app/data"))
MEMORY_FILE = DATA_DIR / "memory.jsonl"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# NODE_ID condiviso con node/main.py: entrambi i processi girano nello
# stesso container con lo stesso DATA_DIR, quindi generate_or_load_identity()
# qui non genera un nuovo keypair — carica quello gia' creato da main.py.
# Prima ollama-proxy usava un NODE_ID separato (env var mai valorizzata),
# risultando sempre "unknown" nei log e nelle entry di memoria.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
try:
    from shared.identity import generate_or_load_identity
    NODE_ID = generate_or_load_identity()["node_id"]
except Exception:
    NODE_ID = os.getenv("NODE_ID", "unknown")

app = FastAPI(title="HyperSpace Ollama Proxy", version="1.03.0")

# ── MEMORIA COLLETTIVA ────────────────────────────────────
def save_memory(entry: dict):
    """Appende un'interazione a memory.jsonl (una riga JSON per entry)."""
    with MEMORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def read_memory(limit: int = 100) -> list:
    """Legge le ultime `limit` righe di memoria."""
    if not MEMORY_FILE.exists():
        return []
    lines = MEMORY_FILE.read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(l) for l in lines[-limit:] if l.strip()]

async def log_to_control_plane(entry: dict):
    """Invia l'interazione al control-plane come log mesh_event."""
    if not CONTROL_PLANE_URL:
        return
    payload = {
        "type":       "webui_interaction",
        "summary":    f"[{NODE_ID[:10]}] {entry.get('model','?')}: {entry.get('prompt','')[:80]}",
        "detail":     json.dumps(entry, ensure_ascii=False),
        "sourceNode": NODE_ID[:12],
        "targetNode": "webui",
        "status":     "success",
        "traceId":    entry.get("interaction_id", "")[:8],
    }
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            await client.post(f"{CONTROL_PLANE_URL}/logs/add", json=payload)
    except Exception:
        pass  # non bloccare il proxy se il CP è giù

async def propagate_to_peers(entry: dict):
    """Invia la memory entry agli hub peer noti tramite /memory/push."""
    boot_peers = [
        p.strip() for p in os.getenv("BOOT_PEERS", "").split(",") if p.strip()
    ]
    if not boot_peers:
        return
    payload = {"node_id": NODE_ID, "entry": entry}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for peer in boot_peers:
            try:
                await client.post(f"{peer.rstrip('/')}/memory/push", json=payload)
            except Exception:
                pass

# ── INTERCETTA E LOGGA ───────────────────────────────────
async def _record_interaction(
    prompt: str, response_text: str, model: str,
    source: str = "webui", duration_ms: int = 0
):
    entry = {
        "interaction_id": str(uuid.uuid4()),
        "ts":             datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "node_id":        NODE_ID,
        "node_tier":      NODE_TIER,
        "source":         source,
        "model":          model,
        "prompt":         prompt,
        "response":       response_text[:2000],  # tronca a 2KB per memoria
        "duration_ms":    duration_ms,
    }
    save_memory(entry)
    await asyncio.gather(
        log_to_control_plane(entry),
        propagate_to_peers(entry),
        return_exceptions=True,
    )

# ── PROXY ROUTES — Ollama-native ─────────────────────────────

@app.get("/api/tags")
async def proxy_tags():
    """Lista modelli — pass-through diretto."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{OLLAMA_URL}/api/tags")
            return Response(content=r.content, media_type="application/json")
    except Exception as e:
        return Response(content=json.dumps({"error": str(e)}),
                        status_code=503, media_type="application/json")

@app.get("/api/version")
async def proxy_version():
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{OLLAMA_URL}/api/version")
            return Response(content=r.content, media_type="application/json")
    except Exception:
        return Response(content=json.dumps({"version": "0.0.0-hyperspace"}),
                        media_type="application/json")

@app.post("/api/generate")
async def proxy_generate(request: Request):
    """Genera testo — intercetta prompt e risposta."""
    body = await request.json()
    prompt = body.get("prompt", "")
    model  = body.get("model", "")
    stream = body.get("stream", True)
    t0 = time.time()

    if stream:
        async def stream_and_log():
            full_response = ""
            try:
                async with httpx.AsyncClient(timeout=300.0) as client:
                    async with client.stream(
                        "POST", f"{OLLAMA_URL}/api/generate", json=body
                    ) as resp:
                        async for line in resp.aiter_lines():
                            if line.strip():
                                yield line + "\n"
                                try:
                                    chunk = json.loads(line)
                                    full_response += chunk.get("response", "")
                                    if chunk.get("done"):
                                        dur = int((time.time() - t0) * 1000)
                                        await _record_interaction(
                                            prompt, full_response, model,
                                            duration_ms=dur
                                        )
                                except Exception:
                                    pass
            except Exception as e:
                yield json.dumps({"error": str(e), "done": True}) + "\n"

        return StreamingResponse(stream_and_log(), media_type="application/x-ndjson")
    else:
        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                r = await client.post(f"{OLLAMA_URL}/api/generate", json=body)
                data = r.json()
                dur  = int((time.time() - t0) * 1000)
                await _record_interaction(
                    prompt, data.get("response", ""), model, duration_ms=dur
                )
                return Response(content=r.content, media_type="application/json")
        except Exception as e:
            return Response(content=json.dumps({"error": str(e)}),
                            status_code=503, media_type="application/json")

@app.post("/api/chat")
async def proxy_chat(request: Request):
    """Chat completions (formato messages[]) — intercetta e logga."""
    body    = await request.json()
    model   = body.get("model", "")
    messages = body.get("messages", [])
    # Estrai testo leggibile dai messages per la memoria
    prompt_summary = " | ".join(
        f"{m.get('role','?')}: {str(m.get('content',''))[:120]}"
        for m in messages[-3:]  # ultimi 3 turni
    )
    stream = body.get("stream", True)
    t0 = time.time()

    if stream:
        async def stream_chat_and_log():
            full_response = ""
            try:
                async with httpx.AsyncClient(timeout=300.0) as client:
                    async with client.stream(
                        "POST", f"{OLLAMA_URL}/api/chat", json=body
                    ) as resp:
                        async for line in resp.aiter_lines():
                            if line.strip():
                                yield line + "\n"
                                try:
                                    chunk = json.loads(line)
                                    msg = chunk.get("message", {})
                                    full_response += msg.get("content", "")
                                    if chunk.get("done"):
                                        dur = int((time.time() - t0) * 1000)
                                        await _record_interaction(
                                            prompt_summary, full_response,
                                            model, duration_ms=dur
                                        )
                                except Exception:
                                    pass
            except Exception as e:
                yield json.dumps({"error": str(e), "done": True}) + "\n"

        return StreamingResponse(stream_chat_and_log(), media_type="application/x-ndjson")
    else:
        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                r = await client.post(f"{OLLAMA_URL}/api/chat", json=body)
                data = r.json()
                dur  = int((time.time() - t0) * 1000)
                content = data.get("message", {}).get("content", "")
                await _record_interaction(
                    prompt_summary, content, model, duration_ms=dur
                )
                return Response(content=r.content, media_type="application/json")
        except Exception as e:
            return Response(content=json.dumps({"error": str(e)}),
                            status_code=503, media_type="application/json")

# ── PROXY ROUTE — OpenAI-compatibile ─────────────────────────
# Punto di ingresso "ufficiale" per le chat instradate dalla mesh: chiamato
# da node/main.py:/v1/chat/completions dopo che questo ha autenticato la
# richiesta del control-plane. Ultimo hop prima di Ollama vero.
@app.post("/v1/chat/completions")
async def proxy_openai_chat(request: Request):
    body = await request.json()
    model    = body.get("model", "")
    messages = body.get("messages", [])
    prompt_summary = " | ".join(
        f"{m.get('role','?')}: {str(m.get('content',''))[:120]}"
        for m in messages[-3:]
    )
    stream = body.get("stream", False)
    t0 = time.time()

    if stream:
        async def stream_and_log():
            full_response = ""
            try:
                async with httpx.AsyncClient(timeout=300.0) as client:
                    async with client.stream(
                        "POST", f"{OLLAMA_URL}/v1/chat/completions", json=body
                    ) as resp:
                        async for raw_chunk in resp.aiter_bytes():
                            if not raw_chunk:
                                continue
                            # Passthrough byte-per-byte: non tocchiamo mai il
                            # framing SSE (righe vuote comprese) inoltrato al
                            # client, per non romperlo.
                            yield raw_chunk
                            # Parsing "best effort" SOLO per il logging: non
                            # deve mai poter interrompere lo streaming.
                            try:
                                for line in raw_chunk.decode("utf-8", errors="ignore").splitlines():
                                    if line.startswith("data: ") and "[DONE]" not in line:
                                        chunk = json.loads(line[len("data: "):])
                                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                                        full_response += delta.get("content", "")
                            except Exception:
                                pass
            except Exception as e:
                yield f'data: {{"error": "{e}"}}\n\n'.encode()
            finally:
                dur = int((time.time() - t0) * 1000)
                await _record_interaction(prompt_summary, full_response, model, duration_ms=dur)

        return StreamingResponse(stream_and_log(), media_type="text/event-stream")
    else:
        try:
            async with httpx.AsyncClient(timeout=300.0) as client:
                r = await client.post(f"{OLLAMA_URL}/v1/chat/completions", json=body)
                dur = int((time.time() - t0) * 1000)
                try:
                    data    = r.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                except Exception:
                    content = ""
                await _record_interaction(prompt_summary, content, model, duration_ms=dur)
                return Response(content=r.content, status_code=r.status_code,
                                media_type=r.headers.get("content-type", "application/json"))
        except Exception as e:
            return Response(content=json.dumps({"error": {"message": str(e), "type": "server_error"}}),
                            status_code=503, media_type="application/json")

# Tutti gli altri endpoint Ollama: pass-through generico
@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_generic(path: str, request: Request):
    method = request.method
    body   = await request.body()
    url    = f"{OLLAMA_URL}/api/{path}"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.request(
                method, url, content=body,
                headers={"Content-Type": request.headers.get("content-type", "application/json")}
            )
            return Response(content=r.content, status_code=r.status_code,
                            media_type=r.headers.get("content-type", "application/json"))
    except Exception as e:
        return Response(content=json.dumps({"error": str(e)}),
                        status_code=503, media_type="application/json")

# ── ENDPOINTS MEMORIA (letti da altri nodi / dashboard) ─────────
@app.get("/memory")
def get_memory(limit: int = 50):
    """Ultime `limit` interazioni della memoria collettiva locale."""
    return {"node_id": NODE_ID, "entries": read_memory(limit)}

@app.post("/memory/push")
async def receive_memory(payload: dict):
    """Riceve una memory entry da un peer e la salva localmente."""
    entry = payload.get("entry", {})
    if entry and entry.get("node_id") != NODE_ID:
        entry["_received_from"] = payload.get("node_id", "unknown")
        save_memory(entry)
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PROXY_PORT)
