"""
HyperSpace-AGI — Infra-UI SSE Bridge  (infra-ui/server.py)
===========================================================
Runs on :8099. Acts as:
  1. SSE gateway  -> GET /events         streams task/memory/heartbeat events
  2. REST proxy   -> GET /proxy/*        forwards to CP (:8085) & Registry (:8086)
  3. Static file  -> GET /               serves dashboard.html

FIX: CP is Flask on :8085 (not :8000). Endpoints:
  /mesh/nodes   (not /nodes/active)
  /tasks        (not /tasks/recent)
  /memory/stats OK
  /logs         used for task feed

Start:
    pip install fastapi uvicorn httpx
    uvicorn server:app --host 0.0.0.0 --port 8099 --reload
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import AsyncGenerator

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

# ── config ────────────────────────────────────────────────────────────────────
# CP is Flask on 8085 (see control-plane/main.py: app.run port=8085)
CP_URL        = os.getenv("CP_URL",        "http://localhost:8085")
REGISTRY_URL  = os.getenv("REGISTRY_URL",  "http://localhost:8086")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "4"))
DASHBOARD_HTML = os.path.join(os.path.dirname(__file__), "dashboard.html")

# ── app ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="HyperSpace-AGI Infra-UI Bridge")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── event bus ─────────────────────────────────────────────────────────────────
_event_queues: list[asyncio.Queue] = []

def _broadcast(event_type: str, data: dict) -> None:
    payload = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    for q in list(_event_queues):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass

async def _event_generator(queue: asyncio.Queue) -> AsyncGenerator[str, None]:
    try:
        while True:
            item = await asyncio.wait_for(queue.get(), timeout=20)
            yield item
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    finally:
        if queue in _event_queues:
            _event_queues.remove(queue)

# ── known node-ids seen from /mesh/nodes, used to build link edges ──────────
_known_node_ids: list[str] = []
_last_log_id: str = ""   # dedup: don't re-broadcast old log entries

# ── background poller ──────────────────────────────────────────────────────
async def _poller() -> None:
    global _last_log_id
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            await asyncio.sleep(POLL_INTERVAL)

            # 1. /mesh/nodes -> heartbeat per nodo attivo
            try:
                r = await client.get(f"{CP_URL}/mesh/nodes")
                if r.status_code == 200:
                    nodes = r.json()
                    if isinstance(nodes, list):
                        _known_node_ids.clear()
                        for node in nodes:
                            nid = node.get("node_id") or node.get("id", "")
                            if not nid:
                                continue
                            _known_node_ids.append(nid)
                            if node.get("status") == "active":
                                _broadcast("heartbeat", {
                                    "node_id":   nid,
                                    "ping":      node.get("ping", 0),
                                    "last_seen": node.get("last_seen"),
                                    "model":     node.get("model", ""),
                                    "tier":      node.get("tier", "leaf"),
                                })
            except Exception:
                pass

            # 2. /memory/stats -> memory_stats
            try:
                r = await client.get(f"{CP_URL}/memory/stats")
                if r.status_code == 200:
                    _broadcast("memory_stats", r.json())
            except Exception:
                pass

            # 3. /logs?type=inter_node_message&per_page=20 -> task events
            # We diff against _last_log_id to avoid re-broadcasting
            try:
                r = await client.get(
                    f"{CP_URL}/logs",
                    params={"type": "inter_node_message", "per_page": 20, "page": 1}
                )
                if r.status_code == 200:
                    data  = r.json()
                    logs  = data.get("logs", [])
                    new_entries = []
                    for log in logs:
                        lid = log.get("id", "")
                        if lid == _last_log_id:
                            break
                        new_entries.append(log)
                    if new_entries:
                        _last_log_id = new_entries[0].get("id", _last_log_id)
                    for log in reversed(new_entries):
                        src = log.get("sourceNode", "cp")
                        tgt = log.get("targetNode", "")
                        if src and tgt:
                            _broadcast("task", {
                                "from":  src,
                                "to":    tgt,
                                "type":  "task",
                                "label": log.get("summary", "task"),
                            })
            except Exception:
                pass

            # 4. /logs?type=memory_sync -> memory_sync events
            try:
                r = await client.get(
                    f"{CP_URL}/logs",
                    params={"type": "memory_sync", "per_page": 5, "page": 1}
                )
                if r.status_code == 200:
                    for log in r.json().get("logs", [])[:2]:
                        src = log.get("sourceNode", "cp")
                        _broadcast("memory_sync", {
                            "from":    src,
                            "to":      log.get("targetNode", "n1"),
                            "entries": 0,
                            "label":   log.get("summary", "memory sync"),
                        })
            except Exception:
                pass

            # 5. /logs?type=dream -> dream events (shown in log panel)
            try:
                r = await client.get(
                    f"{CP_URL}/logs",
                    params={"type": "dream", "per_page": 3, "page": 1}
                )
                if r.status_code == 200:
                    for log in r.json().get("logs", [])[:1]:
                        _broadcast("dream", {
                            "node":  log.get("sourceNode", "?"),
                            "label": log.get("summary", ""),
                        })
            except Exception:
                pass

            # 6. /logs?type=node_chat -> chat events
            try:
                r = await client.get(
                    f"{CP_URL}/logs",
                    params={"type": "node_chat", "per_page": 4, "page": 1}
                )
                if r.status_code == 200:
                    for log in r.json().get("logs", [])[:2]:
                        src = log.get("sourceNode", "")
                        tgt = log.get("targetNode", "")
                        if src and tgt:
                            _broadcast("task", {
                                "from":  src,
                                "to":    tgt,
                                "type":  "chat",
                                "label": log.get("summary", "chat"),
                            })
            except Exception:
                pass


@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(_poller())


# ── routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def serve_dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_HTML, media_type="text/html")


@app.get("/events")
async def sse_events(request: Request) -> StreamingResponse:
    q: asyncio.Queue = asyncio.Queue(maxsize=256)
    _event_queues.append(q)
    # send immediate connect confirmation
    async def _gen():
        yield f"event: connected\ndata: {{\"ok\": true}}\n\n"
        async for item in _event_generator(q):
            yield item
    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.api_route("/proxy/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request) -> StreamingResponse:
    if path.startswith("registry/"):
        target = f"{REGISTRY_URL}/{path[len('registry/'):]}"
    else:
        target = f"{CP_URL}/{path}"
    async with httpx.AsyncClient(timeout=8) as client:
        body = await request.body()
        resp = await client.request(
            method=request.method, url=target,
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            content=body, params=dict(request.query_params),
        )
    return StreamingResponse(
        iter([resp.content]), status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "application/json"),
    )


# ── push endpoints (CP → bridge, optional) ────────────────────────────────────
@app.post("/push/task")
async def push_task(request: Request) -> dict:
    data = await request.json()
    _broadcast("task", data)
    return {"ok": True}


@app.post("/push/memory_sync")
async def push_memory_sync(request: Request) -> dict:
    data = await request.json()
    _broadcast("memory_sync", data)
    return {"ok": True}


@app.get("/status")
async def bridge_status() -> dict:
    return {
        "ok":            True,
        "cp_url":        CP_URL,
        "registry_url":  REGISTRY_URL,
        "clients":       len(_event_queues),
        "known_nodes":   _known_node_ids,
    }
