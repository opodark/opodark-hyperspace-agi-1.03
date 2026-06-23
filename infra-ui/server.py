"""
HyperSpace-AGI — Infra-UI SSE Bridge  (infra-ui/server.py)
===========================================================
Runs on :8099. Acts as:
  1. SSE gateway  → GET /events         streams task/memory/heartbeat events to the dashboard
  2. REST proxy   → GET /proxy/*        forwards requests to CP (:8000) & Registry (:8086)
  3. Static file  → GET /              serves dashboard.html

Start:
    pip install fastapi uvicorn httpx
    uvicorn server:app --host 0.0.0.0 --port 8099 --reload
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from typing import AsyncGenerator

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ── config ────────────────────────────────────────────────────────────────────
CP_URL       = os.getenv("CP_URL",       "http://localhost:8000")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:8086")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "4"))   # seconds between heartbeat polls
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
# In-process queue; workers push SSE-formatted strings, /events drains them.
_event_queues: list[asyncio.Queue] = []


def _broadcast(event_type: str, data: dict) -> None:
    """Push an SSE event to all connected dashboard clients."""
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
        _event_queues.remove(queue)


# ── background poller ─────────────────────────────────────────────────────────
async def _poller() -> None:
    """
    Every POLL_INTERVAL seconds:
      - GET /nodes/active  → emit heartbeat events for each live node
      - GET /memory/stats  → emit a memory_stats event
    Also forwards any /tasks/log endpoint if available.
    """
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            await asyncio.sleep(POLL_INTERVAL)

            # ── active nodes / heartbeats ──────────────────────────────────
            try:
                r = await client.get(f"{CP_URL}/nodes/active")
                if r.status_code == 200:
                    nodes = r.json()
                    if isinstance(nodes, list):
                        for node in nodes:
                            nid = node.get("node_id") or node.get("id", "unknown")
                            _broadcast("heartbeat", {
                                "node_id": nid,
                                "ping": node.get("ping", 0),
                                "last_seen": node.get("last_seen"),
                            })
            except Exception:
                pass

            # ── memory stats ──────────────────────────────────────────────
            try:
                r = await client.get(f"{CP_URL}/memory/stats")
                if r.status_code == 200:
                    stats = r.json()
                    _broadcast("memory_stats", stats)
            except Exception:
                pass

            # ── task log (if CP exposes it) ────────────────────────────────
            try:
                r = await client.get(f"{CP_URL}/tasks/recent", params={"limit": 10})
                if r.status_code == 200:
                    tasks = r.json()
                    if isinstance(tasks, list):
                        for t in tasks:
                            _broadcast("task", {
                                "from": t.get("from_node", "cp"),
                                "to":   t.get("to_node",   "n1"),
                                "type": "task",
                                "label": t.get("label", "task"),
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
    q: asyncio.Queue = asyncio.Queue(maxsize=128)
    _event_queues.append(q)
    return StreamingResponse(
        _event_generator(q),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.api_route("/proxy/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request) -> StreamingResponse:
    """
    Transparent proxy:
      /proxy/memory/*   → CP_URL
      /proxy/nodes/*    → CP_URL
      /proxy/tasks/*    → CP_URL
      /proxy/registry/* → REGISTRY_URL
    """
    if path.startswith("registry/"):
        target = f"{REGISTRY_URL}/{path[len('registry/'):]}"
    else:
        target = f"{CP_URL}/{path}"

    async with httpx.AsyncClient(timeout=8) as client:
        body = await request.body()
        resp = await client.request(
            method=request.method,
            url=target,
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            content=body,
            params=dict(request.query_params),
        )
    return StreamingResponse(
        iter([resp.content]),
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "application/json"),
    )


# ── external push endpoint (CP → bridge) ─────────────────────────────────────
@app.post("/push/task")
async def push_task(request: Request) -> dict:
    """
    Control-plane can POST here to push a task event directly into the SSE stream.
    Body: {"from": "cp", "to": "n3", "type": "task", "label": "Plan: ..."}
    """
    data = await request.json()
    _broadcast("task", data)
    return {"ok": True}


@app.post("/push/memory_sync")
async def push_memory_sync(request: Request) -> dict:
    data = await request.json()
    _broadcast("memory_sync", data)
    return {"ok": True}
