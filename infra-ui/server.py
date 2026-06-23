from __future__ import annotations

import asyncio
import json
import os
from typing import Any, AsyncGenerator

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, StreamingResponse

CP_URL = os.getenv("CP_URL", "http://localhost:8085")
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:8086")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "4"))
DASHBOARD_HTML = os.path.join(os.path.dirname(__file__), "dashboard.html")

app = FastAPI(title="HyperSpace-AGI Infra-UI Bridge")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_event_queues: list[asyncio.Queue] = []
_known_node_ids: list[str] = []
_last_log_ids: dict[str, str] = {
    "inter_node_message": "",
    "memory_sync": "",
    "dream": "",
    "node_chat": "",
}
_last_task_snapshot: dict[str, str] = {}


def _broadcast(event_type: str, data: dict[str, Any]) -> None:
    payload = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    for q in list(_event_queues):
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            pass


async def _event_generator(queue: asyncio.Queue) -> AsyncGenerator[str, None]:
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=20)
                yield item
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        if queue in _event_queues:
            _event_queues.remove(queue)


def _log_detail_text(log: dict[str, Any]) -> str:
    detail = log.get("detail")
    if isinstance(detail, str) and detail.strip():
        return detail
    if isinstance(detail, (dict, list)):
        try:
            return json.dumps(detail, ensure_ascii=False, indent=2)
        except Exception:
            return str(detail)
    result = log.get("result")
    if result not in (None, ""):
        try:
            return json.dumps(result, ensure_ascii=False, indent=2)
        except Exception:
            return str(result)
    return ""


def _task_prompt(task: dict[str, Any]) -> str:
    payload = task.get("payload") or {}
    if isinstance(payload, dict):
        for key in ("prompt", "task", "query", "instruction", "content"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
    for key in ("prompt", "summary", "detail"):
        value = task.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


async def _poll_nodes(client: httpx.AsyncClient) -> None:
    try:
        r = await client.get(f"{CP_URL}/mesh/nodes")
        if r.status_code != 200:
            return
        nodes = r.json()
        if not isinstance(nodes, list):
            return
        _known_node_ids.clear()
        for node in nodes:
            nid = node.get("node_id") or node.get("id") or ""
            if not nid:
                continue
            _known_node_ids.append(nid)
            if node.get("status") == "active":
                _broadcast("heartbeat", {
                    "node_id": nid,
                    "ping": node.get("ping", node.get("latency_ms", 0)),
                    "last_seen": node.get("last_seen"),
                    "model": (node.get("metadata") or {}).get("model") or node.get("model", ""),
                    "tier": node.get("tier", "leaf"),
                    "vram_gb": node.get("vram_gb") or (node.get("metadata") or {}).get("vram_gb"),
                    "uptime_s": node.get("uptime_s") or (node.get("metadata") or {}).get("uptime_s"),
                })
    except Exception:
        pass


async def _poll_memory_stats(client: httpx.AsyncClient) -> None:
    try:
        r = await client.get(f"{CP_URL}/memory/stats")
        if r.status_code == 200:
            _broadcast("memory_stats", r.json())
    except Exception:
        pass


async def _poll_tasks(client: httpx.AsyncClient) -> None:
    global _last_task_snapshot
    try:
        r = await client.get(f"{CP_URL}/tasks")
        if r.status_code != 200:
            return
        data = r.json()
        if not isinstance(data, dict):
            return
        current_snapshot: dict[str, str] = {}
        tasks = sorted(
            data.values(),
            key=lambda t: t.get("created_at") or "",
            reverse=False,
        )
        for task in tasks:
            tid = str(task.get("id", ""))
            if not tid:
                continue
            status = str(task.get("status", "unknown"))
            node = str(task.get("node") or task.get("assigned_node") or "cp")
            signature = "|".join([
                status,
                str(task.get("completed_at") or ""),
                str(task.get("updated_at") or ""),
                node,
            ])
            current_snapshot[tid] = signature
            previous = _last_task_snapshot.get(tid)
            if previous == signature:
                continue
            _broadcast("task_state", {
                "id": tid,
                "from": "cp",
                "to": node,
                "type": "task",
                "status": status,
                "label": task.get("summary") or _task_prompt(task) or f"task #{tid}",
                "created_at": task.get("created_at"),
                "completed_at": task.get("completed_at"),
                "payload": task.get("payload"),
                "result": task.get("result"),
                "error": task.get("error"),
                "task": task,
            })
            if previous is not None:
                _broadcast("task", {
                    "id": tid,
                    "from": "cp",
                    "to": node,
                    "type": "task",
                    "status": status,
                    "label": task.get("summary") or _task_prompt(task) or f"task #{tid}",
                    "detail": _log_detail_text(task),
                })
        _last_task_snapshot = current_snapshot
    except Exception:
        pass


async def _poll_log_type(client: httpx.AsyncClient, log_type: str, per_page: int = 20) -> None:
    try:
        r = await client.get(
            f"{CP_URL}/logs",
            params={"type": log_type, "per_page": per_page, "page": 1},
        )
        if r.status_code != 200:
            return
        data = r.json()
        logs = data.get("logs", []) if isinstance(data, dict) else []
        if not isinstance(logs, list):
            return
        cursor = _last_log_ids.get(log_type, "")
        new_entries: list[dict[str, Any]] = []
        for log in logs:
            lid = str(log.get("id", ""))
            if cursor and lid == cursor:
                break
            new_entries.append(log)
        if new_entries:
            _last_log_ids[log_type] = str(new_entries[0].get("id", _last_log_ids.get(log_type, "")))
        for log in reversed(new_entries):
            src = str(log.get("sourceNode") or "cp")
            tgt = str(log.get("targetNode") or "")
            detail = _log_detail_text(log)
            payload = {
                "id": log.get("id"),
                "from": src,
                "to": tgt,
                "label": log.get("summary") or detail or log_type,
                "detail": detail,
                "ts": log.get("ts") or log.get("timestamp"),
                "raw": log,
            }
            if log_type == "inter_node_message":
                if src and tgt:
                    _broadcast("task", {
                        **payload,
                        "type": "task",
                    })
            elif log_type == "memory_sync":
                _broadcast("memory_sync", {
                    **payload,
                    "entries": log.get("entries") or 0,
                })
            elif log_type == "dream":
                _broadcast("dream", {
                    "id": log.get("id"),
                    "node": src,
                    "label": log.get("summary") or detail or "dream",
                    "detail": detail,
                    "ts": log.get("ts") or log.get("timestamp"),
                    "raw": log,
                })
            elif log_type == "node_chat":
                if src and tgt:
                    _broadcast("chat", {
                        "id": log.get("id"),
                        "from": src,
                        "to": tgt,
                        "label": log.get("summary") or "chat",
                        "detail": detail,
                        "ts": log.get("ts") or log.get("timestamp"),
                        "raw": log,
                    })
                    _broadcast("task", {
                        "id": log.get("id"),
                        "from": src,
                        "to": tgt,
                        "type": "chat",
                        "label": log.get("summary") or "chat",
                        "detail": detail,
                    })
    except Exception:
        pass


async def _poller() -> None:
    async with httpx.AsyncClient(timeout=6) as client:
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            await _poll_nodes(client)
            await _poll_memory_stats(client)
            await _poll_tasks(client)
            await _poll_log_type(client, "inter_node_message", per_page=30)
            await _poll_log_type(client, "memory_sync", per_page=10)
            await _poll_log_type(client, "dream", per_page=10)
            await _poll_log_type(client, "node_chat", per_page=20)


@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(_poller())


@app.get("/")
async def serve_dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_HTML, media_type="text/html")


@app.get("/dashboard")
async def serve_dashboard_alias() -> FileResponse:
    return FileResponse(DASHBOARD_HTML, media_type="text/html")


@app.get("/dashboard.html")
async def serve_dashboard_html() -> FileResponse:
    return FileResponse(DASHBOARD_HTML, media_type="text/html")


@app.get("/events")
async def sse_events(request: Request) -> StreamingResponse:
    q: asyncio.Queue = asyncio.Queue(maxsize=512)
    _event_queues.append(q)

    async def _gen() -> AsyncGenerator[str, None]:
        yield 'event: connected\ndata: {"ok": true}\n\n'
        async for item in _event_generator(q):
            if await request.is_disconnected():
                break
            yield item

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.api_route("/proxy/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(path: str, request: Request) -> Response:
    if path.startswith("registry/"):
        target = f"{REGISTRY_URL}/{path[len('registry/'):]}"
    else:
        target = f"{CP_URL}/{path}"
    async with httpx.AsyncClient(timeout=20) as client:
        body = await request.body()
        resp = await client.request(
            method=request.method,
            url=target,
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            content=body,
            params=dict(request.query_params),
        )
    content_type = resp.headers.get("content-type", "application/json")
    return Response(content=resp.content, status_code=resp.status_code, media_type=content_type)


@app.post("/push/task")
async def push_task(request: Request) -> dict[str, bool]:
    data = await request.json()
    _broadcast("task", data)
    return {"ok": True}


@app.post("/push/memory_sync")
async def push_memory_sync(request: Request) -> dict[str, bool]:
    data = await request.json()
    _broadcast("memory_sync", data)
    return {"ok": True}


@app.get("/status")
async def bridge_status() -> dict[str, Any]:
    return {
        "ok": True,
        "cp_url": CP_URL,
        "registry_url": REGISTRY_URL,
        "clients": len(_event_queues),
        "known_nodes": _known_node_ids,
        "last_log_ids": _last_log_ids,
        "tracked_tasks": len(_last_task_snapshot),
    }
