# shared/db.py
# HyperSpace AGI v1.03 — SQLite persistence layer
# Tabelle: logs, nodes, tasks, federated_peers
# Usato dal control-plane per sostituire i log in-memory e per
# gestire l'allowlist dei control-plane federati.

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "./data/hyperspace.db")


def _ensure_dir():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


@contextmanager
def _conn():
    _ensure_dir()
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db():
    """Crea le tabelle se non esistono."""
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS logs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                log_id     TEXT UNIQUE,
                ts         TEXT DEFAULT (datetime('now')),
                node_id    TEXT DEFAULT '',
                type       TEXT DEFAULT 'system',
                status     TEXT DEFAULT 'info',
                source     TEXT DEFAULT '',
                target     TEXT DEFAULT '',
                trace_id   TEXT DEFAULT '',
                summary    TEXT DEFAULT '',
                detail     TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_logs_type   ON logs(type);
            CREATE INDEX IF NOT EXISTS idx_logs_status ON logs(status);
            CREATE INDEX IF NOT EXISTS idx_logs_ts     ON logs(ts);

            CREATE TABLE IF NOT EXISTS nodes (
                node_id    TEXT PRIMARY KEY,
                endpoint   TEXT DEFAULT '',
                tier       TEXT DEFAULT 'leaf',
                pubkey     TEXT DEFAULT '',
                version    TEXT DEFAULT '',
                vram_gb    REAL DEFAULT 0.0,
                peers_active INTEGER DEFAULT 0,
                status     TEXT DEFAULT 'active',
                last_seen  TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id      TEXT UNIQUE,
                created_at   TEXT DEFAULT (datetime('now')),
                completed_at TEXT,
                node_id      TEXT DEFAULT '',
                endpoint     TEXT DEFAULT '',
                prompt       TEXT DEFAULT '',
                model        TEXT DEFAULT '',
                status       TEXT DEFAULT 'created',
                result       TEXT DEFAULT '',
                error        TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS federated_peers (
                peer_id     TEXT PRIMARY KEY,
                label       TEXT DEFAULT '',
                pubkey      TEXT NOT NULL,
                endpoint    TEXT NOT NULL,
                enabled     INTEGER DEFAULT 1,
                added_at    TEXT DEFAULT (datetime('now')),
                last_seen   TEXT,
                last_status TEXT DEFAULT 'unknown'
            );
        """)
    print(f"[DB] Initialized at {DB_PATH}")


# ── LOGS ──────────────────────────────────────────────────────────────────────

def insert_log(entry: dict):
    with _conn() as con:
        con.execute("""
            INSERT OR IGNORE INTO logs
              (log_id, ts, node_id, type, status, source, target, trace_id, summary, detail)
            VALUES
              (:id, :ts, :sourceNode, :type, :status, :sourceNode, :targetNode, :traceId, :summary, :detail)
        """, entry)


def query_logs(
    type_: str = "",
    status: str = "",
    node: str = "",
    q: str = "",
    page: int = 1,
    per_page: int = 100,
) -> list:
    clauses, params = [], []
    if type_:  clauses.append("type = ?");                params.append(type_)
    if status: clauses.append("status = ?");              params.append(status)
    if node:   clauses.append("(source LIKE ? OR target LIKE ?)"); params += [f"%{node}%", f"%{node}%"]
    if q:      clauses.append("(summary LIKE ? OR detail LIKE ?)"); params += [f"%{q}%", f"%{q}%"]

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    offset = (page - 1) * per_page
    sql = f"SELECT * FROM logs {where} ORDER BY id DESC LIMIT ? OFFSET ?"
    params += [per_page, offset]

    with _conn() as con:
        rows = con.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def count_logs(type_: str = "", status: str = "", node: str = "", q: str = "") -> int:
    clauses, params = [], []
    if type_:  clauses.append("type = ?");                params.append(type_)
    if status: clauses.append("status = ?");              params.append(status)
    if node:   clauses.append("(source LIKE ? OR target LIKE ?)"); params += [f"%{node}%", f"%{node}%"]
    if q:      clauses.append("(summary LIKE ? OR detail LIKE ?)"); params += [f"%{q}%", f"%{q}%"]
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with _conn() as con:
        return con.execute(f"SELECT COUNT(*) FROM logs {where}", params).fetchone()[0]


def clear_logs():
    with _conn() as con:
        con.execute("DELETE FROM logs")


def export_logs(type_: str = "", status: str = "", node: str = "", q: str = "") -> list:
    return query_logs(type_=type_, status=status, node=node, q=q, page=1, per_page=100_000)


# ── NODES ─────────────────────────────────────────────────────────────────────

def upsert_node(info: dict):
    with _conn() as con:
        con.execute("""
            INSERT INTO nodes (node_id, endpoint, tier, pubkey, version, vram_gb, peers_active, status, last_seen)
            VALUES (:node_id, :endpoint, :tier, :pubkey, :version, :vram_gb, :peers_active, :status, :last_seen)
            ON CONFLICT(node_id) DO UPDATE SET
                endpoint     = excluded.endpoint,
                tier         = excluded.tier,
                pubkey       = excluded.pubkey,
                version      = excluded.version,
                vram_gb      = excluded.vram_gb,
                peers_active = excluded.peers_active,
                status       = excluded.status,
                last_seen    = excluded.last_seen
        """, {
            "node_id":      info.get("node_id", ""),
            "endpoint":     info.get("endpoint", ""),
            "tier":         info.get("tier", "leaf"),
            "pubkey":       info.get("pubkey", info.get("public_key", "")),
            "version":      info.get("version", ""),
            "vram_gb":      info.get("vram_gb", 0.0),
            "peers_active": info.get("peers_active", 0),
            "status":       info.get("status", "active"),
            "last_seen":    datetime.utcnow().isoformat(timespec="seconds") + "Z",
        })


def get_all_nodes() -> list:
    with _conn() as con:
        rows = con.execute("SELECT * FROM nodes ORDER BY last_seen DESC").fetchall()
    return [dict(r) for r in rows]


# ── TASKS ─────────────────────────────────────────────────────────────────────

def insert_task(task: dict):
    with _conn() as con:
        con.execute("""
            INSERT OR IGNORE INTO tasks
              (task_id, created_at, node_id, endpoint, prompt, model, status)
            VALUES (:task_id, :created_at, :node_id, :endpoint, :prompt, :model, :status)
        """, {
            "task_id":    task.get("id", ""),
            "created_at": task.get("created_at", datetime.utcnow().isoformat()),
            "node_id":    task.get("node", ""),
            "endpoint":   task.get("endpoint", ""),
            "prompt":     task.get("payload", {}).get("prompt", ""),
            "model":      task.get("payload", {}).get("model", ""),
            "status":     task.get("status", "created"),
        })


def update_task(task_id: str, status: str, result: str = "", error: str = "", node_id: str = "", endpoint: str = ""):
    with _conn() as con:
        con.execute("""
            UPDATE tasks SET
                status       = ?,
                result       = ?,
                error        = ?,
                node_id      = CASE WHEN ? != '' THEN ? ELSE node_id END,
                endpoint     = CASE WHEN ? != '' THEN ? ELSE endpoint END,
                completed_at = CASE WHEN ? IN ('done','failed') THEN datetime('now') ELSE completed_at END
            WHERE task_id = ?
        """, (status, result, error, node_id, node_id, endpoint, endpoint, status, task_id))


def get_all_tasks() -> list:
    with _conn() as con:
        rows = con.execute("SELECT * FROM tasks ORDER BY id DESC").fetchall()
    return [dict(r) for r in rows]


# ── FEDERATED PEERS ────────────────────────────────────────────────────────────
# Allowlist dei control-plane federati fidati. Un peer entra qui SOLO tramite
# pairing manuale (dashboard / API), mai per auto-discovery — a differenza
# dei nodi, che possono autoannunciarsi liberamente.

def upsert_federated_peer(peer: dict):
    with _conn() as con:
        con.execute("""
            INSERT INTO federated_peers (peer_id, label, pubkey, endpoint, enabled, last_seen, last_status)
            VALUES (:peer_id, :label, :pubkey, :endpoint, :enabled, :last_seen, :last_status)
            ON CONFLICT(peer_id) DO UPDATE SET
                label       = excluded.label,
                pubkey      = excluded.pubkey,
                endpoint    = excluded.endpoint,
                enabled     = excluded.enabled
        """, {
            "peer_id":     peer.get("peer_id", ""),
            "label":       peer.get("label", ""),
            "pubkey":      peer.get("pubkey", ""),
            "endpoint":    peer.get("endpoint", ""),
            "enabled":     int(peer.get("enabled", 1)),
            "last_seen":   peer.get("last_seen"),
            "last_status": peer.get("last_status", "unknown"),
        })


def get_all_federated_peers() -> list:
    with _conn() as con:
        rows = con.execute("SELECT * FROM federated_peers ORDER BY added_at DESC").fetchall()
    return [dict(r) for r in rows]


def get_federated_peer(peer_id: str):
    with _conn() as con:
        row = con.execute("SELECT * FROM federated_peers WHERE peer_id = ?", (peer_id,)).fetchone()
    return dict(row) if row else None


def set_federated_peer_enabled(peer_id: str, enabled: bool):
    with _conn() as con:
        con.execute("UPDATE federated_peers SET enabled = ? WHERE peer_id = ?", (int(enabled), peer_id))


def delete_federated_peer(peer_id: str):
    with _conn() as con:
        con.execute("DELETE FROM federated_peers WHERE peer_id = ?", (peer_id,))


def touch_federated_peer(peer_id: str, status: str):
    with _conn() as con:
        con.execute(
            "UPDATE federated_peers SET last_seen = datetime('now'), last_status = ? WHERE peer_id = ?",
            (status, peer_id),
        )
