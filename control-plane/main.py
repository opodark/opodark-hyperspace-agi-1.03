# control-plane/main.py
# ... (file invariato)

# ── MESH ──────────────────────────────────────────────────────────────────────
@app.route('/mesh/announce', methods=['POST'])
def mesh_announce():
    _log_auth_headers(request, "register.request")

    data = request.get_json(force=True, silent=True) or {}
    ep   = _normalize_endpoint(data.get("endpoint", ""))
    nid  = data.get("node_id", "")
    if not ep or not nid:
        return jsonify({"ok": False, "error": "missing endpoint or node_id"}), 400
    existing      = _nodes_by_id.get(nid)
    should_update = True
    if existing:
        existing_ep = _normalize_endpoint(existing.get("endpoint", ""))
        if existing_ep.startswith("https://") and not ep.startswith("https://"):
            should_update = False
    if should_update:
        info = {**data, "endpoint": ep, "status": "active",
                "last_seen": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
        _nodes_by_id[nid] = info
        _known_endpoints.add(ep)
        db.upsert_node(info)
    push_log('mesh_event', f'Node announced: {nid[:12]}',
             f'endpoint={ep} accepted={should_update}', source=nid[:12], status='success')
    return jsonify({"ok": True, "registered": ep, "accepted": should_update})

# (resto del file invariato)