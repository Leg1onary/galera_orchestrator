import random, time, math

_scenario = "normal"
_start = time.time()

# Per-node commit counters (simulate live activity)
_commit_base = {"gc01": 485734, "gc02": 485730}
# Seqno for bootstrap analysis (grastate.dat simulation)
_seqno = {
    "gc01": {"seqno": 485734, "safe_to_bootstrap": 1, "uuid": "5a7b1c2d-dead-beef-cafe-0123456789ab"},
    "gc02": {"seqno": 485730, "safe_to_bootstrap": 0, "uuid": "5a7b1c2d-dead-beef-cafe-0123456789ab"},
}
# garbd mock state
_garbd = {"online": True, "last_seen": time.time()}


def set_scenario(s: str):
    global _scenario
    _scenario = s


def get_scenario() -> str:
    return _scenario


def node_status(node_id: str, node: dict) -> dict:
    elapsed = int(time.time() - _start)
    commits = _commit_base.get(node_id, 100000) + elapsed * 3

    base = {
        "id":   node_id,
        "name": node.get("name", node_id),
        "host": node.get("host", ""),
        "port": node.get("port", 3306),
        "wsrep_cluster_status":      "Primary",
        "wsrep_local_state_comment": "Synced",
        "wsrep_connected":           "ON",
        "wsrep_ready":               "ON",
        "wsrep_cluster_size":        2,
        "wsrep_local_send_queue":    0,
        "wsrep_local_recv_queue":    0,
        "wsrep_flow_control_paused": "0.00",
        "wsrep_cert_deps_distance":  round(random.uniform(0.8, 2.1), 2),
        "wsrep_local_commits":       commits,
        "wsrep_local_cert_failures": 0,
        "wsrep_bf_aborts":           0,
        "wsrep_apply_oooe":          round(random.uniform(0, 0.05), 4),
        "wsrep_cluster_conf_id":     12,
        "wsrep_cluster_state_uuid":  _seqno[node_id]["uuid"] if node_id in _seqno else "unknown",
        "online": True,
        "error":  None,
    }

    if _scenario == "gc01_down" and node_id == "gc01":
        base.update({
            "online": False, "error": "SSH timeout",
            "wsrep_cluster_status": "non-Primary",
            "wsrep_local_state_comment": "Disconnected",
            "wsrep_connected": "OFF", "wsrep_ready": "OFF",
        })
    elif _scenario == "gc02_down" and node_id == "gc02":
        base.update({
            "online": False, "error": "Connection refused",
            "wsrep_cluster_status": "non-Primary",
            "wsrep_local_state_comment": "Disconnected",
            "wsrep_connected": "OFF", "wsrep_ready": "OFF",
        })
    elif _scenario == "flow_control":
        base.update({
            "wsrep_local_send_queue":    random.randint(5, 20),
            "wsrep_local_recv_queue":    random.randint(2, 15),
            "wsrep_flow_control_paused": str(round(random.uniform(0.1, 0.8), 2)),
        })

    return base


# ── seqno / grastate.dat (for bootstrap analysis) ────────────
def mock_seqno(nodes_cfg: list) -> list:
    """Simulate reading /var/lib/mysql/grastate.dat from each node."""
    result = []
    for n in nodes_cfg:
        nid = n["id"]
        s = _seqno.get(nid, {})
        if _scenario in ("gc01_down",) and nid == "gc01":
            result.append({
                "id": nid, "name": n.get("name", nid), "host": n.get("host", ""),
                "reachable": False, "error": "SSH timeout",
                "seqno": -1, "safe_to_bootstrap": 0, "uuid": "unknown",
            })
        elif _scenario in ("gc02_down",) and nid == "gc02":
            result.append({
                "id": nid, "name": n.get("name", nid), "host": n.get("host", ""),
                "reachable": False, "error": "Connection refused",
                "seqno": -1, "safe_to_bootstrap": 0, "uuid": "unknown",
            })
        else:
            result.append({
                "id": nid, "name": n.get("name", nid), "host": n.get("host", ""),
                "reachable": True, "error": None,
                "seqno":             s.get("seqno", 100000),
                "safe_to_bootstrap": s.get("safe_to_bootstrap", 0),
                "uuid":              s.get("uuid", "unknown"),
            })
    return result


# ── garbd mock status ─────────────────────────────────────────
def mock_garbd_status(arb_cfg: dict) -> dict:
    if not arb_cfg.get("enabled"):
        return {"enabled": False, "online": False}
    if _scenario in ("gc01_down", "gc02_down"):
        return {"enabled": True, "online": True, "host": arb_cfg.get("host",""),
                "members": 2, "last_seen_sec": 0}
    return {"enabled": True, "online": True, "host": arb_cfg.get("host",""),
            "members": 2, "last_seen_sec": int(time.time() - _garbd["last_seen"])}


# ── mock SSH action execution ─────────────────────────────────
def mock_ssh_action(node_id: str, action: str, cmd: str) -> dict:
    """Simulate SSH command execution result."""
    time.sleep(0.3)  # simulate network latency
    if _scenario in ("gc01_down",) and node_id == "gc01":
        return {"exit_code": 255, "stdout": "", "stderr": f"ssh: connect to host {node_id}: Connection timed out"}
    if _scenario in ("gc02_down",) and node_id == "gc02":
        return {"exit_code": 1, "stdout": "", "stderr": "Connection refused"}

    outputs = {
        "start":   ("", 0),    # systemctl start exits 0 silently
        "stop":    ("", 0),
        "restart": ("", 0),
        "rejoin":  ("", 0),
        "galera_new_cluster": ("", 0),
    }
    stdout, code = outputs.get(action, ("", 0))
    # update mock seqno on rejoin
    if action in ("start", "restart", "rejoin") and node_id in _seqno:
        elapsed = int(time.time() - _start)
        _seqno[node_id]["seqno"] = _commit_base.get(node_id, 100000) + elapsed * 3
    return {"exit_code": code, "stdout": stdout, "stderr": ""}


# ── mock bootstrap sequence ───────────────────────────────────
def mock_bootstrap(candidate_id: str, all_nodes: list) -> list:
    """Return step-by-step bootstrap result."""
    elapsed = int(time.time() - _start)
    seqno_val = _commit_base.get(candidate_id, 100000) + elapsed * 3
    steps = [
        {"step": 1, "status": "ok",
         "message": f"Анализ grastate.dat: {candidate_id} имеет seqno={seqno_val}, safe_to_bootstrap=1"},
        {"step": 2, "status": "ok",
         "message": f"SSH → {candidate_id}: galera_new_cluster — MariaDB запущена в bootstrap-режиме"},
        {"step": 3, "status": "ok",
         "message": f"{candidate_id}: wsrep_cluster_status = Primary (cluster_size=1) ✓"},
    ]
    joiners = [n for n in all_nodes if n["id"] != candidate_id]
    for i, n in enumerate(joiners, 4):
        steps.append({"step": i, "status": "ok",
                      "message": f"SSH → {n['id']}: systemctl start mariadb.service — IST начат…"})
        steps.append({"step": i+1, "status": "ok",
                      "message": f"{n['id']}: wsrep_local_state_comment = Synced ✓ (cluster_size={i})"})
    steps.append({"step": len(steps)+1, "status": "done",
                  "message": "Bootstrap завершён. Кластер восстановлен."})
    return steps
