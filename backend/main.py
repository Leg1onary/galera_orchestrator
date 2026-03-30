import asyncio, json, logging, os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

from config import load_config, save_config
from galera_client import get_cluster_status
from mock_data import set_scenario, get_scenario

# ── Persistent event log ─────────────────────────────────────────
_LOG_DIR  = Path(__file__).parent.parent / "logs"
_LOG_FILE = _LOG_DIR / "galera-events.log"
_LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("main")

_event_log: deque = deque(maxlen=500)
_prev_status: dict = {}


def _push_event(level: str, message: str, source: str = "system"):
    entry = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level.upper(),
        "message": message,
        "source": source,
    }
    _event_log.appendleft(entry)
    getattr(log, level.lower(), log.info)("[%s] %s", source, message)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg   = load_config()
    nodes = [n["id"] for n in cfg.get("nodes", []) if n.get("enabled")]
    arb   = cfg.get("arbitrator", {}).get("enabled", False)
    mode  = "mock" if cfg.get("settings", {}).get("use_mock", True) else "real"
    log.info(
        f"Starting Galera Orchestrator | nodes={len(nodes)} | "
        f"arbitrator={'yes' if arb else 'no'} | mode={mode}"
    )
    _push_event("info", f"Galera Orchestrator started | nodes={nodes} | mode={mode}", "system")
    yield


app = FastAPI(title="Galera Orchestrator", lifespan=lifespan)

FRONTEND = Path(__file__).parent.parent / "frontend"
ASSETS   = FRONTEND / "assets"
ASSETS.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(ASSETS)), name="assets")


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(str(FRONTEND / "index.html"))

@app.get("/api/status")
async def api_status():
    global _prev_status
    try:
        cfg  = load_config()
        data = await asyncio.get_event_loop().run_in_executor(
            None, get_cluster_status, cfg
        )
        _detect_changes(data)
        return data
    except Exception as e:
        log.error(f"/api/status error: {e}")
        return {
            "cluster_name": "unknown",
            "environment": "unknown",
            "cluster_status": "critical",
            "cluster_size": 0,
            "nodes_total": 0,
            "nodes_synced": 0,
            "nodes_online": 0,
            "flow_control": 0,
            "cert_failures": 0,
            "use_mock": False,
            "arbitrator": {"enabled": False, "online": False, "host": ""},
            "nodes": [],
            "error": str(e),
        }

def _detect_changes(data: dict):
    """Compare new status to previous — push events only on changes."""
    global _prev_status
    new_cs = data.get("cluster_status", "")
    old_cs = _prev_status.get("cluster_status", "")
    if new_cs != old_cs and old_cs:
        lvl = "error" if new_cs == "critical" else "warning" if new_cs == "degraded" else "info"
        _push_event(lvl, f"Cluster status changed: {old_cs} → {new_cs}", "monitor")

    for node in data.get("nodes", []):
        nid  = node["id"]
        prev = _prev_status.get("nodes_map", {}).get(nid, {})
        new_state  = node.get("wsrep_local_state_comment", "")
        prev_state = prev.get("wsrep_local_state_comment", "")
        new_online  = node.get("online", False)
        prev_online = prev.get("online", True)

        if new_state != prev_state and prev_state:
            lvl = "error" if new_state in ("Disconnected","Aborting") else                   "warning" if new_state in ("Joining","Donor/Desynced") else "info"
            _push_event(lvl, f"{nid}: state {prev_state} → {new_state}", "monitor")

        if not new_online and prev_online and prev_state:
            _push_event("error", f"{nid}: node went OFFLINE — {node.get('error','')}", "monitor")
        elif new_online and not prev_online:
            _push_event("info", f"{nid}: node back ONLINE", "monitor")

        fc_new = float(node.get("wsrep_flow_control_paused", 0) or 0)
        fc_old = float(prev.get("wsrep_flow_control_paused", 0) or 0)
        if fc_new > 0.1 and fc_old <= 0.1:
            _push_event("warning", f"{nid}: Flow Control active ({fc_new:.2f})", "monitor")
        elif fc_new <= 0.01 and fc_old > 0.1:
            _push_event("info", f"{nid}: Flow Control cleared", "monitor")

    # Save snapshot for next comparison
    _prev_status = {
        "cluster_status": new_cs,
        "nodes_map": {n["id"]: n for n in data.get("nodes", [])},
    }

@app.post("/api/scenario/{name}")
async def api_scenario(name: str):
    allowed = {"normal", "gc01_down", "gc02_down", "flow_control"}
    if name not in allowed:
        raise HTTPException(400, f"Unknown scenario. Allowed: {allowed}")
    set_scenario(name)
    return {"scenario": name}

@app.get("/api/scenario")
async def api_get_scenario():
    return {"scenario": get_scenario()}

@app.get("/api/config")
async def api_config():
    return load_config()

# ── MODE SWITCH (real swap of nodes) ────────────────────────────
class ModePayload(BaseModel):
    use_mock: bool

@app.get("/api/config/mode")
async def get_mode():
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    return {"use_mock": use_mock, "mode": "mock" if use_mock else "real"}

@app.post("/api/config/mode")
async def set_mode(payload: ModePayload):
    cfg = load_config()

    # Save current active nodes into the appropriate snapshot key
    current_mock = cfg.get("settings", {}).get("use_mock", True)
    if current_mock:
        cfg["mock_nodes"] = cfg.get("nodes", [])
    else:
        cfg["real_nodes"] = cfg.get("nodes", [])

    # Restore target snapshot (or keep existing if snapshot is empty)
    if payload.use_mock:
        restored = cfg.get("mock_nodes") or cfg.get("nodes", [])
    else:
        restored = cfg.get("real_nodes") or cfg.get("nodes", [])

    cfg["nodes"] = restored
    cfg.setdefault("settings", {})["use_mock"] = payload.use_mock
    save_config(cfg)

    mode = "mock" if payload.use_mock else "real"
    log.info(f"Mode switched to: {mode} | active nodes: {[n['id'] for n in restored]}")
    _push_event("info", f"Mode switched to {mode} | nodes: {[n['id'] for n in restored]}", "config")
    return {"ok": True, "use_mock": payload.use_mock, "mode": mode,
            "nodes": [n["id"] for n in restored]}

# ── NODE CRUD ─────────────────────────────────────────────────
class NodePayload(BaseModel):
    id: str
    name: str
    host: str
    port: int = 3306
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key: str = "~/.ssh/id_rsa"

@app.post("/api/config/node")
async def add_node(payload: NodePayload):
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)

    nodes = cfg.setdefault("nodes", [])
    if any(n["id"] == payload.id for n in nodes):
        raise HTTPException(400, f"Node '{payload.id}' already exists")
    new_node = {**payload.model_dump(), "enabled": True}
    nodes.append(new_node)

    # Also update the relevant snapshot
    snap_key = "mock_nodes" if use_mock else "real_nodes"
    snap = cfg.setdefault(snap_key, [])
    if not any(n["id"] == payload.id for n in snap):
        snap.append(new_node)

    save_config(cfg)
    _push_event("info", f"Node added: {payload.id} ({payload.host}:{payload.port})", "config")
    return {"ok": True, "config": cfg}

@app.delete("/api/config/node/{node_id}")
async def delete_node(node_id: str):
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    before = len(cfg.get("nodes", []))
    cfg["nodes"] = [n for n in cfg.get("nodes", []) if n["id"] != node_id]
    if len(cfg["nodes"]) == before:
        raise HTTPException(404, f"Node '{node_id}' not found")

    # Remove from BOTH snapshots (regardless of current mode)
    for snap_key in ("mock_nodes", "real_nodes"):
        if snap_key in cfg:
            cfg[snap_key] = [n for n in cfg[snap_key] if n["id"] != node_id]

    save_config(cfg)
    _push_event("warning", f"Node removed: {node_id}", "config")
    return {"ok": True}

# ── ARBITRATOR ────────────────────────────────────────────────
class ArbitratorPayload(BaseModel):
    host: str
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key: str = "~/.ssh/id_rsa"

@app.post("/api/config/arbitrator")
async def set_arbitrator(payload: ArbitratorPayload):
    cfg = load_config()
    cfg["arbitrator"] = {**payload.model_dump(), "enabled": True}
    save_config(cfg)
    return {"ok": True}

@app.delete("/api/config/arbitrator")
async def remove_arbitrator():
    cfg = load_config()
    cfg["arbitrator"] = {"enabled": False, "host": "", "ssh_port": 22,
                         "ssh_user": "root", "ssh_key": "~/.ssh/id_rsa"}
    save_config(cfg)
    return {"ok": True}

# ── RELOAD ────────────────────────────────────────────────────
async def _do_reload():
    cfg   = load_config()
    nodes = [n["id"] for n in cfg.get("nodes", []) if n.get("enabled")]
    arb   = cfg.get("arbitrator", {}).get("enabled", False)
    mode  = "mock" if cfg.get("settings", {}).get("use_mock", True) else "real"
    log.info(f"Config reloaded | nodes={nodes} | arbitrator={arb} | mode={mode}")
    return {"ok": True, "nodes": nodes, "arbitrator": arb, "mode": mode}

@app.post("/api/reload")
async def reload_config():
    return await _do_reload()

# Алиас для совместимости с ТЗ
@app.post("/api/config/reload")
async def reload_config_alias():
    return await _do_reload()

# ── UI PREFERENCES (theme etc.) ──────────────────────────────
PREFS_PATH = Path(__file__).parent.parent / "config" / "ui_prefs.json"

def load_prefs() -> dict:
    if PREFS_PATH.exists():
        try:
            return json.loads(PREFS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"theme": "dark", "refresh_interval": 10}

def save_prefs(data: dict):
    PREFS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

class PrefsPayload(BaseModel):
    theme: Optional[str] = None
    refresh_interval: Optional[int] = None

@app.get("/api/prefs")
async def get_prefs():
    return load_prefs()

@app.post("/api/prefs")
async def set_prefs(payload: PrefsPayload):
    prefs = load_prefs()
    if payload.theme is not None:
        prefs["theme"] = payload.theme
    if payload.refresh_interval is not None:
        prefs["refresh_interval"] = payload.refresh_interval
        # Also persist to nodes.yaml so WS loop respects it immediately
        cfg = load_config()
        cfg.setdefault("settings", {})["poll_interval"] = payload.refresh_interval
        save_config(cfg)
    save_prefs(prefs)
    log.info(f"Prefs saved: {prefs}")
    return {"ok": True, **prefs}

# ── WEBSOCKET ─────────────────────────────────────────────────
@app.websocket("/ws/cluster")
async def ws_cluster(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            cfg  = load_config()
            data = get_cluster_status(cfg)
            await ws.send_text(json.dumps(data))
            interval = cfg.get("settings", {}).get("poll_interval", 5)
            await asyncio.sleep(interval)
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)




# ── EVENT LOG API ─────────────────────────────────────────────
@app.get("/api/log")
async def get_log(limit: int = 200, level: str = ""):
    """Return recent events from the in-memory ring buffer."""
    entries = list(_event_log)
    if level:
        entries = [e for e in entries if e["level"] == level.upper()]
    return {"events": entries[:limit], "total": len(_event_log)}

@app.delete("/api/log")
async def clear_log():
    _event_log.clear()
    _push_event("info", "Event log cleared by user", "ui")
    return {"ok": True}

# ── SEQNO / GRASTATE ANALYSIS ────────────────────────────────
@app.get("/api/seqno")
async def get_seqno():
    """Read seqno from all nodes for bootstrap candidate selection."""
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])

    if use_mock:
        from mock_data import mock_seqno
        results = mock_seqno(nodes)
        candidate = max((r for r in results if r["reachable"]),
                        key=lambda x: x["seqno"], default=None)
        return {
            "ok": True, "mock": True,
            "nodes": results,
            "candidate": candidate["id"] if candidate else None,
            "candidate_seqno": candidate["seqno"] if candidate else -1,
        }

    # Real mode — SSH: cat /var/lib/mysql/grastate.dat
    try:
        import paramiko, re
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    results = []
    for node in nodes:
        nid = node["id"]
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(node.get("host"), port=int(node.get("ssh_port", 22)),
                           username=node.get("ssh_user","root"),
                           key_filename=str(Path(node.get("ssh_key","~/.ssh/id_rsa")).expanduser()),
                           timeout=8)
            _, so, _ = client.exec_command("cat /var/lib/mysql/grastate.dat", timeout=10)
            raw = so.read().decode(errors="replace")
            client.close()
            seqno_m = re.search(r"seqno:\s*(-?\d+)", raw)
            uuid_m  = re.search(r"uuid:\s*([\w-]+)", raw)
            stb_m   = re.search(r"safe_to_bootstrap:\s*(\d+)", raw)
            results.append({
                "id": nid, "name": node.get("name", nid), "host": node.get("host",""),
                "reachable": True, "error": None,
                "seqno":             int(seqno_m.group(1)) if seqno_m else -1,
                "safe_to_bootstrap": int(stb_m.group(1)) if stb_m else 0,
                "uuid":              uuid_m.group(1) if uuid_m else "unknown",
            })
        except Exception as e:
            results.append({"id": nid, "name": node.get("name", nid), "host": node.get("host",""),
                            "reachable": False, "error": str(e), "seqno": -1,
                            "safe_to_bootstrap": 0, "uuid": "unknown"})

    candidate = max((r for r in results if r["reachable"]), key=lambda x: x["seqno"], default=None)
    return {
        "ok": True, "mock": False,
        "nodes": results,
        "candidate": candidate["id"] if candidate else None,
        "candidate_seqno": candidate["seqno"] if candidate else -1,
    }

# ── BOOTSTRAP ────────────────────────────────────────────────
class BootstrapPayload(BaseModel):
    candidate: str   # node_id to run galera_new_cluster on

@app.post("/api/bootstrap")
async def do_bootstrap(payload: BootstrapPayload):
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])

    candidate = next((n for n in nodes if n["id"] == payload.candidate), None)
    if not candidate:
        raise HTTPException(404, f"Node '{payload.candidate}' not found")

    if use_mock:
        from mock_data import mock_bootstrap
        steps = mock_bootstrap(payload.candidate, nodes)
        _push_event("info", f"Bootstrap (mock) completed, candidate={payload.candidate}", "bootstrap")
        return {"ok": True, "mock": True, "steps": steps}

    # Real mode — SSH bootstrap sequence
    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    steps = []
    def ssh_run(node, cmd):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(node.get("host"), port=int(node.get("ssh_port",22)),
                       username=node.get("ssh_user","root"),
                       key_filename=str(Path(node.get("ssh_key","~/.ssh/id_rsa")).expanduser()),
                       timeout=10)
        _, so, se = client.exec_command(cmd, timeout=30)
        out = so.read().decode(errors="replace").strip()
        err = se.read().decode(errors="replace").strip()
        ec  = so.channel.recv_exit_status()
        client.close()
        return ec, out, err

    # Step 1: galera_new_cluster on candidate
    try:
        ec, out, err = ssh_run(candidate, "galera_new_cluster")
        if ec != 0:
            raise Exception(err or f"exit_code={ec}")
        steps.append({"step":1,"status":"ok",
                      "message":f"galera_new_cluster на {payload.candidate} — OK"})
    except Exception as e:
        steps.append({"step":1,"status":"error","message":f"galera_new_cluster failed: {e}"})
        return {"ok": False, "mock": False, "steps": steps}

    # Step 2: start other nodes
    joiners = [n for n in nodes if n["id"] != payload.candidate]
    for i, n in enumerate(joiners, 2):
        try:
            ec, out, err = ssh_run(n, "systemctl start mariadb.service")
            steps.append({"step":i,"status":"ok",
                          "message":f"systemctl start mariadb на {n['id']} — exit={ec}"})
        except Exception as e:
            steps.append({"step":i,"status":"error","message":f"{n['id']}: {e}"})

    return {"ok": True, "mock": False, "steps": steps}

# ── REJOIN ───────────────────────────────────────────────────
class RejoinPayload(BaseModel):
    node_id: str
    method: str = "ist"  # ist | sst

@app.post("/api/rejoin")
async def do_rejoin(payload: RejoinPayload):
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])

    node = next((n for n in nodes if n["id"] == payload.node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{payload.node_id}' not found")

    steps = []
    if use_mock:
        import time
        steps = [
            {"step":1,"status":"ok","message":f"SSH → {payload.node_id}: systemctl stop mariadb.service"},
            {"step":2,"status":"ok","message":f"Метод: {payload.method.upper()} — "
                                              + ("инкрементальная синхронизация (быстро)" if payload.method=="ist" else "полный SST snapshot (долго)")},
            {"step":3,"status":"ok","message":f"systemctl start mariadb.service → MariaDB запущена"},
            {"step":4,"status":"ok","message":f"{payload.node_id}: wsrep_local_state_comment = Joined"},
            {"step":5,"status":"done","message":f"{payload.node_id}: wsrep_local_state_comment = Synced ✓"},
        ]
        _push_event("info", f"Rejoin (mock) {payload.node_id} via {payload.method} completed", "rejoin")
        return {"ok": True, "mock": True, "node_id": payload.node_id, "method": payload.method, "steps": steps}

    # Real mode SSH
    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    def ssh_run(cmd):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(node.get("host"), port=int(node.get("ssh_port",22)),
                       username=node.get("ssh_user","root"),
                       key_filename=str(Path(node.get("ssh_key","~/.ssh/id_rsa")).expanduser()),
                       timeout=10)
        _, so, se = client.exec_command(cmd, timeout=60)
        out = so.read().decode(errors="replace").strip()
        err = se.read().decode(errors="replace").strip()
        ec  = so.channel.recv_exit_status()
        client.close()
        return ec, out, err

    # Stop → configure wsrep_sst_method if SST → Start
    cmds = ["systemctl stop mariadb.service"]
    if payload.method == "sst":
        cmds.append("sed -i 's/wsrep_sst_method=.*/wsrep_sst_method=rsync/' /etc/mysql/mariadb.conf.d/galera.cnf")
    cmds.append("systemctl start mariadb.service")

    for i, cmd in enumerate(cmds, 1):
        try:
            ec, out, err = ssh_run(cmd)
            steps.append({"step":i,"status":"ok","message":f"{cmd} → exit={ec}"})
            if ec != 0:
                steps.append({"step":i+1,"status":"error","message":err or "non-zero exit"})
                return {"ok": False, "mock": False, "steps": steps}
        except Exception as e:
            steps.append({"step":i,"status":"error","message":str(e)})
            return {"ok": False, "mock": False, "steps": steps}

    steps.append({"step":len(steps)+1,"status":"done","message":f"Rejoin {payload.node_id} запущен. Ждите Synced."})
    return {"ok": True, "mock": False, "node_id": payload.node_id, "method": payload.method, "steps": steps}

# ── GARBD STATUS ─────────────────────────────────────────────
@app.get("/api/garbd")
async def get_garbd():
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    arb = cfg.get("arbitrator", {})

    if not arb.get("enabled", False):
        return {"enabled": False, "online": False}

    if use_mock:
        from mock_data import mock_garbd_status
        return mock_garbd_status(arb)

    # Real mode — SSH: systemctl is-active garbd
    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(arb.get("host"), port=int(arb.get("ssh_port",22)),
                       username=arb.get("ssh_user","root"),
                       key_filename=str(Path(arb.get("ssh_key","~/.ssh/id_rsa")).expanduser()),
                       timeout=6)
        _, so, _ = client.exec_command("systemctl is-active garbd && systemctl status garbd --no-pager -l | head -20", timeout=10)
        out = so.read().decode(errors="replace").strip()
        ec  = so.channel.recv_exit_status()
        client.close()
        return {"enabled": True, "online": ec == 0, "host": arb.get("host",""), "raw": out}
    except Exception as e:
        return {"enabled": True, "online": False, "host": arb.get("host",""), "error": str(e)}

# ── NODE SSH ACTION ──────────────────────────────────────────
class NodeActionPayload(BaseModel):
    action: str  # start | stop | restart | rejoin

ALLOWED_ACTIONS = {
    "start":   "systemctl start mariadb.service",
    "stop":    "systemctl stop mariadb.service",
    "restart": "systemctl restart mariadb.service",
    "rejoin":  "systemctl restart mariadb.service",
}

@app.post("/api/node/{node_id}/action")
async def node_action(node_id: str, payload: NodeActionPayload):
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)

    if payload.action not in ALLOWED_ACTIONS:
        raise HTTPException(400, f"Unknown action '{payload.action}'. Allowed: {list(ALLOWED_ACTIONS)}")

    node = next((n for n in cfg.get("nodes", []) if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    cmd = ALLOWED_ACTIONS[payload.action]

    if use_mock:
        log.info(f"[Mock] node_action {payload.action} on {node_id} → {cmd}")
        return {"ok": True, "mock": True, "node_id": node_id,
                "action": payload.action, "command": cmd,
                "exit_code": 0, "stdout": f"[mock] {cmd} — OK", "stderr": ""}

    # Real mode — try SSH via paramiko
    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed. Run: pip install paramiko")

    ssh_host = node.get("host", "")
    ssh_port = int(node.get("ssh_port", 22))
    ssh_user = node.get("ssh_user", "root")
    ssh_key  = node.get("ssh_key", "~/.ssh/id_rsa")
    key_path = str(Path(ssh_key).expanduser())

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ssh_host, port=ssh_port, username=ssh_user,
                       key_filename=key_path, timeout=10, banner_timeout=10)
        _, stdout, stderr = client.exec_command(cmd, timeout=30)
        stdout_str = stdout.read().decode(errors="replace").strip()
        stderr_str = stderr.read().decode(errors="replace").strip()
        exit_code  = stdout.channel.recv_exit_status()
        client.close()
        log.info(f"[SSH] {node_id} {payload.action} exit={exit_code}")
        _push_event(
            "info" if exit_code == 0 else "error",
            f"SSH action '{payload.action}' on {node_id} → exit={exit_code}" + (f" | {stderr_str}" if stderr_str else ""),
            "ssh"
        )
        return {"ok": exit_code == 0, "mock": False, "node_id": node_id,
                "action": payload.action, "command": cmd,
                "exit_code": exit_code, "stdout": stdout_str, "stderr": stderr_str}
    except Exception as e:
        log.error(f"[SSH] {node_id} action failed: {e}")
        raise HTTPException(502, f"SSH error on {node_id}: {e}")
