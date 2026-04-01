import asyncio, json, logging, os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
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

class _SuppressDevTools(logging.Filter):
    def filter(self, record):
        return "/.well-known/appspecific" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(_SuppressDevTools())

_event_log: deque = deque(maxlen=500)
_prev_status: dict = {}


def _push_event(level: str, message: str, source: str = "system"):
    """Broadcast an event to all WebSocket clients and append to the in-memory log.

    Uses ``app.state.ws_manager`` for WebSocket delivery so there is no
    hidden global dependency — the manager is injected via FastAPI's
    application state during lifespan startup.
    """
    entry = {
        "ts":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level":   level.upper(),
        "message": message,
        "source":  source,
    }
    _event_log.appendleft(entry)
    getattr(
        log,
        level.lower() if level.lower() in ("debug", "info", "warning", "error", "critical") else "info",
        log.info,
    )("[%s] %s", source, message)
    # Broadcast via WebSocket manager stored in app.state (set during lifespan).
    # Falls back gracefully when called before startup (e.g. import-time code).
    try:
        mgr = app.state.ws_manager  # type: ignore[attr-defined]
        loop = asyncio.get_running_loop()
        loop.create_task(mgr.broadcast({"type": "event", **entry}))
    except Exception:
        pass



# ── SSH Action Rate Limiter ──────────────────────────────────
# In-memory rate limit: max 10 calls per 60s per node_id.
# No external dependencies needed.
import time as _time
from collections import defaultdict
_action_calls: dict = defaultdict(list)
_RATE_LIMIT_MAX  = 10   # max requests
_RATE_LIMIT_WINDOW = 60 # seconds

def _check_rate_limit(node_id: str) -> None:
    now = _time.monotonic()
    window_start = now - _RATE_LIMIT_WINDOW
    calls = _action_calls[node_id]
    # Drop stale timestamps
    _action_calls[node_id] = [t for t in calls if t > window_start]
    if len(_action_calls[node_id]) >= _RATE_LIMIT_MAX:
        raise HTTPException(
            429,
            f"Rate limit exceeded for node '{node_id}': "
            f"max {_RATE_LIMIT_MAX} SSH actions per {_RATE_LIMIT_WINDOW}s. Try again later."
        )
    _action_calls[node_id].append(now)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Attach WebSocket manager to app.state so every part of the codebase
    # can reach it via ``app.state.ws_manager`` without touching module globals.
    app.state.ws_manager = _WsManager()

    cfg   = load_config()
    nodes = [n["id"] for n in cfg.get("nodes", []) if n.get("enabled")]
    arbs  = [a for a in cfg.get("arbitrators", []) if a.get("enabled", True)]
    mode  = "mock" if cfg.get("settings", {}).get("use_mock", True) else "real"
    log.info(
        f"Starting Galera Orchestrator | nodes={len(nodes)} | "
        f"arbitrators={len(arbs)} | mode={mode}"
    )
    _push_event("info", f"Galera Orchestrator started | nodes={nodes} | arbitrators={len(arbs)} | mode={mode}", "system")
    yield
    # Graceful shutdown — disconnect all WebSocket clients
    await app.state.ws_manager.shutdown()


app = FastAPI(title="Galera Orchestrator", lifespan=lifespan)

FRONTEND = Path(__file__).parent.parent / "frontend"
ASSETS   = FRONTEND / "assets"
ASSETS.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(ASSETS)), name="assets")


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(str(FRONTEND / "index.html"))

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(str(FRONTEND / "favicon.ico"))

@app.get("/api/status")
async def api_status():
    global _prev_status
    try:
        cfg  = load_config()
        data = await asyncio.get_running_loop().run_in_executor(
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

@app.get("/api/nodes")
async def api_nodes():
    """Return list of configured nodes (without sensitive fields)."""
    cfg = load_config()
    nodes = []
    for n in cfg.get("nodes", []):
        nodes.append({
            "id":       n.get("id"),
            "name":     n.get("name", n.get("id")),
            "host":     n.get("host", ""),
            "port":     n.get("port", 3306),
            "ssh_port": n.get("ssh_port", 22),
            "ssh_user": n.get("ssh_user", "root"),
            "enabled":  n.get("enabled", True),
            "role":     n.get("role", "node"),
        })
    arbs_raw = cfg.get("arbitrators", [])
    if not arbs_raw:
        oa = cfg.get("arbitrator", {})
        if oa.get("host"):
            arbs_raw = [{"id":"arb01","dc":"DC1",**oa,"enabled":oa.get("enabled",False)}]
    arbitrators = [{"id":a.get("id",f"arb{i+1}"),"host":a.get("host",""),
                    "ssh_port":a.get("ssh_port",22),"dc":a.get("dc","DC1"),
                    "enabled":a.get("enabled",True)} for i,a in enumerate(arbs_raw)]
    for nc in cfg.get("nodes",[]):
        ne = next((x for x in nodes if x["id"]==nc.get("id")),None)
        if ne: ne["dc"] = nc.get("dc","")
    return {"nodes":nodes,"arbitrators":arbitrators,"cluster":cfg.get("cluster",{})}

@app.get("/api/node/{node_id}/test-connection")
async def test_connection(node_id: str):
    """Test SSH + MariaDB connectivity for a node. Safe to call before saving."""
    cfg  = load_config()
    node = next((n for n in cfg.get("nodes", []) if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    result = {"node_id": node_id, "ssh": None, "db": None, "errors": []}

    # SSH check
    try:
        import paramiko
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host", ""), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=6, banner_timeout=6,
        )
        _, so, _ = client.exec_command("echo ok", timeout=4)
        out = so.read().decode().strip()
        client.close()
        result["ssh"] = {"ok": out == "ok", "message": "SSH connection successful" if out == "ok" else f"unexpected output: {out}"}
    except Exception as e:
        result["ssh"] = {"ok": False, "message": str(e)}
        result["errors"].append(f"SSH: {e}")

    # DB check
    try:
        import pymysql
        db_cfg = cfg.get("db", {})
        user   = node.get("db_user")   or db_cfg.get("user",     "monitor")
        passwd = node.get("db_password") or db_cfg.get("password", "")
        conn = pymysql.connect(
            host=node["host"], port=int(node.get("port", 3306)),
            user=user, password=passwd,
            connect_timeout=4, read_timeout=4,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        result["db"] = {"ok": True, "message": f"MariaDB connection successful (user={user})"}
    except Exception as e:
        result["db"] = {"ok": False, "message": str(e)}
        result["errors"].append(f"DB: {e}")

    result["ok"] = result["ssh"]["ok"] and result["db"]["ok"]
    return result

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
    dc: Optional[str] = "DC1"

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
    id: Optional[str] = None
    name: Optional[str] = None
    host: str
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key: str = "~/.ssh/id_rsa"
    dc: str = "DC1"

@app.post("/api/config/arbitrator")
async def set_arbitrator(payload: ArbitratorPayload):
    cfg = load_config()
    arbs = cfg.setdefault("arbitrators", [])
    arb_id = payload.id or f"arb{len(arbs)+1:02d}"
    data = {**payload.model_dump(), "id": arb_id, "enabled": True}
    idx = next((i for i,a in enumerate(arbs) if a.get("id")==arb_id), None)
    if idx is not None: arbs[idx] = data
    else: arbs.append(data)
    save_config(cfg)
    _push_event("info", f"Arbitrator added: {arb_id} ({payload.host}) DC={payload.dc}", "config")
    return {"ok": True, "id": arb_id}

@app.delete("/api/config/arbitrator/{arb_id}")
async def remove_arbitrator(arb_id: str):
    cfg = load_config()
    before = len(cfg.get("arbitrators", []))
    cfg["arbitrators"] = [a for a in cfg.get("arbitrators",[]) if a.get("id") != arb_id]
    if len(cfg.get("arbitrators",[])) == before:
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")
    save_config(cfg)
    _push_event("warning", f"Arbitrator removed: {arb_id}", "config")
    return {"ok": True}

@app.delete("/api/config/arbitrator")
async def remove_arbitrator_legacy():
    cfg = load_config(); cfg["arbitrators"] = []; save_config(cfg); return {"ok": True}

@app.put("/api/config/arbitrator/{arb_id}")
async def update_arbitrator(arb_id: str, payload: ArbitratorPayload):
    cfg  = load_config()
    arbs = cfg.setdefault("arbitrators", [])
    idx  = next((i for i, a in enumerate(arbs) if a.get("id") == arb_id), None)
    if idx is None:
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")
    new_id   = payload.id or arb_id
    existing = arbs[idx]
    updated  = {**existing, **payload.model_dump(exclude_none=True), "id": new_id, "enabled": existing.get("enabled", True)}
    arbs[idx] = updated
    # If id changed — remove old entry and use new key
    if new_id != arb_id:
        arbs[idx]["id"] = new_id
    save_config(cfg)
    _push_event("info", f"Arbitrator updated: {new_id} ({payload.host}) DC={payload.dc}", "config")
    return {"ok": True, "id": new_id}

class DbPayload(BaseModel):
    db_user: Optional[str] = None
    db_password: Optional[str] = None
    db_port: Optional[int] = None

@app.post("/api/config/db")
async def save_db_config(payload: DbPayload):
    cfg = load_config()
    db  = cfg.setdefault("db", {})
    if payload.db_user     is not None: db["user"]     = payload.db_user
    if payload.db_password is not None: db["password"] = payload.db_password
    if payload.db_port     is not None:
        cfg.setdefault("settings", {})["db_port"] = payload.db_port
        # Update default port on all nodes that don't have a custom port
        for node in cfg.get("nodes", []):
            if not node.get("port_custom"):
                node["port"] = payload.db_port
    save_config(cfg)
    _push_event("info", f"DB config updated: user={payload.db_user or '(unchanged)'}", "config")
    return {"ok": True}

# ── RELOAD ────────────────────────────────────────────────────
async def _do_reload():
    cfg   = load_config()
    nodes = [n["id"] for n in cfg.get("nodes", []) if n.get("enabled")]
    arbs  = [a for a in cfg.get("arbitrators", []) if a.get("enabled", True)]
    mode  = "mock" if cfg.get("settings", {}).get("use_mock", True) else "real"
    log.info(f"Config reloaded | nodes={nodes} | arbitrators={arbs} | mode={mode}")
    return {"ok": True, "nodes": nodes, "arbitrators": arbs, "mode": mode}

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


# ── C4: GARBD DETAILED LOG ────────────────────────────────────
@app.get("/api/garbd/{arb_id}/log")
async def garbd_log(arb_id: str, lines: int = 30):
    """SSH: journalctl -u garbd last N lines + parse cluster connectivity."""
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)

    arbs = cfg.get("arbitrators", [])
    if not arbs:
        oa = cfg.get("arbitrator", {})
        if oa.get("host"): arbs = [{"id": "arb01", **oa}]
    arb = next((a for a in arbs if a.get("id") == arb_id), None)
    if not arb:
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")

    if use_mock:
        mock_log = [
            "Apr 01 02:00:01 garbd[1234]: Connecting to cluster at gcomm://11.11.11.169:4567,11.11.11.170:4567",
            "Apr 01 02:00:01 garbd[1234]: Established connection to cluster",
            "Apr 01 02:00:02 garbd[1234]: Node state: SYNCED",
            "Apr 01 02:01:00 garbd[1234]: Flow control: state=CLEAR, paused=0",
            "Apr 01 02:02:00 garbd[1234]: Members: 3 (2 nodes + 1 arbitrator)",
        ]
        return {
            "arb_id": arb_id,
            "host": arb.get("host",""),
            "service_status": "active",
            "connected_to_cluster": True,
            "log_lines": mock_log,
            "mock": True
        }

    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            arb.get("host"), port=int(arb.get("ssh_port", 22)),
            username=arb.get("ssh_user", "root"),
            key_filename=str(Path(arb.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=8
        )

        # 1. Service status
        _, so, _ = client.exec_command("systemctl is-active garbd", timeout=5)
        service_status = so.read().decode(errors="replace").strip()

        # 2. Journal log
        _, so2, _ = client.exec_command(
            f"journalctl -u garbd --no-pager -n {lines} --output=short-iso 2>/dev/null || "
            f"journalctl -u garbd --no-pager -n {lines} 2>/dev/null || "
            f"tail -n {lines} /var/log/garbd.log 2>/dev/null || echo 'Log not available'",
            timeout=10
        )
        raw_log = so2.read().decode(errors="replace").strip()
        log_lines = [l for l in raw_log.splitlines() if l.strip()][-lines:]

        # 3. Check cluster connectivity from log
        connected = any(
            kw in raw_log
            for kw in ["Established connection", "SYNCED", "evs::proto", "Connected", "joined cluster"]
        )
        disconnected = any(
            kw in raw_log
            for kw in ["Failed to connect", "Timeout", "Connection refused", "error", "failed"]
        )

        client.close()
        return {
            "arb_id": arb_id,
            "host": arb.get("host", ""),
            "service_status": service_status,
            "connected_to_cluster": connected and not disconnected,
            "log_lines": log_lines,
            "mock": False
        }
    except Exception as e:
        raise HTTPException(502, f"SSH error on arbitrator {arb_id}: {e}")


# ── C5: PROCESSLIST ───────────────────────────────────────────
@app.get("/api/node/{node_id}/processlist")
async def node_processlist(node_id: str):
    """SHOW FULL PROCESSLIST on the specified node."""
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    node = next((n for n in cfg.get("nodes", []) if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        import random, time
        mock_proc = [
            {"Id":1,"User":"monitor_user","Host":"localhost","db":"information_schema","Command":"Query","Time":0,"State":"","Info":"SHOW FULL PROCESSLIST"},
            {"Id":42,"User":"app_user","Host":"10.0.0.5:54321","db":"mydb","Command":"Query","Time":3,"State":"Sending data","Info":"SELECT COUNT(*) FROM orders WHERE status='pending'"},
            {"Id":77,"User":"app_user","Host":"10.0.0.6:55001","db":"mydb","Command":"Sleep","Time":12,"State":"","Info":None},
        ]
        return {"node_id": node_id, "processes": mock_proc, "total": len(mock_proc), "mock": True}

    db_host = node.get("host", "localhost")
    db_port = int(node.get("port", 3306))
    db_user = node.get("db_user") or cfg.get("db", {}).get("user", "monitor_user")
    db_pass = node.get("db_password") or cfg.get("db", {}).get("password", "")

    try:
        import pymysql
    except ImportError:
        raise HTTPException(500, "pymysql not installed")

    try:
        conn = pymysql.connect(
            host=db_host, port=db_port, user=db_user, password=db_pass,
            database="information_schema", connect_timeout=6,
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cur:
            cur.execute("SHOW FULL PROCESSLIST")
            rows = cur.fetchall()
        conn.close()
        processes = [
            {
                "Id": r.get("Id") or r.get("id"),
                "User": r.get("User") or r.get("user"),
                "Host": r.get("Host") or r.get("host"),
                "db": r.get("db"),
                "Command": r.get("Command") or r.get("command"),
                "Time": int(r.get("Time") or r.get("time") or 0),
                "State": r.get("State") or r.get("state") or "",
                "Info": r.get("Info") or r.get("info"),
            }
            for r in rows
        ]
        return {"node_id": node_id, "processes": processes, "total": len(processes), "mock": False}
    except Exception as e:
        raise HTTPException(502, f"DB error on {node_id}: {e}")


class KillQueryPayload(BaseModel):
    process_id: int

@app.post("/api/node/{node_id}/kill-query")
async def node_kill_query(node_id: str, payload: KillQueryPayload):
    """KILL QUERY {id} on a node."""
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    node = next((n for n in cfg.get("nodes", []) if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        _push_event("warning", f"[Mock] KILL QUERY {payload.process_id} on {node_id}", "processlist")
        return {"ok": True, "mock": True, "killed": payload.process_id}

    db_host = node.get("host", "localhost")
    db_port = int(node.get("port", 3306))
    db_user = node.get("db_user") or cfg.get("db", {}).get("user", "monitor_user")
    db_pass = node.get("db_password") or cfg.get("db", {}).get("password", "")

    try:
        import pymysql
    except ImportError:
        raise HTTPException(500, "pymysql not installed")

    try:
        conn = pymysql.connect(
            host=db_host, port=db_port, user=db_user, password=db_pass,
            connect_timeout=6
        )
        with conn.cursor() as cur:
            cur.execute(f"KILL QUERY {int(payload.process_id)}")
        conn.close()
        _push_event("warning", f"KILL QUERY {payload.process_id} executed on {node_id}", "processlist")
        return {"ok": True, "mock": False, "killed": payload.process_id}
    except Exception as e:
        raise HTTPException(502, f"DB error on {node_id}: {e}")


# ── C6: GALERA.CNF COMPARISON ─────────────────────────────────
@app.get("/api/config/compare-galera-cnf")
async def compare_galera_cnf():
    """SSH grep wsrep_ + innodb_ from galera.cnf on all nodes and compare."""
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = [n for n in cfg.get("nodes", []) if n.get("enabled", True)]

    if use_mock:
        mock_result = {}
        for node in nodes:
            nid = node["id"]
            mock_result[nid] = {
                "ok": True,
                "host": node.get("host",""),
                "config": {
                    "wsrep_cluster_name": "test-cluster",
                    "wsrep_cluster_address": "gcomm://11.11.11.169,11.11.11.170",
                    "wsrep_sst_method": "rsync",
                    "wsrep_provider": "/usr/lib/galera/libgalera_smm.so",
                    "wsrep_node_address": node.get("host",""),
                    "innodb_flush_log_at_trx_commit": "0",
                    "innodb_autoinc_lock_mode": "2",
                }
            }
        # Специально создаём расхождение для демо
        if nodes:
            last_id = nodes[-1]["id"]
            mock_result[last_id]["config"]["wsrep_sst_method"] = "mariabackup"

        all_keys = set()
        for v in mock_result.values():
            all_keys.update(v.get("config", {}).keys())

        diffs = {}
        for key in sorted(all_keys):
            vals = {nid: mock_result[nid].get("config", {}).get(key) for nid in mock_result}
            unique_vals = set(v for v in vals.values() if v is not None)
            diffs[key] = {"values": vals, "match": len(unique_vals) <= 1}

        return {"nodes": list(mock_result.keys()), "details": mock_result, "diff": diffs, "mock": True}

    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    CNF_CANDIDATES = [
        "/etc/mysql/mariadb.conf.d/galera.cnf",
        "/etc/mysql/conf.d/galera.cnf",
        "/etc/mysql/galera.cnf",
        "/etc/galera/galera.cnf",
    ]

    def parse_cnf_output(text: str) -> dict:
        """Parse key=value lines from grep output."""
        result = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "=" in line:
                k, _, v = line.partition("=")
                result[k.strip()] = v.strip()
        return result

    node_results = {}
    for node in nodes:
        nid = node["id"]
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                node.get("host"), port=int(node.get("ssh_port", 22)),
                username=node.get("ssh_user", "root"),
                key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
                timeout=8
            )
            # Try each CNF path
            found_cnf = None
            for cnf_path in CNF_CANDIDATES:
                _, so, _ = client.exec_command(f"test -f {cnf_path} && echo exists", timeout=4)
                if "exists" in so.read().decode():
                    found_cnf = cnf_path
                    break

            if not found_cnf:
                node_results[nid] = {"ok": False, "host": node.get("host",""), "error": "galera.cnf not found", "config": {}}
                client.close()
                continue

            _, so, _ = client.exec_command(
                f"grep -E '^[[:space:]]*(wsrep_|innodb_flush|innodb_autoinc|innodb_locks)' {found_cnf} 2>/dev/null",
                timeout=8
            )
            raw = so.read().decode(errors="replace").strip()
            client.close()
            node_results[nid] = {
                "ok": True,
                "host": node.get("host",""),
                "cnf_path": found_cnf,
                "config": parse_cnf_output(raw)
            }
        except Exception as e:
            node_results[nid] = {"ok": False, "host": node.get("host",""), "error": str(e), "config": {}}

    # Build diff matrix
    all_keys = set()
    for v in node_results.values():
        all_keys.update(v.get("config", {}).keys())

    diffs = {}
    for key in sorted(all_keys):
        vals = {nid: node_results[nid].get("config", {}).get(key) for nid in node_results}
        unique_vals = set(v for v in vals.values() if v is not None)
        diffs[key] = {"values": vals, "match": len(unique_vals) <= 1}

    return {"nodes": list(node_results.keys()), "details": node_results, "diff": diffs, "mock": False}




# ── D5: DIAGNOSTICS CHECK-ALL ─────────────────────────────────
@app.get("/api/diagnostics/check-all")
async def diagnostics_check_all():
    """Parallel SSH + DB connectivity check for all enabled nodes."""
    import asyncio, time

    cfg      = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes    = [n for n in cfg.get("nodes", []) if n.get("enabled", True)]

    if use_mock:
        results = []
        for node in nodes:
            results.append({
                "node_id":  node["id"],
                "name":     node.get("name", node["id"]),
                "host":     node.get("host", ""),
                "ok":       True,
                "ssh":      {"ok": True,  "message": "SSH OK (mock)", "latency_ms": 3},
                "db":       {"ok": True,  "message": "MariaDB OK (mock)"},
                "elapsed_ms": 5,
                "mock":     True,
            })
        return {"results": results, "all_ok": True, "checked_at": time.strftime("%H:%M:%S")}

    try:
        import paramiko, pymysql
    except ImportError as e:
        raise HTTPException(500, f"Missing dependency: {e}")

    def check_node(node: dict) -> dict:
        t0  = time.monotonic()
        nid = node["id"]
        out = {
            "node_id": nid,
            "name":    node.get("name", nid),
            "host":    node.get("host", ""),
            "ok":      False,
            "ssh":     {"ok": False, "message": ""},
            "db":      {"ok": False, "message": ""},
            "elapsed_ms": 0,
            "mock":    False,
        }

        # SSH
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                node.get("host", ""), port=int(node.get("ssh_port", 22)),
                username=node.get("ssh_user", "root"),
                key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
                timeout=6, banner_timeout=6,
            )
            _, so, _ = client.exec_command("echo ok", timeout=4)
            ssh_out = so.read().decode().strip()
            client.close()
            out["ssh"] = {"ok": ssh_out == "ok", "message": "SSH OK" if ssh_out == "ok" else f"unexpected: {ssh_out}",
                          "latency_ms": int((time.monotonic() - t0) * 1000)}
        except Exception as e:
            out["ssh"] = {"ok": False, "message": str(e)}

        # DB
        t1 = time.monotonic()
        try:
            db_cfg = cfg.get("db", {})
            user   = node.get("db_user")   or db_cfg.get("user",     "monitor")
            passwd = node.get("db_password") or db_cfg.get("password", "")
            conn = pymysql.connect(
                host=node["host"], port=int(node.get("port", 3306)),
                user=user, password=passwd,
                connect_timeout=4, read_timeout=4,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT @@wsrep_local_state_comment")
                state = cur.fetchone()
            conn.close()
            state_str = state[0] if state else "unknown"
            out["db"] = {"ok": True, "message": f"MariaDB OK · state={state_str}",
                         "latency_ms": int((time.monotonic() - t1) * 1000)}
        except Exception as e:
            out["db"] = {"ok": False, "message": str(e)}

        out["ok"] = out["ssh"]["ok"] and out["db"]["ok"]
        out["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
        return out

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes) or 1) as executor:
        futures = {executor.submit(check_node, n): n for n in nodes}
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    all_ok = all(r["ok"] for r in results)
    _push_event(
        "info" if all_ok else "warning",
        "Diagnostics check-all: " + ("all OK" if all_ok else f"{sum(1 for r in results if not r['ok'])}/{len(results)} FAIL"),
        "diagnostics"
    )
    return {"results": results, "all_ok": all_ok, "checked_at": time.strftime("%H:%M:%S")}


# ── D2: INNODB STATUS ─────────────────────────────────────────
@app.get("/api/node/{node_id}/innodb-status")
async def node_innodb_status(node_id: str):
    """SHOW ENGINE INNODB STATUS — full output with parsed sections."""
    cfg      = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)

    if use_mock:
        mock_output = """=====================================
2026-04-01 02:00:00 0x7f1234 INNODB MONITOR OUTPUT
=====================================
Per second averages calculated from the last 4 seconds
-----------------
BACKGROUND THREAD
-----------------
srv_master_thread loops: 10 srv_active, 0 srv_shutdown, 5 srv_idle
--------------
SEMAPHORES
--------------
OS WAIT ARRAY INFO: reservation count 12, signal count 12
RW-shared spins 0, rounds 0, OS waits 0
--------------
TRANSACTIONS
--------------
Trx id counter 10584
Purge done for trx's n:o < 10580 undo n:o < 0 state: running
History list length 0
LIST OF TRANSACTIONS FOR EACH SESSION:
---TRANSACTION 10583, ACTIVE 0 sec
MySQL thread id 5, OS thread handle 139710, query id 120 localhost root
SHOW ENGINE INNODB STATUS
--------
FILE I/O
--------
I/O thread 0 state: waiting for completed aio requests (insert buffer thread)
I/O thread 1 state: waiting for completed aio requests (log thread)
-------------------------------------
INSERT BUFFER AND ADAPTIVE HASH INDEX
-------------------------------------
Ibuf: size 1, free list len 0, seg size 2, 0 merges
Hash table size 34679, node heap has 0 buffer(s)
---
LOG
---
Log sequence number 17394583
Log flushed up to   17394583
Pages flushed up to 17394583
Last checkpoint at  17394574
----------------------------
BUFFER POOL AND MEMORY
----------------------------
Total large memory allocated 137363456
Buffer pool size   8192
Free buffers       7737
Database pages     453
--------------
ROW OPERATIONS
--------------
0 queries inside InnoDB, 0 queries in queue
----------------------------
END OF INNODB MONITOR OUTPUT
============================"""
        return {
            "node_id": node_id,
            "raw": mock_output,
            "sections": _parse_innodb_sections(mock_output),
            "mock": True,
        }

    nodes = [n for n in cfg.get("nodes", []) if n.get("id") == node_id or n.get("name") == node_id]
    if not nodes:
        raise HTTPException(404, f"Node '{node_id}' not found")
    node = nodes[0]

    try:
        import pymysql
        db_cfg = cfg.get("db", {})
        user   = node.get("db_user")   or db_cfg.get("user",     "monitor")
        passwd = node.get("db_password") or db_cfg.get("password", "")
        conn   = pymysql.connect(
            host=node["host"], port=int(node.get("port", 3306)),
            user=user, password=passwd,
            connect_timeout=6, read_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SHOW ENGINE INNODB STATUS")
            row = cur.fetchone()
        conn.close()
        raw = row[2] if row and len(row) >= 3 else (row[0] if row else "")
        _push_event("info", f"InnoDB status fetched for {node_id}", "diagnostics")
        return {
            "node_id": node_id,
            "raw": raw,
            "sections": _parse_innodb_sections(raw),
            "mock": False,
        }
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")


def _parse_innodb_sections(raw: str) -> dict:
    """Split InnoDB status output into named sections."""
    import re
    section_re = re.compile(r'^-{3,}\n([A-Z][A-Z ]+)\n-{3,}', re.MULTILINE)
    # Also handle ===... headers
    lines = raw.splitlines()
    sections: dict = {}
    current = "HEADER"
    buf: list = []

    for line in lines:
        # Detect section separators like "---" or "====="
        stripped = line.strip("-= \t")
        if set(line.strip()) <= set("-=") and len(line.strip()) >= 3:
            if buf and current:
                sections[current] = "\n".join(buf).strip()
            current = None
            buf = []
        elif current is None and line.strip() and not set(line.strip()) <= set("-="):
            current = line.strip()
        else:
            buf.append(line)

    if current and buf:
        sections[current] = "\n".join(buf).strip()

    # Key metrics extraction
    metrics = {}
    for line in lines:
        if "History list length" in line:
            try: metrics["history_list_length"] = int(line.split()[-1])
            except: pass
        if "queries inside InnoDB" in line:
            try: metrics["active_queries"] = int(line.split()[0])
            except: pass
        if "Buffer pool size" in line and "Buffer pool size   " in line:
            try: metrics["buffer_pool_pages"] = int(line.split()[-1])
            except: pass
        if "Free buffers" in line:
            try: metrics["free_buffers"] = int(line.split()[-1])
            except: pass

    return {"sections": sections, "metrics": metrics}


# ── WEBSOCKET MANAGER ─────────────────────────────────────────
class _WsManager:
    def __init__(self):
        self._clients: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._clients.append(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            try: self._clients.remove(ws)
            except ValueError: pass

    async def broadcast(self, payload: dict):
        msg = json.dumps(payload, ensure_ascii=False, default=str)
        dead = []
        async with self._lock:
            clients = list(self._clients)
        for ws in clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)

    @property
    def count(self):
        return len(self._clients)

    async def shutdown(self):
        """Close all active WebSocket connections on application shutdown."""
        async with self._lock:
            clients = list(self._clients)
            self._clients.clear()
        for ws in clients:
            try:
                await ws.close()
            except Exception:
                pass

# Module-level sentinel: used only during import/startup before lifespan runs.
# After lifespan startup ``app.state.ws_manager`` is the authoritative instance.
_ws_manager_sentinel: "_WsManager | None" = None





@app.websocket("/ws/cluster")
async def ws_cluster(ws: WebSocket, request: Request):
    """Main cluster WebSocket: sends status on interval + events in real time.

    The WebSocket manager is retrieved from ``request.app.state.ws_manager``
    (set during lifespan) instead of a module-level global.
    """
    mgr: _WsManager = request.app.state.ws_manager
    await mgr.connect(ws)
    try:
        # Send initial status immediately
        cfg  = load_config()
        data = get_cluster_status(cfg)
        await ws.send_text(json.dumps({"type": "status", **data}, default=str))

        while True:
            cfg      = load_config()
            interval = cfg.get("settings", {}).get("poll_interval", 5)
            # Wait for interval or until client sends a ping/message
            try:
                text = await asyncio.wait_for(ws.receive_text(), timeout=float(interval))
                if text == "ping":
                    await ws.send_text(json.dumps({"type": "pong"}))
            except asyncio.TimeoutError:
                pass  # Normal — just poll
            except Exception:
                break

            data = get_cluster_status(cfg)
            await ws.send_text(json.dumps({"type": "status", **data}, default=str))

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await mgr.disconnect(ws)

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


# ── DISK / SYSTEM HEALTH ─────────────────────────────────────
@app.get("/api/diagnostics/system-health")
async def diagnostics_system_health():
    """SSH: collect df/free/uptime for every node in parallel.
    Returns per-node disk, memory, and load metrics with threshold flags.
    Thresholds: disk_warn=80%, disk_crit=90%; mem_warn=85%, mem_crit=95%.
    """
    import concurrent.futures, re as _re

    cfg      = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes    = [n for n in cfg.get("nodes", []) if n.get("enabled", True)]

    if not nodes:
        return {"results": [], "all_ok": True, "checked_at": __import__("time").strftime("%H:%M:%S")}

    if use_mock:
        import random, time as _t
        mock_results = []
        for n in nodes:
            disk_pct = random.randint(40, 75)
            mem_pct  = random.randint(30, 65)
            mock_results.append({
                "node_id": n["id"], "name": n.get("name", n["id"]), "host": n.get("host", ""),
                "ok": True,
                "disk_data":  {"path": "/var/lib/mysql", "used_pct": disk_pct, "used": f"{random.randint(10,80)}G", "avail": f"{random.randint(20,120)}G", "total": "200G"},
                "disk_root":  {"path": "/",              "used_pct": random.randint(20, 60), "used": f"{random.randint(5,30)}G",  "avail": f"{random.randint(30,80)}G",  "total": "100G"},
                "memory":     {"used_pct": mem_pct, "used": f"{random.randint(2,12)}G", "total": f"{random.randint(16,64)}G", "free": f"{random.randint(1,8)}G"},
                "load_avg":   {"1m": round(random.uniform(0.1, 2.5), 2), "5m": round(random.uniform(0.1, 2.0), 2), "15m": round(random.uniform(0.1, 1.5), 2)},
                "uptime":     f"up {random.randint(1, 300)} days",
                "warn": disk_pct >= 80 or mem_pct >= 85,
                "crit": disk_pct >= 90 or mem_pct >= 95,
                "mock": True,
            })
        return {"results": mock_results, "all_ok": all(not r["crit"] for r in mock_results), "checked_at": _t.strftime("%H:%M:%S"), "mock": True}

    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    def _parse_df(line: str) -> dict:
        """Parse a single df -h output line: Filesystem Size Used Avail Use% Mountpoint"""
        parts = line.split()
        if len(parts) < 6:
            return {}
        pct_str = parts[4].rstrip("%")
        try:
            pct = int(pct_str)
        except ValueError:
            pct = 0
        return {"path": parts[5], "total": parts[1], "used": parts[2], "avail": parts[3], "used_pct": pct}

    def _parse_free(output: str) -> dict:
        """Parse free -h output; return used_pct, used, free, total."""
        for line in output.splitlines():
            if line.lower().startswith("mem:"):
                parts = line.split()
                if len(parts) >= 3:
                    total_str = parts[1]
                    used_str  = parts[2]
                    free_str  = parts[3] if len(parts) > 3 else "?"
                    # Convert to MB for % calc (strip G/M suffix)
                    def _mb(s):
                        s = s.strip()
                        try:
                            if s.endswith("G") or s.endswith("Gi"): return float(s.rstrip("GiB")) * 1024
                            if s.endswith("M") or s.endswith("Mi"): return float(s.rstrip("MiB"))
                            return float(s)
                        except Exception: return 0
                    t_mb = _mb(total_str)
                    u_mb = _mb(used_str)
                    pct  = int(u_mb / t_mb * 100) if t_mb > 0 else 0
                    return {"total": total_str, "used": used_str, "free": free_str, "used_pct": pct}
        return {"total": "?", "used": "?", "free": "?", "used_pct": 0}

    def _parse_uptime(output: str) -> dict:
        """Parse uptime output for load averages."""
        m = _re.search(r"load average[s]?:\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)", output)
        uptime_m = _re.search(r"up\s+(.+?),\s+\d+ user", output)
        return {
            "1m":  float(m.group(1)) if m else 0.0,
            "5m":  float(m.group(2)) if m else 0.0,
            "15m": float(m.group(3)) if m else 0.0,
        }, (uptime_m.group(1).strip() if uptime_m else output.strip()[:40])

    DISK_WARN = 80
    DISK_CRIT = 90
    MEM_WARN  = 85
    MEM_CRIT  = 95

    def _check_node(node: dict) -> dict:
        nid = node["id"]
        out = {
            "node_id": nid, "name": node.get("name", nid), "host": node.get("host", ""),
            "ok": False, "disk_data": {}, "disk_root": {}, "memory": {},
            "load_avg": {}, "uptime": "", "warn": False, "crit": False, "mock": False,
            "error": None,
        }
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                node.get("host"), port=int(node.get("ssh_port", 22)),
                username=node.get("ssh_user", "root"),
                key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
                timeout=8,
            )
            try:
                cmds = {
                    "df_data": "df -h /var/lib/mysql 2>/dev/null | tail -1",
                    "df_root": "df -h / 2>/dev/null | tail -1",
                    "free":    "free -h 2>/dev/null",
                    "uptime":  "uptime 2>/dev/null",
                }
                raw = {}
                for key, cmd in cmds.items():
                    _, so, _ = client.exec_command(cmd, timeout=6)
                    raw[key] = so.read().decode(errors="replace").strip()
            finally:
                client.close()

            out["disk_data"] = _parse_df(raw.get("df_data", ""))
            out["disk_root"] = _parse_df(raw.get("df_root", ""))
            out["memory"]    = _parse_free(raw.get("free", ""))
            load_dict, uptime_str = _parse_uptime(raw.get("uptime", ""))
            out["load_avg"]  = load_dict
            out["uptime"]    = uptime_str
            out["ok"]        = True

            # Compute alert flags
            d_pct = out["disk_data"].get("used_pct", 0)
            r_pct = out["disk_root"].get("used_pct", 0)
            m_pct = out["memory"].get("used_pct", 0)
            out["warn"] = d_pct >= DISK_WARN or r_pct >= DISK_WARN or m_pct >= MEM_WARN
            out["crit"] = d_pct >= DISK_CRIT or r_pct >= DISK_CRIT or m_pct >= MEM_CRIT

        except Exception as e:
            out["error"] = str(e)
        return out

    import time as _t
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(nodes), 8)) as pool:
        futures  = {pool.submit(_check_node, n): n for n in nodes}
        results  = [f.result() for f in concurrent.futures.as_completed(futures)]

    crit_count = sum(1 for r in results if r["crit"])
    warn_count = sum(1 for r in results if r["warn"] and not r["crit"])
    all_ok     = not crit_count and not warn_count

    _push_event(
        "error"   if crit_count else "warning" if warn_count else "info",
        f"system-health: " + (
            f"{crit_count} CRIT, {warn_count} WARN" if not all_ok else "all OK"
        ),
        "diagnostics",
        )
    return {
        "results": results, "all_ok": all_ok,
        "crit_count": crit_count, "warn_count": warn_count,
        "checked_at": _t.strftime("%H:%M:%S"),
    }

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

    def _ssh_node_run(node, *cmds, timeout=30):
        """Open ONE SSH connection, run all cmds sequentially, return list of (ec, out, err)."""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host"), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=10,
        )
        results = []
        try:
            for cmd in cmds:
                _, so, se = client.exec_command(cmd, timeout=timeout)
                out = so.read().decode(errors="replace").strip()
                err = se.read().decode(errors="replace").strip()
                ec  = so.channel.recv_exit_status()
                results.append((ec, out, err))
        finally:
            client.close()
        return results

    # Step 1: pre-bootstrap systemd check — mariadb must be stopped on non-candidate nodes
    active_nodes = []
    for node in nodes:
        if node["id"] == payload.candidate:
            continue
        try:
            [(ec, state, _)] = _ssh_node_run(node, "systemctl is-active mariadb.service", timeout=8)
            if state == "active":
                active_nodes.append(node["id"])
        except Exception as e:
            steps.append({"step": 1, "status": "error", "message": f"systemd check failed on {node['id']}: {e}"})
            return {"ok": False, "mock": False, "steps": steps}

    if active_nodes:
        steps.append({
            "step": 1, "status": "error",
            "message": "Bootstrap blocked: stop mariadb.service on non-candidate nodes first: " + ", ".join(active_nodes)
        })
        return {"ok": False, "mock": False, "steps": steps}

    steps.append({"step": 1, "status": "ok",
                  "message": "Pre-bootstrap systemd check passed: mariadb.service stopped on all non-candidate nodes"})

    # Step 2: galera_new_cluster on candidate (single SSH session)
    try:
        [(ec, out, err)] = _ssh_node_run(candidate, "galera_new_cluster", timeout=60)
        if ec != 0:
            raise Exception(err or f"exit_code={ec}")
        steps.append({"step": 2, "status": "ok",
                      "message": f"galera_new_cluster на {payload.candidate} — OK"})
    except Exception as e:
        steps.append({"step": 2, "status": "error", "message": f"galera_new_cluster failed: {e}"})
        return {"ok": False, "mock": False, "steps": steps}

    # Step 3: start other nodes — each gets one SSH session for the start command
    joiners = [n for n in nodes if n["id"] != payload.candidate]
    for i, n in enumerate(joiners, 3):
        try:
            [(ec, out, err)] = _ssh_node_run(n, "systemctl start mariadb.service", timeout=30)
            steps.append({"step": i, "status": "ok",
                          "message": f"systemctl start mariadb на {n['id']} — exit={ec}"})
        except Exception as e:
            steps.append({"step": i, "status": "error", "message": f"{n['id']}: {e}"})

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

    # Build command list, then run all in a SINGLE SSH session
    cmds = ["systemctl stop mariadb.service"]
    if payload.method == "sst":
        cmds.append("sed -i 's/wsrep_sst_method=.*/wsrep_sst_method=rsync/' /etc/mysql/mariadb.conf.d/galera.cnf")
    cmds.append("systemctl start mariadb.service")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            node.get("host"), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=10,
        )
        for i, cmd in enumerate(cmds, 1):
            try:
                _, so, se = client.exec_command(cmd, timeout=60)
                out = so.read().decode(errors="replace").strip()
                err = se.read().decode(errors="replace").strip()
                ec  = so.channel.recv_exit_status()
                steps.append({"step": i, "status": "ok", "message": f"{cmd} → exit={ec}"})
                if ec != 0:
                    steps.append({"step": i + 1, "status": "error", "message": err or "non-zero exit"})
                    return {"ok": False, "mock": False, "steps": steps}
            except Exception as e:
                steps.append({"step": i, "status": "error", "message": str(e)})
                return {"ok": False, "mock": False, "steps": steps}
    finally:
        client.close()

    steps.append({"step":len(steps)+1,"status":"done","message":f"Rejoin {payload.node_id} запущен. Ждите Synced."})
    return {"ok": True, "mock": False, "node_id": payload.node_id, "method": payload.method, "steps": steps}

# ── SET SST DONOR ───────────────────────────────────────────
class SetDonorPayload(BaseModel):
    donor: str  # node name (wsrep_node_name) to use as SST donor

@app.post("/api/node/{node_id}/set-donor")
async def set_sst_donor(node_id: str, payload: SetDonorPayload):
    """Set wsrep_sst_donor on a node via SQL to control which node provides SST."""
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])
    node = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    if use_mock:
        _push_event("info", f"[Mock] wsrep_sst_donor on {node_id} set to {payload.donor}", "sst")
        return {"ok": True, "mock": True, "node_id": node_id, "donor": payload.donor}
    try:
        import pymysql
    except ImportError:
        raise HTTPException(500, "pymysql not installed")
    db_cfg = cfg.get("db", {})
    user   = node.get("db_user") or db_cfg.get("user", "monitor")
    passwd = node.get("db_password") or db_cfg.get("password", "")
    try:
        conn = pymysql.connect(
            host=node["host"], port=int(node.get("port", 3306)),
            user=user, password=passwd,
            connect_timeout=4, read_timeout=5,
            cursorclass=pymysql.cursors.Cursor,
        )
        with conn.cursor() as cur:
            cur.execute(f"SET GLOBAL wsrep_sst_donor = %s", (payload.donor,))
        conn.close()
        _push_event("info", f"wsrep_sst_donor on {node_id} set to {payload.donor}", "sst")
        return {"ok": True, "mock": False, "node_id": node_id, "donor": payload.donor}
    except Exception as e:
        log.error(f"set-donor {node_id}: {e}")
        raise HTTPException(502, f"SQL error: {e}")

# ── RESET GRASTATE ──────────────────────────────────────────
@app.post("/api/node/{node_id}/reset-grastate")
async def reset_grastate(node_id: str):
    """Set safe_to_bootstrap=1 in grastate.dat via SSH.
    Use ONLY when all nodes have seqno=-1 after full cluster crash.
    Must be done on the node with the highest seqno.
    """
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])
    node = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    if use_mock:
        _push_event("warn", f"[Mock] grastate.dat reset: safe_to_bootstrap=1 on {node_id}", "recovery")
        return {"ok": True, "mock": True, "node_id": node_id,
                "message": f"[mock] sed grastate.dat safe_to_bootstrap: 0 → 1 on {node_id}"}
    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host"), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=10
        )
        grastate_path = "/var/lib/mysql/grastate.dat"
        cmd = f"sed -i 's/safe_to_bootstrap: 0/safe_to_bootstrap: 1/' {grastate_path}"
        _, so, se = client.exec_command(cmd, timeout=15)
        ec = so.channel.recv_exit_status()
        err = se.read().decode(errors="replace").strip()
        client.close()
        if ec != 0:
            raise Exception(f"exit={ec} | {err}")
        _push_event("warn", f"grastate.dat MODIFIED: safe_to_bootstrap=1 on {node_id} — ready for bootstrap", "recovery")
        return {"ok": True, "mock": False, "node_id": node_id,
                "message": f"safe_to_bootstrap set to 1 on {node_id}. Run Bootstrap next."}
    except Exception as e:
        log.error(f"reset-grastate {node_id}: {e}")
        raise HTTPException(502, f"SSH error: {e}")

# ── PC.BOOTSTRAP (non-Primary fix via SQL) ───────────────────
@app.post("/api/node/{node_id}/pc-bootstrap")
async def pc_bootstrap(node_id: str):
    """Set wsrep_provider_options='pc.bootstrap=YES' on node via SQL.
    Soft-fixes a cluster stuck in non-Primary WITHOUT restarting MariaDB.
    Use ONLY when quorum was lost but network is restored and no split-brain.
    """
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])
    node = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        _push_event("warn", f"[Mock] pc.bootstrap=YES on {node_id} — cluster promoted to Primary", "recovery")
        return {"ok": True, "mock": True, "node_id": node_id,
                "message": f"[mock] SET GLOBAL wsrep_provider_options='pc.bootstrap=YES' on {node_id}"}

    try:
        import pymysql
    except ImportError:
        raise HTTPException(500, "pymysql not installed")

    db_cfg = cfg.get("db", {})
    user   = node.get("db_user") or db_cfg.get("user", "monitor")
    passwd = node.get("db_password") or db_cfg.get("password", "")

    try:
        conn = pymysql.connect(
            host=node["host"], port=int(node.get("port", 3306)),
            user=user, password=passwd,
            connect_timeout=4, read_timeout=10,
            cursorclass=pymysql.cursors.Cursor,
        )
        with conn.cursor() as cur:
            cur.execute("SET GLOBAL wsrep_provider_options='pc.bootstrap=YES'")
            # Verify state after bootstrap
            import time; time.sleep(1)
            cur.execute("SHOW STATUS LIKE 'wsrep_cluster_status'")
            row = cur.fetchone()
            new_status = row[1] if row else "unknown"
        conn.close()
        ok = new_status == "Primary"
        _push_event(
            "info" if ok else "warn",
            f"pc.bootstrap=YES on {node_id} → wsrep_cluster_status={new_status}",
            "recovery"
        )
        return {"ok": ok, "mock": False, "node_id": node_id,
                "cluster_status": new_status,
                "message": f"wsrep_cluster_status = {new_status}"}
    except Exception as e:
        log.error(f"pc-bootstrap {node_id}: {e}")
        raise HTTPException(502, f"SQL error: {e}")

# ── WSREP_RECOVER (seqno when grastate=-1) ───────────────────
@app.post("/api/node/{node_id}/wsrep-recover")
async def wsrep_recover(node_id: str):
    """Run mysqld --wsrep-recover via SSH to determine real seqno
    when grastate.dat shows seqno: -1 (dirty shutdown / full crash).
    MariaDB must be STOPPED on the node before calling this.
    """
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])
    node = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        import random, time
        elapsed = int(time.time())
        fake_seqno = 485734 + random.randint(0, 50)
        _push_event("info", f"[Mock] wsrep-recover on {node_id}: seqno={fake_seqno}", "recovery")
        return {"ok": True, "mock": True, "node_id": node_id,
                "seqno": fake_seqno,
                "uuid": "5a7b1c2d-dead-beef-cafe-0123456789ab",
                "message": f"[mock] Recovered position: 5a7b1c2d-dead-beef-cafe-0123456789ab:{fake_seqno}"}

    try:
        import paramiko, re
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host"), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=10
        )
        # mysqld --wsrep-recover writes result to stderr
        cmd = ("mysqld --wsrep-recover 2>&1 | grep -E 'Recovered position|WSREP: Recovered' | tail -1")
        _, so, _ = client.exec_command(cmd, timeout=60)
        output = so.read().decode(errors="replace").strip()
        client.close()

        # Parse "Recovered position: <uuid>:<seqno>"
        m = re.search(r'Recovered position.*?([0-9a-f-]{36}):(-?\d+)', output, re.IGNORECASE)
        if m:
            uuid_val  = m.group(1)
            seqno_val = int(m.group(2))
        else:
            uuid_val  = "unknown"
            seqno_val = -1

        _push_event("info", f"wsrep-recover {node_id}: seqno={seqno_val} uuid={uuid_val}", "recovery")
        return {"ok": True, "mock": False, "node_id": node_id,
                "seqno": seqno_val, "uuid": uuid_val,
                "raw": output,
                "message": f"Recovered position: {uuid_val}:{seqno_val}"}
    except Exception as e:
        log.error(f"wsrep-recover {node_id}: {e}")
        raise HTTPException(502, f"SSH error: {e}")


# ── WSREP_RECOVER — BATCH (all nodes) ────────────────────────
@app.post("/api/wsrep-recover-all")
async def wsrep_recover_all():
    """Run mysqld --wsrep-recover on ALL nodes in parallel.
    MariaDB must be STOPPED on every node before calling this.
    Returns per-node seqno + auto-selected bootstrap candidate (highest seqno).
    """
    cfg      = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes    = cfg.get("nodes", [])

    if not nodes:
        raise HTTPException(400, "No nodes configured")

    if use_mock:
        import random, time
        base = 485700 + random.randint(0, 100)
        results = []
        for i, n in enumerate(nodes):
            seqno = base + random.randint(0, 80) if i > 0 else base + 120
            results.append({
                "node_id": n["id"], "name": n.get("name", n["id"]),
                "host": n.get("host", ""),
                "ok": True, "seqno": seqno,
                "uuid": "5a7b1c2d-dead-beef-cafe-0123456789ab",
                "message": f"[mock] Recovered position: 5a7b1c2d-dead-beef-cafe-0123456789ab:{seqno}",
            })
        best = max(results, key=lambda r: r["seqno"])
        _push_event("info", f"[Mock] wsrep-recover-all: candidate={best['node_id']} seqno={best['seqno']}", "recovery")
        return {
            "ok": True, "mock": True,
            "results": results,
            "candidate": best["node_id"],
            "candidate_seqno": best["seqno"],
        }

    try:
        import paramiko, re, concurrent.futures
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    def _recover_node(node: dict) -> dict:
        nid = node["id"]
        result = {
            "node_id": nid, "name": node.get("name", nid),
            "host": node.get("host", ""),
            "ok": False, "seqno": -1, "uuid": "unknown",
            "raw": "", "message": "",
        }
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                node.get("host"), port=int(node.get("ssh_port", 22)),
                username=node.get("ssh_user", "root"),
                key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
                timeout=10,
            )
            try:
                cmd = "mysqld --wsrep-recover 2>&1 | grep -E 'Recovered position|WSREP: Recovered' | tail -1"
                _, so, _ = client.exec_command(cmd, timeout=90)
                output = so.read().decode(errors="replace").strip()
            finally:
                client.close()

            m = re.search(r'Recovered position.*?([0-9a-f-]{36}):(-?\d+)', output, re.IGNORECASE)
            if m:
                result["uuid"]  = m.group(1)
                result["seqno"] = int(m.group(2))
                result["ok"]    = True
            result["raw"]     = output
            result["message"] = f"Recovered position: {result['uuid']}:{result['seqno']}"
        except Exception as e:
            result["message"] = str(e)
        return result

    max_workers = min(len(nodes), 8)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_recover_node, n): n for n in nodes}
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    ok_results = [r for r in results if r["ok"] and r["seqno"] >= 0]
    if ok_results:
        best = max(ok_results, key=lambda r: r["seqno"])
    else:
        best = None

    _push_event(
        "info" if best else "error",
        "wsrep-recover-all: " + (
            f"candidate={best['node_id']} seqno={best['seqno']}" if best
            else "no node returned valid seqno"
        ),
        "recovery",
        )
    return {
        "ok": bool(best),
        "mock": False,
        "results": results,
        "candidate":       best["node_id"]  if best else None,
        "candidate_seqno": best["seqno"]    if best else -1,
    }

# ── GARBD STATUS ─────────────────────────────────────────────
@app.get("/api/garbd")
async def get_garbd():
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    arbs = cfg.get("arbitrators", [])
    if not arbs:
        oa = cfg.get("arbitrator", {})
        if oa.get("host"): arbs = [{"id":"arb01","dc":"DC1",**oa}]
    enabled = [a for a in arbs if a.get("enabled", True) and a.get("host")]
    if not enabled: return {"arbitrators":[], "enabled":False}
    if use_mock:
        return {"arbitrators":[{"id":a.get("id"),"dc":a.get("dc","DC1"),"host":a.get("host",""),"online":True,"enabled":True} for a in enabled],"enabled":True}
    try: import paramiko
    except ImportError: raise HTTPException(500,"paramiko not installed")
    results=[]
    for arb in enabled:
        try:
            c=paramiko.SSHClient(); c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            c.connect(arb.get("host"),port=int(arb.get("ssh_port",22)),
                      username=arb.get("ssh_user","root"),
                      key_filename=str(Path(arb.get("ssh_key","~/.ssh/id_rsa")).expanduser()),timeout=6)
            _,so,_=c.exec_command("systemctl is-active garbd",timeout=10)
            out=so.read().decode(errors="replace").strip(); ec=so.channel.recv_exit_status(); c.close()
            results.append({"id":arb.get("id"),"dc":arb.get("dc",""),"host":arb.get("host",""),"online":ec==0,"enabled":True})
        except Exception as e:
            results.append({"id":arb.get("id"),"dc":arb.get("dc",""),"host":arb.get("host",""),"online":False,"enabled":True,"error":str(e)})
    return {"arbitrators":results,"enabled":True}


# ── NODE SSH PING ─────────────────────────────────────────────
@app.get("/api/node/{node_id}/ping")
async def node_ping(node_id: str):
    """Quick SSH reachability check + systemctl is-active."""
    cfg      = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    node     = next((n for n in cfg.get("nodes", []) if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        return {"ok": True, "mock": True, "node_id": node_id,
                "reachable": True, "latency_ms": 2, "service": "active"}

    import time
    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")

    t0 = time.monotonic()
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host", ""), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=6, banner_timeout=6,
        )
        _, so, _ = client.exec_command("systemctl is-active mariadb.service", timeout=5)
        service_state = so.read().decode(errors="replace").strip()
        client.close()
        latency = int((time.monotonic() - t0) * 1000)
        return {"ok": True, "mock": False, "node_id": node_id,
                "reachable": True, "latency_ms": latency, "service": service_state}
    except Exception as e:
        latency = int((time.monotonic() - t0) * 1000)
        return {"ok": False, "mock": False, "node_id": node_id,
                "reachable": False, "latency_ms": latency,
                "service": "unknown", "error": str(e)}


# ── NODE SSH ACTION ──────────────────────────────────────────
class NodeActionPayload(BaseModel):
    action: str  # start | stop | restart | rejoin | set_read_only | set_read_write

ALLOWED_ACTIONS = {
    "start":          "systemctl start mariadb.service",
    "stop":           "systemctl stop mariadb.service",
    "restart":        "systemctl restart mariadb.service",
    "rejoin":         "systemctl restart mariadb.service",
    "set_read_only":  'mysql -e "SET GLOBAL read_only = ON;"',
    "set_read_write": 'mysql -e "SET GLOBAL read_only = OFF;"',
}

@app.post("/api/node/{node_id}/action")
async def node_action(node_id: str, payload: NodeActionPayload):
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)

    if payload.action not in ALLOWED_ACTIONS:
        raise HTTPException(400, f"Unknown action '{payload.action}'. Allowed: {list(ALLOWED_ACTIONS)}")

    if not cfg.get("settings", {}).get("use_mock", True):
        _check_rate_limit(node_id)

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

# ── SST PROGRESS ─────────────────────────────────────────────
@app.get("/api/node/{node_id}/sst-status")
async def sst_status(node_id: str):
    """Return SST/IST progress for a node.
    Reads wsrep_local_state_comment, wsrep_local_recv_queue via SQL.
    SSH fallback: detect rsync/mariabackup process.
    """
    cfg = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    nodes = cfg.get("nodes", [])
    node = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        import time as _t, random
        elapsed = int(_t.time()) % 30
        if elapsed < 5:
            state = "Joining"; progress = 10 + elapsed * 5
        elif elapsed < 20:
            state = "Joined";  progress = 40 + (elapsed - 5) * 4
        else:
            state = "Synced";  progress = 100
        return {"ok": True, "mock": True, "node_id": node_id,
                "state": state, "progress_pct": min(progress, 100),
                "recv_queue": random.randint(0, 50) if state != "Synced" else 0,
                "sst_method": "rsync", "donor": nodes[0]["id"] if nodes else None,
                "message": f"{node_id}: {state} ({min(progress,100)}%)"}

    result = {"ok": True, "mock": False, "node_id": node_id,
              "state": "unknown", "progress_pct": 0,
              "recv_queue": 0, "send_queue": 0,
              "sst_method": None, "donor": None, "message": ""}

    # SQL: wsrep state
    db_cfg = cfg.get("db", {})
    user   = node.get("db_user") or db_cfg.get("user", "monitor")
    passwd = node.get("db_password") or db_cfg.get("password", "")
    try:
        import pymysql
        conn = pymysql.connect(
            host=node["host"], port=int(node.get("port", 3306)),
            user=user, password=passwd, connect_timeout=4, read_timeout=5,
            cursorclass=pymysql.cursors.Cursor,
        )
        with conn.cursor() as cur:
            for var, key in [
                ("wsrep_local_state_comment", "state"),
                ("wsrep_local_recv_queue",    "recv_queue"),
                ("wsrep_local_send_queue",    "send_queue"),
            ]:
                cur.execute(f"SHOW STATUS LIKE '{var}'")
                row = cur.fetchone()
                if row:
                    result[key] = row[1] if key == "state" else int(row[1])
        conn.close()
    except Exception:
        pass

    # SSH: detect active SST process
    try:
        import paramiko, re as _re
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host"), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=6
        )
        _, so, _ = client.exec_command(
            "pgrep -la rsync 2>/dev/null || pgrep -la mariabackup 2>/dev/null || echo none",
            timeout=8
        )
        proc_out = so.read().decode(errors="replace").strip()
        if "rsync" in proc_out:      result["sst_method"] = "rsync"
        elif "mariabackup" in proc_out: result["sst_method"] = "mariabackup"
        client.close()
    except Exception:
        pass

    state_progress = {
        "Synced": 100, "Joined": 95, "Donor/Desynced": 50,
        "Joining": 15, "Open": 5, "unknown": 0
    }
    result["progress_pct"] = state_progress.get(result["state"], 10)
    result["message"] = f"{node_id}: {result['state']} (recv_queue={result['recv_queue']})"
    return result
