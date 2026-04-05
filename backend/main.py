import asyncio, concurrent.futures, json, logging, os, re, subprocess, time as _time, warnings
from auth import init_auth, is_auth_enabled, verify_password, create_token, require_auth, decode_token, get_token_from_request
from collections import deque, defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

# paramiko uses TripleDES internally which triggers CryptographyDeprecationWarning
# (a subclass of UserWarning, not DeprecationWarning) on import. Suppress it
# until paramiko ships a fix — this is a third-party issue, not ours.
# Must be set BEFORE importing paramiko.
warnings.filterwarnings(
    "ignore",
    message="TripleDES has been moved",
    category=UserWarning,
)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, status
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional

import paramiko
import pymysql
import pymysql.cursors

from config import (
    load_config, save_config,
    get_runtime_mode, set_runtime_mode,
    get_selection, set_selection,
    get_active_cluster, list_contours,
)
from galera_client import get_cluster_status
from mock_data import (
    set_scenario, get_scenario,
    mock_innodb_status, mock_seqno, mock_bootstrap,
    _node_base_seqno,
)


# ── Config cache ──────────────────────────────────────────────
_cfg_cache:      Optional[dict] = None
_cfg_cache_ts:   float          = 0.0
_CFG_CACHE_TTL:  float          = 2.0   # seconds


def _load_config_cached() -> dict:
    """Return config, re-reading from disk at most every _CFG_CACHE_TTL seconds."""
    global _cfg_cache, _cfg_cache_ts
    now = _time.monotonic()
    if _cfg_cache is None or (now - _cfg_cache_ts) > _CFG_CACHE_TTL:
        _cfg_cache    = load_config()
        _cfg_cache_ts = now
    return _cfg_cache


def _invalidate_config_cache():
    """Force next call to _load_config_cached() to re-read the file."""
    global _cfg_cache
    _cfg_cache = None


# ── Shared SSH helper ────────────────────────────────────────
def ssh_run(node: dict, *cmds: str, timeout: int = 30) -> list:
    """Open ONE SSH connection, run all cmds sequentially.

    Returns list of (exit_code, stdout, stderr) tuples.
    Raises ``paramiko.SSHException`` / ``socket.error`` on connection failure.
    """
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
            ec = so.channel.recv_exit_status()
            results.append((ec, out, err))
    finally:
        client.close()
    return results


# ── Unified DB connection helper ─────────────────────────────
def _db_connect(node: dict, cfg: dict, **kwargs):
    db_cfg = cfg.get("db", {})
    host   = node.get("host")
    port   = int(node.get("port") or node.get("db_port") or 3306)
    user   = node.get("db_user")     or db_cfg.get("user",     "root")
    passwd = node.get("db_password") or node.get("db_pass") or db_cfg.get("password", "")
    kwargs.setdefault("connect_timeout", 5)
    return pymysql.connect(
        host=host, port=port, user=user, password=passwd,
        **kwargs
    )


# ── Persistent event log ─────────────────────────────────────────
_LOG_DIR  = Path(__file__).parent.parent / "logs"
_LOG_FILE = _LOG_DIR / "events.log"
# Note: _LOG_DIR is created lazily inside _write_log_entry to avoid
# triggering watchfiles reload on startup.

_event_log: deque = deque(maxlen=500)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("galera_orchestrator")


def _push_event(level: str, msg: str, source: str = "system"):
    entry = {
        "ts":     datetime.utcnow().isoformat() + "Z",
        "level":  level.upper(),
        "msg":    msg,
        "source": source,
    }
    _event_log.append(entry)
    # Write log file in a thread pool to avoid blocking the event loop
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, _write_log_entry, json.dumps(entry))
    except RuntimeError:
        # Called outside async context (e.g. startup) — write synchronously
        _write_log_entry(json.dumps(entry))
    try:
        mgr  = app.state.ws_manager
        loop = asyncio.get_running_loop()
        loop.create_task(mgr.broadcast({"type": "event", **entry}))
    except RuntimeError:
        pass
    except Exception:
        pass


def _write_log_entry(line: str):
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(_LOG_FILE, "a") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


# ── WebSocket manager ────────────────────────────────────────────
class _WsManager:
    """
    Tracks active WebSocket connections and broadcasts JSON messages.
    All public methods are coroutines and must be called from an async context.
    """
    def __init__(self):
        self._connections: list = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket):
        self._connections = [c for c in self._connections if c is not ws]

    async def broadcast(self, data: dict):
        dead = []
        for ws in list(self._connections):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def shutdown(self):
        for ws in list(self._connections):
            try:
                await ws.close()
            except Exception:
                pass
        self._connections.clear()


# ── Rate limiter for SSH actions ─────────────────────────────────
_action_calls: dict = defaultdict(list)
_RATE_LIMIT_MAX    = 5   # max SSH action requests per node
_RATE_LIMIT_WINDOW = 60  # seconds

# Separate rate-limit bucket for cluster-wide operations
_cluster_action_calls: list = []


def _check_rate_limit(node_id: str):
    now          = _time.monotonic()
    window_start = now - _RATE_LIMIT_WINDOW
    _action_calls[node_id] = [t for t in _action_calls[node_id] if t > window_start]
    if len(_action_calls[node_id]) >= _RATE_LIMIT_MAX:
        raise HTTPException(
            429,
            f"Rate limit exceeded for node '{node_id}': "
            f"max {_RATE_LIMIT_MAX} SSH actions per {_RATE_LIMIT_WINDOW}s. Try again later.",
        )
    _action_calls[node_id].append(now)


def _check_cluster_rate_limit():
    """Rate-limit cluster-wide operations (bootstrap, rejoin, wsrep-recover-all)."""
    global _cluster_action_calls
    now          = _time.monotonic()
    window_start = now - _RATE_LIMIT_WINDOW
    _cluster_action_calls = [t for t in _cluster_action_calls if t > window_start]
    if len(_cluster_action_calls) >= _RATE_LIMIT_MAX:
        raise HTTPException(
            429,
            f"Rate limit exceeded for cluster operations: "
            f"max {_RATE_LIMIT_MAX} per {_RATE_LIMIT_WINDOW}s. Try again later.",
        )
    _cluster_action_calls.append(now)


# ── api_version cache (module-level, not function attribute) ──
_version_cache: Optional[dict] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.ws_manager = _WsManager()
    cfg  = _load_config_cached()
    nodes = [n["id"] for n in get_active_cluster(cfg).get("nodes", []) if n.get("enabled")]
    arbs  = [a for a in get_active_cluster(cfg).get("arbitrators", []) if a.get("enabled", True)]
    init_auth(cfg)  # load auth config (enabled/disabled, credentials)
    auth_state = "enabled" if is_auth_enabled() else "disabled"
    contours_info = list_contours(cfg)
    cluster = get_active_cluster(cfg)
    log.info(f"Starting Galera Orchestrator | contours={list(contours_info.keys())} | active_cluster={cluster.get('name')} | auth={auth_state}")
    _push_event("info", f"Galera Orchestrator started | nodes={nodes} | arbitrators={len(arbs)}", "system")
    yield
    await app.state.ws_manager.shutdown()


app = FastAPI(title="Galera Orchestrator", lifespan=lifespan)

FRONTEND = Path(__file__).parent.parent / "frontend"
# /assets mount removed — index.html is a self-contained SPA,
# no static assets are served from the backend.


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(str(FRONTEND / "index.html"))


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(str(FRONTEND / "favicon.ico"))


# ── Health endpoint ───────────────────────────────────────────
@app.get("/api/health")
async def health():
    """Lightweight health check for systemd, load balancers, and uptime monitors."""
    return {"ok": True, "status": "healthy", "ts": datetime.utcnow().isoformat() + "Z"}


# ── Auth endpoints ────────────────────────────────────────────────

@app.get("/api/auth/status")
async def auth_status():
    """Returns whether auth is enabled (public endpoint — used by frontend on load)."""
    return {"enabled": is_auth_enabled()}


@app.post("/api/auth/login")
async def auth_login(request: Request):
    """Login with username+password, receive JWT token."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", ""))

    if not is_auth_enabled():
        # Auth disabled — return a dummy token so frontend works uniformly
        return {"ok": True, "token": create_token("admin"), "username": "admin"}

    cfg = _load_config_cached()
    expected_user = cfg.get("auth", {}).get("username", "admin")

    if username != expected_user or not verify_password(password):
        _push_event("warn", f"Failed login attempt for user '{username}'", "auth")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    token = create_token(username)
    _push_event("info", f"User '{username}' logged in", "auth")
    return {"ok": True, "token": token, "username": username}


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    """Logout — client should discard the token."""
    token = get_token_from_request(request)
    if token:
        username = decode_token(token)
        if username:
            _push_event("info", f"User '{username}' logged out", "auth")
    return {"ok": True}


@app.get("/api/auth/me")
async def auth_me(request: Request):
    """Returns current user info. 401 if not authenticated."""
    require_auth(request)
    token = get_token_from_request(request)
    username = decode_token(token) if token else None
    return {"username": username, "auth_enabled": is_auth_enabled()}


# ── Auth middleware ───────────────────────────────────────────────

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Protect all /api/* routes (except public ones) when auth is enabled."""
    if not is_auth_enabled():
        return await call_next(request)

    path = request.url.path

    # Always public
    if path in {"/", "/api/health", "/api/auth/login", "/api/auth/status", "/favicon.ico"}:
        return await call_next(request)

    # Static frontend files
    if not path.startswith("/api") and not path.startswith("/ws"):
        return await call_next(request)

    # Check token
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "Not authenticated"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    return await call_next(request)


@app.get("/api/status")
async def api_status():
    global _prev_status
    try:
        cfg     = _load_config_cached()
        cluster = get_active_cluster(cfg)
        data    = await asyncio.get_event_loop().run_in_executor(
            None, get_cluster_status, cluster, cfg
        )
        # Add selection metadata to response
        sel = get_selection()
        data["contour"]       = cluster.get("_contour", sel.get("contour", "test"))
        data["cluster_index"] = cluster.get("_index",   sel.get("cluster_index", 0))
        status = data.get("cluster", {}).get("status", "unknown")
        if status != _prev_status:
            _push_event("info", f"Cluster status changed: {_prev_status} → {status}", "monitor")
            _prev_status = status
        return data
    except Exception as e:
        log.error(f"api_status error: {e}")
        raise HTTPException(500, str(e))

_prev_status = None


@app.post("/api/scenario/{name}")
async def set_scenario_api(name: str):
    set_scenario(name)
    _push_event("info", f"Mock scenario set: {name}", "ui")
    return {"ok": True, "scenario": name}


@app.get("/api/scenario")
async def get_scenario_api():
    return {"scenario": get_scenario()}


@app.get("/api/config")
async def get_config():
    cfg = _load_config_cached()
    # Also return active selection for frontend init
    sel     = get_selection()
    cluster = get_active_cluster(cfg)
    return {**cfg, "_selection": sel, "_active_cluster": cluster}


# ── Contour / Cluster selection ───────────────────────────────────────────────

@app.get("/api/contours")
async def get_contours():
    """List all contours and their clusters."""
    cfg  = _load_config_cached()
    sel  = get_selection()
    return {
        "contours":      list_contours(cfg),
        "selection":     sel,
        "active_cluster": get_active_cluster(cfg),
    }


@app.post("/api/contours/select")
async def select_cluster(request: Request):
    """Set active contour + cluster. Body: {contour: str, cluster_index: int}"""
    body    = await request.json()
    cfg     = _load_config_cached()
    contour = str(body.get("contour", "test"))
    idx     = int(body.get("cluster_index", 0))

    contours = cfg.get("contours", {})
    if contour not in contours:
        raise HTTPException(404, f"Contour '{contour}' not found")
    clusters = contours[contour].get("clusters", [])
    if not clusters:
        raise HTTPException(404, f"Contour '{contour}' has no clusters")
    if idx >= len(clusters):
        raise HTTPException(400, f"cluster_index {idx} out of range (max {len(clusters)-1})")

    set_selection(contour, idx)
    _invalidate_config_cache()
    cluster = get_active_cluster(_load_config_cached())
    _push_event("info", f"Active cluster: {cluster.get('name')} ({contour})", "ui")
    return {"ok": True, "contour": contour, "cluster_index": idx, "cluster_name": cluster.get("name")}


@app.post("/api/contours/cluster")
async def add_cluster(request: Request):
    """Add a new cluster to a contour. Body: {contour, name, ...}"""
    body    = await request.json()
    contour = str(body.get("contour", "test"))
    name    = str(body.get("name", "")).strip()
    if not name:
        raise HTTPException(400, "Cluster name is required")

    cfg = load_config()
    cfg.setdefault("contours", {})
    cfg["contours"].setdefault(contour, {"clusters": []})
    cfg["contours"][contour].setdefault("clusters", [])

    # Check no duplicate name in this contour
    existing = [c["name"] for c in cfg["contours"][contour]["clusters"]]
    if name in existing:
        raise HTTPException(409, f"Cluster '{name}' already exists in '{contour}'")

    new_cluster = {
        "name":         name,
        "nodes":        [],
        "arbitrators":  [],
    }
    cfg["contours"][contour]["clusters"].append(new_cluster)
    save_config(cfg)
    _invalidate_config_cache()

    # Auto-select the new cluster
    new_idx = len(cfg["contours"][contour]["clusters"]) - 1
    set_selection(contour, new_idx)

    _push_event("info", f"New cluster '{name}' created in '{contour}'", "ui")
    return {"ok": True, "contour": contour, "cluster_index": new_idx, "name": name}


@app.delete("/api/contours/{contour}/cluster/{idx}")
async def delete_cluster(contour: str, idx: int):
    """Delete a cluster from a contour by index."""
    cfg = load_config()
    clusters = cfg.get("contours", {}).get(contour, {}).get("clusters", [])
    if idx >= len(clusters):
        raise HTTPException(404, "Cluster not found")
    removed = clusters.pop(idx)
    save_config(cfg)
    _invalidate_config_cache()
    # Reset selection to first cluster in contour
    new_idx = 0 if clusters else 0
    set_selection(contour, new_idx)
    _push_event("info", f"Cluster '{removed.get('name')}' deleted from '{contour}'", "ui")
    return {"ok": True}


# ── Nodes API (v2 — scoped to active cluster) ──────────────────────────────────

@app.get("/api/nodes")
async def list_nodes():
    cfg     = _load_config_cached()
    cluster = get_active_cluster(cfg)
    return {"nodes": cluster.get("nodes", [])}


def _get_active_nodes(cfg: dict) -> list:
    """Helper: enabled nodes from active cluster."""
    return [n for n in get_active_cluster(cfg).get("nodes", []) if n.get("enabled", True)]


def _find_node(cfg: dict, node_id: str) -> dict | None:
    """Find a node by id in the active cluster."""
    cluster = get_active_cluster(cfg)
    return next((n for n in cluster.get("nodes", []) if n["id"] == node_id), None)


def _find_node_globally(cfg: dict, node_id: str) -> dict | None:
    """Find a node by id across ALL contours and clusters."""
    for contour_data in cfg.get("contours", {}).values():
        for cluster in contour_data.get("clusters", []):
            for n in cluster.get("nodes", []):
                if n["id"] == node_id:
                    return n
    return None


@app.get("/api/node/{node_id}/test-connection")
async def test_node_connection(node_id: str):
    """SSH + DB connectivity check."""
    cfg  = _load_config_cached()
    node = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    # SSH check
    try:
        [(ec, out, err)] = ssh_run(node, "echo ok", timeout=8)
        ssh_ok  = ec == 0 and out.strip() == "ok"
        ssh_msg = "Connected" if ssh_ok else (err or out or "Failed")
    except Exception as e:
        ssh_ok  = False
        ssh_msg = str(e)

    # DB check
    db_ok  = False
    db_msg = "Not tested"
    try:
        conn = _db_connect(node, cfg, connect_timeout=4)
        conn.close()
        db_ok  = True
        db_msg = "Connected"
    except Exception as e:
        db_msg = str(e)

    return {
        "ok":  ssh_ok,
        "ssh": {"ok": ssh_ok, "message": ssh_msg},
        "db":  {"ok": db_ok,  "message": db_msg},
    }


class NodeActionRequest(BaseModel):
    action: str


@app.post("/api/node/{node_id}/action")
async def node_action(node_id: str, body: NodeActionRequest):
    """Execute a predefined SSH action on a single Galera node."""
    _check_rate_limit(node_id)
    cfg  = _load_config_cached()
    node = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    msgs = {
        "stop":          "systemctl stop mariadb",
        "start":         "systemctl start mariadb",
        "restart":       "systemctl restart mariadb",
        "rejoin":        "systemctl restart mariadb",
        "set_read_only": 'mysql -e "SET GLOBAL read_only=1;"',
        "set_read_write":'mysql -e "SET GLOBAL read_only=0;"',
    }
    cmd = msgs.get(body.action)
    if not cmd:
        raise HTTPException(400, f"Unknown action '{body.action}'. Allowed: {list(msgs)}")

    # Mock mode: simulate the action, no real SSH
    if get_runtime_mode():
        from mock_data import mock_ssh_action
        result = mock_ssh_action(node_id, body.action, cmd)
        ok  = result["exit_code"] == 0
        msg = result.get("stdout") or result.get("stderr") or ("ok" if ok else "error")
        _push_event("info" if ok else "error", f"[MOCK] Action '{body.action}' on {node_id}: {msg}", "ui")
        return {"ok": ok, "mock": True, "msg": msg}

    try:
        [(ec, out, err)] = ssh_run(node, cmd, timeout=30)
        ok  = ec == 0
        msg = out or err or ("ok" if ok else "error")
        _push_event("info" if ok else "error", f"Action '{body.action}' on {node_id}: {msg}", "ui")
        return {"ok": ok, "msg": msg}
    except Exception as e:
        _push_event("error", f"Action '{body.action}' on {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


@app.get("/api/config/mode")
async def get_mode():
    use_mock = get_runtime_mode()
    return {"use_mock": use_mock, "mode": "mock" if use_mock else "real"}


@app.post("/api/config/mode")
async def set_mode(request: Request):
    body = await request.json()
    # Accept both use_mock (bool) and mode ("mock"/"real") from frontend
    if "mode" in body:
        use_mock = body["mode"] != "real"
    else:
        use_mock = bool(body.get("use_mock", True))
    # Write to config/mode.json only — nodes.yaml is never touched on mode switch
    set_runtime_mode(use_mock)
    _push_event("info", f"Data mode changed to {'mock' if use_mock else 'real'}", "ui")
    return {"ok": True, "use_mock": use_mock, "mode": "mock" if use_mock else "real"}


class NodeConfig(BaseModel):
    id:       str
    label:    str
    host:     str
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key:  str = "~/.ssh/id_rsa"
    db_port:  int = 3306
    db_user:  str = "root"
    db_pass:  str = ""
    enabled:  bool = True


@app.post("/api/config/node")
async def add_node(node: NodeConfig):
    cfg   = load_config()
    nodes = get_active_cluster(cfg).get("nodes", [])
    if any(n["id"] == node.id for n in nodes):
        raise HTTPException(409, f"Node '{node.id}' already exists")
    node_dict = {
        "id":          node.id,
        "name":        node.label,
        "host":        node.host,
        "port":        node.db_port,
        "ssh_port":    node.ssh_port,
        "ssh_user":    node.ssh_user,
        "ssh_key":     node.ssh_key,
        "db_user":     node.db_user,
        "db_password": node.db_pass,
        "enabled":     node.enabled,
    }
    nodes.append(node_dict)
    cfg["nodes"] = nodes
    save_config(cfg)
    _invalidate_config_cache()
    _push_event("info", f"Node added: {node.id} ({node.host})", "ui")
    return {"ok": True, "node": node_dict}


@app.delete("/api/config/node/{node_id}")
async def delete_node(node_id: str):
    cfg       = load_config()
    nodes     = get_active_cluster(cfg).get("nodes", [])
    new_nodes = [n for n in nodes if n["id"] != node_id]
    if len(new_nodes) == len(nodes):
        raise HTTPException(404, f"Node '{node_id}' not found")
    cfg["nodes"] = new_nodes
    save_config(cfg)
    _invalidate_config_cache()
    _push_event("info", f"Node removed: {node_id}", "ui")
    return {"ok": True}


class ArbitratorConfig(BaseModel):
    id:         str
    label:      str
    host:       str
    ssh_port:   int = 22
    ssh_user:   str = "root"
    ssh_key:    str = "~/.ssh/id_rsa"
    garbd_port: int = 4567
    enabled:    bool = True


@app.post("/api/config/arbitrator")
async def add_arbitrator(arb: ArbitratorConfig):
    cfg  = load_config()
    arbs = get_active_cluster(cfg).get("arbitrators", [])
    if any(a["id"] == arb.id for a in arbs):
        raise HTTPException(409, f"Arbitrator '{arb.id}' already exists")
    arbs.append(arb.dict())
    cfg["arbitrators"] = arbs
    save_config(cfg)
    _invalidate_config_cache()
    _push_event("info", f"Arbitrator added: {arb.id} ({arb.host})", "ui")
    return {"ok": True, "arbitrator": arb.dict()}


@app.delete("/api/config/arbitrator/{arb_id}")
async def delete_arbitrator(arb_id: str):
    cfg      = load_config()
    arbs     = get_active_cluster(cfg).get("arbitrators", [])
    new_arbs = [a for a in arbs if a["id"] != arb_id]
    if len(new_arbs) == len(arbs):
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")
    cfg["arbitrators"] = new_arbs
    save_config(cfg)
    _invalidate_config_cache()
    _push_event("info", f"Arbitrator removed: {arb_id}", "ui")
    return {"ok": True}


@app.delete("/api/config/arbitrator")
async def delete_all_arbitrators():
    cfg = load_config()
    cfg["arbitrators"] = []
    save_config(cfg)
    _invalidate_config_cache()
    _push_event("info", "All arbitrators removed", "ui")
    return {"ok": True}


@app.put("/api/config/arbitrator/{arb_id}")
async def update_arbitrator(arb_id: str, body: ArbitratorConfig):
    """Update an existing arbitrator. Validated via Pydantic — only known fields are accepted."""
    cfg  = load_config()
    arbs = get_active_cluster(cfg).get("arbitrators", [])
    if isinstance(arbs, dict):
        arbs = [arbs]
    idx = next((i for i, a in enumerate(arbs) if a.get("id") == arb_id), None)
    if idx is None:
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")
    # Merge: preserve existing id, update the rest
    arbs[idx].update(body.dict())
    arbs[idx]["id"] = arb_id   # ensure id cannot be changed via body
    cfg["arbitrators"] = arbs
    save_config(cfg)
    _invalidate_config_cache()
    _push_event("info", f"Arbitrator updated: {arb_id}", "ui")
    return {"ok": True, "arbitrator": arbs[idx]}


class DBCredentials(BaseModel):
    db_user: str
    db_pass: str


@app.post("/api/config/db")
async def update_db_credentials(creds: DBCredentials):
    """Update DB credentials for all nodes."""
    cfg   = load_config()
    nodes = get_active_cluster(cfg).get("nodes", [])
    for node in nodes:
        node["db_user"]     = creds.db_user
        node["db_password"] = creds.db_pass
    cfg["nodes"] = nodes
    save_config(cfg)
    _invalidate_config_cache()
    _push_event("info", "DB credentials updated for all nodes", "ui")
    return {"ok": True}


@app.post("/api/reload")
async def reload_config_legacy():
    return {"ok": True, "msg": "Config reloaded (legacy endpoint)"}


@app.post("/api/config/reload")
async def reload_config():
    _invalidate_config_cache()
    cfg = _load_config_cached()
    init_auth(cfg)  # re-read auth settings after reload
    _push_event("info", "Config reloaded via API", "ui")
    return {"ok": True, "nodes": len(get_active_cluster(cfg).get("nodes", []))}


@app.get("/api/prefs")
async def get_prefs():
    cfg = _load_config_cached()
    return cfg.get("prefs", {})


@app.post("/api/prefs")
async def save_prefs(request: Request):
    body = await request.json()
    cfg  = load_config()
    cfg["prefs"] = body
    save_config(cfg)
    _invalidate_config_cache()
    return {"ok": True}


@app.get("/api/garbd/{arb_id}/log")
async def garbd_log(arb_id: str, lines: int = 100):
    """SSH: tail the garbd log from the arbitrator host."""
    cfg = _load_config_cached()
    arbs = get_active_cluster(cfg).get("arbitrators", [])
    arb  = next((a for a in arbs if a["id"] == arb_id), None)
    if not arb:
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")
    cmd = (
        f"UNIT=$(systemctl list-units --type=service --no-legend 2>/dev/null "
        f"| awk '{{print $1}}' | grep -E 'garb|galera-arb' | head -1); "
        f"if [ -n \"$UNIT\" ]; then "
        f"  journalctl -u \"$UNIT\" --no-pager -n {lines} --output=short-iso 2>/dev/null; "
        f"else "
        f"  tail -n {lines} /var/log/garbd.log 2>/dev/null "
        f"  || echo 'Log not found'; "
        f"fi"
    )
    try:
        [(ec, out, err)] = ssh_run(arb, cmd, timeout=15)
        return {"ok": True, "log": out or err}
    except Exception as e:
        return {"ok": False, "log": str(e)}


# ── wsrep-recover (single node) ───────────────────────────────
@app.post("/api/node/{node_id}/wsrep-recover")
async def wsrep_recover(node_id: str):
    """SSH: run galera_recovery / mysqld --wsrep-recover on a single node."""
    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    node     = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        import time as _t
        elapsed   = int(_t.time())
        seqno_val = _node_base_seqno(node_id) + elapsed * 3
        return {
            "ok":      True,
            "mock":    True,
            "node_id": node_id,
            "seqno":   f"5a7b1c2d-dead-beef-cafe-0123456789ab:{seqno_val}",
            "raw":     f"WSREP: Recovered position: 5a7b1c2d-dead-beef-cafe-0123456789ab:{seqno_val}",
        }

    recover_cmd = (
        "galera_recovery 2>/dev/null "
        "|| mysqld --wsrep-recover 2>&1 | grep 'Recovered position' "
        "|| mariadbd --wsrep-recover 2>&1 | grep 'Recovered position'"
    )
    try:
        [(ec, out, err)] = ssh_run(node, recover_cmd, timeout=60)
        text = out or err
        m = re.search(r'Recovered position.*?(\d+:\d+)', text) or \
            re.search(r'position:\s*(\S+)', text)
        seqno_str = m.group(1) if m else "unknown"
        return {"ok": True, "node_id": node_id, "seqno": seqno_str, "raw": text}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── seqno (read grastate.dat from all nodes) ──────────────────
@app.post("/api/seqno")
async def get_seqno(request: Request):
    """SSH: read /var/lib/mysql/grastate.dat from all nodes in parallel.

    Accepts optional body: {nodes?: list[str], candidate?: str, candidate_seqno?: int}
    Returns per-node seqno info used by the Bootstrap Wizard.
    """
    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    nodes    = _get_active_nodes(cfg)

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    if use_mock:
        result = mock_seqno(nodes)
        return {"ok": True, "mock": True, "nodes": result}

    _check_cluster_rate_limit()

    def _read_grastate(node):
        nid = node["id"]
        cmd = (
            "cat /var/lib/mysql/grastate.dat 2>/dev/null || "
            "cat /var/lib/mysql/grastate.dat 2>/dev/null || "
            "echo 'not found'"
        )
        try:
            [(ec, out, err)] = ssh_run(node, cmd, timeout=15)
            text    = out or err
            seqno   = -1
            uuid    = "unknown"
            safe_to = 0
            for line in text.splitlines():
                line = line.strip()
                if line.startswith("seqno:"):
                    try:
                        seqno = int(line.split(":", 1)[1].strip())
                    except ValueError:
                        pass
                elif line.startswith("uuid:"):
                    uuid = line.split(":", 1)[1].strip()
                elif line.startswith("safe_to_bootstrap:"):
                    try:
                        safe_to = int(line.split(":", 1)[1].strip())
                    except ValueError:
                        pass
            return {
                "id": nid, "name": node.get("name", nid), "host": node.get("host", ""),
                "reachable": True, "error": None,
                "seqno": seqno, "safe_to_bootstrap": safe_to, "uuid": uuid,
            }
        except Exception as e:
            return {
                "id": nid, "name": node.get("name", nid), "host": node.get("host", ""),
                "reachable": False, "error": str(e),
                "seqno": -1, "safe_to_bootstrap": 0, "uuid": "unknown",
            }

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_read_grastate, nodes))

    return {"ok": True, "mock": False, "nodes": results}


# ── wsrep-recover-all (parallel, all nodes) ───────────────────
@app.post("/api/wsrep-recover-all")
async def wsrep_recover_all():
    """SSH: run wsrep-recover on ALL nodes in parallel.

    Used by the Bootstrap Wizard to determine which node has the highest seqno.
    """
    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    nodes    = _get_active_nodes(cfg)

    if use_mock:
        import time as _t
        elapsed = int(_t.time())
        results = []
        for n in nodes:
            nid       = n["id"]
            seqno_val = _node_base_seqno(nid) + elapsed * 3
            results.append({
                "node_id": nid,
                "ok":      True,
                "seqno":   f"5a7b1c2d-dead-beef-cafe-0123456789ab:{seqno_val}",
                "raw":     f"WSREP: Recovered position: 5a7b1c2d-dead-beef-cafe-0123456789ab:{seqno_val}",
            })
        return {"ok": True, "mock": True, "nodes": results}

    _check_cluster_rate_limit()

    def _recover_one(node):
        nid = node["id"]
        cmd = (
            "galera_recovery 2>/dev/null "
            "|| mysqld --wsrep-recover 2>&1 | grep 'Recovered position' "
            "|| mariadbd --wsrep-recover 2>&1 | grep 'Recovered position'"
        )
        try:
            [(ec, out, err)] = ssh_run(node, cmd, timeout=60)
            text = out or err
            m = re.search(r'Recovered position.*?(\S+:\d+)', text) or \
                re.search(r'position:\s*(\S+)', text)
            seqno_str = m.group(1) if m else "unknown"
            return {"node_id": nid, "ok": True, "seqno": seqno_str, "raw": text}
        except Exception as e:
            return {"node_id": nid, "ok": False, "seqno": "unknown", "error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_recover_one, nodes))

    return {"ok": True, "mock": False, "nodes": results}


# ── Bootstrap Wizard (6-step orchestrated bootstrap) ─────────
@app.post("/api/bootstrap/wizard")
async def bootstrap_wizard(request: Request):
    """Orchestrated 6-step cluster bootstrap.

    Body: {candidate_id: str}
    Returns step-by-step progress so the frontend wizard can display each stage.
    """
    body         = await request.json()
    candidate_id = body.get("candidate_id") or body.get("node_id")

    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    nodes    = _get_active_nodes(cfg)

    if not nodes:
        raise HTTPException(400, "No enabled nodes found in the active cluster")

    # Auto-select candidate if not provided: pick node with highest seqno (mock)
    # or first available node (real — let the wizard guide the user)
    if not candidate_id:
        if use_mock:
            # In mock mode pick node with highest deterministic seqno
            from mock_data import _node_base_seqno
            candidate_id = max(nodes, key=lambda n: _node_base_seqno(n["id"]))["id"]
        else:
            candidate_id = nodes[0]["id"]

    candidate = next((n for n in nodes if n["id"] == candidate_id), None)
    if not candidate:
        raise HTTPException(404, f"Node '{candidate_id}' not found in active cluster")

    if use_mock:
        steps = mock_bootstrap(candidate_id, nodes)
        return {"ok": True, "mock": True, "candidate_id": candidate_id, "steps": steps}

    _check_cluster_rate_limit()

    steps = []

    def _step(n: int, status: str, msg: str):
        steps.append({"step": n, "status": status, "message": msg})

    # Step 1: Read seqno / grastate
    _step(1, "ok", f"Reading grastate.dat on all nodes…")

    # Step 2: Stop MariaDB on all non-candidate nodes
    other_nodes = [n for n in nodes if n["id"] != candidate_id]
    for n in other_nodes:
        try:
            [(ec, out, err)] = ssh_run(n, "systemctl stop mariadb 2>&1", timeout=30)
            status = "ok" if ec == 0 else "warn"
            _step(2, status, f"SSH → {n['id']}: systemctl stop mariadb — {out or err or 'ok'}")
        except Exception as e:
            _step(2, "error", f"SSH → {n['id']}: stop failed — {e}")

    # Step 3: galera_new_cluster on candidate
    try:
        [(ec, out, err)] = ssh_run(
            candidate,
            "galera_new_cluster 2>&1 || systemctl start mariadb@bootstrap.service 2>&1",
            timeout=60,
        )
        ok  = ec == 0
        msg = out or err or ("Bootstrap started" if ok else "Bootstrap failed")
        _step(3, "ok" if ok else "error", f"SSH → {candidate_id}: galera_new_cluster — {msg}")
        if not ok:
            return {"ok": False, "candidate_id": candidate_id, "steps": steps}
    except Exception as e:
        _step(3, "error", f"SSH → {candidate_id}: {e}")
        return {"ok": False, "candidate_id": candidate_id, "steps": steps}

    _push_event("info", f"Bootstrap wizard: candidate={candidate_id}", "ui")

    # Steps 4+: Start MariaDB on remaining nodes one by one
    for i, n in enumerate(other_nodes, 4):
        try:
            [(ec, out, err)] = ssh_run(n, "systemctl start mariadb 2>&1", timeout=60)
            ok  = ec == 0
            msg = out or err or ("started" if ok else "failed")
            _step(i, "ok" if ok else "warn", f"SSH → {n['id']}: systemctl start mariadb — {msg}")
        except Exception as e:
            _step(i, "error", f"SSH → {n['id']}: {e}")

    _step(len(steps) + 1, "done", "Bootstrap complete.")

    return {"ok": True, "mock": False, "candidate_id": candidate_id, "steps": steps}


# ── reset-grastate ─────────────────────────────────────────────
@app.post("/api/node/{node_id}/reset-grastate")
async def reset_grastate(node_id: str):
    """SSH: reset safe_to_bootstrap flag in grastate.dat."""
    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    node     = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    _check_rate_limit(node_id)

    if use_mock:
        _push_event("info", f"[MOCK] reset-grastate on {node_id}", "ui")
        return {"ok": True, "mock": True, "node_id": node_id,
                "msg": "grastate.dat reset (mock)"}

    cmd = (
        "sed -i 's/^safe_to_bootstrap:.*/safe_to_bootstrap: 0/' "
        "/var/lib/mysql/grastate.dat 2>&1 && "
        "echo 'grastate.dat updated' || echo 'sed failed'"
    )
    try:
        [(ec, out, err)] = ssh_run(node, cmd, timeout=15)
        ok  = ec == 0
        msg = out or err or ("ok" if ok else "error")
        _push_event("info" if ok else "error", f"reset-grastate on {node_id}: {msg}", "ui")
        return {"ok": ok, "node_id": node_id, "msg": msg}
    except Exception as e:
        _push_event("error", f"reset-grastate on {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


# ── pc.bootstrap (force primary component) ────────────────────
@app.post("/api/node/{node_id}/pc-bootstrap")
async def pc_bootstrap(node_id: str):
    """DB: SET GLOBAL wsrep_provider_options='pc.bootstrap=YES' to force Primary Component."""
    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    node     = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    _check_rate_limit(node_id)

    if use_mock:
        _push_event("info", f"[MOCK] pc.bootstrap on {node_id}", "ui")
        return {"ok": True, "mock": True, "node_id": node_id,
                "msg": "pc.bootstrap=YES applied (mock)"}

    try:
        conn = _db_connect(node, cfg)
        with conn.cursor() as cur:
            cur.execute("SET GLOBAL wsrep_provider_options='pc.bootstrap=YES'")
        conn.close()
        _push_event("info", f"pc.bootstrap=YES on {node_id}", "ui")
        return {"ok": True, "node_id": node_id, "msg": "pc.bootstrap=YES applied"}
    except Exception as e:
        _push_event("error", f"pc.bootstrap on {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


# ── rejoin (cluster-level, with method) ───────────────────────
@app.post("/api/rejoin")
async def do_rejoin_cluster(request: Request):
    """SSH: stop + start MariaDB on a node to re-join the cluster.

    Body: {node_id: str, method?: str}
    This is a cluster-level rejoin (no node_id in path) used by the Recovery page.
    """
    body    = await request.json()
    node_id = body.get("node_id")
    if not node_id:
        raise HTTPException(400, "node_id is required")

    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    node     = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    _check_rate_limit(node_id)

    if use_mock:
        _push_event("info", f"[MOCK] rejoin {node_id}", "ui")
        return {"ok": True, "mock": True, "node_id": node_id,
                "msg": f"Node {node_id} rejoined cluster (mock)"}

    try:
        results = ssh_run(
            node,
            "systemctl stop mariadb 2>&1",
            "sleep 2",
            "systemctl start mariadb 2>&1",
            timeout=60,
        )
        ok  = all(r[0] == 0 for r in results)
        msg = " | ".join(r[1] or r[2] or "ok" for r in results)
        _push_event("info" if ok else "error", f"Rejoin {node_id}: {msg}", "ui")
        return {"ok": ok, "node_id": node_id, "msg": msg}
    except Exception as e:
        _push_event("error", f"Rejoin {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


# ── set-donor (frontend calls /api/node/{id}/set-donor) ───────
@app.post("/api/node/{node_id}/set-donor")
async def set_donor_alias(node_id: str, request: Request):
    """Alias: frontend calls set-donor, backend was named sst-donor. Both work."""
    return await force_sst_donor(node_id, request)


@app.post("/api/bootstrap")
async def do_bootstrap(request: Request):
    """Bootstrap the Galera cluster from the node with the highest seqno."""
    body    = await request.json()
    node_id = body.get("node_id")
    cfg     = _load_config_cached()
    nodes   = _get_active_nodes(cfg)
    if not nodes:
        raise HTTPException(400, "No enabled nodes in config")
    candidate = next((n for n in nodes if n["id"] == node_id), None) if node_id else None
    if not candidate:
        raise HTTPException(404, f"Node '{node_id}' not found")

    _check_cluster_rate_limit()

    other_nodes  = [n for n in nodes if n["id"] != node_id]
    active_others = []
    for n in other_nodes:
        try:
            [(ec, out, _)] = ssh_run(n, "systemctl is-active mariadb.service 2>/dev/null || echo inactive", timeout=8)
            if out.strip() == "active":
                active_others.append(n["id"])
        except Exception:
            pass

    if active_others:
        raise HTTPException(
            409,
            f"Cannot bootstrap: MariaDB is still active on node(s): {', '.join(active_others)}. "
            f"Stop MariaDB on those nodes first (systemctl stop mariadb).",
        )

    try:
        [(ec, out, err)] = ssh_run(
            candidate,
            "galera_new_cluster 2>&1 || systemctl start mariadb@bootstrap.service 2>&1",
            timeout=60,
        )
        ok  = ec == 0
        msg = out or err or ("Bootstrap started" if ok else "Bootstrap failed")
        _push_event("info" if ok else "error", f"Bootstrap on {node_id}: {msg}", "ui")
        return {"ok": ok, "msg": msg, "node_id": node_id}
    except Exception as e:
        _push_event("error", f"Bootstrap on {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


@app.post("/api/node/{node_id}/rejoin")
async def do_rejoin(node_id: str):
    """SSH: stop + start MariaDB on the given node to re-join the cluster."""
    cfg  = _load_config_cached()
    node = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    try:
        results = ssh_run(
            node,
            "systemctl stop mariadb 2>&1",
            "sleep 2",
            "systemctl start mariadb 2>&1",
            timeout=60,
        )
        ok  = all(r[0] == 0 for r in results)
        msg = " | ".join(r[1] or r[2] or "ok" for r in results)
        _push_event("info" if ok else "error", f"Rejoin {node_id}: {msg}", "ui")
        return {"ok": ok, "msg": msg}
    except Exception as e:
        _push_event("error", f"Rejoin {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


@app.post("/api/node/{node_id}/sst-donor")
async def force_sst_donor(node_id: str, request: Request):
    """SSH: set wsrep_sst_donor on the recipient node."""
    body     = await request.json()
    donor_id = body.get("donor_id")
    cfg      = _load_config_cached()
    nodes    = get_active_cluster(cfg).get("nodes", [])
    node     = next((n for n in nodes if n["id"] == node_id), None)
    donor    = next((n for n in nodes if n["id"] == donor_id), None) if donor_id else None
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    donor_host = donor.get("host", donor_id) if donor else donor_id
    try:
        [(ec, out, err)] = ssh_run(
            node,
            f"mysql -e \"SET GLOBAL wsrep_sst_donor='{donor_host}'\"",
            timeout=15,
        )
        ok  = ec == 0
        msg = out or err or ("ok" if ok else "failed")
        _push_event("info" if ok else "error", f"SST donor set {donor_host} → {node_id}: {msg}", "ui")
        return {"ok": ok, "msg": msg}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/node/{node_id}/sst-status")
async def sst_status(node_id: str):
    """SSH + DB: monitor SST progress on the given node."""
    cfg  = _load_config_cached()
    node = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    result = {
        "node_id":     node_id,
        "state":       "unknown",
        "recv_queue":  0,
        "send_queue":  0,
        "sst_method":  None,
        "progress_pct":0,
        "message":     "",
    }

    # DB query — parameterized to prevent SQL injection
    try:
        conn = _db_connect(node, cfg)
        with conn.cursor() as cur:
            for var, key in [
                ("wsrep_local_state_comment", "state"),
                ("wsrep_local_recv_queue",    "recv_queue"),
                ("wsrep_local_send_queue",    "send_queue"),
            ]:
                cur.execute("SHOW STATUS LIKE %s", (var,))
                row = cur.fetchone()
                if row:
                    result[key] = row[1] if key == "state" else int(row[1])
        conn.close()
    except Exception:
        pass

    # SSH: detect active SST process
    try:
        [(_, proc_out, _)] = ssh_run(
            node,
            "pgrep -la rsync 2>/dev/null || pgrep -la mariabackup 2>/dev/null || echo none",
            timeout=8,
        )
        if "rsync"       in proc_out: result["sst_method"] = "rsync"
        elif "mariabackup" in proc_out: result["sst_method"] = "mariabackup"
    except Exception:
        pass

    state_progress = {
        "Synced": 100, "Joined": 95, "Donor/Desynced": 50,
        "Joining": 15, "Open": 5,   "unknown": 0,
    }
    result["progress_pct"] = state_progress.get(result["state"], 10)
    result["message"]      = f"{node_id}: {result['state']} (recv_queue={result['recv_queue']})"
    return result


@app.get("/api/node/{node_id}/processlist")
async def get_processlist(node_id: str, min_time: int = 0):
    """DB: SHOW FULL PROCESSLIST filtered by minimum query time."""
    cfg  = _load_config_cached()
    node = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    try:
        conn = _db_connect(node, cfg, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute("SHOW FULL PROCESSLIST")
            rows = cur.fetchall()
        conn.close()
        if min_time:
            rows = [r for r in rows if (r.get("Time") or 0) >= min_time]
        return {"ok": True, "node_id": node_id, "processes": rows, "total": len(rows),}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/node/{node_id}/kill-query")
async def kill_query(node_id: str, request: Request):
    """DB: KILL QUERY on the given node."""
    body    = await request.json()
    proc_id = body.get("process_id")
    if not proc_id:
        raise HTTPException(400, "process_id required")
    cfg  = _load_config_cached()
    node = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    try:
        conn = _db_connect(node, cfg)
        with conn.cursor() as cur:
            cur.execute(f"KILL QUERY {int(proc_id)}")
        conn.close()
        _push_event("info", f"KILL QUERY {proc_id} on {node_id}", "ui")
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/config/compare-galera-cnf")
async def compare_galera_cnf():
    """SSH: read galera.cnf from all nodes and return diff-friendly structure."""
    cfg   = _load_config_cached()
    nodes = _get_active_nodes(cfg)
    if not nodes:
        return {"ok": True, "nodes": [], "params": {}}

    CNF_PATHS = [
        "/etc/mysql/conf.d/galera.cnf",
        "/etc/mysql/mariadb.conf.d/galera.cnf",
    ]

    def _read_cnf(node):
        cmd = (
            "( for f in /etc/mysql/conf.d/galera.cnf /etc/mysql/mariadb.conf.d/galera.cnf; do "
            "  [ -f \"$f\" ] && echo \"__cnf_path__:$f\" && cat \"$f\" && break; "
            "done ) 2>/dev/null "
            "|| grep -A 200 '\\[galera\\]' /etc/mysql/my.cnf 2>/dev/null "
            "|| grep -r 'wsrep' /etc/mysql/ 2>/dev/null | head -60"
        )
        try:
            [(ec, out, err)] = ssh_run(node, cmd, timeout=12)
            raw = out or err
            cnf_path = None
            clean_lines = []
            for line in raw.splitlines():
                if line.startswith("__cnf_path__:"):
                    cnf_path = line.split(":", 1)[1].strip()
                else:
                    clean_lines.append(line)
            clean_raw = "\n".join(clean_lines)
            return node["id"], clean_raw, None, cnf_path
        except Exception as e:
            return node["id"], "", str(e), None

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for nid, raw, err, cnf_path in ex.map(_read_cnf, nodes):
            params = {}
            for line in raw.splitlines():
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    params[k.strip()] = v.strip()
            results[nid] = {
                "params":   params,
                "raw":      raw,
                "error":    err,
                "cnf_path": cnf_path,
            }

    all_keys = set()
    for v in results.values():
        all_keys.update(v["params"])

    params_matrix = {}
    diff = {}
    for key in sorted(all_keys):
        params_matrix[key] = {nid: results[nid]["params"].get(key, "") for nid in results}
        values = {nid: results[nid]["params"].get(key, "") for nid in results}
        unique_vals = set(v for v in values.values() if v)
        diff[key] = {
            "match":  len(unique_vals) <= 1,
            "values": values,
        }

    details = {
        nid: {
            "ok":       results[nid]["error"] is None,
            "error":    results[nid]["error"] or "",
            "cnf_path": results[nid]["cnf_path"] or "/etc/mysql/conf.d/galera.cnf",
        }
        for nid in results
    }

    common_cnf_path = next(
        (results[nid]["cnf_path"] for nid in results if results[nid]["cnf_path"]),
        "/etc/mysql/conf.d/galera.cnf",
    )

    return {
        "ok":       True,
        "nodes":    [n["id"] for n in nodes],
        "params":   params_matrix,
        "diff":     diff,
        "raw":      {nid: results[nid]["raw"]   for nid in results},
        "errors":   {nid: results[nid]["error"] for nid in results if results[nid]["error"]},
        "cnf_path": common_cnf_path,
        "details":  details,
    }


@app.get("/api/diagnostics/check-all")
async def check_all():
    """Run a comprehensive cluster health check across all nodes."""
    cfg   = _load_config_cached()
    nodes = _get_active_nodes(cfg)
    mode  = get_runtime_mode()

    results  = []
    warnings = []
    errors   = []

    if mode:
        import time as _t
        elapsed = int(_t.time())
        for node in nodes:
            nid = node["id"]
            results.append({
                "node_id":        nid,
                "status":         "ok",
                "wsrep_connected":True,
                "wsrep_ready":    True,
                "wsrep_state":    "Synced",
                "seqno":          _node_base_seqno(nid) + elapsed * 3,
                "recv_queue":     0,
                "flow_control":   0.0,
            })
        return {
            "ok": True, "mode": "mock", "nodes": results,
            "warnings": warnings, "errors": errors,
            "summary": f"Mock check: {len(nodes)} nodes OK",
        }

    # Real mode — parameterized queries
    wsrep_vars = [
        "wsrep_connected", "wsrep_ready", "wsrep_local_state_comment",
        "wsrep_last_committed", "wsrep_local_recv_queue", "wsrep_flow_control_paused",
    ]

    def _check_node(node):
        nid = node["id"]
        try:
            conn     = _db_connect(node, cfg)
            row_data = {}
            with conn.cursor() as cur:
                for var in wsrep_vars:
                    cur.execute("SHOW STATUS LIKE %s", (var,))
                    r = cur.fetchone()
                    if r:
                        row_data[var.replace("wsrep_", "")] = r[1]
            conn.close()
            return {"node_id": nid, "status": "ok", **row_data}
        except Exception as e:
            return {"node_id": nid, "status": "error", "error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_check_node, nodes))

    for r in results:
        if r.get("status") == "error":
            errors.append(f"{r['node_id']}: {r.get('error', 'unknown')}")
        elif r.get("local_state_comment") not in ("Synced", None):
            warnings.append(f"{r['node_id']}: state={r.get('local_state_comment')}")

    return {
        "ok":       len(errors) == 0,
        "mode":     "real",
        "nodes":    results,
        "warnings": warnings,
        "errors":   errors,
        "summary":  f"{len(nodes)} nodes checked: {len(errors)} errors, {len(warnings)} warnings",
    }


@app.get("/api/node/{node_id}/innodb-status")
async def innodb_status(node_id: str):
    """DB: SHOW ENGINE INNODB STATUS — returns raw output for deadlock analysis."""
    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    node     = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        return {"ok": True, "mock": True, "node_id": node_id,
                "status": mock_innodb_status(node_id)}

    try:
        conn = _db_connect(node, cfg)
        with conn.cursor() as cur:
            cur.execute("SHOW ENGINE INNODB STATUS")
            row = cur.fetchone()
        conn.close()
        raw = row[2] if row and len(row) >= 3 else ""
        return {"ok": True, "mock": False, "node_id": node_id, "status": raw}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.websocket("/ws/cluster")
async def ws_cluster(websocket: WebSocket):
    """WebSocket endpoint — streams cluster events to connected browsers."""
    mgr = websocket.app.state.ws_manager
    await mgr.connect(websocket)
    try:
        await websocket.send_json({
            "type":   "log_snapshot",
            "events": list(_event_log),
        })
        while True:
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        mgr.disconnect(websocket)
    except Exception:
        mgr.disconnect(websocket)


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


# ── VERSION / UPDATE CHECK ────────────────────────────────────
@app.get("/api/version")
async def api_version():
    """Return current local commit SHA and check GitHub for the latest commit.
    Uses a 5-minute server-side cache stored at module level.
    """
    global _version_cache

    import shutil
    _GIT = shutil.which("git") or r"C:\Program Files\Git\cmd\git.exe"

    base_dir = Path(__file__).parent.parent
    try:
        local_sha = subprocess.check_output(
            [_GIT, "-C", str(base_dir), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL, timeout=5,
        ).decode().strip()
        local_short = local_sha[:7]
        branch = subprocess.check_output(
            [_GIT, "-C", str(base_dir), "branch", "--show-current"],
            stderr=subprocess.DEVNULL, timeout=5,
        ).decode().strip() or "master"
    except Exception:
        local_sha   = "unknown"
        local_short = "unknown"
        branch      = "master"

    now = _time.time()
    if _version_cache is None or (now - _version_cache.get("ts", 0)) > 300:
        import urllib.request
        remote_sha = remote_short = error = None
        try:
            url = f"https://api.github.com/repos/Leg1onary/galera_orchestrator/commits/{branch}"
            req = urllib.request.Request(
                url,
                headers={"Accept": "application/vnd.github.v3+json", "User-Agent": "galera-orchestrator"},
            )
            with urllib.request.urlopen(req, timeout=8) as resp:
                data        = json.loads(resp.read())
                remote_sha   = data["sha"]
                remote_short = remote_sha[:7]
        except Exception as e:
            error = str(e)
        _version_cache = {"ts": now, "remote_sha": remote_sha,
                          "remote_short": remote_short, "error": error}
    else:
        remote_sha   = _version_cache["remote_sha"]
        remote_short = _version_cache["remote_short"]
        error        = _version_cache.get("error")

    up_to_date = (local_sha == remote_sha) if (local_sha != "unknown" and remote_sha) else None

    return {
        "local_sha":        local_sha,
        "local_short":      local_short,
        "remote_sha":       remote_sha,
        "remote_short":     remote_short,
        "branch":           branch,
        "up_to_date":       up_to_date,
        "update_available": (up_to_date is False),
        "github_url":       "https://github.com/Leg1onary/galera_orchestrator",
        "error":            error,
    }


# ── DISK / SYSTEM HEALTH ─────────────────────────────────────
@app.get("/api/diagnostics/system-health")
async def diagnostics_system_health():
    """SSH: collect df/free/uptime for every node in parallel.
    Thresholds: disk_warn=80%, disk_crit=90%; mem_warn=85%, mem_crit=95%.
    """
    cfg   = _load_config_cached()
    nodes = _get_active_nodes(cfg)
    use_mock = get_runtime_mode()

    if not nodes:
        return {"ok": True, "nodes": []}

    # ── Mock branch ──────────────────────────────────────────
    if use_mock:
        import time as _t
        mock_nodes = []
        for node in nodes:
            nid = node["id"]
            mock_nodes.append({
                "node_id":   nid,
                "name":      node.get("name", nid),
                "host":      node.get("host", ""),
                "ok":        True,
                "warn":      False,
                "crit":      False,
                "disk_data": {"total": "100G", "used": "42G", "avail": "58G", "used_pct": 42},
                "disk_root": {"total": "50G",  "used": "15G", "avail": "35G", "used_pct": 30},
                "memory":    {"total": "8192M", "used": "3500M", "used_pct": 43},
                "load_avg":  {"1m": 0.12, "5m": 0.09, "15m": 0.08},
                "uptime":    " 10:00:00 up 30 days,  2:15,  1 user,  load average: 0.12, 0.09, 0.08",
            })
        return {"ok": True, "mock": True, "nodes": mock_nodes}

    DISK_WARN = 80; DISK_CRIT = 90
    MEM_WARN  = 85; MEM_CRIT  = 95

    def _collect(node):
        nid = node["id"]
        try:
            results = ssh_run(
                node,
                "df -h /var/lib/mysql 2>/dev/null | tail -1 || df -h / 2>/dev/null | tail -1",
                "df -h / 2>/dev/null | tail -1",
                "free -m 2>/dev/null | awk '/^Mem/{print $2, $3}'",
                "uptime 2>/dev/null",
                timeout=12,
            )
            def _parse_df(line):
                if not line:
                    return {}
                parts = line.split()
                if len(parts) < 5:
                    return {}
                try:
                    pct = int(parts[4].rstrip("%"))
                except (ValueError, IndexError):
                    pct = None
                return {"total": parts[1], "used": parts[2], "avail": parts[3], "used_pct": pct}

            disk_data = _parse_df(results[0][1].strip())
            disk_root = _parse_df(results[1][1].strip())

            mem_raw = results[2][1].strip() if results[2][1] else ""
            mem_total = mem_used = mem_pct = None
            if mem_raw:
                parts = mem_raw.split()
                if len(parts) >= 2:
                    try:
                        mem_total = int(parts[0])
                        mem_used  = int(parts[1])
                        mem_pct   = round(mem_used / mem_total * 100) if mem_total else None
                    except ValueError:
                        pass

            uptime_raw = results[3][1].strip() if results[3][1] else None
            load_avg = {}
            if uptime_raw:
                m = re.search(r'load average[s]?:\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)', uptime_raw)
                if m:
                    load_avg = {"1m": float(m.group(1)), "5m": float(m.group(2)), "15m": float(m.group(3))}

            disk_warn = disk_data.get("used_pct", 0) or 0
            disk_crit_flag = disk_warn >= DISK_CRIT
            disk_warn_flag = disk_warn >= DISK_WARN

            mem_crit_flag = (mem_pct or 0) >= MEM_CRIT
            mem_warn_flag = (mem_pct or 0) >= MEM_WARN

            return {
                "node_id":   nid,
                "name":      node.get("name", nid),
                "host":      node.get("host", ""),
                "ok":        True,
                "warn":      disk_warn_flag or mem_warn_flag,
                "crit":      disk_crit_flag or mem_crit_flag,
                "disk_data": disk_data,
                "disk_root": disk_root,
                "memory":    {"total": f"{mem_total}M", "used": f"{mem_used}M", "used_pct": mem_pct} if mem_total else {},
                "load_avg":  load_avg,
                "uptime":    uptime_raw,
            }
        except Exception as e:
            return {"node_id": nid, "name": node.get("name", nid), "host": node.get("host",""),
                    "ok": False, "warn": False, "crit": False, "error": str(e),
                    "disk_data": {}, "disk_root": {}, "memory": {}, "load_avg": {}, "uptime": None}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        node_results = list(ex.map(_collect, nodes))

    return {"ok": True, "nodes": node_results}


# ── NODE SSH PING ─────────────────────────────────────────────
@app.get("/api/node/{node_id}/ping")
async def node_ping(node_id: str):
    """Quick SSH reachability check + systemctl is-active mariadb.service."""
    cfg      = _load_config_cached()
    use_mock = get_runtime_mode()
    node     = _find_node(cfg, node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        return {"ok": True, "mock": True, "node_id": node_id,
                "reachable": True, "latency_ms": 2, "service": "active"}

    t0 = _time.monotonic()
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host"), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=6, banner_timeout=6,
        )
        _, so, _ = client.exec_command("systemctl is-active mariadb.service", timeout=5)
        service_state = so.read().decode(errors="replace").strip()
        client.close()
        latency = int((_time.monotonic() - t0) * 1000)
        return {"ok": True, "mock": False, "node_id": node_id,
                "reachable": True, "latency_ms": latency, "service": service_state}
    except Exception as e:
        latency = int((_time.monotonic() - t0) * 1000)
        return {"ok": False, "mock": False, "node_id": node_id,
                "reachable": False, "latency_ms": latency,
                "service": "unknown", "error": str(e)}


# ── ENTRYPOINT ────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
