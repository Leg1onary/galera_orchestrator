import logging
from config import load_config
from mock_data import node_status as mock_node_status

log = logging.getLogger("galera_client")

try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False
    log.warning("pymysql not installed — real mode unavailable. Run: pip install pymysql")

# wsrep variables to collect
WSREP_VARS = [
    "wsrep_cluster_status",
    "wsrep_local_state_comment",
    "wsrep_connected",
    "wsrep_ready",
    "wsrep_cluster_size",
    "wsrep_local_send_queue",
    "wsrep_local_recv_queue",
    "wsrep_flow_control_paused",
    "wsrep_local_commits",
    "wsrep_local_cert_failures",
    "wsrep_bf_aborts",
    "wsrep_cert_deps_distance",
    "wsrep_apply_oooe",
    "wsrep_cluster_conf_id",
    "wsrep_cluster_state_uuid",
]


def USE_MOCK(cfg: dict) -> bool:
    return cfg.get("settings", {}).get("use_mock", True)


def get_cluster_status(cfg: dict) -> dict:
    nodes_cfg = cfg.get("nodes", [])
    arb_cfg   = cfg.get("arbitrator", {})
    results   = []

    for n in nodes_cfg:
        if not n.get("enabled", True):
            continue
        if USE_MOCK(cfg):
            results.append(mock_node_status(n["id"], n))
        else:
            results.append(_real_node_status(n, cfg))

    synced    = sum(1 for r in results if r.get("wsrep_local_state_comment") == "Synced")
    online    = sum(1 for r in results if r.get("online"))
    primary   = all(r.get("wsrep_cluster_status") == "Primary" for r in results if r.get("online"))
    fc_paused = max((float(r.get("wsrep_flow_control_paused", 0)) for r in results), default=0)
    cert_fail = sum(r.get("wsrep_local_cert_failures", 0) for r in results)

    cluster_status = (
        "healthy"  if (primary and synced == len(results) and len(results) > 0) else
        "degraded" if online > 0 else
        "critical"
    )

    return {
        "cluster_name":   cfg.get("cluster", {}).get("name", "galera-cluster"),
        "environment":    cfg.get("cluster", {}).get("environment", "test"),
        "cluster_status": cluster_status,
        "cluster_size":   results[0]["wsrep_cluster_size"] if results else 0,
        "nodes_total":    len(results),
        "nodes_synced":   synced,
        "nodes_online":   online,
        "flow_control":   round(fc_paused, 2),
        "cert_failures":  cert_fail,
        "use_mock":       USE_MOCK(cfg),
        "arbitrator":     _arb_status(arb_cfg, cfg),
        "nodes":          results,
    }


def _real_node_status(node: dict, cfg: dict) -> dict:
    """Connect via TCP to MariaDB and fetch wsrep status variables."""
    base = {
        "id":   node["id"],
        "name": node.get("name", node["id"]),
        "host": node.get("host", ""),
        "port": node.get("port", 3306),
        "online": False,
        "error": None,
    }

    if not HAS_PYMYSQL:
        base["error"] = "pymysql not installed"
        return base

    db_cfg = cfg.get("db", {})
    user   = db_cfg.get("user", "monitor")
    passwd = db_cfg.get("password", "")

    try:
        conn = pymysql.connect(
            host=node["host"],
            port=int(node.get("port", 3306)),
            user=user,
            password=passwd,
            connect_timeout=4,
            read_timeout=5,
            cursorclass=pymysql.cursors.Cursor,
        )
        with conn.cursor() as cur:
            cur.execute("SHOW STATUS LIKE 'wsrep%'")
            rows = cur.fetchall()
        conn.close()

        status = {row[0]: row[1] for row in rows}

        # cast numeric fields
        def _int(k):
            try: return int(status.get(k, 0))
            except: return 0

        def _float(k):
            try: return float(status.get(k, 0))
            except: return 0.0

        base.update({
            "online":                       True,
            "wsrep_cluster_status":         status.get("wsrep_cluster_status", "unknown"),
            "wsrep_local_state_comment":    status.get("wsrep_local_state_comment", "unknown"),
            "wsrep_connected":              status.get("wsrep_connected", "OFF"),
            "wsrep_ready":                  status.get("wsrep_ready", "OFF"),
            "wsrep_cluster_size":           _int("wsrep_cluster_size"),
            "wsrep_local_send_queue":       _int("wsrep_local_send_queue"),
            "wsrep_local_recv_queue":       _int("wsrep_local_recv_queue"),
            "wsrep_flow_control_paused":    str(round(_float("wsrep_flow_control_paused"), 4)),
            "wsrep_local_commits":          _int("wsrep_local_commits"),
            # wsrep_last_committed — последний применённый seqno, нужен для bootstrap-анализа
            "wsrep_last_committed":         _int("wsrep_last_committed"),
            "wsrep_local_cert_failures":    _int("wsrep_local_cert_failures"),
            "wsrep_bf_aborts":              _int("wsrep_bf_aborts"),
            "wsrep_cert_deps_distance":     round(_float("wsrep_cert_deps_distance"), 2),
            "wsrep_apply_oooe":             round(_float("wsrep_apply_oooe"), 4),
            "wsrep_cluster_conf_id":        _int("wsrep_cluster_conf_id"),
            "wsrep_cluster_state_uuid":     status.get("wsrep_cluster_state_uuid", ""),
        })
        log.debug(f"[{node['id']}] real status OK — {base['wsrep_local_state_comment']}")

    except pymysql.err.OperationalError as e:
        base["error"] = f"DB connect error: {e.args[1] if len(e.args)>1 else str(e)}"
        log.warning(f"[{node['id']}] {base['error']}")
    except Exception as e:
        base["error"] = str(e)
        log.warning(f"[{node['id']}] unexpected error: {e}")

    return base


def _arb_status(arb_cfg: dict, cfg: dict) -> dict:
    if not arb_cfg.get("enabled"):
        return {"enabled": False, "online": False, "host": ""}
    if USE_MOCK(cfg):
        from mock_data import mock_garbd_status
        return mock_garbd_status(arb_cfg)
    # Real mode — SSH: systemctl is-active garbd
    try:
        import paramiko
        from pathlib import Path
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            arb_cfg.get("host", ""),
            port=int(arb_cfg.get("ssh_port", 22)),
            username=arb_cfg.get("ssh_user", "root"),
            key_filename=str(Path(arb_cfg.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=6,
        )
        _, so, _ = client.exec_command("systemctl is-active garbd", timeout=8)
        out = so.read().decode(errors="replace").strip()
        ec  = so.channel.recv_exit_status()
        client.close()
        return {
            "enabled": True,
            "online":  ec == 0 and out == "active",
            "host":    arb_cfg.get("host", ""),
            "state":   out,
            "error":   None,
        }
    except ImportError:
        return {"enabled": True, "online": None, "host": arb_cfg.get("host", ""),
                "error": "paramiko not installed"}
    except Exception as e:
        log.warning(f"[garbd] SSH check failed: {e}")
        return {"enabled": True, "online": False, "host": arb_cfg.get("host", ""),
                "error": str(e)}
