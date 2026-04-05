"""
Galera cluster status collector.

get_cluster_status(cluster, cfg) accepts a cluster dict
{name, nodes, arbitrators} and the top-level cfg for db credentials.
In mock mode: returns generated data from mock_data.py.
In real mode: connects via pymysql + paramiko SSH.
"""

import logging
from config import get_runtime_mode
from mock_data import node_status as mock_node_status

log = logging.getLogger("galera_orchestrator")

try:
    import pymysql
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False
    log.warning("pymysql not installed — real mode unavailable. Run: pip install pymysql")

# wsrep variables to collect from SHOW STATUS
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
    "wsrep_incoming_addresses",
    "wsrep_gcomm_uuid",
    "wsrep_protocol_version",
]


def get_cluster_status(cluster: dict, cfg: dict) -> dict:
    """
    cluster: {name, nodes, arbitrators, ...}
    cfg:     top-level config dict (for db credentials, settings)
    Returns the full status payload sent to frontend.
    """
    nodes_cfg = cluster.get("nodes", [])
    arbs_cfg  = cluster.get("arbitrators", [])

    enabled_nodes = [n for n in nodes_cfg if n.get("enabled", True)]
    use_mock      = get_runtime_mode()

    # ── Node statuses ───────────────────────────────────────────────
    results = []
    if use_mock:
        cluster_size = len(enabled_nodes)
        for n in enabled_nodes:
            results.append(mock_node_status(
                n["id"], n,
                cluster_size=cluster_size,
                all_nodes=enabled_nodes,
            ))
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        ordered = {n["id"]: None for n in enabled_nodes}
        with ThreadPoolExecutor(max_workers=max(len(enabled_nodes), 1)) as pool:
            fmap = {pool.submit(_real_node_status, n, cfg): n["id"] for n in enabled_nodes}
            for fut in as_completed(fmap, timeout=12):
                nid = fmap[fut]
                try:
                    ordered[nid] = fut.result()
                except Exception as e:
                    ordered[nid] = {
                        "id": nid, "name": nid, "host": "", "port": 3306,
                        "online": False, "error": f"executor error: {e}",
                        "state": "Offline",
                    }
        results = [ordered[n["id"]] for n in enabled_nodes]

    # Enrich with ssh/dc fields from config
    _cfg_by_id = {n["id"]: n for n in enabled_nodes}
    for r in results:
        nid  = r.get("id")
        ncfg = _cfg_by_id.get(nid, {})
        r.setdefault("ssh_port", ncfg.get("ssh_port", 22))
        r.setdefault("ssh_user", ncfg.get("ssh_user", "root"))
        r.setdefault("ssh_key",  ncfg.get("ssh_key",  "~/.ssh/id_rsa"))
        if ncfg.get("dc"):
            r.setdefault("dc", ncfg["dc"])
        if ncfg.get("db_user"):
            r.setdefault("db_user", ncfg["db_user"])

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

    # ── Arbitrator statuses ─────────────────────────────────────────
    from mock_data import mock_garbd_status
    arb_statuses = []
    mock_cluster_size = len(enabled_nodes)
    for arb in arbs_cfg:
        if not arb.get("enabled", True):
            continue
        if use_mock:
            arb_statuses.append(mock_garbd_status(arb, cluster_size=mock_cluster_size))
        else:
            arb_statuses.append(_arb_status_real(arb))

    return {
        "cluster_name":   cluster.get("name", "galera-cluster"),
        "cluster_status": cluster_status,
        "cluster_size":   results[0]["wsrep_cluster_size"] if results else 0,
        "nodes_total":    len(results),
        "nodes_synced":   synced,
        "nodes_online":   online,
        "flow_control":   round(fc_paused, 2),
        "cert_failures":  cert_fail,
        "use_mock":       use_mock,
        # Legacy single-arb field (kept for backwards compat)
        "arbitrator":     arb_statuses[0] if arb_statuses else {"enabled": False, "online": False},
        "arbitrators":    arb_statuses,
        "nodes":          results,
    }


def _real_node_status(node: dict, cfg: dict) -> dict:
    """Connect via TCP to MariaDB and fetch wsrep status variables."""
    base = {
        "id":     node["id"],
        "name":   node.get("name", node["id"]),
        "host":   node.get("host", ""),
        "port":   node.get("port", 3306),
        "online": False,
        "error":  None,
        "state":  "Offline",
    }

    if not HAS_PYMYSQL:
        base["error"] = "pymysql not installed"
        return base

    db_cfg = cfg.get("db", {})
    user   = node.get("db_user")     or db_cfg.get("user",     "monitor")
    passwd = node.get("db_password") or db_cfg.get("password", "")

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
            cur.execute("SHOW GLOBAL VARIABLES LIKE 'read_only'")
            ro_rows = cur.fetchall()
        conn.close()

        status = {row[0]: row[1] for row in rows}
        read_only_val = ro_rows[0][1].upper() if ro_rows else "OFF"

        def _int(k):
            try:   return int(status.get(k, 0))
            except: return 0

        def _float(k):
            try:   return float(status.get(k, 0))
            except: return 0.0

        state_comment = status.get("wsrep_local_state_comment", "unknown")

        base.update({
            "online":                    True,
            "state":                     state_comment,
            "wsrep_cluster_status":      status.get("wsrep_cluster_status", "unknown"),
            "wsrep_local_state_comment": state_comment,
            "wsrep_connected":           status.get("wsrep_connected", "OFF"),
            "wsrep_ready":               status.get("wsrep_ready",     "OFF"),
            "wsrep_cluster_size":        _int("wsrep_cluster_size"),
            "wsrep_local_send_queue":    _int("wsrep_local_send_queue"),
            "wsrep_local_recv_queue":    _int("wsrep_local_recv_queue"),
            "wsrep_flow_control_paused": str(round(_float("wsrep_flow_control_paused"), 4)),
            "wsrep_local_commits":       _int("wsrep_local_commits"),
            "wsrep_last_committed":      _int("wsrep_last_committed"),
            "wsrep_local_cert_failures": _int("wsrep_local_cert_failures"),
            "wsrep_bf_aborts":           _int("wsrep_bf_aborts"),
            "wsrep_cert_deps_distance":  round(_float("wsrep_cert_deps_distance"), 2),
            "wsrep_apply_oooe":          round(_float("wsrep_apply_oooe"), 4),
            "wsrep_cluster_conf_id":     _int("wsrep_cluster_conf_id"),
            "wsrep_cluster_state_uuid":  status.get("wsrep_cluster_state_uuid", ""),
            "read_only":                 read_only_val == "ON",
        })
        _already = set(base.keys())
        for _var in WSREP_VARS:
            if _var not in _already and _var in status:
                base[_var] = status[_var]
        log.debug(f"[{node['id']}] real status OK — {state_comment}")

    except Exception as e:
        msg = e.args[1] if (hasattr(e, 'args') and len(e.args) > 1) else str(e)
        base["error"] = f"DB error: {msg}"
        log.warning(f"[{node['id']}] {base['error']}")

    return base


def _arb_status_real(arb_cfg: dict) -> dict:
    """Real mode SSH check for a single arbitrator."""
    try:
        import paramiko
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
            "id":      arb_cfg.get("id",   ""),
            "dc":      arb_cfg.get("dc",   ""),
            "state":   out,
            "error":   None,
        }
    except ImportError:
        return {"enabled": True, "online": None, "host": arb_cfg.get("host", ""),
                "error": "paramiko not installed"}
    except Exception as e:
        log.warning(f"[garbd {arb_cfg.get('id', '')}] SSH check failed: {e}")
        return {"enabled": True, "online": False, "host": arb_cfg.get("host", ""),
                "id": arb_cfg.get("id", ""), "error": str(e)}


# missing import needed for _arb_status_real
from pathlib import Path
