import random, time, math

_scenario = "normal"
_start = time.time()

# garbd mock state
_garbd = {"online": True, "last_seen": time.time()}


def set_scenario(s: str):
    global _scenario
    _scenario = s


def get_scenario() -> str:
    return _scenario


def _node_base_seqno(node_id: str) -> int:
    """Deterministic base seqno per node derived from its id."""
    return 485730 + sum(ord(c) for c in node_id) % 20


def node_status(node_id: str, node: dict, cluster_size: int = 2, all_nodes: list | None = None) -> dict:
    elapsed = int(time.time() - _start)
    base_seqno = _node_base_seqno(node_id)
    commits = base_seqno + elapsed * 3

    # Build incoming_addresses from actual node list when available
    if all_nodes:
        incoming = ",".join(n.get("host", "127.0.0.1") + ":4567" for n in all_nodes)
    else:
        incoming = node.get("host", "10.0.0.10") + ":4567"

    base = {
        "id":   node_id,
        "name": node.get("name", node_id),
        "host": node.get("host", ""),
        "port": node.get("port", 3306),
        "wsrep_cluster_status":      "Primary",
        "wsrep_local_state_comment": "Synced",
        "wsrep_connected":           "ON",
        "wsrep_ready":               "ON",
        "wsrep_cluster_size":        cluster_size,
        "wsrep_local_send_queue":    0,
        "wsrep_local_recv_queue":    0,
        "wsrep_flow_control_paused": "0.00",
        "wsrep_cert_deps_distance":  round(random.uniform(0.8, 2.1), 2),
        "wsrep_local_commits":       commits,
        "wsrep_local_cert_failures": 0,
        "wsrep_bf_aborts":           0,
        "wsrep_apply_oooe":          round(random.uniform(0, 0.05), 4),
        "wsrep_cluster_conf_id":     12,
        "wsrep_cluster_state_uuid":  "5a7b1c2d-dead-beef-cafe-0123456789ab",
        "wsrep_incoming_addresses":  incoming,
        "wsrep_gcomm_uuid":          "5a7b1c2d-dead-beef-cafe-0123456789ab",
        "wsrep_protocol_version":    "9",
        "read_only":             False,
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


# ── seqno / grastate.dat (for bootstrap analysis) ────────────────────
def mock_seqno(nodes_cfg: list) -> list:
    """Simulate reading /var/lib/mysql/grastate.dat from each node."""
    result = []
    for n in nodes_cfg:
        nid = n["id"]
        base_seqno = _node_base_seqno(nid)
        elapsed = int(time.time() - _start)
        seqno_val = base_seqno + elapsed * 3

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
                "seqno":             seqno_val,
                "safe_to_bootstrap": 1 if nid == nodes_cfg[0]["id"] else 0,
                "uuid":              "5a7b1c2d-dead-beef-cafe-0123456789ab",
            })
    return result


# ── garbd mock status ────────────────────────────────────────
def mock_garbd_status(arb_cfg: dict, cluster_size: int = 2) -> dict:
    if not arb_cfg.get("enabled"):
        return {"enabled": False, "online": False}
    base = {
        "enabled":      True,
        "online":       True,
        "id":           arb_cfg.get("id", ""),
        "host":         arb_cfg.get("host", ""),
        "dc":           arb_cfg.get("dc", ""),
        "last_seen_sec": int(time.time() - _garbd["last_seen"]),
    }
    if _scenario in ("gc01_down", "gc02_down"):
        base["members"]      = cluster_size - 1
        base["last_seen_sec"] = 0
    else:
        base["members"] = cluster_size
    return base


# ── mock SSH action execution ─────────────────────────────────
def mock_ssh_action(node_id: str, action: str, cmd: str) -> dict:
    """Simulate SSH command execution result."""
    time.sleep(0.3)
    if _scenario in ("gc01_down",) and node_id == "gc01":
        return {"exit_code": 255, "stdout": "", "stderr": f"ssh: connect to host {node_id}: Connection timed out"}
    if _scenario in ("gc02_down",) and node_id == "gc02":
        return {"exit_code": 1, "stdout": "", "stderr": "Connection refused"}

    outputs = {
        "start":   ("", 0),
        "stop":    ("", 0),
        "restart": ("", 0),
        "rejoin":  ("", 0),
        "galera_new_cluster": ("", 0),
    }
    stdout, code = outputs.get(action, ("", 0))
    return {"exit_code": code, "stdout": stdout, "stderr": ""}


# ── mock bootstrap sequence ──────────────────────────────────
def mock_bootstrap(candidate_id: str, all_nodes: list) -> list:
    """Return step-by-step bootstrap result."""
    elapsed = int(time.time() - _start)
    seqno_val = _node_base_seqno(candidate_id) + elapsed * 3
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
        steps.append({"step": i + 1, "status": "ok",
                      "message": f"{n['id']}: wsrep_local_state_comment = Synced ✓ (cluster_size={i})"})
    steps.append({"step": len(steps) + 1, "status": "done",
                  "message": "Bootstrap завершён. Кластер восстановлен."})
    return steps


# ── mock InnoDB status ────────────────────────────────────────
MOCK_INNODB_STATUS = """=====================================
2026-04-03 10:00:00 0x7f1234567890 INNODB MONITOR OUTPUT
=====================================
Per second averages calculated from the last 30 seconds
-----------------
BACKGROUND THREAD
-----------------
srv_master_thread loops: 10 srv_active, 0 srv_shutdown, 20 srv_idle
srv_master_thread log flush and writes: 30
----------
SEMAPHORES
----------
OS WAIT ARRAY INFO: reservation count 5
OS WAIT ARRAY INFO: signal count 5
RW-shared spins 0, rounds 0, OS waits 0
RW-excl spins 0, rounds 0, OS waits 0
RW-sx spins 0, rounds 0, OS waits 0
Spin rounds per wait: 0.00 RW-shared, 0.00 RW-excl, 0.00 RW-sx
------------
TRANSACTIONS
------------
Trx id counter 485742
Purge done for trx's n:o < 485742 undo n:o < 0 state: running but idle
History list length 0
LIST OF TRANSACTIONS FOR EACH SESSION:
---TRANSACTION 421234567890, not started
0 lock struct(s), heap size 1136, 0 row lock(s)
--------
FILE I/O
--------
I/O thread 0 state: waiting for completed aio requests (insert buffer thread)
I/O thread 1 state: waiting for completed aio requests (log thread)
I/O thread 2 state: waiting for completed aio requests (read thread)
I/O thread 3 state: waiting for completed aio requests (write thread)
Pending normal aio reads: [0, 0, 0, 0] , aio writes: [0, 0] ,
ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
Pending flushes (fsync) log: 0; buffer pool: 0
250 OS file reads, 100 OS file writes, 50 OS fsyncs
0.00 reads/s, 0 avg bytes/read, 0.00 writes/s, 0.00 fsyncs/s
-------------------------------------
INSERT BUFFER AND ADAPTIVE HASH INDEX
-------------------------------------
Ibuf: size 1, free list len 0, seg size 2, 0 merges
merged operations:
 insert 0, delete mark 0, delete 0
discarded operations:
 insert 0, delete mark 0, delete 0
Hash table size 34679, node heap has 0 buffer(s)
Hash table size 34679, node heap has 0 buffer(s)
0.00 hash searches/s, 0.00 non-hash searches/s
---
LOG
---
Log sequence number 12345678
Log flushed up to   12345678
Pages flushed up to 12345678
Last checkpoint at  12345678
0 pending log flushes, 0 pending chkp writes
10 log i/o's done, 0.00 log i/o's/second
----------------------
BUFFER POOL AND MEMORY
----------------------
Total large memory allocated 137428992
Dictionary memory allocated 393800
Buffer pool size   8192
Free buffers       7919
Database pages     273
Old database pages 0
Modified db pages  0
Percent of dirty pages(LRU & free pages): 0.000
Max dirty pages percent: 75.000
Pending reads 0
Pending writes: LRU 0, flush list 0, single page 0
Pages made young 0, not young 0
0.00 youngs/s, 0.00 non-youngs/s
Pages read 250, created 23, written 100
0.00 reads/s, 0.00 creates/s, 0.00 writes/s
Buffer pool hit rate 1000 / 1000, young-making rate 0 / 1000 not 0 / 1000
Pages read ahead 0.00/s, evicted without access 0.00/s, Random read ahead 0.00/s
LRU len: 273, unzip_LRU len: 0
I/O sum[0]:cur[0], unzip sum[0]:cur[0]
--------------
ROW OPERATIONS
--------------
0 queries inside InnoDB, 0 queries in queue
0 read views open inside InnoDB
Process ID=1234, Main thread ID=0x7f1234567890, state: sleeping
Number of rows inserted 1000, updated 500, deleted 100, read 50000
0.00 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
----------------------------
END OF INNODB MONITOR OUTPUT
============================"""


def mock_innodb_status(node_id: str) -> str:
    """Return mock InnoDB status output for the given node."""
    import time
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    return MOCK_INNODB_STATUS.replace("2026-04-03 10:00:00", ts)
