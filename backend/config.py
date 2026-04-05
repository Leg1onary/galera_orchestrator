"""
Config loader for Galera Orchestrator.

New nodes.yaml structure (v2):
  contours:
    test:
      clusters:
        - name: my-galera-cluster
          nodes: [...]
          arbitrators: [...]
    prod:
      clusters:
        - name: prod-cluster-1
          nodes: [...]
          arbitrators: [...]

  db:
    user: monitor_user
    password: CHANGE_ME

  settings:
    poll_interval: 5

  auth:
    enabled: false
    ...

Legacy flat format (v1) is auto-migrated on load.
"""

import json
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ValidationError, field_validator

CONFIG_PATH = Path(__file__).parent.parent / "config" / "nodes.yaml"

# Runtime mode — stored separately so nodes.yaml is never touched on mode switch
_MODE_PATH   = Path(__file__).parent.parent / "config" / "mode.json"
# Active selection — contour + cluster index, persisted across restarts
_SELECT_PATH = Path(__file__).parent.parent / "config" / "selection.json"


# ── Pydantic schemas ──────────────────────────────────────────────────

class NodeSchema(BaseModel):
    id: str
    host: str
    name: Optional[str] = None
    port: int = 3306
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key: str = "~/.ssh/id_rsa"
    enabled: bool = True
    dc: Optional[str] = "DC1"
    role: Optional[str] = "node"
    db_user: Optional[str] = None
    db_password: Optional[str] = None

    @field_validator("port", "ssh_port")
    @classmethod
    def check_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError(f"Port must be 1–65535, got {v}")
        return v


class ArbitratorSchema(BaseModel):
    id: str
    host: str
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key: str = "~/.ssh/id_rsa"
    enabled: bool = True
    dc: Optional[str] = "DC1"


class ClusterSchema(BaseModel):
    name: str
    nodes: List[NodeSchema] = Field(default_factory=list)
    arbitrators: List[ArbitratorSchema] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class ContourSchema(BaseModel):
    clusters: List[ClusterSchema] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class AuthSchema(BaseModel):
    enabled: bool = False
    username: str = "admin"
    password_hash: str = ""
    token_expire_hours: int = 24
    secret_key: str = "change-me-32+"


class DbSchema(BaseModel):
    user: str = "monitor"
    password: str = ""


class SettingsSchema(BaseModel):
    poll_interval: int = 5


class ConfigSchemaV2(BaseModel):
    contours: Dict[str, ContourSchema] = Field(default_factory=dict)
    db: DbSchema = Field(default_factory=DbSchema)
    settings: SettingsSchema = Field(default_factory=SettingsSchema)
    auth: AuthSchema = Field(default_factory=AuthSchema)

    model_config = {"extra": "allow"}


# ── Mode / selection persistence ────────────────────────────────────

def get_runtime_mode() -> bool:
    """Return use_mock flag. Reads mode.json; defaults to True (mock)."""
    if _MODE_PATH.exists():
        try:
            return bool(json.loads(_MODE_PATH.read_text(encoding="utf-8")).get("use_mock", True))
        except Exception:
            pass
    return True


def set_runtime_mode(use_mock: bool) -> None:
    """Persist use_mock to mode.json only — never touches nodes.yaml."""
    _MODE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _MODE_PATH.write_text(json.dumps({"use_mock": use_mock}, indent=2), encoding="utf-8")


def get_selection() -> dict:
    """Return active contour/cluster selection: {contour: str, cluster_index: int}."""
    defaults = {"contour": "test", "cluster_index": 0}
    if _SELECT_PATH.exists():
        try:
            d = json.loads(_SELECT_PATH.read_text(encoding="utf-8"))
            return {**defaults, **d}
        except Exception:
            pass
    return defaults


def set_selection(contour: str, cluster_index: int) -> None:
    """Persist active contour + cluster index."""
    _SELECT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _SELECT_PATH.write_text(
        json.dumps({"contour": contour, "cluster_index": cluster_index}, indent=2),
        encoding="utf-8",
    )


# ── Migration v1 → v2 ────────────────────────────────────────────────

def _migrate_v1_to_v2(raw: dict) -> dict:
    """
    Detect legacy flat format (has top-level 'nodes' key) and wrap it
    into the new contours structure under the 'test' contour.
    Returns the new-format dict without modifying the original file.
    """
    if "nodes" not in raw:
        return raw  # already v2

    nodes = raw.get("nodes", [])
    arbitrators = raw.get("arbitrators", [])
    arb_single = raw.get("arbitrator", {})
    if arb_single and not arbitrators:
        arbitrators = [arb_single]

    cluster_name = raw.get("cluster", {}).get("name", "galera-cluster")
    environment  = raw.get("cluster", {}).get("environment", "test")
    contour_key  = environment if environment in ("test", "prod") else "test"

    v2 = {
        "contours": {
            contour_key: {
                "clusters": [
                    {
                        "name":         cluster_name,
                        "nodes":        nodes,
                        "arbitrators":  arbitrators,
                    }
                ]
            }
        },
        "db":       raw.get("db",       {}),
        "settings": {k: v for k, v in raw.get("settings", {}).items()
                     if k not in ("use_mock",)},  # drop use_mock — lives in mode.json
        "auth":     raw.get("auth",     {}),
    }
    return v2


# ── Load / save ──────────────────────────────────────────────────────

def load_config() -> dict:
    """Load nodes.yaml, auto-migrate v1→v2, validate, return raw dict."""
    try:
        raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        raise RuntimeError(f"Config file not found: {CONFIG_PATH}")
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Invalid YAML in {CONFIG_PATH}: {exc}")

    raw = _migrate_v1_to_v2(raw)

    try:
        ConfigSchemaV2(**raw)
    except ValidationError as exc:
        errors = "; ".join(
            f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        )
        raise RuntimeError(f"Config validation error in {CONFIG_PATH}: {errors}")

    return raw


def save_config(data: dict) -> None:
    CONFIG_PATH.write_text(
        yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )


# ── Convenience helpers ──────────────────────────────────────────────

def get_active_cluster(cfg: dict) -> dict:
    """
    Return the active cluster dict (has 'name', 'nodes', 'arbitrators').
    Falls back to first available cluster if selection is out of range.
    """
    sel     = get_selection()
    contour = sel.get("contour", "test")
    idx     = int(sel.get("cluster_index", 0))

    contours = cfg.get("contours", {})

    # Try requested contour; fall back to any available
    contour_data = contours.get(contour)
    if not contour_data:
        if contours:
            contour = next(iter(contours))
            contour_data = contours[contour]
        else:
            return {"name": "no-cluster", "nodes": [], "arbitrators": []}

    clusters = contour_data.get("clusters", [])
    if not clusters:
        return {"name": "no-cluster", "nodes": [], "arbitrators": []}

    if idx >= len(clusters):
        idx = 0

    cluster = dict(clusters[idx])
    cluster["_contour"] = contour
    cluster["_index"]   = idx
    return cluster


def list_contours(cfg: dict) -> Dict[str, List[str]]:
    """Return {contour: [cluster_name, ...]} for all defined contours."""
    result = {}
    for cname, cdata in cfg.get("contours", {}).items():
        result[cname] = [cl.get("name", f"cluster-{i}")
                         for i, cl in enumerate(cdata.get("clusters", []))]
    return result


def mutate_active_cluster(cfg: dict, field: str, value) -> None:
    """
    Write `value` into the active cluster's field (e.g. 'nodes', 'arbitrators').
    Modifies cfg in-place so the next save_config(cfg) persists it correctly.

    Usage:
        nodes = get_active_cluster(cfg).get('nodes', [])
        nodes.append(new_node)
        mutate_active_cluster(cfg, 'nodes', nodes)
        save_config(cfg)
    """
    sel     = get_selection()
    contour = sel.get("contour", "test")
    idx     = int(sel.get("cluster_index", 0))

    contours = cfg.get("contours", {})
    if contour not in contours:
        # Fallback: first available contour
        if contours:
            contour = next(iter(contours))
        else:
            return

    clusters = contours[contour].get("clusters", [])
    if not clusters:
        return
    if idx >= len(clusters):
        idx = 0

    clusters[idx][field] = value
