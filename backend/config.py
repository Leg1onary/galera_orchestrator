import json
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ValidationError, field_validator

CONFIG_PATH = Path(__file__).parent.parent / "config" / "nodes.yaml"
# Runtime mode is stored separately so nodes.yaml is never rewritten on mode switch.
# This preserves hand-written comments and key ordering in nodes.yaml.
_MODE_PATH  = Path(__file__).parent.parent / "config" / "mode.json"


def get_runtime_mode() -> bool:
    """Return use_mock flag. Reads mode.json; falls back to nodes.yaml setting."""
    if _MODE_PATH.exists():
        try:
            return bool(json.loads(_MODE_PATH.read_text(encoding="utf-8")).get("use_mock", True))
        except Exception:
            pass
    # fallback: read from nodes.yaml (first boot, or legacy setup)
    try:
        raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
        return bool(raw.get("settings", {}).get("use_mock", True))
    except Exception:
        return True


def set_runtime_mode(use_mock: bool) -> None:
    """Persist use_mock to mode.json only — never touches nodes.yaml."""
    _MODE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _MODE_PATH.write_text(json.dumps({"use_mock": use_mock}, indent=2), encoding="utf-8")


# ── Schema ───────────────────────────────────────────────────
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


class DbSchema(BaseModel):
    user: str = "monitor"
    password: str = ""


class SettingsSchema(BaseModel):
    use_mock: bool = True
    poll_interval: int = 5
    db_port: int = 3306


class ConfigSchema(BaseModel):
    nodes: List[NodeSchema] = Field(default_factory=list)
    arbitrators: List[ArbitratorSchema] = Field(default_factory=list)
    db: DbSchema = Field(default_factory=DbSchema)
    settings: SettingsSchema = Field(default_factory=SettingsSchema)
    cluster: Dict[str, Any] = Field(default_factory=dict)
    # snapshot keys for mode-switching — store as-is (not validated deeply)
    mock_nodes: List[Dict[str, Any]] = Field(default_factory=list)
    real_nodes: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = {"extra": "allow"}


def load_config() -> dict:
    """Load and validate nodes.yaml; raise RuntimeError with a clear message on bad config."""
    try:
        raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        raise RuntimeError(f"Config file not found: {CONFIG_PATH}")
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Invalid YAML in {CONFIG_PATH}: {exc}")

    try:
        ConfigSchema(**raw)
    except ValidationError as exc:
        errors = "; ".join(f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}" for e in exc.errors())
        raise RuntimeError(f"Config validation error in {CONFIG_PATH}: {errors}")

    return raw


def save_config(data: dict) -> None:
    CONFIG_PATH.write_text(
        yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
