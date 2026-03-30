import yaml
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent / "config" / "nodes.yaml"

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def save_config(data: dict):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
