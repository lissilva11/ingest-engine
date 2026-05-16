import yaml
from pathlib import Path
from typing import Dict, Any

def load_config(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with p.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg

def get_paths(cfg, name):
    for d in cfg.get("datasets", []):
        if d.get("name") == name:
            return d.get("bronze_path"), d.get("silver_path")
    raise RuntimeError("Dataset not found in config")