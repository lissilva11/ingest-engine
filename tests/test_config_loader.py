import yaml
import pytest
from ingest_engine.core.utils.config_loader import load_config, get_paths

def test_load_config(tmp_path):
    cfg = {"datasets": [{"name": "test", "type": "file"}]}
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(cfg))
    loaded = load_config(str(p))
    assert "datasets" in loaded
    assert loaded["datasets"][0]["name"] == "test"

def test_get_paths():
    cfg = {
        "datasets": [
            {
                "name": "sales",
                "bronze_path": "/fake/bronze",
                "silver_path": "/fake/silver"
            }
        ]
    }
    bronze, silver = get_paths(cfg, "sales")
    assert bronze == "/fake/bronze"
    assert silver == "/fake/silver"

def test_get_paths_not_found():
    cfg = {"datasets": [{"name": "events"}]}
    with pytest.raises(RuntimeError, match="Dataset not found"):
        get_paths(cfg, "sales")