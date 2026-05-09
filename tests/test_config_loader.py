import tempfile
import yaml
from core.engine.config_loader import load_config

def test_load_config(tmp_path):
    cfg = {"datasets": [{"name": "test", "type": "file"}]}
    p = tmp_path / "cfg.yaml"
    p.write_text(yaml.safe_dump(cfg))
    loaded = load_config(str(p))
    assert "datasets" in loaded
    assert loaded["datasets"][0]["name"] == "test"
