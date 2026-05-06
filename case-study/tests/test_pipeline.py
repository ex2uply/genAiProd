"""Tests for Smart Agriculture pipeline."""
from pathlib import Path

import pytest
import yaml


def test_config_loads():
    """Verify config.yaml is valid and contains required keys."""
    cfg_path = Path(__file__).resolve().parent.parent / "pipeline" / "config.yaml"
    assert cfg_path.exists(), "config.yaml should exist"

    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    assert cfg["app_name"]
    assert cfg["data_dir"]
    assert cfg["bronze_path"]
    assert cfg["silver_path"]
    assert cfg["gold_path"]
    assert cfg["watermark_path"]


def test_data_files_exist():
    """Verify all required data files are present."""
    data_dir = Path(__file__).resolve().parent.parent / "data"
    assert (data_dir / "fields.csv").exists()
    assert (data_dir / "sensor_day1.csv").exists()
    assert (data_dir / "sensor_day2.csv").exists()


def test_watermark_functions():
    """Test watermark read/write functionality."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pipeline"))
    from main import ensure_watermark, read_watermark, write_watermark

    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        wm_path = Path(tmpdir) / "watermark.txt"
        ensure_watermark(wm_path, "2024-01-01")
        assert read_watermark(wm_path) == "2024-01-01"

        write_watermark(wm_path, "2024-01-02")
        assert read_watermark(wm_path) == "2024-01-02"


def test_alert_config_defaults():
    """Test alert configuration returns sensible defaults."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pipeline"))
    from main import get_alert_config

    # Without AI intent, should return defaults
    config = get_alert_config(None)
    assert "high_temp_threshold" in config
    assert "low_moisture_threshold" in config
    assert config["high_temp_threshold"] == 35
    assert config["low_moisture_threshold"] == 40


def test_genai_module_imports():
    """Verify genai module can be imported."""
    import sys
    sys_path = list(sys.path)
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from genai.ollama_client import ask_alert_config
        from genai.json_extract import extract_json
        assert callable(ask_alert_config)
        assert callable(extract_json)
    finally:
        sys.path = sys_path
