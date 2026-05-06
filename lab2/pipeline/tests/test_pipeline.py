"""Smoke tests for configuration and basic setup."""
from pathlib import Path

import yaml


def test_config_exists():
    """Verify settings.yaml exists."""
    cfg_path = Path(__file__).resolve().parent.parent / "config" / "settings.yaml"
    assert cfg_path.is_file()


def test_config_loads():
    """Verify config loads and has required keys."""
    cfg_path = Path(__file__).resolve().parent.parent / "config" / "settings.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    assert cfg["app_name"]
    assert cfg["bronze_path"]
    assert cfg["silver_path"]
    assert cfg["gold_path"]
    assert cfg["input_csv"]
    assert cfg["last_run_date"]


def test_input_csv_exists():
    """Verify input data file exists."""
    root = Path(__file__).resolve().parent.parent.parent
    cfg_path = Path(__file__).resolve().parent.parent / "config" / "settings.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    csv_path = root / cfg["input_csv"]
    assert csv_path.is_file()
