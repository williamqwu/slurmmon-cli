"""Tests for config system."""

from __future__ import annotations

import os

from slurmwatch.config import SlurmwatchConfig, load_config


class TestConfigDefaults:
    def test_defaults_without_file(self, tmp_path):
        cfg = SlurmwatchConfig(str(tmp_path / "nonexistent.ini"))
        assert cfg.get("general", "osc") == "false"
        assert cfg.getboolean("general", "osc") is False
        assert cfg.get("general", "refresh_interval") == "30"
        assert cfg.get("general", "retention_days") == "30"
        assert cfg.get("general", "db_path") == ""

    def test_missing_key_returns_fallback(self, tmp_path):
        cfg = SlurmwatchConfig(str(tmp_path / "nonexistent.ini"))
        assert cfg.get("general", "nonexistent") == ""
        assert cfg.getboolean("general", "nonexistent") is False


class TestConfigLoad:
    def test_load_from_file(self, tmp_path):
        ini = tmp_path / "config.ini"
        ini.write_text("[general]\nosc = true\nretention_days = 60\n")
        cfg = SlurmwatchConfig(str(ini))
        assert cfg.getboolean("general", "osc") is True
        assert cfg.get("general", "retention_days") == "60"
        # Defaults still work for unset keys
        assert cfg.get("general", "refresh_interval") == "30"


class TestConfigSet:
    def test_set_and_save(self, tmp_path):
        ini = tmp_path / "config.ini"
        cfg = SlurmwatchConfig(str(ini))
        cfg.set("general", "osc", "true")
        cfg.save()
        assert os.path.isfile(str(ini))
        # Reload and verify
        cfg2 = SlurmwatchConfig(str(ini))
        assert cfg2.getboolean("general", "osc") is True

    def test_set_new_section(self, tmp_path):
        ini = tmp_path / "config.ini"
        cfg = SlurmwatchConfig(str(ini))
        cfg.set("custom", "key", "value")
        cfg.save()
        cfg2 = SlurmwatchConfig(str(ini))
        assert cfg2.get("custom", "key") == "value"


class TestConfigBooleans:
    def test_true_values(self, tmp_path):
        ini = tmp_path / "config.ini"
        for val in ("true", "yes", "1", "on"):
            ini.write_text(f"[general]\nosc = {val}\n")
            cfg = SlurmwatchConfig(str(ini))
            assert cfg.getboolean("general", "osc") is True, f"Failed for '{val}'"

    def test_false_values(self, tmp_path):
        ini = tmp_path / "config.ini"
        for val in ("false", "no", "0", "off"):
            ini.write_text(f"[general]\nosc = {val}\n")
            cfg = SlurmwatchConfig(str(ini))
            assert cfg.getboolean("general", "osc") is False, f"Failed for '{val}'"


class TestConfigSections:
    def test_sections(self, tmp_path):
        cfg = SlurmwatchConfig(str(tmp_path / "nonexistent.ini"))
        assert "general" in cfg.sections()

    def test_items(self, tmp_path):
        cfg = SlurmwatchConfig(str(tmp_path / "nonexistent.ini"))
        items = dict(cfg.items("general"))
        assert "osc" in items
        assert "db_path" in items


class TestLoadConfig:
    def test_load_config_function(self, tmp_path):
        ini = tmp_path / "config.ini"
        ini.write_text("[general]\nosc = true\n")
        cfg = load_config(str(ini))
        assert cfg.getboolean("general", "osc") is True

    def test_load_config_no_path(self):
        # Should not raise, uses default path
        cfg = load_config(None)
        assert cfg.getboolean("general", "osc") is False
