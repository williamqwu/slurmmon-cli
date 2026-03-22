"""Tests for TUI formatting utilities."""

from __future__ import annotations

from slurmwatch.tui.formatting import (
    format_duration,
    format_mem,
    pct_str,
    progress_bar,
    sparkline,
    truncate,
)


class TestProgressBar:
    def test_full(self):
        assert progress_bar(100, 100, 10) == "██████████"

    def test_empty(self):
        assert progress_bar(0, 100, 10) == "░░░░░░░░░░"

    def test_half(self):
        bar = progress_bar(50, 100, 10)
        assert bar == "█████░░░░░"

    def test_zero_total(self):
        assert progress_bar(50, 0, 10) == "░░░░░░░░░░"

    def test_over_100(self):
        assert progress_bar(200, 100, 10) == "██████████"


class TestFormatDuration:
    def test_seconds(self):
        assert format_duration(45) == "45s"

    def test_minutes(self):
        assert format_duration(125) == "2m 5s"

    def test_hours(self):
        assert format_duration(7380) == "2h 3m"

    def test_days(self):
        assert format_duration(90061) == "1d 1h"

    def test_none(self):
        assert format_duration(None) == "-"

    def test_negative(self):
        assert format_duration(-1) == "-"

    def test_zero(self):
        assert format_duration(0) == "0s"


class TestFormatMem:
    def test_gigabytes(self):
        assert format_mem(16384) == "16.0G"

    def test_megabytes(self):
        assert format_mem(512) == "512M"

    def test_none(self):
        assert format_mem(None) == "-"


class TestTruncate:
    def test_short_string(self):
        assert truncate("hello", 10) == "hello"

    def test_exact_length(self):
        assert truncate("hello", 5) == "hello"

    def test_long_string(self):
        result = truncate("hello world", 8)
        assert len(result) == 8
        assert result.endswith("…")

    def test_very_short_width(self):
        assert truncate("hello", 2) == "he"


class TestSparkline:
    def test_basic(self):
        result = sparkline([1, 2, 3, 4, 5])
        assert len(result) == 5
        assert result[0] == "▁"
        assert result[-1] == "█"

    def test_empty(self):
        assert sparkline([]) == ""

    def test_constant(self):
        result = sparkline([5, 5, 5])
        assert len(result) == 3

    def test_width_limit(self):
        result = sparkline(list(range(100)), width=10)
        assert len(result) == 10


class TestPctStr:
    def test_value(self):
        assert pct_str(89.66) == "90%"

    def test_none(self):
        assert pct_str(None) == "-"

    def test_zero(self):
        assert pct_str(0.0) == "0%"
