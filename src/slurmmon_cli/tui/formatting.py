"""Terminal formatting utilities for the curses TUI."""

from __future__ import annotations


def progress_bar(value: float, total: float, width: int = 20) -> str:
    """Return a Unicode block progress bar like '████████░░░░'."""
    if total <= 0:
        return "░" * width
    ratio = min(value / total, 1.0)
    filled = int(ratio * width)
    return "█" * filled + "░" * (width - filled)


def format_duration(seconds: float | int | None) -> str:
    """Format seconds as a compact human-readable duration."""
    if seconds is None or seconds < 0:
        return "-"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    if s < 86400:
        h, rem = divmod(s, 3600)
        m = rem // 60
        return f"{h}h {m}m"
    d, rem = divmod(s, 86400)
    h = rem // 3600
    return f"{d}d {h}h"


def format_mem(mb: float | None) -> str:
    """Format megabytes as human-readable."""
    if mb is None:
        return "-"
    if mb >= 1024:
        return f"{mb / 1024:.1f}G"
    return f"{mb:.0f}M"


def truncate(s: str, width: int) -> str:
    """Truncate string with ellipsis if it exceeds width."""
    if len(s) <= width:
        return s
    if width <= 3:
        return s[:width]
    return s[: width - 1] + "…"


SPARK_CHARS = "▁▂▃▄▅▆▇█"


def sparkline(values: list[float | int], width: int = 30) -> str:
    """Return a sparkline string from numeric values."""
    if not values:
        return ""
    # Take last `width` values
    vals = values[-width:]
    lo = min(vals)
    hi = max(vals)
    rng = hi - lo if hi != lo else 1
    return "".join(
        SPARK_CHARS[min(int((v - lo) / rng * (len(SPARK_CHARS) - 1)), len(SPARK_CHARS) - 1)]
        for v in vals
    )


def pct_str(val: float | None) -> str:
    """Format percentage or dash."""
    if val is None:
        return "-"
    return f"{val:.0f}%"
