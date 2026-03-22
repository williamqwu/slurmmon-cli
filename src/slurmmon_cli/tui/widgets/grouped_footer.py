"""Grouped footer widget - displays key bindings organized by category."""

from __future__ import annotations

from textual.widgets import Static

# Shared fragments for composing screen-specific footers.
NAV = "[dim]Nav[/] \\[M]onitor \\[X]plore \\[E]fficiency"
TAIL = "\\[?] Settings  \\[Q] Quit"


def footer_markup(*action_keys: str, tabs: bool = False) -> str:
    """Build a grouped footer markup string.

    *action_keys* are pre-formatted Rich markup fragments for the
    current screen's action group (e.g. ``"\\\\[R]efresh"``).
    Set *tabs=True* to include a Tab hint for tabbed screens.
    """
    parts = [f" {NAV}"]
    if action_keys:
        parts.append("[dim]Action[/] " + " ".join(action_keys))
    if tabs:
        parts.append("\\[Tab] switch tab")
    parts.append(TAIL)
    return "  ".join(parts)


class GroupedFooter(Static):
    """Single-line footer with grouped key hints."""

    DEFAULT_CSS = """
    GroupedFooter {
        dock: bottom;
        height: 1;
        background: $accent;
        color: auto;
    }
    """

    def __init__(self, markup_text: str, **kwargs):
        super().__init__(markup_text, **kwargs)
