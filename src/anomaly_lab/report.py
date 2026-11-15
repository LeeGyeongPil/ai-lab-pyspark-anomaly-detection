"""Plain-text table output."""

from __future__ import annotations

import unicodedata


def fmt(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.0%}"


def _width(text: str) -> int:
    """Display width: Korean characters take two columns."""
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def _pad(text: str, width: int, right: bool) -> str:
    gap = " " * (width - _width(text))
    return gap + text if right else text + gap


def table(headers: list[str], rows: list[list[object]]) -> str:
    cells = [[fmt(c) for c in row] for row in rows]
    widths = [
        max([_width(h)] + [_width(r[i]) for r in cells]) for i, h in enumerate(headers)
    ]
    lines = ["  ".join(_pad(h, widths[i], i > 0) for i, h in enumerate(headers))]
    lines.append("  ".join("-" * w for w in widths))
    for row in cells:
        lines.append("  ".join(_pad(c, widths[i], i > 0) for i, c in enumerate(row)))
    return "\n".join(lines)
