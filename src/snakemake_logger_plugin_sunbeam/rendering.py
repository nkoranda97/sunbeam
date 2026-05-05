from __future__ import annotations

import time

from rich.text import Text

from .settings import C_AMBER, C_LEAF, C_ROSE, SPARK_BLOCKS


def _mono_elapsed(secs: float) -> str:
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _sparkline(values: list[float], width: int = 24) -> Text:
    """Unicode block sparkline — returns Text; empty if no data."""
    if not values:
        return Text("")
    tail = values[-width:]
    if len(tail) < width:
        tail = [0.0] * (width - len(tail)) + tail
    peak = max(tail) or 1.0
    chars = []
    for v in tail:
        if v <= 0:
            idx = 0
        else:
            idx = min(len(SPARK_BLOCKS) - 1, int((v / peak) * (len(SPARK_BLOCKS) - 1)) + 1)
        chars.append(SPARK_BLOCKS[idx])
    return Text("".join(chars), style=C_LEAF)


def _progress_bar_markup(done: int, running: int, failed: int, total: int, width: int = 48) -> Text:
    """Solid progress bar: leaf/green (done) + amber (running) + rose (failed)."""
    total = max(total, 1)
    done_cells = round(done / total * width)
    fail_cells = round(failed / total * width)
    run_cells = round(running / total * width)
    used = done_cells + fail_cells + run_cells
    if used > width:
        overflow = used - width
        trim = min(overflow, run_cells)
        run_cells -= trim
        overflow -= trim
        trim = min(overflow, fail_cells)
        fail_cells -= trim
        overflow -= trim
        done_cells = max(0, done_cells - overflow)
    bar = Text()
    bar.append("█" * done_cells, style=C_LEAF)
    bar.append("█" * run_cells, style=C_AMBER)
    bar.append("█" * fail_cells, style=C_ROSE)
    return bar


def _rule_bar_markup(done: int, running: int, failed: int, total: int, width: int = 14) -> Text:
    return _progress_bar_markup(done, running, failed, total, width=width)


def _spinner_frame() -> str:
    """Braille spinner frame derived from wall-clock time so all jobs animate in lockstep."""
    frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    return frames[int(time.time() * 10) % len(frames)]
