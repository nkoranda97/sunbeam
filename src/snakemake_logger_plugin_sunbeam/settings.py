from __future__ import annotations

from dataclasses import dataclass, field

from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase

# ── palette ──────────────────────────────────────────────────────────
C_AMBER       = "#f5a524"   # primary accent
C_AMBER_SOFT  = "#c97a14"
C_GOLD        = "#ffd166"
C_EMBER       = "#e8703a"
C_ROSE        = "#e05a4a"   # failures
C_SAGE        = "#8fb07a"   # success
C_LEAF        = "#a3c76d"
C_SKY         = "#78b5c9"   # info events
C_VIOLET      = "#a88fbf"

C_INK         = "#f4e6cd"   # primary text
C_INK_SOFT    = "#c9b893"
C_INK_DIM     = "#8a7a5d"
C_INK_FAINT   = "#5a4e3a"

C_LINE        = "#3a2f22"
C_BG_ON_AMBER = "#1a1208"   # tmux badge fg

# ── sizing ────────────────────────────────────────────────────────────
EVENT_RING_SIZE    = 8
SPARK_HISTORY_SIZE = 24   # throughput samples (one per refresh tick)
SPARK_WINDOW_SECS  = 60   # samples used for peak/current throughput
SPARK_BLOCKS       = "▁▂▃▄▅▆▇█"

HERO_STATS = ("total", "done", "running", "queued", "failed", "cores")


@dataclass
class LogHandlerSettings(LogHandlerSettingsBase):
    theme: str | None = field(
        default=None,
        metadata={"help": "Pygments theme for syntax-highlighted shell/python blocks.", "type": str},
    )
