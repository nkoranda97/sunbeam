from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class _JobEntry:
    job_id: int
    rule: str
    wildcards_str: str = ""
    threads: int = 1
    resources_str: str = ""
    started_at: float = field(default_factory=time.time)


@dataclass
class _RuleStats:
    total: int = 0
    done: int = 0
    running: int = 0
    failed: int = 0


@dataclass
class _EventEntry:
    t: str       # "HH:MM"
    glyph: str   # "▸" "✓" "✗" "!" "i"
    kind: str    # info | ok | warn | err
    markup: str  # rich markup
