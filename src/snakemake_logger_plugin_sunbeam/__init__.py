from __future__ import annotations

import io
import os
import re
import select
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from logging import LogRecord
from pathlib import Path
from typing import Any, Callable, Deque, Optional

try:
    import termios
    _HAS_TERMIOS = True
except ImportError:
    _HAS_TERMIOS = False

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.padding import Padding
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.common import LogEvent
from snakemake_interface_logger_plugins.settings import (
    LogHandlerSettingsBase,
    OutputSettingsLoggerInterface,
)


# ────────────────────────────── palette ──────────────────────────────
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

# Max items in the bounded event ring and sparkline history.
EVENT_RING_SIZE = 8
SPARK_HISTORY_SIZE = 24        # throughput samples (one per refresh tick)
SPARK_WINDOW_SECS = 60         # samples used for peak/current throughput

# Unicode block chars for the sparkline, low → high.
SPARK_BLOCKS = "▁▂▃▄▅▆▇█"

# Hero stat column order and default widths.
HERO_STATS = ("total", "done", "running", "queued", "failed", "cores")


# ───────────────────────────── settings ──────────────────────────────

@dataclass
class LogHandlerSettings(LogHandlerSettingsBase):
    theme: Optional[str] = field(
        default=None,
        metadata={"help": "Pygments theme for syntax-highlighted shell/python blocks.", "type": str},
    )


# ────────────────────────── internal state ───────────────────────────

@dataclass
class _JobEntry:
    job_id: int
    rule: str
    wildcards_str: str = ""
    threads: int = 1
    resources_str: str = ""
    started_at: float = field(default_factory=time.time)
    state: str = "running"   # running | done | fail | queued


@dataclass
class _RuleStats:
    total: int = 0
    done: int = 0
    running: int = 0
    failed: int = 0


@dataclass
class _EventEntry:
    t: str          # "HH:MM"
    glyph: str      # "▸" "✓" "✗" "!" "i"
    kind: str       # info | ok | warn | err
    markup: str     # rich markup


# ────────────────────── custom renderable pieces ─────────────────────

def _mono_elapsed(secs: float) -> str:
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _sparkline(values: list[float], width: int = 24) -> Text:
    """Unicode block sparkline. Returns Text; empty if no data."""
    if not values:
        return Text("")
    # Use the last `width` samples; pad left with zeros.
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
    return Text("".join(chars), style=C_AMBER)


def _progress_bar_markup(done: int, running: int, failed: int, total: int, width: int = 48) -> Text:
    """Solid progress bar: amber (done) + gold (running) + rose (failed). No pending segment."""
    total = max(total, 1)
    done_cells = round(done / total * width)
    fail_cells = round(failed / total * width)
    run_cells  = round(running / total * width)
    used = done_cells + fail_cells + run_cells
    if used > width:
        overflow = used - width
        trim = min(overflow, run_cells);  run_cells  -= trim; overflow -= trim
        trim = min(overflow, fail_cells); fail_cells -= trim; overflow -= trim
        done_cells -= overflow
    bar = Text()
    bar.append("█" * done_cells, style=C_AMBER)
    bar.append("█" * run_cells,  style=C_GOLD)
    bar.append("█" * fail_cells, style=C_ROSE)
    return bar


def _rule_bar_markup(done: int, running: int, failed: int, total: int, width: int = 14) -> Text:
    """Compact per-rule progress bar."""
    return _progress_bar_markup(done, running, failed, total, width=width)


def _spinner_frame() -> str:
    """Pick a braille spinner frame from wall-clock time.

    We used ``rich.spinner.Spinner`` before, but each Spinner instance mutates
    its own frame counter tied to Live refresh cadence. Since our Live redraws
    only when state changes, that made spinners stutter. Deriving the frame
    from ``time.time()`` means all running jobs animate in lockstep regardless
    of how often Live refreshes.
    """
    frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    idx = int(time.time() * 10) % len(frames)
    return frames[idx]


# ─────────────────────────── main handler ────────────────────────────

class LogHandler(LogHandlerBase):
    def __init__(
        self,
        common_settings: OutputSettingsLoggerInterface,
        settings: Optional[LogHandlerSettings] = None,
    ) -> None:
        super().__init__(common_settings=common_settings, settings=settings)

    # LogHandlerBase calls __post_init__ from __init__ (dataclass-style).
    def __post_init__(self) -> None:
        self.console = Console(
            file=sys.stdout if self.common_settings.stdout else sys.stderr,
            no_color=self.common_settings.nocolor,
            highlight=False,
        )

        # Live frame + tty state
        self._live: Optional[Live] = None
        self._old_tty_settings: Optional[list] = None

        # Keyboard / TUI interaction state
        self._kb_lock: threading.Lock = threading.Lock()
        self._kb_thread: Optional[threading.Thread] = None
        self._kb_running: bool = False
        self._verbose_override: bool = False   # toggled by 'v' key
        self._filter_active: bool = False      # True while '/' mode is on
        self._filter_text: str = ""            # characters accumulated after '/'
        self._quit_pending: bool = False       # True after first 'q', waiting for confirm

        # Run state
        self._workflow_name: str = "workflow"
        self._snakefile: Optional[str] = None
        self._workflow_id: Optional[str] = None
        self._workflow_start_time: Optional[float] = None

        # Job tracking
        self._active_jobs: dict[int, _JobEntry] = {}
        self._job_specs: dict[int, _JobEntry] = {}   # seen via JOB_INFO but not yet started
        self._rule_stats: dict[str, _RuleStats] = {}
        self._total_jobs: int = 0
        self._jobs_done: int = 0
        self._jobs_failed: int = 0

        # Resources
        self._cores: Optional[int] = None
        self._nodes: list[str] = []

        # Throughput sparkline (completed-per-minute, sampled by refresh tick)
        self._completion_times: Deque[float] = deque(maxlen=512)
        self._spark_history: Deque[float] = deque(maxlen=SPARK_HISTORY_SIZE)
        self._last_spark_sample: float = 0.0

        # Events ring
        self._events: Deque[_EventEntry] = deque(maxlen=EVENT_RING_SIZE)

        # In-frame log pane (Rich renderables replacing _print_above in fullscreen mode)
        self._log_lines: Deque[Any] = deque(maxlen=20)

        # Scroll state (viewport into the rendered panel)
        self._scroll_offset: int = 999999  # clamped to max on first render → starts at bottom
        self._scroll_max: int = 0

        # Last shell command (rule, cmd, job ref)
        self._last_shell: Optional[tuple[str, str, str]] = None

        # Syntax theme
        self._syntax_theme: str = (
            self.settings.theme
            if self.settings and self.settings.theme
            else "monokai"
        )

        # ETA smoothing
        self._eta_secs: Optional[float] = None

        # Summary flag — once the workflow ends, we freeze the frame.
        self._finished: bool = False

        self._dispatch: dict[str, Callable[[LogRecord], None]] = {
            LogEvent.RUN_INFO:         self._handle_run_info,
            LogEvent.WORKFLOW_STARTED: self._handle_workflow_started,
            LogEvent.SHELLCMD:         self._handle_shellcmd,
            LogEvent.JOB_INFO:         self._handle_job_info,
            LogEvent.JOB_STARTED:      self._handle_job_started,
            LogEvent.JOB_FINISHED:     self._handle_job_finished,
            LogEvent.JOB_ERROR:        self._handle_job_error,
            LogEvent.GROUP_INFO:       self._handle_group_info,
            LogEvent.GROUP_ERROR:      self._handle_group_error,
            LogEvent.RESOURCES_INFO:   self._handle_resources_info,
            LogEvent.DEBUG_DAG:        self._handle_debug_dag,
            LogEvent.PROGRESS:         self._handle_progress,
            LogEvent.RULEGRAPH:        self._handle_rulegraph,
            LogEvent.ERROR:            self._handle_error,
        }

    # ─── logging interface ────────────────────────────────────────────

    def emit(self, record: LogRecord) -> None:
        try:
            event = getattr(record, "event", None)
            self._debug_event(event, record)
            handler = self._dispatch.get(event)
            if handler is not None:
                handler(record)
            else:
                msg = self.format(record)
                if msg:
                    # Parse Slurm submission messages to label active jobs with real rule names.
                    self._maybe_update_job_from_slurm_msg(msg)
                    self._add_log_line(Text(msg, style=C_INK_SOFT))
        except Exception:
            self.handleError(record)

    def _debug_event(self, event: Any, record: LogRecord) -> None:
        """Write every event + its custom attributes to /tmp/sunbeam_debug.log.

        Only active when the SUNBEAM_DEBUG environment variable is set.
        Remove this method (and its call in emit) once the cluster event
        names and attributes are confirmed.
        """
        if not os.environ.get("SUNBEAM_DEBUG"):
            return
        try:
            skip = frozenset(vars(LogRecord("", 0, "", 0, "", (), None)))
            extras = {k: repr(v) for k, v in vars(record).items() if k not in skip}
            line = f"EVENT={event!r}  {extras}\n"
            with open("/tmp/sunbeam_debug.log", "a") as f:
                f.write(line)
        except Exception:
            pass

    def close(self) -> None:
        # If we never showed the summary (e.g. interrupted), print it now.
        if self._workflow_start_time is not None and not self._finished:
            self._stop_live()
            self._print_workflow_summary()
        else:
            self._stop_live()
        super().close()

    # ─── Live plumbing ────────────────────────────────────────────────

    def _ensure_live(self) -> None:
        """Start Live if not already running; safe to call many times."""
        if self._live is not None:
            return
        self._live = Live(
            get_renderable=self._render_frame,  # called each tick → spinner animates
            console=self.console,
            screen=True,                        # fullscreen alternate-screen buffer
            refresh_per_second=8,
            transient=False,
            redirect_stdout=False,
            redirect_stderr=False,
        )
        self._live.start()
        # Put stdin into raw mode: suppress echo AND line-buffering so we can
        # read single keypresses without waiting for Enter.
        if _HAS_TERMIOS and sys.stdin.isatty():
            try:
                self._old_tty_settings = termios.tcgetattr(sys.stdin)
                new = termios.tcgetattr(sys.stdin)
                new[3] &= ~(termios.ECHO | termios.ICANON)
                new[6][termios.VMIN] = 1
                new[6][termios.VTIME] = 0
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, new)
            except termios.error:
                self._old_tty_settings = None
        self._start_keyboard_thread()

    def _refresh(self) -> None:
        """Trigger an immediate re-render (get_renderable calls _render_frame each tick)."""
        if self._live is None:
            return
        self._sample_throughput()
        self._live.refresh()

    def _stop_live(self) -> None:
        # Stop keyboard thread before TTY restore to avoid a race.
        self._stop_keyboard_thread()
        if self._live is not None:
            try:
                self._live.stop()
            finally:
                self._live = None
                if _HAS_TERMIOS and self._old_tty_settings is not None:
                    try:
                        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self._old_tty_settings)
                    except termios.error:
                        pass
                    finally:
                        self._old_tty_settings = None

    def _print_above(self, renderable: Any) -> None:
        """Print content without tearing the frame — uses Live.console.print if live."""
        if self._live is not None and self._live.is_started:
            self._live.console.print(renderable)
        else:
            self.console.print(renderable)

    # ─── keyboard input thread ────────────────────────────────────────

    def _start_keyboard_thread(self) -> None:
        if not sys.stdin.isatty():
            return
        self._kb_running = True
        self._kb_thread = threading.Thread(
            target=self._keyboard_reader, name="sunbeam-kb", daemon=True
        )
        self._kb_thread.start()

    def _stop_keyboard_thread(self) -> None:
        self._kb_running = False
        if self._kb_thread is not None:
            self._kb_thread.join(timeout=1.0)
            self._kb_thread = None

    def _keyboard_reader(self) -> None:
        while self._kb_running:
            try:
                ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                if ready:
                    ch = sys.stdin.read(1)
                    if ch:
                        self._on_key(ch)
            except (OSError, ValueError):
                break

    def _on_key(self, ch: str) -> None:
        with self._kb_lock:
            if self._filter_active:
                if ch == "\x1b":                        # ESC — clear filter
                    self._filter_active = False
                    self._filter_text = ""
                elif ch in ("\x7f", "\x08"):            # Backspace / DEL
                    self._filter_text = self._filter_text[:-1]
                elif ch in ("\r", "\n"):                # Enter — commit
                    self._filter_active = bool(self._filter_text)
                elif ch.isprintable():
                    self._filter_text += ch
            elif self._quit_pending:
                if ch in ("q", "Q"):                    # confirmed
                    self._quit_pending = False
                    os.kill(os.getpid(), signal.SIGINT)
                    return
                else:                                   # any other key cancels
                    self._quit_pending = False
            else:
                if ch in ("q", "Q"):
                    self._quit_pending = True           # first press: arm confirmation
                elif ch in ("v", "V"):
                    self._verbose_override = not self._verbose_override
                elif ch == "/":
                    self._filter_active = True
                    self._filter_text = ""
                elif ch == "j":
                    self._scroll_offset = min(self._scroll_offset + 3, 999999)
                elif ch == "k":
                    self._scroll_offset = max(0, self._scroll_offset - 3)
                elif ch == "g":
                    self._scroll_offset = 0
                elif ch == "G":
                    self._scroll_offset = 999999
        self._refresh()

    # ─── log pane ────────────────────────────────────────────────────

    def _add_log_line(self, renderable: Any) -> None:
        """Queue a Rich renderable into the in-frame log pane.

        While the TUI is running: buffer into _log_lines (shown in the LOG pane).
        Otherwise (pre-TUI, post-TUI, CI, tests): print directly to the console so
        critical messages — including DAG errors before any jobs start — are never lost.
        """
        if self._live is not None and self._live.is_started:
            self._log_lines.append(renderable)
        else:
            self.console.print(renderable)

    def _render_log_pane(self) -> list[Any]:
        if not self._log_lines:
            return []
        title = self._render_section_title("LOG")
        items = list(self._log_lines)[-6:]
        return [Text(""), title, *items]

    # ─── throughput sampling ──────────────────────────────────────────

    def _sample_throughput(self) -> None:
        """Compute jobs/min over the trailing window and push to history ring."""
        now = time.time()
        # Only sample at most a few times per second to keep the line smooth.
        if now - self._last_spark_sample < 1.0 and self._spark_history:
            return
        self._last_spark_sample = now
        cutoff = now - SPARK_WINDOW_SECS
        recent = sum(1 for t in self._completion_times if t >= cutoff)
        # jobs per minute in the window
        rate = recent * (60.0 / SPARK_WINDOW_SECS)
        self._spark_history.append(rate)

    def _current_throughput(self) -> float:
        return self._spark_history[-1] if self._spark_history else 0.0

    def _peak_throughput(self) -> float:
        return max(self._spark_history) if self._spark_history else 0.0

    def _compute_eta(self) -> Optional[float]:
        """Exponential moving average on jobs/sec, smoothed against remaining."""
        if not self._workflow_start_time or self._total_jobs <= 0:
            return None
        remaining = self._total_jobs - self._jobs_done - self._jobs_failed
        if remaining <= 0:
            return 0.0
        elapsed = time.time() - self._workflow_start_time
        if self._jobs_done <= 0 or elapsed <= 0:
            return None
        rate = self._jobs_done / elapsed  # jobs / sec, averaged over the whole run
        if rate <= 0:
            return None
        return remaining / rate

    # ─── frame rendering ──────────────────────────────────────────────

    def _build_panel(self) -> Panel:
        body = Group(
            self._render_banner(),
            Text(""),
            self._render_hero(),
            self._render_progress_bar_row(),
            Text(""),
            self._render_sparkline_row(),
            Text(""),
            self._render_two_col(),
            *self._render_log_pane(),
            *self._render_shell_block(),
            *self._render_summary(),
        )
        width = self._get_width()
        return Panel(
            Group(self._render_tmux_header(width), body, self._render_status_bar(width)),
            border_style=C_LINE,
            padding=(0, 0),
            box=box.ROUNDED,
        )

    def _render_frame(self) -> Any:
        """Called by Live's get_renderable on every tick.

        Renders the full panel into an ANSI buffer, slices the visible window
        based on _scroll_offset, and returns the cropped view as a Text object.
        Starts pinned to the bottom so the status bar (keybindings) is always
        visible; j/k scroll through the content.
        """
        panel = self._build_panel()
        try:
            h = self.console.size.height
            w = self._get_width()
        except Exception:
            return panel
        if h <= 0:
            return panel

        buf_file = io.StringIO()
        buf = Console(
            file=buf_file,
            width=w,
            force_terminal=True,
            color_system="truecolor",
            no_color=self.console.no_color,
            highlight=False,
        )
        buf.print(panel)
        ansi = buf_file.getvalue()

        all_lines = ansi.split("\n")
        if all_lines and not all_lines[-1]:
            all_lines = all_lines[:-1]

        total = len(all_lines)
        self._scroll_max = max(0, total - h)
        self._scroll_offset = max(0, min(self._scroll_offset, self._scroll_max))

        visible = all_lines[self._scroll_offset:self._scroll_offset + h]
        return Text.from_ansi("\n".join(visible))

    def _get_width(self) -> int:
        try:
            w = self.console.size.width
        except Exception:
            w = 120
        return max(80, min(w, 160))

    # ─── tmux header / footer ────────────────────────────────────────

    def _render_tmux_header(self, width: int) -> Table:
        t = Table.grid(expand=True, padding=(0, 0))
        t.add_column(no_wrap=True)                     # SUNBEAM badge
        t.add_column(no_wrap=True)                     # window tabs
        t.add_column(justify="right", no_wrap=True)    # meta
        badge = Text(" SUNBEAM ", style=f"bold {C_BG_ON_AMBER} on {C_AMBER}")
        tabs = Text()
        tabs.append("  ")
        tabs.append("0", style=f"bold {C_AMBER}"); tabs.append(":snakemake", style=C_INK)
        tabs.append("*", style=C_AMBER)
        tabs.append("   ")
        tabs.append("1", style=C_INK_DIM); tabs.append(":editor", style=C_INK_FAINT)
        tabs.append("   ")
        tabs.append("2", style=C_INK_DIM); tabs.append(":shell", style=C_INK_FAINT)
        meta = Text()
        host = os.uname().nodename if hasattr(os, "uname") else "local"
        py = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        meta.append(f"host ", style=C_INK_FAINT); meta.append(host, style=C_INK_SOFT)
        meta.append("   ", style=C_INK_FAINT)
        meta.append(f"py ",   style=C_INK_FAINT); meta.append(py, style=C_INK_SOFT)
        meta.append("   ", style=C_INK_FAINT)
        meta.append(time.strftime("%H:%M"), style=C_INK)
        meta.append(" ", style=C_INK)
        t.add_row(badge, tabs, meta)
        return t

    def _render_status_bar(self, width: int) -> Table:
        t = Table.grid(expand=True, padding=(0, 0))
        t.add_column(no_wrap=True)
        t.add_column(justify="right", no_wrap=True)
        left = Text()
        left.append(" logger ", style=C_INK_FAINT); left.append("sunbeam", style=C_INK_SOFT)
        left.append("   ", style=C_INK_FAINT)
        left.append("theme ", style=C_INK_FAINT); left.append(self._syntax_theme, style=C_INK_SOFT)
        left.append("   ", style=C_INK_FAINT)
        left.append("cwd ",   style=C_INK_FAINT); left.append(self._short_cwd(), style=C_INK_SOFT)
        # Snapshot interactive state under lock for this render pass.
        with self._kb_lock:
            filter_active = self._filter_active
            filter_text = self._filter_text
            verbose_on = self._verbose_override
            quit_pending = self._quit_pending
        scroll_offset = self._scroll_offset
        scroll_max = self._scroll_max
        right = Text()
        if quit_pending:
            right.append(" QUIT? ", style=f"bold {C_INK} on {C_ROSE}")
            right.append("  q  ", style=f"bold {C_ROSE} on {C_LINE}")
            right.append(" confirm  ", style=C_ROSE)
            right.append(" ESC ", style=f"{C_INK_SOFT} on {C_LINE}")
            right.append(" cancel  ", style=C_INK_FAINT)
        elif filter_active:
            right.append(" / ", style=f"bold {C_AMBER} on {C_LINE}")
            right.append(f" {filter_text}", style=C_INK)
            right.append("█", style=C_AMBER)
            right.append("  ESC clear  ", style=C_INK_FAINT)
        else:
            for k, v in (("q", "quit"), ("v", "verbose"), ("/", "filter"), ("k", "▲"), ("j", "▼")):
                k_style = f"bold {C_AMBER} on {C_LINE}" if (k == "v" and verbose_on) else f"{C_INK_SOFT} on {C_LINE}"
                right.append(f" {k} ", style=k_style)
                right.append(f" {v}  ", style=C_INK_FAINT)
        # Scroll position indicator on the left when not at the bottom.
        if scroll_max > 0:
            pct = int((scroll_offset / scroll_max) * 100) if scroll_max else 100
            indicator_style = C_SKY if scroll_offset > 0 else C_INK_FAINT
            left.append("   ", style=C_INK_FAINT)
            left.append(f"↕ {pct}%", style=indicator_style)
        t.add_row(Padding(left, (0, 1)), Padding(right, (0, 1)))
        return t

    @staticmethod
    def _short_cwd() -> str:
        try:
            p = Path.cwd()
            home = Path.home()
            if p == home or home in p.parents:
                return "~/" + str(p.relative_to(home))
            return str(p)
        except Exception:
            return "."

    # ─── banner ──────────────────────────────────────────────────────

    def _render_banner(self) -> Any:
        left = Table.grid(padding=(0, 1))
        left.add_column(no_wrap=True)
        left.add_column()
        sun = Text("☀", style=f"bold {C_AMBER}")
        title = Text()
        title.append(self._workflow_name, style=f"bold {C_INK}")
        title.append("  workflow  ", style=C_INK_DIM)
        title.append_text(self._chip())
        sub = Text()
        if self._snakefile:
            sub.append(self._short_path(self._snakefile) + "  ", style=C_INK_DIM)
        if self._workflow_id:
            sub.append("id ", style=C_INK_FAINT); sub.append(self._workflow_id[:8], style=C_INK_SOFT)
        left_col = Group(title, sub) if sub.plain else title
        left.add_row(Padding(sun, (0, 1, 0, 1)), left_col)

        right = Text(justify="right")
        elapsed = _mono_elapsed(time.time() - self._workflow_start_time) if self._workflow_start_time else "—"
        eta = self._compute_eta()
        eta_str = _mono_elapsed(eta) if (eta is not None and eta > 0) else "—"
        right.append("elapsed ", style=C_INK_FAINT)
        right.append(elapsed, style=f"bold {C_INK}")
        right.append("   eta ", style=C_INK_FAINT)
        right.append(eta_str, style=f"bold {C_AMBER if eta and eta > 0 else C_INK_DIM}")

        layout = Table.grid(expand=True, padding=(0, 2))
        layout.add_column(ratio=1)
        layout.add_column(justify="right", no_wrap=True)
        layout.add_row(left, right)
        return Padding(layout, (1, 2, 0, 2))

    def _chip(self) -> Text:
        if self._finished:
            if self._jobs_failed > 0:
                return Text(" FAILED ", style=f"bold {C_ROSE} on #2a1410")
            return Text(" COMPLETE ", style=f"bold {C_LEAF} on #18200d")
        if self._workflow_start_time is None:
            return Text(" IDLE ", style=f"bold {C_INK_DIM} on {C_LINE}")
        if self._total_jobs == 0:
            return Text(" STARTING ", style=f"bold {C_AMBER} on #2a1f0a")
        return Text(" RUNNING ", style=f"bold {C_AMBER} on #2a1f0a")

    # ─── hero stats ──────────────────────────────────────────────────

    def _render_hero(self) -> Any:
        stats = Table.grid(expand=True, padding=(0, 1))
        for _ in HERO_STATS:
            stats.add_column(ratio=1)
        label_row = []
        value_row = []
        total = self._total_jobs
        done = self._jobs_done
        failed = self._jobs_failed
        running = len(self._active_jobs)
        queued = max(0, total - done - failed - running)
        pct = int((done / total) * 100) if total else 0
        cores_txt = f"{self._cores}/32" if self._cores else "—"
        data = {
            "total":   (Text("TOTAL",     style=C_INK_FAINT), Text(str(total),   style=f"bold {C_INK}")),
            "done":    (Text("COMPLETED", style=C_INK_FAINT), Text(f"{done}  {pct}%", style=f"bold {C_LEAF}")),
            "running": (Text("RUNNING",   style=C_INK_FAINT), Text(str(running), style=f"bold {C_AMBER}")),
            "queued":  (Text("QUEUED",    style=C_INK_FAINT), Text(str(queued),  style=f"bold {C_INK_SOFT}")),
            "failed":  (Text("FAILED",    style=C_INK_FAINT), Text(str(failed),  style=f"bold {C_ROSE if failed else C_INK_DIM}")),
            "cores":   (Text("CORES",     style=C_INK_FAINT), Text(cores_txt,    style=f"bold {C_INK}")),
        }
        for k in HERO_STATS:
            l, v = data[k]
            label_row.append(l)
            value_row.append(v)
        stats.add_row(*label_row)
        stats.add_row(*value_row)
        # accent bar on the left of the total column
        return Padding(stats, (0, 2, 0, 2))

    def _render_progress_bar_row(self) -> Any:
        total = max(self._total_jobs, 1)
        done = self._jobs_done
        failed = self._jobs_failed
        running = len(self._active_jobs)
        width = max(20, self._get_width() - 30)
        bar = _progress_bar_markup(done, running, failed, total, width=width)
        caption = Text()
        caption.append(f"{done}", style=f"bold {C_INK}")
        caption.append(f"/{self._total_jobs} completed  ·  ", style=C_INK_DIM)
        if running:
            caption.append(f"{running} running", style=C_AMBER)
            caption.append("  ·  ", style=C_INK_DIM)
        if failed:
            caption.append(f"{failed} failed", style=C_ROSE)
            caption.append("  ·  ", style=C_INK_DIM)
        eta = self._compute_eta()
        if eta is not None and eta > 0:
            caption.append("eta ", style=C_INK_FAINT)
            caption.append(_mono_elapsed(eta), style=f"bold {C_AMBER}")
        g = Group(bar, caption)
        return Padding(g, (0, 2, 0, 2))

    # ─── sparkline ──────────────────────────────────────────────────

    def _render_sparkline_row(self) -> Any:
        row = Table.grid(expand=True, padding=(0, 1))
        row.add_column(no_wrap=True)
        row.add_column(no_wrap=True)
        row.add_column(ratio=1)
        row.add_column(justify="right", no_wrap=True)
        label = Text("THROUGHPUT", style=f"{C_INK_FAINT}")
        cur = self._current_throughput()
        val = Text()
        val.append(f"{cur:.1f}", style=f"bold {C_INK}")
        val.append("/min", style=C_INK_DIM)
        spark = _sparkline(list(self._spark_history), width=max(16, self._get_width() - 42))
        peak = self._peak_throughput()
        peak_txt = Text()
        peak_txt.append("peak ", style=C_INK_FAINT)
        peak_txt.append(f"{peak:.1f}/min", style=C_INK_SOFT)
        row.add_row(label, val, spark, peak_txt)
        return Padding(row, (0, 2, 0, 2))

    # ─── two-column (jobs | rules+events) ───────────────────────────

    def _render_two_col(self) -> Any:
        left = self._render_jobs_panel()
        right = self._render_right_panel()
        two = Table.grid(expand=True, padding=(0, 2))
        two.add_column(ratio=5)
        two.add_column(ratio=4)
        two.add_row(left, right)
        return Padding(two, (0, 2, 0, 2))

    def _render_section_title(self, text: str, right: Optional[Text] = None) -> Any:
        tbl = Table.grid(expand=True, padding=(0, 1))
        tbl.add_column(no_wrap=True)
        tbl.add_column(ratio=1)
        tbl.add_column(justify="right", no_wrap=True)
        dash = Text("─", style=C_AMBER)
        title = Text(text, style=f"bold {C_INK_DIM}")
        tbl.add_row(dash, title, right or Text(""))
        return tbl

    def _render_jobs_panel(self) -> Any:
        n_running = len(self._active_jobs)
        n_submitted = len(self._job_specs)
        right = Text()
        right.append(f"{n_running} running", style=C_AMBER)
        if n_submitted:
            right.append(" · ", style=C_INK_FAINT)
            right.append(f"{n_submitted} submitted", style=C_SKY)
        right.append(" · ", style=C_INK_FAINT)
        total = self._total_jobs
        queued = max(0, total - self._jobs_done - self._jobs_failed - n_running - n_submitted)
        right.append(f"{queued} queued", style=C_INK_SOFT)
        header = self._render_section_title("ACTIVE JOBS", right)
        rows: list[Any] = [header]
        with self._kb_lock:
            f_active = self._filter_active
            f_text = self._filter_text.lower()
        spinner = _spinner_frame()
        matched = 0
        for jid in sorted(self._active_jobs.keys()):
            je = self._active_jobs[jid]
            if f_active and f_text and f_text not in f"{je.rule} {je.wildcards_str}".lower():
                continue
            matched += 1
            rows.append(self._render_job_row(je, spinner, submitted=False))
        for jid in sorted(self._job_specs.keys()):
            je = self._job_specs[jid]
            if f_active and f_text and f_text not in f"{je.rule} {je.wildcards_str}".lower():
                continue
            matched += 1
            rows.append(self._render_job_row(je, "→", submitted=True))
        if matched == 0:
            if f_active and f_text:
                rows.append(Padding(
                    Text(f"  (no jobs matching \"{f_text}\")", style=C_INK_FAINT),
                    (0, 0, 0, 0),
                ))
            else:
                rows.append(Padding(Text("  (no active jobs)", style=C_INK_FAINT), (0, 0, 0, 0)))
        return Group(*rows)

    def _render_job_row(self, je: _JobEntry, spinner: str, submitted: bool = False) -> Any:
        elapsed = _mono_elapsed(time.time() - je.started_at)
        with self._kb_lock:
            show_res = self._verbose_override or bool(self.common_settings.verbose)
        row = Table.grid(expand=True, padding=(0, 1))
        row.add_column(width=2, no_wrap=True)    # spinner / status icon
        row.add_column(width=5, no_wrap=True)    # #id
        row.add_column(ratio=1)                  # rule + wildcards
        row.add_column(no_wrap=True)             # resources (hidden unless verbose)
        row.add_column(width=6, no_wrap=True, justify="right")  # timer
        spin = Text(spinner, style=C_SKY if submitted else C_AMBER)
        idt = Text(f"#{je.job_id:02d}", style=C_INK_FAINT)
        rule_txt = Text()
        name_style = C_INK_DIM if submitted else f"bold {C_INK}"
        rule_txt.append(je.rule, style=name_style)
        if je.wildcards_str:
            rule_txt.append(f"  {je.wildcards_str}", style=C_INK_DIM)
        res = Text(je.resources_str, style=C_INK_FAINT) if show_res else Text("")
        timer_style = C_INK_DIM if submitted else C_AMBER
        timer = Text(elapsed, style=timer_style)
        row.add_row(spin, idt, rule_txt, res, timer)
        return row

    def _render_right_panel(self) -> Any:
        rules_title = self._render_section_title("RULES")
        rules_body = self._render_rules_rows()
        events_title = self._render_section_title("RECENT EVENTS")
        events_body = self._render_events_rows()
        return Group(rules_title, rules_body, Text(""), events_title, events_body)

    def _render_rules_rows(self) -> Any:
        if not self._rule_stats:
            return Padding(Text("  (waiting for jobs)", style=C_INK_FAINT), (0, 0, 0, 0))
        tbl = Table.grid(expand=True, padding=(0, 1))
        tbl.add_column(ratio=1)           # rule name
        tbl.add_column(width=16, no_wrap=True)  # bar
        tbl.add_column(width=11, no_wrap=True, justify="right")  # counts
        # Stable sort: rules with running > failed > done-but-incomplete > done > pending
        def sort_key(item):
            name, s = item
            touched = s.done + s.running + s.failed
            return (0 if s.running else 1 if touched and touched < s.total else 2 if s.failed else 3 if s.total and s.done == s.total else 4, name)
        for name, s in sorted(self._rule_stats.items(), key=sort_key):
            name_style = C_INK if s.running else C_INK_SOFT
            name_txt = Text(name, style=name_style, overflow="ellipsis", no_wrap=True)
            if s.running:
                name_txt.append(" ●", style=C_AMBER)
            bar = _rule_bar_markup(s.done, s.running, s.failed, max(s.total, 1), width=14)
            counts = Text()
            counts.append(f"{s.done}", style=f"bold {C_INK}")
            counts.append(f"/{s.total}", style=C_INK_DIM)
            if s.failed:
                counts.append(f"·{s.failed}✗", style=C_ROSE)
            tbl.add_row(name_txt, bar, counts)
        return tbl

    def _render_events_rows(self) -> Any:
        with self._kb_lock:
            f_active = self._filter_active
            f_text = self._filter_text.lower()
        if not self._events:
            return Padding(Text("  (no events yet)", style=C_INK_FAINT), (0, 0, 0, 0))
        tbl = Table.grid(expand=True, padding=(0, 1))
        tbl.add_column(width=5, no_wrap=True)   # time
        tbl.add_column(width=1, no_wrap=True)   # glyph
        tbl.add_column(ratio=1)                 # message
        glyph_color = {"info": C_SKY, "ok": C_SAGE, "warn": C_AMBER, "err": C_ROSE}
        matched = 0
        for e in self._events:
            if f_active and f_text:
                plain = re.sub(r"\[/?[^\]]*\]", "", e.markup).lower()
                if f_text not in plain:
                    continue
            matched += 1
            t = Text(e.t, style=C_INK_FAINT)
            g = Text(e.glyph, style=f"bold {glyph_color.get(e.kind, C_INK_DIM)}")
            m = Text.from_markup(e.markup)
            tbl.add_row(t, g, m)
        if matched == 0:
            msg = f"  (no events matching \"{f_text}\")" if (f_active and f_text) else "  (no events yet)"
            return Padding(Text(msg, style=C_INK_FAINT), (0, 0, 0, 0))
        return tbl

    # ─── shell block ────────────────────────────────────────────────

    def _render_shell_block(self) -> list[Any]:
        if not self._last_shell or not self.common_settings.printshellcmds:
            return []
        rule, cmd, job_ref = self._last_shell
        right = Text()
        right.append("rule ", style=C_INK_FAINT)
        right.append(rule, style=f"bold {C_AMBER}")
        if job_ref:
            right.append("  ·  ", style=C_INK_FAINT)
            right.append(job_ref, style=C_INK_DIM)
        title = self._render_section_title("LAST SHELL COMMAND", right)
        # Use Syntax with word-wrap so long commands don't break the frame.
        syn = Syntax(
            cmd.strip(),
            "bash",
            theme=self._syntax_theme,
            word_wrap=True,
            background_color="default",
            padding=(0, 2),
        )
        return [Text(""), title, syn]

    # ─── summary (shown when finished) ──────────────────────────────

    def _render_summary(self) -> list[Any]:
        if not self._finished:
            return []
        if self._jobs_failed:
            style = C_ROSE
            title = "Workflow failed"
            detail = Text()
            detail.append(f"{self._jobs_failed} jobs failed", style=f"bold {C_ROSE}")
            detail.append(f"  ·  {self._jobs_done} completed", style=C_INK_SOFT)
            if self._workflow_start_time:
                detail.append(f"  ·  ", style=C_INK_DIM)
                detail.append(_mono_elapsed(time.time() - self._workflow_start_time), style=C_INK)
        else:
            style = C_LEAF
            title = "Workflow complete"
            detail = Text()
            detail.append(f"{self._jobs_done} jobs finished", style=f"bold {C_LEAF}")
            if self._workflow_start_time:
                detail.append(f"  in  ", style=C_INK_DIM)
                detail.append(_mono_elapsed(time.time() - self._workflow_start_time), style=f"bold {C_INK}")
        body = Group(Text(title, style=f"bold {style}"), detail)
        p = Panel(body, border_style=style, padding=(0, 1), box=box.HEAVY)
        return [Text(""), Padding(p, (0, 2))]

    # ─── utility ────────────────────────────────────────────────────

    @staticmethod
    def _short_path(p: str) -> str:
        try:
            path = Path(p)
            home = Path.home()
            if home in path.parents or path == home:
                return "~/" + str(path.relative_to(home))
            return str(path)
        except Exception:
            return str(p)

    def _maybe_update_job_from_slurm_msg(self, msg: str) -> None:
        """Parse Slurm submission messages to back-fill rule/wildcard info.

        The Slurm executor logs e.g.:
          Job 22 has been submitted with SLURM jobid 48989767
          (log: .../.snakemake/slurm_logs/rule_align_pe/TEAD1-2/48989767.log).
        We extract the Snakemake job ID (22), the rule name (align_pe), and the
        wildcard string (TEAD1-2) and update the already-running _JobEntry.
        """
        m = re.search(
            r"Job\s+(\d+)\s+has been submitted.*?slurm_logs/rule_([^/]+)/([^/]+)/",
            msg,
        )
        if not m:
            return
        jid = int(m.group(1))
        rule = m.group(2)
        wc = m.group(3)
        je = self._active_jobs.get(jid)
        if je is None:
            return
        old_rule = je.rule
        je.rule = rule
        je.wildcards_str = wc
        # Fix rule running-counts: the placeholder was skipped, now count the real rule.
        if old_rule != rule:
            if old_rule in self._rule_stats and self._rule_stats[old_rule].running > 0:
                self._bump_rule(old_rule, running_delta=-1)
            self._bump_rule(rule, running_delta=1)
        if self._live is not None:
            self._refresh()

    def _push_event(self, glyph: str, kind: str, markup: str) -> None:
        self._events.append(_EventEntry(time.strftime("%H:%M"), glyph, kind, markup))

    def _bump_rule(self, rule: str, *, total_delta: int = 0, done_delta: int = 0,
                   running_delta: int = 0, failed_delta: int = 0) -> None:
        s = self._rule_stats.setdefault(rule, _RuleStats())
        s.total += total_delta
        s.done += done_delta
        s.running += running_delta
        s.failed += failed_delta

    # ─────────────────────── event handlers ────────────────────────

    def _handle_workflow_started(self, record: LogRecord) -> None:
        self._snakefile = getattr(record, "snakefile", None)
        wid = getattr(record, "workflow_id", None)
        self._workflow_id = str(wid) if wid else None
        if self._snakefile:
            # Derive a pretty workflow name from the parent directory.
            try:
                self._workflow_name = Path(self._snakefile).parent.name or "workflow"
            except Exception:
                pass
        self._workflow_start_time = time.time()
        dryrun = " [DRY RUN]" if self.common_settings.dryrun else ""
        self._push_event("i", "info", f"[bold]workflow started[/]{dryrun}")
        self._add_log_line(Rule(Text(f"Workflow Started{dryrun}", style=f"bold {C_AMBER}"), style=C_AMBER))
        # Do NOT start the TUI here — WORKFLOW_STARTED fires before DAG validation and
        # resource checks. Starting the alternate screen this early swallows Snakemake's
        # own stderr output and can cause silent failures. The TUI starts on the first
        # JOB_INFO event, after Snakemake is ready to actually run jobs.
        if self._live is not None:
            self._refresh()

    def _handle_run_info(self, record: LogRecord) -> None:
        # Local executor: per_rule_job_counts + total_job_count
        # Cluster executor (Slurm): stats dict with rule names and a 'total' key
        per_rule: Optional[dict[str, int]] = getattr(record, "per_rule_job_counts", None)
        total: Optional[int] = getattr(record, "total_job_count", None)
        if per_rule is None:
            raw = getattr(record, "stats", None)
            if isinstance(raw, dict):
                total = int(raw.get("total", 0))
                per_rule = {k: int(v) for k, v in raw.items() if k != "total"}
            else:
                per_rule, total = {}, 0
        per_rule = per_rule or {}
        total = total or 0
        self._total_jobs = total
        for rule_name, count in per_rule.items():
            self._rule_stats.setdefault(rule_name, _RuleStats()).total = count
        self._push_event("i", "info",
                         f"resolved DAG · [bold]{total}[/] jobs across {len(per_rule)} rules")
        # Print the job-count table once above the frame — useful for scrollback/CI logs.
        table = Table(title=None, box=box.SIMPLE, show_header=True, header_style=C_INK_DIM)
        table.add_column("Rule", style=C_AMBER, no_wrap=True)
        table.add_column("Jobs", justify="right", style=C_INK)
        for rule_name, count in sorted(per_rule.items()):
            table.add_row(rule_name, str(count))
        table.add_row("[bold]total[/]", f"[bold]{total}[/]")
        self._add_log_line(table)
        if self._live is not None:
            self._refresh()

    def _handle_resources_info(self, record: LogRecord) -> None:
        cores = getattr(record, "cores", None)
        nodes = getattr(record, "nodes", None)
        if cores is not None:
            self._cores = cores
        if nodes:
            try:
                self._nodes = list(nodes)
            except TypeError:
                self._nodes = [str(nodes)]
        provided = getattr(record, "provided_resources", None)
        parts = []
        if cores is not None:
            parts.append(f"{cores} cores")
        if self._nodes:
            parts.append(f"nodes {', '.join(self._nodes)}")
        if provided:
            parts.append(f"resources {provided}")
        if parts:
            self._push_event("i", "info", "provisioning · " + " · ".join(parts))
        if self._live is not None:
            self._refresh()

    def _handle_shellcmd(self, record: LogRecord) -> None:
        if not self.common_settings.printshellcmds:
            return
        rule_name = getattr(record, "rule_name", None) or "?"
        shellcmd = getattr(record, "shellcmd", None)
        jobid = getattr(record, "jobid", None)
        if shellcmd:
            job_ref = f"job #{jobid}" if jobid is not None else ""
            self._last_shell = (rule_name, shellcmd, job_ref)
        if self._live is not None:
            self._refresh()

    def _handle_job_info(self, record: LogRecord) -> None:
        rule_name: str = getattr(record, "rule_name", "?") or "?"
        quiet = self.common_settings.quiet
        if quiet and rule_name in quiet:
            return
        jobid: int = getattr(record, "jobid", -1)
        threads: int = getattr(record, "threads", 1) or 1
        wildcards = getattr(record, "wildcards", None)
        resources = getattr(record, "resources", None)
        wc_str = ""
        if wildcards:
            try:
                wc_str = ",".join(f"{k}={v}" for k, v in dict(wildcards).items())
            except Exception:
                wc_str = str(wildcards)
        res_parts = [f"threads={threads}"]
        if resources:
            try:
                for k, v in dict(resources).items():
                    if k in ("_cores", "_nodes"):
                        continue
                    res_parts.append(f"{k}={v}")
            except Exception:
                res_parts.append(str(resources))
        res_str = " · ".join(res_parts)
        self._job_specs[jobid] = _JobEntry(
            job_id=jobid, rule=rule_name, wildcards_str=wc_str,
            threads=threads, resources_str=res_str,
        )
        # Ensure the rule appears in the rules list even if run_info didn't list it.
        self._rule_stats.setdefault(rule_name, _RuleStats())
        # Verbose mode: dump a detail table. Do this BEFORE _ensure_live so that for
        # the very first job the Panel falls back to direct console output (pre-TUI).
        if self.common_settings.verbose:
            header = Text()
            header.append(f"Job {jobid}  ", style=f"bold {C_AMBER}")
            header.append(f"· {rule_name}", style=C_INK)
            table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
            table.add_column(style=C_INK_DIM, no_wrap=True)
            table.add_column(style=C_INK)
            table.add_row("Rule", rule_name)
            table.add_row("Threads", str(threads))
            for k in ("input", "output"):
                v = getattr(record, k, None)
                if v:
                    table.add_row(k.title(), ", ".join(v))
            if wc_str:
                table.add_row("Wildcards", wc_str)
            if resources:
                table.add_row("Resources", str(resources))
            reason = getattr(record, "reason", None)
            if reason:
                table.add_row("Reason", str(reason))
            shellcmd = getattr(record, "shellcmd", None)
            if shellcmd and self.common_settings.printshellcmds:
                self._add_log_line(Panel(Group(table, Syntax(shellcmd, "bash", theme=self._syntax_theme)),
                                         title=header, border_style=C_AMBER))
            else:
                self._add_log_line(Panel(table, title=header, border_style=C_AMBER))
        # Start the TUI now so cluster jobs show as "submitted" even if JOB_STARTED
        # never fires (e.g. Slurm executor doesn't always emit it).
        self._ensure_live()
        if self._live is not None:
            self._refresh()

    def _handle_job_started(self, record: LogRecord) -> None:
        # Local executor uses 'job_ids'; Slurm executor uses 'jobs'.
        raw = getattr(record, "jobs", None) or getattr(record, "job_ids", None) or []
        job_ids: list[int] = list(raw) if raw else []
        self._ensure_live()
        for jid in job_ids:
            spec = self._job_specs.pop(jid, None) or _JobEntry(job_id=jid, rule=f"job_{jid}")
            spec.started_at = time.time()
            spec.state = "running"
            self._active_jobs[jid] = spec
            # Only bump the rule counter if we have a real rule name; placeholder names
            # get updated (and counted) later when the Slurm submission message arrives.
            if spec.rule in self._rule_stats:
                self._bump_rule(spec.rule, running_delta=1)
            self._push_event("▸", "ok", f"job [bold]#{jid}[/] started · [{C_AMBER}]{spec.rule}[/]")
        self._refresh()

    def _handle_job_finished(self, record: LogRecord) -> None:
        job_id: int = getattr(record, "job_id", getattr(record, "jobid", -1))
        # For cluster executors (e.g. Slurm), jobs may complete without passing
        # through _active_jobs if JOB_STARTED was never emitted — check _job_specs too.
        je = self._active_jobs.pop(job_id, None) or self._job_specs.pop(job_id, None)
        if je is not None:
            self._bump_rule(je.rule, running_delta=-1, done_delta=1)
            dur = _mono_elapsed(time.time() - je.started_at)
            self._push_event("✓", "ok", f"job [bold]#{job_id}[/] finished · [{C_AMBER}]{je.rule}[/] [dim]in {dur}[/]")
        else:
            self._push_event("✓", "ok", f"job [bold]#{job_id}[/] finished")
        self._jobs_done += 1
        self._completion_times.append(time.time())
        if self._live is not None:
            self._refresh()

    def _handle_job_error(self, record: LogRecord) -> None:
        jobid: int = getattr(record, "jobid", -1)
        je = self._active_jobs.pop(jobid, None) or self._job_specs.pop(jobid, None)
        if je is not None:
            self._bump_rule(je.rule, running_delta=-1, failed_delta=1)
            self._push_event("✗", "err", f"job [bold]#{jobid}[/] [bold {C_ROSE}]failed[/] · [{C_AMBER}]{je.rule}[/]")
        else:
            self._push_event("✗", "err", f"job [bold]#{jobid}[/] [bold {C_ROSE}]failed[/]")
        self._jobs_failed += 1
        self._add_log_line(Panel(
            Text(f"Job {jobid} failed", style=f"bold {C_ROSE}"),
            border_style=C_ROSE, title=Text("Job Error", style=f"bold {C_ROSE}"),
        ))
        if self._live is not None:
            self._refresh()

    def _handle_group_info(self, record: LogRecord) -> None:
        group_id = getattr(record, "group_id", "?")
        self._push_event("i", "info", f"group [bold]{group_id}[/] started")
        if self._live is not None:
            self._refresh()

    def _handle_group_error(self, record: LogRecord) -> None:
        groupid = getattr(record, "groupid", "?")
        job_error_info = getattr(record, "job_error_info", {}) or {}
        aux_logs = getattr(record, "aux_logs", []) or []
        self._push_event("✗", "err", f"group [bold]{groupid}[/] [bold {C_ROSE}]failed[/]")
        lines = [Text(f"Group {groupid} failed", style=f"bold {C_ROSE}")]
        if job_error_info:
            lines.append(Text(str(job_error_info), style=C_INK_SOFT))
        if aux_logs and self.common_settings.show_failed_logs:
            for log in aux_logs:
                lines.append(Text(str(log), style=C_INK_SOFT))
        self._add_log_line(Panel(Group(*lines), title=Text("Group Error", style=f"bold {C_ROSE}"),
                                 border_style=C_ROSE))
        if self._live is not None:
            self._refresh()

    def _handle_progress(self, record: LogRecord) -> None:
        done: int = getattr(record, "done", 0) or 0
        total: int = getattr(record, "total", 0) or 0
        if total > 0:
            self._total_jobs = total
            self._ensure_live()
        # If Snakemake reports a done count we haven't seen job-by-job, sync up.
        if done > self._jobs_done:
            self._jobs_done = done
        if self._live is not None:
            self._refresh()
        if total > 0 and done >= total:
            # Finalize: freeze a summary inside the frame, then tear down Live.
            self._finished = True
            if self._live is not None:
                self._refresh()
            self._stop_live()
            self._print_workflow_summary()

    def _handle_error(self, record: LogRecord) -> None:
        exception = getattr(record, "exception", None)
        traceback_str = getattr(record, "traceback", None)
        location = getattr(record, "location", None)
        rule = getattr(record, "rule", None)
        file_ = getattr(record, "file", None)
        line = getattr(record, "line", None)
        msg = record.getMessage()
        parts: list[Any] = []
        if rule:
            parts.append(Text(f"Rule: {rule}", style=f"bold {C_ROSE}"))
        if location:
            parts.append(Text(f"Location: {location}", style=C_INK_DIM))
        elif file_:
            loc = f"{file_}:{line}" if line else file_
            parts.append(Text(f"File: {loc}", style=C_INK_DIM))
        if exception:
            parts.append(Text(str(exception), style=f"bold {C_ROSE}"))
        if msg:
            parts.append(Text(msg, style=C_ROSE))
        if traceback_str:
            parts.append(Syntax(str(traceback_str), "python", theme=self._syntax_theme))
        self._add_log_line(Panel(
            Group(*parts) if parts else Text("An error occurred", style=C_ROSE),
            title=Text("Error", style=f"bold {C_ROSE}"),
            border_style=C_ROSE,
        ))
        self._push_event("✗", "err", (str(exception) if exception else msg) or "error")
        if self._live is not None:
            self._refresh()

    def _handle_debug_dag(self, record: LogRecord) -> None:
        if not getattr(self.common_settings, "debug_dag", False):
            return
        status = getattr(record, "status", "")
        file_ = getattr(record, "file", "")
        self._add_log_line(Text(f"[DAG] {status} {file_}", style=C_INK_DIM))

    def _handle_rulegraph(self, record: LogRecord) -> None:
        pass  # not implemented

    # ─── final summary ──────────────────────────────────────────────

    def _print_workflow_summary(self) -> None:
        elapsed_str = ""
        if self._workflow_start_time is not None:
            secs = time.time() - self._workflow_start_time
            elapsed_str = f" in {secs:.1f}s"
        if self._jobs_failed > 0:
            body = Text()
            body.append(f"{self._jobs_failed} job(s) failed", style=f"bold {C_ROSE}")
            body.append(", ", style=C_INK_DIM)
            body.append(f"{self._jobs_done} completed{elapsed_str}", style=C_INK_SOFT)
            summary = Panel(body, title=Text("Workflow Failed", style=f"bold {C_ROSE}"),
                            border_style=C_ROSE)
        else:
            body = Text()
            body.append(f"{self._jobs_done} job(s) completed{elapsed_str}", style=f"bold {C_LEAF}")
            summary = Panel(body, title=Text("Workflow Complete", style=f"bold {C_LEAF}"),
                            border_style=C_LEAF)
        # In TTY/TUI mode: summary is already visible in the final TUI frame (_render_summary).
        # Only print to the terminal in non-TTY mode (CI, pipes) for log file capture.
        if not self.console.is_terminal:
            self._print_above(summary)
        # Reset counters so a subsequent run in the same process starts clean.
        self._jobs_done = 0
        self._jobs_failed = 0
        self._workflow_start_time = None
        self._finished = False
        self._active_jobs.clear()
        self._job_specs.clear()
        self._rule_stats.clear()
        self._events.clear()
        self._log_lines.clear()
        self._spark_history.clear()
        self._completion_times.clear()
        self._total_jobs = 0

    # ─── abstract properties ────────────────────────────────────────

    @property
    def writes_to_stream(self) -> bool:
        return True

    @property
    def writes_to_file(self) -> bool:
        return False

    @property
    def has_filter(self) -> bool:
        return False

    @property
    def has_formatter(self) -> bool:
        return False

    @property
    def needs_rulegraph(self) -> bool:
        return False
