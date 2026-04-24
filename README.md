# Sunbeam

A beautiful terminal logger plugin for [Snakemake](https://snakemake.github.io), built with [Rich](https://rich.readthedocs.io).

## What it looks like

- Workflow start/end banners with elapsed time
- Live progress bar (jobs completed / total)
- Per-job spinners while jobs run
- Syntax-highlighted shell commands
- Detailed job info tables (in verbose mode)
- Red error panels with tracebacks

## Installation

```bash
pip install snakemake-logger-plugin-sunbeam
```

Or in development:

```bash
git clone https://github.com/you/sunbeam
cd sunbeam
uv sync
```

## Usage

Pass `--logger sunbeam` when running Snakemake:

```bash
snakemake --logger sunbeam
```

### Options

| Flag | Effect |
|---|---|
| `--logger-sunbeam-theme <name>` | Pygments syntax theme for code blocks (default: `monokai`) |
| `--printshellcmds` | Show shell commands as syntax-highlighted blocks |
| `--verbose` | Show full job detail tables (inputs, outputs, wildcards, resources) |
| `--quiet` | Suppress job output for specified rules |
| `--dryrun` | Adds a `[DRY RUN]` label to the workflow banner |
| `--nocolor` | Disable all colour output |

### Example

```bash
# Verbose run with shell commands printed, using a different theme
snakemake --logger sunbeam --verbose --printshellcmds --logger-sunbeam-theme vim

# Dry run preview
snakemake --logger sunbeam --dryrun
```

## Development

```bash
uv sync          # install deps + dev deps
uv run pytest -v # run the test suite (29 tests)
```

The test suite uses `Console(record=True)` to capture rendered output and assert on
what each log event produces — no real Snakemake process needed.
