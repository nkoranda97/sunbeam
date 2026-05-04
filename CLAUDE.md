# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Sunbeam is a Snakemake logger plugin that uses `rich` for beautiful, informative terminal output. It implements the `snakemake-interface-logger-plugins` plugin interface.

## Commands

This project uses `uv` for dependency management (Python 3.14).

```bash
uv sync                                                    # Install dependencies
uv run python -m snakemake_logger_plugin_sunbeam.demo      # Run the interactive demo
```

## Architecture

The plugin must implement the interface defined by `snakemake-interface-logger-plugins`. The key integration points are:

- **Plugin interface**: `snakemake-interface-logger-plugins` defines the abstract base classes and plugin registration mechanism. Snakemake discovers logger plugins via Python package entry points.
- **Rendering**: `rich` is used for all terminal output — use `rich.console.Console`, `rich.panel`, `rich.progress`, etc. rather than plain `print`.
- **Entry point**: The plugin must be registered in `pyproject.toml` under `[project.entry-points."snakemake_logger_plugins"]` so Snakemake can discover it.
