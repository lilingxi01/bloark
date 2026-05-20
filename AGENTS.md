# AGENTS.md

## Cursor Cloud specific instructions

### Overview

BloArk (Blocks Architecture) is a pure Python library for processing Wikipedia revision history into memory-friendly, parallelizable blocks. It has three core components: **Builder** (ingests XML dumps), **Reader** (reads warehouses), and **Modifier** (transforms warehouses). No external services (databases, Docker, web servers) are required.

### Development commands

- **Install dependencies:** `poetry install` (uses `pyproject.toml` + `poetry.lock`)
- **Run tests:** `poetry run pytest` (8 tests; all sample data is bundled in `tests/sample_data/`)
- **Build package:** `poetry build`
- **Lint:** No linter is configured in this project.

### Caveats

- `python3-dev` must be installed for native extensions (e.g. `brotli`) to compile. The update script handles this.
- Poetry may be installed via pip but may not be on `$PATH` by default. Ensure `$HOME/.local/bin` is on `$PATH`.
- Test fixtures generate mock `.7z` and `.zst` files at session scope using multiprocessing; the first test run creates these in the working directory and they persist for subsequent runs.
- DeprecationWarnings about `fork()` in multi-threaded contexts are expected in tests and can be ignored.
