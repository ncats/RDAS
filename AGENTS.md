# Agent Instructions

## Project
This repo builds/maintains the RDAS Memgraph data pipeline.

## Working Style
- Keep changes small and focused.
- Do not rewrite unrelated files.
- Prefer existing patterns over new abstractions.
- Reuse helper methods from `utils/tools.py` whenever possible before adding local helper functions.
- Ask before destructive Git operations.
- When writing code, add detailed comments whenever they help explain the implementation.
- When modifying existing code or fixing an error, add detailed comments beside the changed logic explaining how it works and why the change is needed.
- While working, send brief visible progress notes that explain what you are checking, what you found, and why you are making an edit. Keep final answers concise, but preserve this trail of progress updates during the work.

## Commands
- Run tests with: `pytest`
- Use the `rdas` conda environment for runtime checks that need project dependencies: `conda run -n rdas python <script-or-test>`
- Check formatting with: `<command here>`
- Run pipeline locally with: `<command here>`

## SQL
- When writing or optimizing SQL queries, use `rdas_db_indexes.txt` and `rdas_db_schema.sql` as references.

## Code Style
- Use one-line function signatures: keep all parameters, default values, and return annotations on the same `def` line.
- Keep a blank line immediately after each Python `def` line.
- Convert only multi-line `#` comment blocks into triple-quoted comment strings; leave single-line `#` comments alone.
- Avoid short wrapper functions that only forward to another call; keep logic inline unless a helper adds meaningful validation, reuse, naming, or separation of concerns.

## Concurrency
- Use multithreading, usually `ThreadPoolExecutor`, for I/O-bound work such as HTTP/API calls, database waits, file downloads, and other tasks where workers mostly wait on external services. Keep database writes in the parent task when practical, and have worker threads return insert/update-ready values.
- Use multiprocessing, usually `ProcessPoolExecutor` or `multiprocessing.Pool`, for CPU-bound Python work such as expensive text matching, person disambiguation, NLP scoring, or transformations where the GIL would limit threads.
- Do not use multiprocessing only for network/API calls unless there is a specific isolation need, such as a non-thread-safe model/runtime that must live separately per worker.
- Do not share open database connections, cursors, or task instances across process workers. Prefer module-level worker functions for multiprocessing so they are pickle-safe; each process should create its own external resources if needed.
- Keep worker counts portable. Use `_resolve_worker_count()` from `utils/tools.py` for thread/process pools instead of hard-coding machine-specific CPU counts; for model-heavy process pools, also consider RAM and model-loading overhead.
- When adding or changing concurrency, leave a short code comment explaining why threads or processes are the right fit for that workload.

## Git
- Do not commit unless explicitly asked.
- Leave existing user changes intact.
- After each code modification, generate one meaningful GitHub commit message for only the work just completed. Put it on a single physical line so it is easy to copy/paste while committing; do not make it shorter or less specific merely because it is one line.
- Use one of these commit message formats: `Refactor: the-message-about what you have done`, `Feature: the-message-about what you have done`, or `Fix: the-message-about what you have done`.
- Present the commit message as a complete command: `git commit -m "the-commit-messages"`.

## Notes
- Ignore generated/intermediate data files.
- Keep `Z_Alert/` templates tracked.
