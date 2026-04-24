---
name: verify
description: Run the full local check suite (mypy, flake8, isort, unit tests) before claiming a change is done. Mirrors .github/workflows/main.yml so a green `/verify` should match CI.
---

Run these commands from the repo root, in order. Stop at the first failure and report the output:

```bash
poetry run mypy .
poetry run flake8 .
poetry run isort --check-only .
poetry run pytest ./tests/unit -Werror
```

Notes:
- `isort --check-only` reports drift; run `poetry run isort .` to fix, then re-run `/verify`.
- `pytest -Werror` matches CI — new warnings fail. Fix the cause, or add a `filterwarnings` entry in `pyproject.toml` `[tool.pytest.ini_options]` if the warning is known-benign.
- This does NOT run integration tests (`tests/integration/`) — those need infra. If a change plausibly affects integration behavior, flag it for the user to run manually.
- If poetry is not installed, surface that to the user — don't `pip install` around it.

After all four pass, report a one-line "verify: pass (mypy, flake8, isort, pytest-unit)".
