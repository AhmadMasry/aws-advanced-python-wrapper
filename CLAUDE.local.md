# CLAUDE.local.md

Private preferences for this repo. Not shared — gitignored.

## About me

- **Role:** consumer / integrator of this wrapper. I use it from another project and dip in here occasionally.
- Assume I'm comfortable with Python/poetry but not steeped in this wrapper's plugin internals (failover, IAM auth, host monitoring, read/write splitting). Explain plugin-architecture specifics when they're load-bearing for a change.

## Where to look

Source root: `aws_advanced_python_wrapper/`.

- **Using / configuring the wrapper** (start here for consumer questions):
  - `docs/using-the-python-wrapper/UsingThePythonWrapper.md` — main usage guide
  - `docs/using-the-python-wrapper/using-plugins/UsingThe<Name>Plugin.md` — one per plugin, covers config params and behavior (Failover, Failover2, IamAuthentication, AwsSecretsManager, ReadWriteSplitting, HostMonitoring, FastestResponseStrategy, Limitless, BlueGreen, CustomEndpoint, AuroraConnectionTracker, AuroraInitialConnectionStrategy, FederatedAuthentication, OktaAuthentication, Developer, SimpleReadWriteSplitting)
  - `docs/using-the-python-wrapper/` also has: `AwsCredentials.md`, `DatabaseDialects.md`, `DriverDialects.md`, `DjangoSupport.md`, `FailoverConfigurationGuide.md`, `GlobalDatabases.md`, `HostAvailabilityStrategy.md`, `ReaderSelectionStrategies.md`, `SessionState.md`, `SupportForRDSMultiAzDBCluster.md`, `Telemetry.md`, `ClusterId.md`
  - `docs/examples/*.py` — runnable per-plugin × per-driver samples (PG… / MySQL…). Grep here first to see how a plugin is wired end-to-end.
- **Understanding internals** (when the change needs it):
  - `docs/development-guide/Architecture.md` — overall design
  - `docs/development-guide/PluginManager.md` + `PluginService.md` — plugin lifecycle
  - `docs/development-guide/LoadablePlugins.md` — how plugins are discovered/loaded
  - `docs/development-guide/Pipelines.md` — request pipeline

## Communication

- **Plan before coding** on non-trivial changes: propose the approach, wait for go-ahead, then edit. For one-line fixes or obvious renames, just do it.

## Project gotchas

- Python matrix is 3.10–3.14 **inclusive** on the feat branches (commit `19f7cca` added 3.14; `pyproject.toml` has `requires-python = ">=3.10,<4.0"`). Don't use 3.11+ syntax (`Self`, `TypeAlias` from typing, PEP 695 type params are not; `match` is fine). Don't require features from a newer Python even if my local env has it. Upstream main may not have 3.14 yet — when rebasing for the single upstream PR, re-check README's "How to Contribute" matrix.
- Tool commands (all via poetry): `poetry run mypy .`, `poetry run flake8 .`, `poetry run isort --check-only .` (drop `--check-only` to fix), `poetry run python -m pytest ./tests/unit`.
- CI runs `pytest ./tests/unit -Werror` — warnings fail. If a test surfaces a new warning, fix the cause or add a `filterwarnings` entry in `pyproject.toml` (see `[tool.pytest.ini_options]`).
- flake8 `max-line-length = 150` — don't reformat to 79/88.
- flake8-type-checking (TC/TC1) is on — imports used only for typing belong under `if TYPE_CHECKING:`.
- `mypy.ini` excludes a list of example/integration files (`PGFailover.py`, `MySQLFailover.py`, etc.). Don't add new type errors in files not in the exclude list.
- Integration tests are separate (`tests/integration/`) and require infra; don't run them by default.
- Conventional-commit style in recent history (`fix:`, `refactor:`, `chore(deps):`, `docs:`). Match it for PR titles.

## Verification

- Run `/verify` before claiming a change is done.

## Contributing / new-feature process

Canonical source is `CONTRIBUTING.md` + `Maintenance.md`. Non-obvious bits Claude should honor:

- **Discuss first.** For anything more than a small bug fix or doc tweak, open (or point me at) a GitHub issue before writing code. Significant work without a linked issue tends to bounce in review.
- **Branch from latest `main`.** Not from a stale fork point — rebase before sending.
- **Keep the diff focused.** Don't reformat files you didn't need to touch, don't renumber imports across the repo, don't bundle refactors into a feature PR. CONTRIBUTING explicitly calls this out.
- **PRs squash-merge.** The PR body *is* the squashed commit message (see `.github/PULL_REQUEST_TEMPLATE.md` — "Body of this PR and the intended squashed commit message"). Match the conventional-commit style used in recent history (`feat:`, `fix:`, `refactor:`, `chore(deps):`, `docs:`) in the PR title.
- **Target the current major.** New features land on 2.x (current). 1.x is in its maintenance window (ends 2027-01-14) — maintainers don't back-port; don't scope features against it unless I ask.
- **SemVer is strict.** Breaking public-API changes → next major only. Prefer deprecation + shim on the current major; surface the trade-off if a clean break looks necessary.
- **Deprecate before removing.** If a change retires a public symbol, leave a deprecated shim + warning for at least one minor; don't delete.
- **Release cadence: last week of every month** (skipped if there were no changes since the last release). Context for "when will this ship?" questions. `Maintenance.md` rounds this to "approximately every four weeks".
- **Docs CI gotcha.** `.github/workflows/main.yml` `paths-ignore`s `**/*.md` and `docs/**` for unit tests, but the `markdown-link-check` job still runs on `docs/`. Doc-only PRs fail if any link in `docs/` is broken — verify links when editing docs.
- **Security issues → AWS vulnerability reporting page**, never a public GitHub issue. If a change surfaces a security concern, flag it to me and stop; don't file it on GitHub.
- **License:** Apache-2.0. The PR template auto-captures the confirmation; no extra DCO/sign-off needed.

## Changelog hygiene

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/) + SemVer strictly.

- **Categories & emojis** (match existing entries exactly):
  - `### :magic_wand: Added` — new features
  - `### :bug: Fixed` — bug fixes
  - `### :crab: Changed` — perf, refactors, doc updates that change behavior
  - `### :crab: Breaking Changes` — only in major releases; lead with a `> [!WARNING]` block listing what was removed
- Every entry links the PR (`([PR #1234](.../pull/1234))`).
- Maintainers batch-update CHANGELOG at release time, but user-facing PRs should include a proposed line in the PR description under the right category so it can be copied in.
- **Dropping a Python version is a breaking change** (see 2.0.0 dropping 3.8/3.9). Don't propose it outside a major bump.

## Driver / plugin gotchas (from README "Known Limitations")

- **MySQL + `use_pure`**: for Aurora MySQL the README recommends `use_pure=True` (pure-Python connector) because the C extension's `is_connected` blocks indefinitely on network failure. **BUT** the IAM Authentication Plugin is incompatible with `use_pure=True` (MySQL's pure-Python driver truncates passwords at 255 chars; IAM tokens are longer — expect `int1store requires 0 <= i <= 255` / `struct.error: ubyte format requires 0 <= number <= 255`). A feature touching MySQL + IAM must surface this trade-off to the user and not silently pick one.
- **Blue/Green deployments** need specific engine versions (RDS PG `rds_tools v1.7` / PG 17.1, 16.5, 15.9, 14.14, 13.17, 12.21+; Aurora PG 17.5, 16.9, 15.13, 14.18, 13.21+; Aurora MySQL engine `3.07`+). Aurora Global DB + Multi-AZ clusters are NOT supported with Blue/Green. If a change touches the BG plugin, honor this matrix.
- **Toxiproxy integration tests** (`tests/integration/`): shutting down Python while a host is disabled via Toxiproxy can hang exit (MySQL) or segfault (Psycopg) because EFM helper threads get stuck reading from the underlying driver. Re-enable all proxied hosts before the test process exits. Don't file "Python hangs on exit" bugs from Toxiproxy runs without first ruling this out.

## Benchmarks

- Live in `benchmarks/`. They measure wrapper-call **overhead only** — not target-driver perf, not failover-process perf.
- Not in poetry deps: install per-session with `pip install pytest-benchmark`, then run one file at a time: `pytest benchmarks/<file>.py`. Don't add `pytest-benchmark` to `pyproject.toml` without a maintainer's call.
- Perf-sensitive changes (plugin pipeline, host selection, connection lifecycle) should include a benchmark run before/after in the PR description.

## Resource-cleanup invariant (consumer-critical)

Several past bugs stemmed from relying on `__del__` / GC for cleanup (PRs #1063, #1066, #1078). The current rule:

- Consumers should call `aws_advanced_python_wrapper.release_resources()` at program exit to tear down background threads/pools cleanly.
- **New features must not create background threads at import time** and must not rely on `__del__` to release resources — hook into the existing `release_resources()` path or an explicit `close()` instead. If a change adds a new long-lived resource, flag where it's registered for cleanup.
