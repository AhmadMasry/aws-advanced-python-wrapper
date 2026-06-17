# Async Parity — Overview & Phase Index

**Context:** Four parallel audits (2026-04-20) identified 25 functional gaps between the async (`aws_advanced_python_wrapper/aio/`) and sync implementations shipped in the F3-B branch. Sync is the upstream-authored ground truth. Goal: close all 25 gaps on the current branch, land as a single upstream PR once done.

**Landing target:** continue on `feat/post-3.0-followups`; one upstream PR at the end.

**Python matrix:** 3.10–3.14 inclusive (see CLAUDE.local.md). No `Self`, no PEP 695, no `TypeAlias` from `typing`. `match` is fine.

**Telemetry:** OpenTelemetry parity is in scope — every new async plugin surface that has a sync telemetry counter must emit an equivalent async one.

**Quality gate:** `/verify` (mypy + flake8 + isort --check-only + pytest tests/unit -Werror) green after every phase.

## Phase sequence

| # | Phase | Fixes (from audit) | Depends on |
|---|---|---|---|
| A | `AsyncPluginService` foundation — DatabaseDialect, ExceptionManager, host availability, strategy host selection, host list provider, release_resources | P0 #1 #2 partial; unlocks B/C/D/F/G | — |
| B | Failover v2 parity — host availability tracking, retry loop, reader strategy, `TransactionResolutionUnknownError`, replace string-match heuristic | P0 #1 #2 #3 #4 | A |
| C | Host monitoring (EFM) — standing background watchdog, per-host alias tracking, failure count across intervals | P0 #5 | A |
| D | Aurora connection tracker — track all connections per writer, invalidate-on-failover, prune dead refs | P0 #6 | A |
| E | Async auth — retry-on-401, honor `IAM_EXPIRATION`/`SECRETS_MANAGER_EXPIRATION`, `FORCE_CONNECT`, endpoint+region discovery, ARN parsing | P0 #7 #8, P1 #14 | A |
| F | Read/write splitting — `READER_HOST_SELECTOR_STRATEGY`, txn gating, initial role verification, pool-aware close, topology validation | P0 #9 #10, P2 #24 | A, B |
| G | Cluster topology monitor — panic mode, high-freq refresh, ignore-request window, per-host probe tasks, verified-writer handoff | P0 #11 | A, B |
| H | Plugin registry + host list providers — FailoverV2, StaleDns, Limitless, FastestResponseStrategy, BlueGreen, AuroraInitialConnectionStrategy, SimpleRWS factories; MultiAz + GlobalAurora host list providers | P1 #12 #13 | A–G |
| I | PEP 249 cursor surface + Developer parity + OpenTelemetry plumbing + P2 cleanups — `lastrowid`, `scroll`, `callproc`, `nextset`, `setinputsizes`, `setoutputsize`, autocommit/isolation properties on connection, Developer callback injection modes, async telemetry factories, Okta SAML regex, clock source, aiohttp proxy | P1 #15 #17 #18, P2 #20–25 | A |
| J | CustomEndpoint `wait-for-info` — block connect until monitor populated or timeout expires | P1 #19 | A |

Each phase is its own plan document written immediately before execution.

## Working rules

- **One phase per commit** (or tight logical group; never mix phases).
- **TDD**: red, green, commit per sub-task.
- **No fallbacks for hypothetical scenarios** (per system prompt). If sync doesn't validate something, async doesn't either, unless the audit flagged it.
- **No speculative refactors.** Each phase touches only files its gaps require.
- Commits use conventional-commit style (`feat:`, `fix:`, `refactor:` — see `git log --oneline` for examples).
- Cite audit ID (e.g., `P0 #3`) in commit body.

## Per-phase plan filenames

- `2026-04-20-phase-a-async-plugin-service-foundation.md` ← written now
- `2026-MM-DD-phase-b-async-failover-v2-parity.md` ← written after A lands
- `2026-MM-DD-phase-c-async-efm-standing-watchdog.md` ← written after A lands
- …etc.
