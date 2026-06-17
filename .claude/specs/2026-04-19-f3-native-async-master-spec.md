# F3-B — Native async wrapper (master spec)

**Date:** 2026-04-19
**Status:** Master spec — decomposes F3-B into sub-projects. Each sub-project gets its own brainstorm → spec → plan → implementation cycle in a later session.
**Target release:** 3.0.0 (major; breaking public API additions and internal architecture rewrite).
**Branch:** `feat/sqlalchemy-async-f3-native` (off `feat/sqlalchemy-creator-and-py314-f1` which carries F1+F2).

## Why this is a master spec, not a regular spec

F3-B is not a single feature — it's a multi-week architectural effort that rewrites the wrapper's internal pipeline with asyncio while preserving 100% of F1/F2's sync surface. Attempting to fit it into one spec + one plan + one subagent-driven pass would produce an unreviewable diff and unreliable code. Instead, this master spec:

1. Names the sub-projects and sequences them.
2. Identifies the cross-cutting invariants each sub-project must preserve.
3. Flags the interfaces between sub-projects so they can be built and tested independently.
4. Lists what explicitly is and isn't in scope for 3.0.0.

Each named sub-project will get its own `.claude/specs/<date>-f3b-<project>.md` and `.claude/plans/<date>-f3b-<project>.md` in a later session. Only one sub-project is implemented per session.

## Goal

Deliver a first-class async path through the wrapper so applications using SQLAlchemy's `create_async_engine` (or writing directly against an `AsyncAwsWrapperConnection`) get all wrapper features — failover, EFM, R/W splitting, IAM auth, Aurora topology monitoring — running natively on asyncio without greenlet bridges or thread hops per call.

## Non-goals for 3.0.0

- **MySQL async.** mysql-connector-python has no native async API. Deferred until an async MySQL driver (`aiomysql`, `asyncmy`) can be formally supported. The F3-B async path ships PG-only.
- **Removing the sync path.** Sync `AwsWrapperConnection` + sync plugin pipeline stays fully supported. F1 and F2 users upgrading to 3.0.0 must see no behavior change in their existing code. Async is additive.
- **Rewriting the storage/cache layer.** `SlidingExpirationCache`, `CacheMap`, connection properties — these stay sync. Async code that uses them does so via the existing sync helpers (they're not on the hot path per-connection).
- **Django async support.** Django's own async-view story for databases is still evolving; out of scope here.

## Cross-cutting invariants (every sub-project must honor)

1. **No greenlets, no thread-bridge.** Async paths use `async def` / `await` end-to-end. If a path goes through `loop.run_in_executor`, that's a pipeline-external call (e.g., into a sync stdlib function), not an internal abstraction.
2. **Sync path untouched.** No sync public API changes. Sync unit tests keep passing with zero modification.
3. **Separate namespaces, shared types.** Async classes live under `aws_advanced_python_wrapper.aio.` (or similar — naming decided in sub-project 0). Shared value types (`HostInfo`, `Properties`, `HostRole`, `DialectCode`, exception classes) are shared across sync and async — both paths import them from their existing modules.
4. **Plugin contract parallelism.** Every sync `Plugin` subclass gets an async counterpart with matching method names prefixed/suffixed per convention (e.g., `AsyncPlugin.connect` returns `Awaitable`). Contracts are enforced by type checking.
5. **`psycopg.AsyncConnection` is the only async DBAPI in 3.0.0.** No aiomysql, no asyncpg (asyncpg has a different type surface that doesn't match SA's `postgresql+psycopg_async` dialect).
6. **Test parity.** Every sync unit test targeting a plugin or core service has an async counterpart. Integration tests added for the SA async engine path.
7. **Release-resource symmetry.** The wrapper's shutdown contract (`release_resources()`) grows an async variant (`async release_resources()` or equivalent) that drains async background tasks in addition to the existing sync cleanup.
8. **Driver extensibility (load-bearing).** Adding a future async MySQL driver (aiomysql / asyncmy / an eventual mysql-connector async API) must be a ~5-file additive change with zero edits to failover / EFM / RWS / auth / topology / host-list-provider / plugin-service / plugin-manager code. Guaranteed by invariants 8a–8d below.

### Driver-extensibility invariants (detail of invariant 8)

These are the concrete guarantees that make "minimal-effort async MySQL later" actually hold. SP-0 must design the ABCs and file layout such that:

**8a. `AsyncDriverDialect` is the SOLE async-driver-specific interface.** Every plugin, core service, and connection class receives it via dependency injection. No async plugin file may `import psycopg`, `import aiomysql`, or similar. The ABC exposes exactly the operations failover/EFM/RWS/auth/etc. need:

```
async connect(host_info, props) -> AsyncConnection
async close(conn)
async abort(conn)
async is_closed(conn) -> bool
async is_in_transaction(conn) -> bool
async get_autocommit(conn) -> bool
async set_autocommit(conn, value)
async is_read_only(conn) -> bool
async set_read_only(conn, value)
async transfer_session_state(from_conn, to_conn)
async prepare_connect_info(host_info, props) -> Properties
async unwrap_connection(conn_obj) -> AsyncConnection
# plus any method the sync DriverDialect exposes
```

Concrete subclasses for 3.0.0: `AsyncPsycopgDriverDialect`. Future: `AsyncAiomysqlDriverDialect`, `AsyncAsyncmyDriverDialect`, `AsyncMysqlConnectorAsyncDriverDialect` (when/if mysql-connector ships async).

**8b. `DatabaseDialect` stays shared sync/async.** Aurora-PG, Aurora-MySQL, Global-Aurora-*, Multi-AZ-*, plus standard PG and MySQL dialects — unchanged from sync. They define SQL strings like `SELECT pg_catalog.aurora_db_instance_identifier()` and `SELECT @@aurora_server_id` and are consumed by the async code through `AsyncDriverDialect`. No async-side duplicate of these classes.

**8c. `AsyncAwsWrapperConnection` is driver-agnostic.** It holds an `AsyncDriverDialect` + raw target connection and delegates every operation through the ABC. No driver-specific attribute access, no psycopg-specific type hints exposed to callers. Instance-level proxying (for SA's dialect introspection) is done through a single `__getattr__` that forwards to the underlying target connection, which is valid for any async DBAPI connection object.

**8d. DBAPI submodules and SA dialects are thin facades.** The per-driver DBAPI submodule (`aws_advanced_python_wrapper.aio.psycopg`, later `aws_advanced_python_wrapper.aio.aiomysql`) contains only:
   - import of the target driver's connect function
   - one `async def connect(conninfo, **kwargs)` wrapper that builds an `AsyncAwsWrapperConnection`
   - `_dbapi.install_async(...)` call to populate the module's PEP 249/async-DBAPI surface (mirrors F1's `_dbapi.install()` helper)
   - PEP 562 `__getattr__` forwarding to the target driver module

Every new async driver submodule should be a near-verbatim copy of this template with the target driver import swapped. Same for the SA dialect subclass (`import_dbapi` returning the new submodule; driver attribute set).

### Concrete cost accounting for a future async MySQL addition

If these invariants hold, a future PR adding e.g. `aiomysql` looks like:

- `aws_advanced_python_wrapper/aio/aiomysql.py` (new) — ~25 LOC
- `aws_advanced_python_wrapper/aio/driver_dialect/aiomysql.py` (new) — ~60 LOC implementing `AsyncDriverDialect` against aiomysql
- `aws_advanced_python_wrapper/driver_dialect_manager.py` — add aiomysql to the dialect-code → class map (~3 LOC)
- `aws_advanced_python_wrapper/sqlalchemy_dialects/mysql_async.py` (new) — ~25 LOC SA dialect subclass
- `pyproject.toml` — 4 new entry-points
- `tests/unit/test_aiomysql_submodule.py` (new) — ~80 LOC
- `tests/integration/container/test_async_sqlalchemy.py` — 2 parametrize additions for MySQL async

Zero touches to: any failover plugin, any EFM plugin, any RWS plugin, any auth plugin, `AsyncPluginService`, `AsyncPluginManager`, `AsyncAwsWrapperConnection` (except possibly one line if a new driver-code enum value must be imported).

**If an SP finds itself needing driver-specific code inside a plugin**, that's a signal the `AsyncDriverDialect` ABC is missing an operation — add it to the ABC instead of branching on driver type in the plugin.

## Sub-project decomposition

Each item below is the header of a future dedicated spec. Sequencing matters — earlier items build the foundation later items depend on.

### SP-0 — Architecture and naming decisions

**What gets decided:**
- Namespace layout (`aws_advanced_python_wrapper.aio.*` vs a flat `a*` prefix vs `async_*`).
- Base class naming (`AsyncAwsWrapperConnection`, `AsyncPlugin`, etc.).
- Whether async plugins inherit from sync plugins or from a new `AsyncPlugin` ABC.
- The `AsyncDriverDialect` ABC signature (per invariant 8a — this is the load-bearing interface for driver-pluggability).
- Per-driver file layout under `aws_advanced_python_wrapper/aio/driver_dialect/` (one file per concrete driver dialect; 3.0.0 ships only `psycopg.py`).
- How `DatabaseDialect` is shared sync/async without duplication (invariant 8b).
- Logging / telemetry strategy under asyncio.

**Why it's first:** every later sub-project writes against these names and hierarchies. Getting the ABC wrong here means either refactoring every plugin later OR locking in driver-specific code inside plugins (which breaks invariant 8a and kills the future-MySQL-async cost advantage).

**Deliverables:**
- Naming-decisions document.
- `aws_advanced_python_wrapper/aio/__init__.py` package skeleton.
- `aws_advanced_python_wrapper/aio/driver_dialect/__init__.py` with the `AsyncDriverDialect` ABC — the full method signature list, no implementations.
- Stub abstract base classes (`AsyncPlugin`, `AsyncConnectionProvider`, etc.) with no logic.
- Unit test that exercises the ABC via a fake driver dialect — proves the contract is complete enough that a "fake" driver (one that returns pre-canned values) can drive the whole ABC without special-casing. If the fake can't satisfy it, the ABC is missing something that will later force driver-specific code into a plugin.

### SP-1 — Async core services (services_container, plugin_service shell)

**What gets built:**
- `AsyncPluginService` mirroring `PluginService` — holds current connection, host list, driver dialect, database dialect. Most methods become `async def`.
- `services_container` gains async-side helpers (`get_async_storage_service()`, etc.), sharing the same underlying storage where possible.
- `AsyncPluginManager` — the equivalent of `PluginManager`, builds the async plugin chain and dispatches `connect` / `execute` calls to it.

**Why second:** core services are the foundation every plugin hooks into. Must exist before plugin rewrites.

**Deliverables:** `aws_advanced_python_wrapper/aio/plugin_service.py`, `aws_advanced_python_wrapper/aio/plugin_manager.py`, unit tests that hit the async service shell with a pair of toy async plugins (no real plugin behavior yet).

### SP-2 — Async `AsyncAwsWrapperConnection` + psycopg.AsyncConnection driver dialect

**What gets built:**
- `AsyncAwsWrapperConnection` — mirrors `AwsWrapperConnection` but `async def __init__`, `async close()`, `async commit()`, `async rollback()`, `async cursor()`, etc.
- `AsyncAwsWrapperCursor` — matches psycopg's `AsyncCursor` contract (`await cursor.execute(...)`, `async for row in cursor`, etc.).
- `AsyncPsycopgDriverDialect` — the async equivalent of `PsycopgDriverDialect`, using `psycopg.AsyncConnection` as the target.
- An `aws_advanced_python_wrapper.aio.psycopg` submodule that exposes `connect(dsn, **kwargs)` returning an `AsyncAwsWrapperConnection` — DBAPI surface similar to F1's `aws_advanced_python_wrapper.psycopg` but async.

**Why third:** needs SP-1's plugin pipeline. Establishes the connection contract every plugin later awaits against.

**Deliverables:** `aws_advanced_python_wrapper/aio/wrapper.py`, `aws_advanced_python_wrapper/aio/driver_dialect/psycopg.py`, `aws_advanced_python_wrapper/aio/psycopg.py`, unit tests using a mocked `psycopg.AsyncConnection`.

### SP-3 — Async host list provider + cluster topology monitor

**What gets built:**
- `AsyncAuroraHostListProvider` — awaitable topology queries against the Aurora cluster, replacing sync ones.
- `AsyncClusterTopologyMonitor` — uses `asyncio.Task` instead of threads for periodic topology refresh.
- Shared cache layer: same `SlidingExpirationCache` instance as sync, but async code interacts with it through short sync calls (cache ops are memory-only, no I/O).

**Why fourth:** async failover (SP-4) needs async topology refresh. Without this, failover can't find replacement writers without blocking.

**Deliverables:** `aws_advanced_python_wrapper/aio/host_list_provider.py`, `aws_advanced_python_wrapper/aio/cluster_topology_monitor.py`, unit tests with a mocked AsyncConnection.

### SP-4 — Async failover plugin

**What gets built:**
- `AsyncFailoverPlugin` — listens on exception propagation in the async plugin chain, triggers topology refresh, opens new `AsyncAwsWrapperConnection` against the new writer, propagates `FailoverSuccessError` / `FailoverFailedError` to the caller.
- Configuration: same connection properties (`failover_mode`, `failover_timeout_sec`, etc.) shared with sync failover plugin.

**Why fifth:** the flagship wrapper feature; must land before any integration test claims "async works with failover."

**Deliverables:** `aws_advanced_python_wrapper/aio/failover_plugin.py`, unit tests using mocked topology + connection.

### SP-5 — Async EFM (host monitoring) plugin

**What gets built:**
- `AsyncHostMonitoringPlugin` — replaces the ThreadPoolExecutor-based EFM v2 machinery with `asyncio.Task`s per monitored host.
- `AsyncMonitorService`, `AsyncMonitor` — spawns per-host async monitors, detects network failures via awaitable probes.

**Why sixth:** EFM is closely coupled to failover (EFM detects outages that failover responds to). Reusing sync EFM under async would require thread bridging.

**Deliverables:** `aws_advanced_python_wrapper/aio/host_monitoring_plugin.py`, `aws_advanced_python_wrapper/aio/monitor_service.py`, unit tests.

### SP-6 — Async R/W splitting + async connection provider

**What gets built:**
- `AsyncReadWriteSplittingPlugin` — the async twin of `ReadWriteSplittingPlugin`.
- `AsyncConnectionProvider` abstract base + `AsyncSqlAlchemyPooledConnectionProvider` (or a new asyncio-native per-instance pooling strategy — decided in SP-6's brainstorm).

**Why seventh:** RWS is the second flagship feature after failover.

**Deliverables:** `aws_advanced_python_wrapper/aio/read_write_splitting_plugin.py`, `aws_advanced_python_wrapper/aio/connection_provider.py`, unit tests.

### SP-7 — Async auth plugins (IAM, Secrets Manager, Federated, Okta)

**What gets built:**
- `AsyncIamAuthenticationPlugin` — generates IAM tokens via `aiobotocore` or the stdlib `concurrent.futures` bridge (SP-7 brainstorm decides).
- `AsyncAwsSecretsManagerPlugin`
- `AsyncFederatedAuthenticationPlugin`
- `AsyncOktaAuthenticationPlugin`

**Why eighth:** auth plugins are independent of failover/EFM — they contribute to the `connect` path only. Can be done in parallel with SP-6 in principle, but sequenced after for reviewability.

**Deliverables:** four async plugin files + tests.

### SP-8 — Async Aurora connection tracker, initial connection strategy, custom endpoint, blue-green, limitless, fastest-response

**What gets built:** async twins of the remaining plugins. Most are small.

**Why ninth:** these plugins are minor in size but non-trivial in number; batching them last keeps the main flagship features (SP-4, SP-5, SP-6) unambiguous.

**Deliverables:** one file per plugin under `aws_advanced_python_wrapper/aio/`.

### SP-9 — SQLAlchemy async dialect registration

**What gets built:**
- `AwsWrapperPGPsycopgAsyncDialect` subclasses SA's `PGDialectAsync_psycopg`, overrides `import_dbapi()` to return `aws_advanced_python_wrapper.aio.psycopg`.
- Entry-point registration: `aws_wrapper_postgresql.psycopg_async` and `aws_wrapper_postgresql_async` pointing at the new dialect.
- `wrapper_plugins` URL alias translation still applies.

**Why tenth:** needs SP-2's async submodule as the DBAPI target. Everything else in F3-B could conceivably be reached only via direct `AsyncAwsWrapperConnection.connect(...)` usage, but the SA async dialect is the natural app-level entry point.

**Deliverables:** `aws_advanced_python_wrapper/sqlalchemy_dialects/pg_async.py`, updated `pyproject.toml` entry-points, unit tests for registry / URL resolution, one example script.

### SP-10 — `release_resources` async surface + documentation overhaul

**What gets built:**
- `async_release_resources()` (or `release_resources_async()` — SP-0 naming decision) that awaits on async task cancellation for all async monitors/topology refreshers. Sync `release_resources()` continues to drain sync threads as it does today.
- Updated `docs/using-the-python-wrapper/SqlAlchemySupport.md` — new "Async usage" section with `create_async_engine` example; removes the "sync only" limitation bullet.
- Updated README pointing at async support.
- CHANGELOG entries for 3.0.0.

**Why last:** wraps the user-facing polish once all the code exists. Docs reference concrete APIs that don't exist until SP-9 ships.

**Deliverables:** `aws_advanced_python_wrapper/aio/cleanup.py`, docs updates, CHANGELOG lines, one SA async example (`PGSQLAlchemyAsyncFailover.py`).

## Parallel-work notes

- **SP-7 (auth plugins) can be developed in parallel with SP-4 / SP-5 / SP-6** once SP-1 and SP-2 land. Auth plugins don't depend on failover/EFM/RWS.
- **SP-8 (minor plugins) can start in parallel with SP-7** for the same reason.
- **SP-9 (SA dialect) depends on SP-2 only** — could technically ship as soon as SP-2 is green, giving app developers a no-plugin async path to experiment with. Worth considering for early user feedback.

## Integration test strategy

- Each sub-project adds unit tests with mocked `psycopg.AsyncConnection`.
- Integration tests go into a single `tests/integration/container/test_async_sqlalchemy.py` file added in SP-9 (or earlier if useful during SP-4 failover work). Four cases planned: async PG failover with and without R/W splitting; async PG topology recovery; async PG IAM auth end-to-end. MySQL async cases are explicitly skipped per the 3.0.0 scope.
- CI matrix unchanged from F1 (3.10–3.14 via `main.yml`).
- `integration_tests.yml` gains new jobs for the async suite.

## Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Sync/async code divergence over time — bug fixes land on one side only | High | Every bug fix in a sync plugin is a task to mirror into the async plugin. Codify as a PR-checklist item. Shared value types help. |
| Async semantics for `FailoverSuccessError` aren't preserved — caller's awaited coroutine sees a subtly different exception than sync | Medium | Share the exception classes (F1 already has PEP 249 classification); async path just `raise`s them normally. Test parity. |
| `psycopg.AsyncConnection` evolves in a breaking way between 3.0.0 and 3.14 driver versions | Medium | Pin psycopg to the same caret range as sync. Surface driver-version issues in CI. |
| Thread-pool cache / storage primitives hold a lock long enough to starve event loop | Medium | Audit locks touched on the async hot path. Use `asyncio.Lock` for new async-only primitives; reuse `threading.Lock` only for short CPU-bound sections. |
| Users confuse `plugins=` on sync creator-pattern vs `wrapper_plugins=` on URL | Low | Already documented in F2 docs; async just inherits the rule. Add an explicit note in the async section. |

## Success criteria for 3.0.0 release

- All F1 + F2 unit tests continue to pass (regression-free).
- Every sub-project's unit tests pass on Python 3.10–3.14.
- `tests/integration/container/test_async_sqlalchemy.py` passes on Aurora PG (maintainer-triggered).
- `create_async_engine("aws_wrapper_postgresql+psycopg_async://...")` produces a working engine that fails over, does R/W splitting, and authenticates via IAM/Secrets Manager/Federated/Okta where applicable.
- `SqlAlchemySupport.md` covers async with a working example.
- CHANGELOG + Maintenance.md reflect 3.0.0 release notes and compatibility matrix.
- README's "Known Limitations" no longer says "sync only."

## What lands in this session

Only the foundation:
- Branch set up off F1+F2 (done).
- Version bumped to 3.0.0 (done).
- Dependencies refreshed to latest within existing caret constraints (done).
- This master spec written and committed.

Actual async code starts in SP-0 in a future session.

## Out-of-scope escape hatches

If during SP implementation any of the following emerge, they warrant their own escalated spec (not silent scope creep inside an SP):

- Removing or changing any sync public API.
- Dropping a supported Python version.
- Switching SA major version.
- Switching psycopg major version.
- Adding a new async-MySQL driver dependency.
- Any async-path feature that doesn't have a sync analog (e.g., async-only plugins).
