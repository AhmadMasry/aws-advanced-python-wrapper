# SP-0 — Architecture and naming decisions

**Date:** 2026-04-20
**Status:** Draft — SP-0 of the F3-B native-async master spec.
**Parent spec:** `.claude/specs/2026-04-19-f3-native-async-master-spec.md`
**Branch:** `feat/sqlalchemy-async-f3-native`

## Goal

Lock the naming, namespace, ABC signatures, and file layout that every later sub-project will build on. Ship the skeleton (empty package + ABCs + stub classes + acid test) so SP-1 has a real foundation to write against.

## Non-goals

- Any plugin logic, any connection logic, any real driver interaction.
- Any SA dialect registration — that's SP-9.
- Decisions about failover/EFM/RWS/auth/etc. — those belong to their respective sub-projects.

## Decisions

### D1. Namespace

`aws_advanced_python_wrapper.aio.*` for all async code. Chosen over `async_*` prefix or a flat namespace because `.aio` is the idiomatic Python convention for async-parallel packages (aiohttp, aiomysql, aiofiles, aiobotocore, aiodns, etc.) — users expect it when looking for async support.

### D2. Class naming

All async classes use the `Async` prefix:

- `AsyncAwsWrapperConnection`
- `AsyncAwsWrapperCursor`
- `AsyncDriverDialect`
- `AsyncPlugin`
- `AsyncConnectionProvider`
- `AsyncPluginService`, `AsyncPluginManager` (added in SP-1)
- `AsyncFailoverPlugin`, `AsyncHostMonitoringPlugin`, etc. (added in their respective SPs)

This matches psycopg's `Connection` / `AsyncConnection` pairing and SQLAlchemy's `Engine` / `AsyncEngine` pairing. It's unambiguous when both sync and async variants are imported in the same file — common in code that bridges the two layers (e.g., integration tests, future examples).

### D3. Plugin class hierarchy

Parallel. `AsyncPlugin` is an independent ABC, NOT a subclass of `Plugin`. Async plugin classes do not inherit from sync plugin classes.

Rationale:
- Mixing `def` and `async def` methods on one class is error-prone (`await`ing a sync method silently wraps it in `asyncio.coroutine`-like behavior).
- Sync code calling `super()` from an async context, or vice versa, would create subtle bugs the type system can't catch.
- SA made the same call for `Engine` / `AsyncEngine` — no parent/child relationship.
- The duplication cost is real but bounded (plugin constructors + state init are small); mechanical checklist to keep in sync.

### D4. `DatabaseDialect` is shared

No `AsyncDatabaseDialect`. The existing sync `DatabaseDialect` subclasses (`AuroraPgDialect`, `AuroraMysqlDialect`, `RdsPgDialect`, etc.) are consumed by async code unchanged. These classes expose SQL strings (e.g., the Aurora writer-probe query) and static metadata (e.g., `dialect_update_candidates`); they have no I/O of their own. Async code runs their SQL through `AsyncDriverDialect` methods.

### D5. Per-driver file layout

```
aws_advanced_python_wrapper/aio/driver_dialect/
├── __init__.py     # re-exports AsyncDriverDialect ABC
├── base.py         # AsyncDriverDialect ABC (THIS SP-0 delivers it)
└── psycopg.py      # AsyncPsycopgDriverDialect (SP-2 delivers it)
```

Future drivers (aiomysql, asyncmy, an eventual mysql-connector async API) add one file here and one entry in `DriverDialectManager`'s registration map. No changes to `base.py` when adding a driver, unless a new operation is needed by the plugins — in which case it's added to `base.py` FIRST, then implemented in every existing driver.

### D6. Logging and telemetry

Reuse existing `aws_advanced_python_wrapper.utils.log.Logger`. It's structured logging over stdlib `logging`; works in async contexts without changes. Telemetry (X-Ray, OpenTelemetry) is already abstracted via `TelemetryFactory` in the sync pipeline — SP-1 will extend it with async context tracking, not rebuild it.

### D7. `AsyncDriverDialect` ABC signatures

The full method list parallels `DriverDialect` (see `aws_advanced_python_wrapper/driver_dialect.py`), converted to `async def` for any method that could touch the network or the driver-side connection state. Pure property helpers stay sync.

| Method | Sync | Async form | Notes |
|---|---|---|---|
| `driver_name` (property) | sync | sync | Static string |
| `dialect_code` (property) | sync | sync | Static string |
| `network_bound_methods` (property) | sync | sync | Static set |
| `is_read_only(conn)` | sync (cached attr) | **async** | Some drivers probe the server |
| `set_read_only(conn, val)` | sync | **async** | May issue `SET TRANSACTION READ ONLY` |
| `get_autocommit(conn)` | sync (cached attr) | **async** | Driver-dependent |
| `set_autocommit(conn, val)` | sync | **async** | May issue `SET autocommit = ?` |
| `supports_connect_timeout` | sync | sync | Static capability |
| `supports_socket_timeout` | sync | sync | Static capability |
| `supports_tcp_keepalive` | sync | sync | Static capability |
| `supports_abort_connection` | sync | sync | Static capability |
| `can_execute_query(conn)` | sync | **async** | May check conn state |
| `set_password(props, pwd)` | sync | sync | Pure dict operation |
| `is_dialect(connect_func)` | sync | sync | Introspects the callable |
| `prepare_connect_info(host_info, props)` | sync | sync | Pure dict operation |
| `is_closed(conn)` | sync | **async** | Driver probes conn state |
| `abort_connection(conn)` | sync | **async** | Driver call |
| `is_in_transaction(conn)` | sync | **async** | Driver probes |
| `get_connection_from_obj(obj)` | sync | sync | Attribute access |
| `unwrap_connection(conn_obj)` | sync | sync | Attribute access |
| `transfer_session_state(from, to)` | sync | **async** | May issue state-copy SQL |
| `ping(conn)` | sync | **async** | Issues `SELECT 1` |
| `connect(host_info, props, connect_func)` | — | **async** | NEW on async side; sync side handles this through a different path |

The `async connect(...)` method is new: on the sync side the `DriverDialect` doesn't open connections (that's the `PluginManager`'s job); on the async side we centralize connect-via-driver in the ABC because the async driver's connect API is driver-specific and plugins need to dispatch through the ABC.

`execute` is NOT mirrored on the async side — sync's `execute()` wraps a thread-pool-based timeout. Async uses `asyncio.wait_for` instead, so per-query timeout lives in `AsyncPluginService` or `AsyncAwsWrapperCursor`, not in `AsyncDriverDialect`.

### D8. Abstract-base validation test (the acid test)

A single test file `tests/unit/test_aio_contracts.py` implements:

1. **`FakeAsyncDriverDialect`** — concrete subclass of `AsyncDriverDialect` with canned return values and a call-log dict.
2. **`_FakeAsyncConnection`** — a stand-in object returned by `FakeAsyncDriverDialect.connect`.
3. **`FakeAsyncPlugin`** — concrete subclass of `AsyncPlugin` that, in its `connect` override, drives every operation on its injected `AsyncDriverDialect` — proving plugins can be written without driver-specific imports.
4. A test that exercises the fake plugin end-to-end against the fake dialect and asserts the call log matches expectations.

**Acid-test acceptance criterion:** if the ABC is missing an operation a future plugin would need, the fake plugin CAN'T be written without casting / direct driver access. At that point the ABC must grow — cheap to do now (zero downstream code), expensive to do after real plugins exist.

## Components (what files get created)

- `aws_advanced_python_wrapper/aio/__init__.py` — package marker, short docstring, public re-exports.
- `aws_advanced_python_wrapper/aio/driver_dialect/__init__.py` — re-exports `AsyncDriverDialect`.
- `aws_advanced_python_wrapper/aio/driver_dialect/base.py` — the ABC itself. All methods raise `NotImplementedError` (or use `@abstractmethod`) for abstract ones; property-like static ones have defaults.
- `aws_advanced_python_wrapper/aio/plugin.py` — `AsyncPlugin` ABC and `AsyncConnectionProvider` ABC (signatures only).
- `aws_advanced_python_wrapper/aio/wrapper.py` — `AsyncAwsWrapperConnection` class stub (constructor + method signatures raising `NotImplementedError`, populated in SP-2).
- `tests/unit/test_aio_contracts.py` — the acid test.

## Out of scope for SP-0

- Any real implementation of any method (SP-1, SP-2, and later fill these in).
- `AsyncPluginService` / `AsyncPluginManager` (SP-1).
- Async submodule (`aio/psycopg.py`) that applications can import (SP-2).
- SA async dialect registration (SP-9).

## Success criteria

- New files compile, no import errors on both Python 3.13 and 3.14.
- `poetry run python -m pytest tests/unit/test_aio_contracts.py` passes.
- Full unit suite still passes (`1167 + X` where X is the new count).
- flake8, mypy, isort all clean on the new files.
- Every method on `AsyncDriverDialect` is exercised at least once by the fake-plugin acid test.

## Sequencing notes for implementation

1. Create the `aio/` package skeleton first so imports of the ABC resolve.
2. Write `AsyncDriverDialect` ABC next with full method list — this is the load-bearing interface.
3. Write `AsyncPlugin` and `AsyncConnectionProvider` ABC stubs.
4. Write `AsyncAwsWrapperConnection` class stub.
5. Write the acid test using fakes — test-drives the ABC to prove it's complete.
6. Run full suite + lint + mypy + isort.
7. Commit as a single focused commit: `feat: add aio package skeleton and AsyncDriverDialect ABC (F3-B SP-0)`.
