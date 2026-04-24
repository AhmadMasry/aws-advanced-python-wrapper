# Task 2 — Async MySQL driver (aiomysql)

**Date:** 2026-04-20
**Status:** Draft — post-3.0.0 follow-up.
**Target release:** 3.2.0 (additive minor; no breaking changes).
**Branch:** `feat/post-3.0-followups` (off F3-B final).
**Depends on:** F3-B SP-0 through SP-10 (master-spec invariant 8a is what makes this a 5-file additive change).

## Goal

Add native async MySQL support via **aiomysql** only.

## Driver choice rationale (why aiomysql, not asyncmy)

Evaluated both; aiomysql wins on five axes:

1. **Maturity** — aiomysql is aio-libs org, in production since 2015. Asyncmy is newer (2020+), smaller contributor base.
2. **SA native support** — `sqlalchemy.dialects.mysql.aiomysql.MySQLDialect_aiomysql` ships in SA 2.0, making the Task 2-C subclass a one-line override. Asyncmy support in SA is less mature.
3. **Pure Python, no build issues** — aiomysql is PyMySQL-derived; wheels install on every platform + Python version where Python itself works (including day-one of each Python release). Asyncmy is Cython-compiled; wheels lag new Python releases.
4. **Container ergonomics** — aiomysql has no `libmysqlclient` dependency; works in Lambda / App Runner / minimal ECR images without system-lib fiddling.
5. **Realistic perf is a wash** — asyncmy's 2–3× driver-level speedup vanishes once Aurora network latency + wrapper plugin pipeline overhead dominates. The perf win is real only for microbenchmarks, not Aurora workloads.

**Asyncmy** support deferred to a community-driven request backed by a concrete workload showing the perf matters. When that happens, it's a second-file addition under `aio/driver_dialect/asyncmy.py` — not a refactor.

## Non-goals

- asyncmy driver support (deferred — second file added on-demand)
- asyncpg driver support (dropped — not PEP 249-compliant, would need a SA-adapter layer)
- Replacing sync MySQL support — sync path continues to use mysql-connector-python

## Architecture

Follows F3-B SP-2's psycopg pattern directly:

```
aws_advanced_python_wrapper/
├── aio/
│   ├── driver_dialect/
│   │   └── aiomysql.py                   # NEW: AsyncAiomysqlDriverDialect
│   └── aiomysql.py                       # NEW: aio DBAPI submodule
└── sqlalchemy_dialects/
    └── mysql_async.py                    # NEW: SA async MySQL dialect subclass
```

Entry-points registered in `pyproject.toml`:
- `aws_wrapper_mysql_async`
- `aws_wrapper_mysql.aiomysql_async`

## Sub-project decomposition

### T2-A — `AsyncAiomysqlDriverDialect`

**What gets built:**
- `aws_advanced_python_wrapper/aio/driver_dialect/aiomysql.py`:
  - `AsyncAiomysqlDriverDialect` subclasses `AsyncDriverDialect` (F3-B SP-0).
  - Implements all 11 abstract methods against aiomysql's `Connection` API.
  - Notable mappings:
    - `is_closed(conn)` → `not conn.open`
    - `is_in_transaction(conn)` → check `conn.get_autocommit()` + track state manually (aiomysql doesn't expose a transaction-status flag directly; mirror sync MySQL dialect's heuristic).
    - `set_autocommit(conn, val)` → `await conn.autocommit(val)` (aiomysql uses a method, not a setter).
    - `ping(conn)` → `await conn.ping(reconnect=False)`; return True on no exception, False otherwise.
    - `abort_connection(conn)` → `conn.close()` (aiomysql has no abort primitive; closing is the closest analog and matches sync MySQL driver dialect).
    - `transfer_session_state` — copy autocommit and read-only flags where applicable; MySQL session state transfer is simpler than PG's.
  - `is_dialect(connect_func)` identifies aiomysql's connect by `__func__` identity.

**Test coverage:** ABC parallel to `test_aio_contracts.py`'s fake-dialect acid test — `FakeAsyncConnection` replaced with a mock aiomysql-shaped object; every abstract method exercised.

**Estimated effort:** ~1 commit (~80 LOC dialect + ~150 LOC test).

### T2-B — `aio.aiomysql` submodule

- Mirror of `aio.psycopg` layout.
- `async def connect(conninfo, **kwargs) -> AsyncAwsWrapperConnection` — calls `aiomysql.connect(...)`, wraps result via `AsyncAwsWrapperConnection.connect`.
- `_dbapi.install(sys.modules[__name__].__dict__, connect=connect)` for PEP 249 surface.
- PEP 562 `__getattr__` forwards to real `aiomysql` module so SA's introspection works (matches SP-2's pattern for psycopg).

**Estimated effort:** ~1 commit bundled with T2-A. ~25 LOC.

### T2-C — SA async MySQL dialect

- `aws_advanced_python_wrapper/sqlalchemy_dialects/mysql_async.py`:
  - `AwsWrapperMySQLAiomysqlAsyncDialect` subclasses SA's `MySQLDialect_aiomysql`.
  - Overrides `import_dbapi` to return `aws_advanced_python_wrapper.aio.aiomysql`.
  - Overrides `get_async_dialect_cls` / `get_dialect_cls` to return itself (same trick as F3-B SP-9's PG async dialect).
  - `create_connect_args` override translates `wrapper_plugins` → `plugins` URL alias.
- `pyproject.toml` entry-points:
  - `aws_wrapper_mysql_async = ...` (bare URL form)
  - `aws_wrapper_mysql.aiomysql_async = ...` (driver-slot form)
- `tests/unit/test_aio_sqlalchemy_mysql_dialect.py`: 10 tests mirroring SP-9's PG coverage.

**Estimated effort:** ~1 commit (~80 LOC source + ~200 LOC test).

### T2-D — Docs + CHANGELOG

- Update `docs/using-the-python-wrapper/SqlAlchemySupport.md`:
  - Remove the "Async MySQL is not supported in 3.0.0" bullet from limitations.
  - Add MySQL async section showing `aws_wrapper_mysql+aiomysql_async://` usage.
  - Note asyncmy / asyncpg deferrals (explicitly list them in a "Not supported" sub-bullet with rationale).
- Update `README.md` async-support bullet.
- CHANGELOG entries.
- One example script: `docs/examples/MySQLSQLAlchemyAsyncFailover.py` (mirror of `PGSQLAlchemyAsyncFailover.py`). Add to `mypy.ini` exclude list.

**Estimated effort:** ~1 commit.

## Sequencing

Serial: **T2-A + T2-B (one commit)** → **T2-C** → **T2-D**.

T2-A and T2-B are tightly coupled — the submodule uses the driver dialect. Bundling them in one commit matches how SP-2 shipped psycopg.

## Cross-cutting decisions

1. **Aurora MySQL topology query**: same SQL as sync Aurora MySQL dialect (`SELECT server_id, session_id FROM mysql.ro_replica_status WHERE replica_lag_in_milliseconds <= 300000 OR session_id = ...`). Shared `TopologyAwareDatabaseDialect` subclass from F3-B's design continues to work.
2. **Connection URI format**: aiomysql uses kwargs-style connection (`connect(host=..., port=..., user=...)`). Our submodule handles the kwargs-from-DSN path identically to `aio.psycopg`.
3. **SSL**: aiomysql takes `ssl=...` kwargs; pass through unchanged.
4. **use_pure caveat**: doesn't apply — aiomysql has no C extension switch. The sync mysql-connector `use_pure` caveat from README's Known Limitations goes away for the async path.

## Success criteria

- `create_async_engine("aws_wrapper_mysql+aiomysql_async://...")` produces a working engine end-to-end against Aurora MySQL.
- All 11 `AsyncDriverDialect` abstract methods implemented and unit-tested.
- Failover, EFM, RWS, auth plugins from F3-B work on aiomysql **unchanged** (invariant 8a proof — the point of this whole task).
- No driver-specific imports leaked into plugin code.
- SP-9's `wrapper_plugins` URL alias works identically on the MySQL async dialect.

## Open questions

None after the aiomysql-only scope decision. The SA `MySQLDialect_aiomysql` parent is stable in SA 2.0.49 per current lock file.
