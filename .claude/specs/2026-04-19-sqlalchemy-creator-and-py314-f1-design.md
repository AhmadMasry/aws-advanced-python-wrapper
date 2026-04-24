# F1 — SQLAlchemy creator-pattern support + Python 3.14

**Date:** 2026-04-19
**Status:** Draft — pending user approval, then writing-plans.
**Target release:** 2.2.0 (additive, minor bump).

## Scope decomposition

This spec covers **F1 only**. Two follow-on features are explicitly deferred to their own specs:

- **F2** — Custom SQLAlchemy URL-scheme dialects: `aws-wrapper-postgresql+psycopg`, `aws-wrapper-mysql+mysqlconnector`. Sits on top of F1 and reuses F1's per-driver submodules as its DBAPI targets.
- **F3** — Async SQLAlchemy support via `psycopg.AsyncConnection` (PG only; mysql-connector-python has no async API, and the `aiomysql` / `asyncmy` question is deferred to F3's own brainstorm).

## Goal

Make `aws_advanced_python_wrapper` usable as a PEP 249 DBAPI module so SQLAlchemy's `create_engine(url, creator=...)` pattern works end-to-end for both Postgres (via psycopg v3) and MySQL (via mysql-connector-python). Add Python 3.14 to the supported matrix.

## Non-goals

- Custom SA URL-scheme dialects (F2).
- Async engine support (F3).
- Free-threaded (no-GIL, `python3.14t`) build support.
- Changes to the plugin pipeline, driver dialects, or host monitoring.
- Remapping target-driver exceptions (psycopg / mysql-connector) into the wrapper's own hierarchy.

## Background — the current bug

`aws_advanced_python_wrapper.__init__` declares `apilevel = "2.0"`, `threadsafety`, `paramstyle`, and exposes `connect = AwsWrapperConnection.connect`. This is enough to *look like* a PEP 249 module, but two concrete gaps break SA integration:

1. **`module.connect(dsn)` fails.** `AwsWrapperConnection.connect(target, conninfo, *args, **kwargs)` requires the underlying driver's connect callable as its first positional argument. A SA user who writes `create_engine(url, module=aws_advanced_python_wrapper)` triggers `aws_advanced_python_wrapper.connect(dsn)` — no target callable — raising `[Wrapper] Target driver should be a target driver's connect() method/function.` (Verified empirically: see "Confirmation" below.)

2. **DBAPI exception and type surface is not re-exported.** `aws_advanced_python_wrapper.Error`, `.OperationalError`, `.DatabaseError`, etc. resolve to `None` / `AttributeError`. Type constructors (`Date`, `Time`, `Timestamp`, `Binary`, `DateFromTicks`, `TimeFromTicks`, `TimestampFromTicks`) and type singletons (`STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`) are absent. SA's `_handle_dbapi_exception` walks `module.Error.__mro__` to classify errors — when `module.Error` is `None`, the error path itself raises `AttributeError`, masking the real exception.

Confirmation (run during brainstorm):

```
DBAPI module-level attributes on aws_advanced_python_wrapper today:
  connect, apilevel, threadsafety, paramstyle      → present
  Warning, Error, InterfaceError, DatabaseError,
  DataError, OperationalError, IntegrityError,
  InternalError, ProgrammingError, NotSupportedError → MISSING
  Date, Time, Timestamp, Binary,
  STRING, BINARY, NUMBER, DATETIME, ROWID           → MISSING

Signature: AwsWrapperConnection.connect(target: Union[None, str, Callable] = None,
                                         conninfo: str = '', *args, **kwargs)
Calling module.connect(dsn) today: raises AwsWrapperError
  "[Wrapper] Target driver should be a target driver's connect() method/function."
```

## Architecture

New files (all under `aws_advanced_python_wrapper/`):

```
aws_advanced_python_wrapper/
├── _dbapi.py                 # NEW: canonical PEP 249 surface + install() helper
├── psycopg.py                # NEW: DBAPI submodule bound to psycopg v3
├── mysql_connector.py        # NEW: DBAPI submodule bound to mysql.connector
├── __init__.py               # MODIFIED: full PEP 249 surface via _dbapi.install()
├── errors.py                 # MODIFIED: reparent wrapper errors to specific PEP 249 classes
├── pep249.py                 # UNCHANGED
├── wrapper.py                # UNCHANGED (AwsWrapperConnection.connect signature preserved)
└── …                         # everything else unchanged
```

- `wrapper.py::AwsWrapperConnection.connect(target_callable, dsn, …)` keeps its current signature. Existing user code keeps working.
- New per-driver submodules wrap that call to produce a no-target-arg `connect(dsn, **kwargs)` that SA's `creator=` can call directly.
- `__init__.py` calls `_dbapi.install(...)` at import time so the top-level module also satisfies `module.Error`, `module.Date`, etc. — fixes the current `module.Error == None` gap.
- Plugin pipeline, driver dialects, host monitoring: untouched. F1 is a pure additive layer at the module surface.

## Components

### `_dbapi.py` — canonical PEP 249 surface

Single source of truth for the DBAPI module contract. Exports:

- Exception classes (re-exported from `pep249.py`): `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`.
- Type constructors: `Date`, `Time`, `Timestamp` (aliased from `datetime`), `DateFromTicks`, `TimeFromTicks`, `TimestampFromTicks`, `Binary`.
- Type singletons: `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID` — implemented as `_DBAPISet(frozenset)` with overridden `__eq__` so comparisons against driver-specific type codes (psycopg OIDs, mysql-connector `FieldType` ints) work. Source tables: `psycopg.postgres.types` (OIDs per type class) and `mysql.connector.FieldType` (int constants). Union both into each singleton at implementation time.
- Module-level attributes: `apilevel = "2.0"`, `threadsafety = 2`, `paramstyle = "pyformat"`.
- `install(module_ns: dict, connect: Callable | None = None) -> None` — populates the caller's namespace with all of the above, optionally setting `module.connect` to the provided callable. Uses the same tuple `_PEP249_NAMES` for both populating and building `__all__`.

### `psycopg.py` — psycopg-v3-bound submodule (~15 LOC)

```python
import sys
from psycopg import Connection as _PGConnection
from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection

def connect(conninfo: str = "", **kwargs) -> AwsWrapperConnection:
    return AwsWrapperConnection.connect(_PGConnection.connect, conninfo, **kwargs)

_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
```

### `mysql_connector.py` — mysql-connector-python-bound submodule (~15 LOC)

```python
import sys
from mysql.connector import connect as _mysql_connect
from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection

def connect(conninfo: str = "", **kwargs) -> AwsWrapperConnection:
    return AwsWrapperConnection.connect(_mysql_connect, conninfo, **kwargs)

_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
```

### `__init__.py` — gains full PEP 249 surface

Replaces the hand-rolled three-attribute declaration with:

```python
from . import _dbapi
_dbapi.install(sys.modules[__name__].__dict__, connect=AwsWrapperConnection.connect)
```

Top-level `connect` still references `AwsWrapperConnection.connect` (requires target callable first arg) — unchanged behavior for existing callers.

## Data flow (SA creator path, PG example; MySQL identical with the other submodule)

1. **Import time.** `from aws_advanced_python_wrapper.psycopg import connect` triggers the submodule's `_dbapi.install(...)` — submodule is now a valid PEP 249 DBAPI module.

2. **`create_engine` with `creator=`.**
   ```python
   engine = create_engine(
       "postgresql+psycopg://",
       creator=lambda: aws_advanced_python_wrapper.psycopg.connect(
           "host=… user=… dbname=…",
           wrapper_dialect="aurora-pg",
           plugins="failover,efm",
       ),
   )
   ```
   SA builds a `QueuePool` with the lambda as its factory; defers actual connect.

3. **First `engine.connect()`.** SA calls the lambda → `aws_advanced_python_wrapper.psycopg.connect(...)` → `AwsWrapperConnection.connect(psycopg.Connection.connect, dsn, **kwargs)`. Inside: properties parsed, driver dialect selected, database dialect picked (`aurora-pg` initially, upgraded after first connection based on server metadata), plugin chain built (`failover → efm → default`), `psycopg.Connection.connect(dsn)` runs at the bottom of the chain. `AwsWrapperConnection` returned and stashed as a `PoolProxiedConnection`.

4. **Query execution.** `conn.execute(text("SELECT 1"))` → `AwsWrapperCursor.execute` → plugin chain intercepts (execute-time timing, failover watchdog, EFM host monitor) → target call lands on `psycopg.Cursor.execute`. Results flow back through the cursor. SA's `postgresql+psycopg` dialect handles result-typing — independent of the wrapper.

5. **Exception path.** A psycopg `OperationalError` raised during `execute` propagates up through the plugin chain. SA's `_handle_dbapi_exception` looks up `module.OperationalError` on the connection's DBAPI module (`aws_advanced_python_wrapper.psycopg`). **Before F1:** `AttributeError` masks the original. **After F1:** `module.OperationalError` is present, SA wraps into `sqlalchemy.exc.OperationalError`, consumer sees the normal SA error chain.

6. **Release and cleanup.** `PoolProxiedConnection` returns to SA's pool on `conn.close()`. `SqlAlchemyDriverDialect` (already in the codebase) handles unwrapping when the wrapper's plugin pipeline needs the raw conn. At process exit, consumer calls `engine.dispose()` (SA pool teardown) then `aws_advanced_python_wrapper.release_resources()` (wrapper background-thread teardown) — both needed, order matters.

## Error handling

`AwsWrapperError` already inherits from `pep249.Error` (`errors.py:20`). `FailoverError` too (`errors.py:41`). SA's PEP 249 error walk currently stops at the `Error` level, so every wrapper-originated error classifies as the generic `sqlalchemy.exc.DBAPIError` — consumers can't `except sqlalchemy.exc.OperationalError` and catch failover events.

F1 adds a second parent to each wrapper error so SA picks the most specific SA wrapper class:

| Wrapper error | New parent | SA mapping after F1 |
|---|---|---|
| `AwsWrapperError` | `Error` (unchanged — generic catch-all) | `sqlalchemy.exc.DBAPIError` |
| `AwsConnectError` | `OperationalError` | `sqlalchemy.exc.OperationalError` |
| `FailoverError` | `OperationalError` | `sqlalchemy.exc.OperationalError` |
| `FailoverSuccessError` | `OperationalError` | `sqlalchemy.exc.OperationalError` |
| `FailoverFailedError` | `OperationalError` | `sqlalchemy.exc.OperationalError` |
| `TransactionResolutionUnknownError` | `OperationalError` | `sqlalchemy.exc.OperationalError` |
| `QueryTimeoutError` | `OperationalError` | `sqlalchemy.exc.OperationalError` |
| `ReadWriteSplittingError` | `InterfaceError` | `sqlalchemy.exc.InterfaceError` |
| `UnsupportedOperationError` | `NotSupportedError` | `sqlalchemy.exc.NotSupportedError` |

MRO: multi-parent (e.g., `class AwsConnectError(AwsWrapperError, OperationalError)`). Both parents ultimately derive from `pep249.Error`; Python's C3 linearization handles this as long as `Error` is a single ancestor — verified on paper, asserted in tests.

Exception chaining: keep the existing `original_error → .driver_error` attribute. Additionally, use `raise AwsWrapperError(...) from original` at call sites that catch target-driver exceptions so `__cause__` chains through — SA walks `__cause__` already.

What F1 does NOT change:

- **Target-driver exception classes are not remapped.** psycopg and mysql-connector exceptions propagate as-is; SA's PG/MySQL dialects handle them via their own imports.
- **No new wrapper error classes.** Only reparenting.

## Testing

### Unit tests (new files under `tests/unit/`)

- `test_error_hierarchy.py` — `isinstance` checks for each reclassified error against both old and new parents; MRO-shape assertions; confirmation that pre-existing `except AwsWrapperError` callers still catch everything they used to.
- `test_dbapi_module_contract.py` — parametrized over `aws_advanced_python_wrapper`, `aws_advanced_python_wrapper.psycopg`, `aws_advanced_python_wrapper.mysql_connector`: every PEP 249 name from `_PEP249_NAMES` is present; `apilevel == "2.0"`; `threadsafety == 2`; `paramstyle == "pyformat"`; exception classes satisfy the required parent relationships; `Binary(b"x") == b"x"`; type singletons support `==` against integer type codes.
- `test_psycopg_submodule.py` — patches `psycopg.Connection.connect` with a mock; calls `aws_advanced_python_wrapper.psycopg.connect(dsn, wrapper_dialect="aurora-pg")`; asserts the mock received the DSN and kwargs and an `AwsWrapperConnection` is returned.
- `test_mysql_connector_submodule.py` — symmetric, patches `mysql.connector.connect`.

No fake-DBAPI SA unit test. Rationale: the existing `tests/unit/` do not mock SA internals, and the SA integration surface is meaningful to prove only against real Aurora. Follows the existing split where integration tests use no mocks.

### Integration tests (one new file: `tests/integration/container/test_sqlalchemy.py`)

Single file with four cases, parametrized over `(driver, plugin)`:

| Case | Proves |
|---|---|
| PG + Failover via creator-pattern | SA retries on OperationalError-mapped `FailoverSuccessError` after Aurora failover; execution resumes on new writer. |
| MySQL + Failover via creator-pattern | Same on Aurora MySQL. |
| PG + RWS via creator-pattern | `execution_options(postgresql_readonly=True)` routes to a reader; next txn goes back to writer. |
| MySQL + RWS via creator-pattern | Same on MySQL. |

Each test builds an SA `Engine` with `creator=`, runs a workload, triggers the cluster event (via existing conftest fixtures + RDS SDK or Toxiproxy), verifies the outcome. Uses the cluster fixtures already in `tests/integration/container/`. CI-gating: unit tests on every PR via `main.yml`; integration tests via `integration_tests.yml` (maintainer-triggered, requires AWS credentials).

### What is NOT tested in F1

- F2's URL-scheme dialect.
- F3's async SA engine.
- Django (already has its own tests; orthogonal).
- Free-threaded 3.14 build.

## Python 3.14 support

### Changes

- `pyproject.toml` — add `"Programming Language :: Python :: 3.14"` classifier. `python = "^3.10.0"` constraint already permits 3.14 (caret).
- `.github/workflows/main.yml` — add `"3.14"` to the build matrix.
- `.github/workflows/integration_tests.yml` — add if it has a matrix; else leave.
- `docs/development-guide/DevelopmentGuide.md` — update "Python 3.10 – 3.13 (inclusive)" phrasing to 3.14.
- `README.md` — same phrasing update in the "How to Contribute" section.

### Preemptive deprecation sweep — executed during brainstorming, zero hits

Scans run across `aws_advanced_python_wrapper/**` and `tests/**`:

| Pattern | Status in 3.14 | Hits |
|---|---|---|
| `typing.ByteString` / `collections.abc.ByteString` | removed | 0 |
| `ast.Str` / `ast.Num` / `ast.Bytes` / `ast.NameConstant` / `ast.Ellipsis` | removed | 0 |
| `datetime.utcnow()` / `datetime.utcfromtimestamp()` | deprecated | 0 |
| `asyncio.{get,set}_child_watcher`, `asyncio.{get,set}_event_loop_policy`, `asyncio.iscoroutinefunction` | deprecated/removed | 0 |
| PEP 594 module imports (`xdrlib`, `audioop`, `chunk`, `aifc`, `sunau`, `telnetlib`, `nis`, `ossaudiodev`, `spwd`, `crypt`, `cgi`, `cgitb`, `mailcap`, `pipes`, `msilib`, `imghdr`) | removed in 3.13 | 0 |

No call sites to fix in F1. `pytest -Werror` on the 3.14 matrix is the ongoing guard — a newly-surfaced deprecation warning under 3.14 will fail CI without needing a standing test.

### Runtime smoke

`poetry env use 3.14 && poetry install && poetry run python -m pytest ./tests/unit -Werror`. Fix anything that breaks during F1 implementation.

### Out of F1 3.14 scope

- Free-threaded (`python3.14t`) build validation.
- PEP 649 deferred annotation semantics (the codebase uses `from __future__ import annotations` which continues to work).
- Sub-interpreters, per-interpreter GIL.

## Documentation and examples

### New: `docs/using-the-python-wrapper/SqlAlchemySupport.md`

Mirrors `DjangoSupport.md`. Sections: Prerequisites; Using with SQLAlchemy (PostgreSQL); Using with SQLAlchemy (MySQL); Error handling (incl. mapping table from the "Error handling" section); Resource cleanup (`engine.dispose()` then `release_resources()`, order and why); Combining with plugins (links to `UsingTheFailoverPlugin.md`, `UsingTheReadWriteSplittingPlugin.md`); Limitations (sync only — F3 pointer; no URL-scheme dialect yet — F2 pointer; MySQL `use_pure=True` + IAM incompatibility from README); See also (DjangoSupport, UsingThePythonWrapper, the four new examples).

### Updated: `docs/README.md` — ToC

One new entry under "Using the AWS Python Wrapper":

```markdown
  - [SQLAlchemy Support](using-the-python-wrapper/SqlAlchemySupport.md)
```

### Updated: `README.md` — brief mention

One-liner pointing at `SqlAlchemySupport.md` under "Using the AWS Advanced Python Wrapper". Deep content stays in the docs page.

### New: four example scripts under `docs/examples/`

- `PGSQLAlchemyFailover.py` — models `PGFailover.py`: engine with failover plugin, workload loop, `except sqlalchemy.exc.OperationalError` → retry, print instance id. Imports `aws_advanced_python_wrapper.psycopg`.
- `MySQLSQLAlchemyFailover.py` — MySQL variant, imports `aws_advanced_python_wrapper.mysql_connector`.
- `PGSQLAlchemyReadWriteSplitting.py` — models `PGReadWriteSplitting.py`: engine with RWS plugin, `execution_options(postgresql_readonly=True)` to bounce to reader, back to writer, verify instance id.
- `MySQLSQLAlchemyReadWriteSplitting.py` — MySQL variant.

Each ~60-80 LOC, `if __name__ == "__main__"`, `try/finally` with `release_resources()`.

### Updated: `docs/using-the-python-wrapper/using-plugins/UsingTheReadWriteSplittingPlugin.md`

Add cross-links to `PGSQLAlchemyReadWriteSplitting.py` / `MySQLSQLAlchemyReadWriteSplitting.py` alongside the existing PG/MySQL non-SA example references.

### Updated: `mypy.ini`

Add the four new example filenames to the existing exclude regex, following the precedent set by the other `docs/examples/` files.

## Back-compatibility

- **Additive only.** No existing public API changes shape or behavior.
- **Top-level `aws_advanced_python_wrapper.connect`** still binds to `AwsWrapperConnection.connect` — same required target-callable first argument. Existing calls unchanged.
- **`AwsWrapperConnection.connect(target, dsn, ...)`** — signature preserved byte-for-byte.
- **Multi-parent exception classes** — `isinstance(err, AwsWrapperError)` still true; only new is that `isinstance(err, OperationalError)` becomes true for the reclassified ones. No existing exception handler is broken (you can only *widen* what matches, not narrow it).
- **New module-level names** (`Warning`, `Error`, `DataError`, …, `Date`, `STRING`, …) — none collide with anything currently exported. `__all__` gains them; pickling is unaffected.

## Success criteria

- Unit tests pass on 3.10, 3.11, 3.12, 3.13, 3.14 matrix.
- Four integration tests pass against Aurora PG + Aurora MySQL.
- SA `creator=` pattern works end-to-end for both drivers, proven by the integration tests.
- `module.Error` and all PEP 249 classes resolve on `aws_advanced_python_wrapper`, `aws_advanced_python_wrapper.psycopg`, and `aws_advanced_python_wrapper.mysql_connector`.
- `docs/using-the-python-wrapper/SqlAlchemySupport.md` published; four example scripts runnable.
- Proposed CHANGELOG lines included in the PR body (maintainers place them in the appropriate version section at release time — the repo has no `[Unreleased]` section; CHANGELOG is batch-updated per release).

## CHANGELOG entries (proposed lines)

```markdown
### :magic_wand: Added
* SQLAlchemy creator-pattern support with dedicated `aws_advanced_python_wrapper.psycopg` and `aws_advanced_python_wrapper.mysql_connector` DBAPI submodules ([PR #XXXX]).
* Python 3.14 support ([PR #XXXX]).
* Examples: `PGSQLAlchemyFailover.py`, `MySQLSQLAlchemyFailover.py`, `PGSQLAlchemyReadWriteSplitting.py`, `MySQLSQLAlchemyReadWriteSplitting.py` ([PR #XXXX]).
* `docs/using-the-python-wrapper/SqlAlchemySupport.md` ([PR #XXXX]).

### :crab: Changed
* Wrapper errors (`AwsConnectError`, `FailoverError` and subclasses, `QueryTimeoutError`, `ReadWriteSplittingError`, `UnsupportedOperationError`) now inherit the appropriate PEP 249 subclass so SQLAlchemy maps them to the correct `sqlalchemy.exc.*` type ([PR #XXXX]).
```

## Open questions

None at spec time. All design decisions are closed:

1. F1 = Option A (per-driver submodules) — locked.
2. F2 naming = `aws-wrapper-postgresql` / `aws-wrapper-mysql` — deferred to F2 spec, but compatible with F1's submodule structure.
3. F3 async scope = psycopg only (MySQL async revisited in F3 spec).
4. 3.14 = classifier + CI matrix + sweep (ran clean, no code changes).
5. Exception reclassification — approved per Section 4 table.
6. Testing = 4 unit suites + 4 real-Aurora integration tests, no fake-DBAPI unit test.

## Sequencing notes for implementation

Roughly the order the writing-plans skill will produce, but for reviewer orientation:

1. Add `_dbapi.py` with the install helper + full surface.
2. Rewire `__init__.py` to use `_dbapi.install(...)`.
3. Reparent exception classes in `errors.py`.
4. Unit tests for the above (error hierarchy + module contract).
5. Add `psycopg.py` and `mysql_connector.py` submodules + their unit tests.
6. Add Python 3.14 to classifier, CI matrix, docs phrasing.
7. Run `pytest -Werror` on 3.14 locally; fix if anything surfaces.
8. Write the four example scripts + `SqlAlchemySupport.md` + `docs/README.md` ToC entry + `README.md` one-liner + `UsingTheReadWriteSplittingPlugin.md` cross-links.
9. Add the four example filenames to `mypy.ini` exclude.
10. Add the four integration tests.
11. Include the proposed CHANGELOG lines in the PR description under the right category so a maintainer can copy them into the CHANGELOG at release.
