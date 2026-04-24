# F2 — Custom SQLAlchemy URL-scheme dialects

**Date:** 2026-04-19
**Status:** Draft — pending approval, then writing-plans.
**Target release:** 2.3.0 (additive, minor bump; lands after F1 ships in 2.2.0).
**Depends on:** F1 (per-driver submodules `aws_advanced_python_wrapper.psycopg` / `.mysql_connector`).

## Goal

Register two SQLAlchemy dialects via entry-points so `create_engine` can be driven by URL alone:

```
aws-wrapper-postgresql://user:pwd@host/db?wrapper_dialect=aurora-pg&plugins=failover,efm
aws-wrapper-postgresql+psycopg://user:pwd@host/db?...
aws-wrapper-mysql://user:pwd@host/db?...
aws-wrapper-mysql+mysqlconnector://user:pwd@host/db?...
```

Unblocks URL-driven configs (Alembic `alembic.ini`, 12-factor `DATABASE_URL`, framework starters) that don't want to embed a `creator=` Python callable.

## Non-goals

- New wrapper features beyond dialect registration.
- Async support (F3).
- New drivers beyond psycopg v3 and mysql-connector-python (both remain defaults).
- SA 1.x support.

## Naming (locked from F1 brainstorm)

- Dialect family names: `aws-wrapper-postgresql`, `aws-wrapper-mysql`. Echoes JDBC's `jdbc:aws-wrapper:postgresql://` / `jdbc:aws-wrapper:mysql://`.
- Driver slot: preserved for SA's DBAPI-driver disambiguation. `+psycopg` is the sole PG driver in F2; `+mysqlconnector` is the sole MySQL driver. Both are defaults (bare `aws-wrapper-postgresql://` resolves to psycopg).

## Architecture

Two thin SA dialect subclasses that inherit SQL compilation / type system from SA's standard `postgresql+psycopg` / `mysql+mysqlconnector` dialects, and swap the DBAPI module to F1's per-driver wrapper submodules:

```
aws_advanced_python_wrapper/
├── sqlalchemy_dialects/
│   ├── __init__.py          # NEW: package marker (empty)
│   ├── pg.py                # NEW: AwsWrapperPGPsycopgDialect
│   └── mysql.py             # NEW: AwsWrapperMySQLConnectorDialect
└── …
tests/
└── unit/
    └── test_sqlalchemy_dialects.py   # NEW
```

No changes to core wrapper code. No changes to F1's `_dbapi.py`, `psycopg.py`, `mysql_connector.py` submodules — they become the DBAPI targets these dialects reference.

## Components

### `sqlalchemy_dialects/pg.py`

```python
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

class AwsWrapperPGPsycopgDialect(PGDialect_psycopg):
    driver = "psycopg"  # surfaces as dialect.driver for SA diagnostics
    supports_statement_cache = True  # inherited is fine; restate to be explicit

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.psycopg as dbapi
        return dbapi
```

### `sqlalchemy_dialects/mysql.py`

```python
from sqlalchemy.dialects.mysql.mysqlconnector import MySQLDialect_mysqlconnector

class AwsWrapperMySQLConnectorDialect(MySQLDialect_mysqlconnector):
    driver = "mysqlconnector"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.mysql_connector as dbapi
        return dbapi
```

### Entry-point registration (`pyproject.toml`)

```toml
[tool.poetry.plugins."sqlalchemy.dialects"]
"aws-wrapper-postgresql" = "aws_advanced_python_wrapper.sqlalchemy_dialects.pg:AwsWrapperPGPsycopgDialect"
"aws-wrapper-postgresql.psycopg" = "aws_advanced_python_wrapper.sqlalchemy_dialects.pg:AwsWrapperPGPsycopgDialect"
"aws-wrapper-mysql" = "aws_advanced_python_wrapper.sqlalchemy_dialects.mysql:AwsWrapperMySQLConnectorDialect"
"aws-wrapper-mysql.mysqlconnector" = "aws_advanced_python_wrapper.sqlalchemy_dialects.mysql:AwsWrapperMySQLConnectorDialect"
```

The `{dialect}` entry makes bare `aws-wrapper-postgresql://` resolve to psycopg (default). The `{dialect}.{driver}` entry handles `aws-wrapper-postgresql+psycopg://` explicitly.

## Data flow

`create_engine("aws-wrapper-postgresql+psycopg://user:pw@host:5432/db?wrapper_dialect=aurora-pg&plugins=failover")`:

1. SA parses URL → dialect name `aws-wrapper-postgresql`, driver `psycopg`.
2. SA's dialect plugin loader resolves the entry-point → `AwsWrapperPGPsycopgDialect`.
3. `AwsWrapperPGPsycopgDialect.import_dbapi()` returns `aws_advanced_python_wrapper.psycopg` module — our F1 submodule that has `connect(conninfo, **kwargs)` bound to psycopg's target driver.
4. SA builds connect args via its standard `PGDialect_psycopg.create_connect_args`:
   - Positional: the conninfo string
   - Keyword: user/password/host/port/dbname extracted from URL + every query-string arg passed through as kwargs
5. First `engine.connect()` calls `aws_advanced_python_wrapper.psycopg.connect(conninfo, user=..., password=..., wrapper_dialect="aurora-pg", plugins="failover")` → `AwsWrapperConnection.connect(psycopg.Connection.connect, conninfo, **kwargs)`.
6. Plugin chain builds (`failover` in this example), Aurora dialect auto-detected, query runs.

SA's standard query-string passthrough is load-bearing here — no custom `create_connect_args` override is needed as long as psycopg/mysql-connector accept the wrapper-specific kwargs (they do, because the wrapper just passes its own kwargs through to `AwsWrapperConnection.connect` after the driver args).

## Error handling

No new error surface. The F1 wrapper exception classification already maps to `sqlalchemy.exc.*` correctly; F2 doesn't change that path.

## Testing

### Unit tests (new `tests/unit/test_sqlalchemy_dialects.py`)

No mocking of SA internals. Tests verify:

1. `AwsWrapperPGPsycopgDialect` subclasses `PGDialect_psycopg`.
2. `AwsWrapperMySQLConnectorDialect` subclasses `MySQLDialect_mysqlconnector`.
3. `import_dbapi()` on each returns the correct F1 submodule (identity check against `aws_advanced_python_wrapper.psycopg` / `aws_advanced_python_wrapper.mysql_connector`).
4. `sqlalchemy.dialects.registry.load("aws-wrapper-postgresql")` returns `AwsWrapperPGPsycopgDialect`.
5. `sqlalchemy.dialects.registry.load("aws-wrapper-postgresql.psycopg")` same.
6. `sqlalchemy.dialects.registry.load("aws-wrapper-mysql")` returns `AwsWrapperMySQLConnectorDialect`.
7. `sqlalchemy.dialects.registry.load("aws-wrapper-mysql.mysqlconnector")` same.
8. `sqlalchemy.engine.url.make_url("aws-wrapper-postgresql+psycopg://u:p@h/db?wrapper_dialect=aurora-pg").get_dialect()` returns the subclass.
9. URL query args flow through to the DBAPI connect call: patch `aws_advanced_python_wrapper.psycopg.connect` with a mock, call `create_engine(...).connect()`, verify the mock received `wrapper_dialect=` and `plugins=` kwargs.

### No integration tests for F2

F1's integration tests exercise the wrapper pipeline end-to-end. F2 only changes how the engine is constructed — the connection path downstream is identical. URL-to-kwargs is standard SA behavior verified in unit tests above. If the user reports an issue tied specifically to URL handling, add an integration test then.

## Documentation

### Updated `docs/using-the-python-wrapper/SqlAlchemySupport.md`

- Add a top-level **"Using the custom SQLAlchemy dialects (URL-based)"** section right after the creator-pattern sections. Show both PG and MySQL URL usage.
- Remove the **"No custom SA URL-scheme dialect yet"** bullet from the "Limitations" section.
- Update the "Using the wrapper with SQLAlchemy" sections to mention both options — creator-pattern (still supported) and URL-based (new in F2).

### Updated CHANGELOG lines for PR

```markdown
### :magic_wand: Added
* Custom SQLAlchemy dialects `aws-wrapper-postgresql[+psycopg]` and `aws-wrapper-mysql[+mysqlconnector]` for URL-based `create_engine` configuration ([PR #XXXX]).

### :crab: Changed
* `docs/using-the-python-wrapper/SqlAlchemySupport.md` — added URL-based dialect usage section; removed the corresponding "Limitations" bullet ([PR #XXXX]).
```

### No new examples in F2

The existing four F1 examples cover the wrapper+SA combinations. The `SqlAlchemySupport.md` URL section is sufficient documentation of the F2 surface. If user feedback after ship suggests URL-based examples are valuable, add them in a follow-up.

## Back-compatibility

- **Fully additive.** No existing user code paths change. Dialects register via entry-points at install time; existing creator-pattern users see no behavior change.
- **New pyproject entry-point block.** `[tool.poetry.plugins."sqlalchemy.dialects"]` is a new section in pyproject.toml — purely additive, doesn't touch existing deps/classifiers.

## Success criteria

- Unit tests pass on Python 3.10–3.14 matrix.
- `create_engine("aws-wrapper-postgresql+psycopg://…")` constructs an engine whose DBAPI module is `aws_advanced_python_wrapper.psycopg` (verified via `engine.dialect.dbapi is aws_advanced_python_wrapper.psycopg`).
- `create_engine("aws-wrapper-mysql+mysqlconnector://…")` same for mysql-connector.
- SA's standard query-string passthrough carries `wrapper_dialect`, `plugins`, etc. into the DBAPI `connect()` call without custom `create_connect_args` logic.
- `SqlAlchemySupport.md` updated with URL-based usage; the "no URL dialect yet" limitation removed.
- CHANGELOG lines drafted in PR body.

## Open questions

None. All design decisions locked:
- Dialect names: `aws-wrapper-postgresql` / `aws-wrapper-mysql`.
- Default DBAPIs: psycopg v3 / mysql-connector-python.
- Subclass hierarchy: SA's `PGDialect_psycopg` / `MySQLDialect_mysqlconnector` directly.
- Query-arg passthrough: rely on SA's standard behavior (no custom override).
- No async, no new drivers, no new integration tests in F2.

## Sequencing notes for implementation

1. Create `aws_advanced_python_wrapper/sqlalchemy_dialects/` package (empty `__init__.py`).
2. Add `pg.py` + unit tests for the PG dialect (TDD).
3. Add `mysql.py` + unit tests for the MySQL dialect (TDD).
4. Add entry-point registration to `pyproject.toml`.
5. Verify registration works: `poetry install --no-interaction` then test dialect lookup via `sqlalchemy.dialects.registry.load(...)`.
6. Update `SqlAlchemySupport.md`.
7. Final verification on py3.13 + py3.14, lint, mypy, isort.
8. CHANGELOG lines drafted in PR description.
