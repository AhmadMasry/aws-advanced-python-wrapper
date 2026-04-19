# SQLAlchemy Support

The AWS Advanced Python Wrapper can be used as a PEP 249 DBAPI module with SQLAlchemy's `create_engine` via the `creator=` factory pattern, for both PostgreSQL (via psycopg v3) and MySQL (via mysql-connector-python).

## Prerequisites

- Python 3.10 – 3.14 (inclusive)
- SQLAlchemy 2.x
- One of:
  - [psycopg v3](https://www.psycopg.org/psycopg3/) for PostgreSQL
  - [mysql-connector-python](https://dev.mysql.com/doc/connector-python/en/) for MySQL

## Using the wrapper with SQLAlchemy (PostgreSQL)

```python
from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.psycopg import connect

engine = create_engine(
    "postgresql+psycopg://",
    creator=lambda: connect(
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com "
        "dbname=db user=john password=pwd",
        wrapper_dialect="aurora-pg",
        plugins="failover,efm",
    ),
)

try:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT pg_catalog.aurora_db_instance_identifier()")).one()
        print(row)
finally:
    engine.dispose()
    release_resources()
```

The wrapper's connection options (`wrapper_dialect`, `plugins`, etc.) are passed as kwargs to `connect`; the `postgresql+psycopg://` URL tells SQLAlchemy which SQL compiler and type system to use.

## Using the wrapper with SQLAlchemy (MySQL)

```python
from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import connect

engine = create_engine(
    "mysql+mysqlconnector://",
    creator=lambda: connect(
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com "
        "database=db user=john password=pwd",
        wrapper_dialect="aurora-mysql",
        plugins="failover,efm",
        use_pure=True,
    ),
)

try:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT @@aurora_server_id")).one()
        print(row)
finally:
    engine.dispose()
    release_resources()
```

> **Note — `use_pure` + IAM authentication:** For Aurora MySQL, we recommend `use_pure=True` because the C extension's `is_connected` can block indefinitely on network failure. However, the [IAM Authentication Plugin](using-plugins/UsingTheIamAuthenticationPlugin.md) is incompatible with `use_pure=True` (the pure-Python driver truncates passwords at 255 chars; IAM tokens are longer). See the README's "Known Limitations" section for details.

## Using the custom SQLAlchemy dialects (URL-based)

The wrapper registers two SQLAlchemy dialects via entry-points so `create_engine` can be driven by URL alone — no `creator=` lambda needed. This is the idiomatic path for Alembic, 12-factor `DATABASE_URL` configs, and framework starters that expect a URL string.

PostgreSQL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "aws_wrapper_postgresql+psycopg://john:pwd@"
    "database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/db"
    "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,efm"
)
```

MySQL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "aws_wrapper_mysql+mysqlconnector://john:pwd@"
    "database.cluster-xyz.us-east-1.rds.amazonaws.com:3306/db"
    "?wrapper_dialect=aurora-mysql&wrapper_plugins=failover,efm&use_pure=True"
)
```

### Naming

The dialect names (`aws_wrapper_postgresql`, `aws_wrapper_mysql`) echo the JDBC wrapper's `jdbc:aws-wrapper:postgresql://` / `jdbc:aws-wrapper:mysql://` URL scheme, using underscores because SQLAlchemy's URL grammar (`(?P<name>[\w\+]+)://`) does not accept hyphens in the dialect name. The driver slot (`+psycopg`, `+mysqlconnector`) defaults to the wrapper's supported DBAPIs; bare `aws_wrapper_postgresql://` resolves to `+psycopg`.

### URL parameter `wrapper_plugins` (not `plugins`)

SQLAlchemy's `create_engine` reserves the query-string `plugins=` key for its own engine-plugin loader and strips it from the URL before the dialect sees it. To pass the wrapper's `plugins` connection property via URL, spell it **`wrapper_plugins=`** — the dialect translates it back to `plugins=` before calling the wrapper's `connect()`. In the creator-pattern path (where you call `connect()` directly in Python), continue to use the normal `plugins=` kwarg; the `wrapper_plugins` alias is URL-only.

All other wrapper connection options (`wrapper_dialect`, plugin-specific parameters like `failover_timeout_sec`, auth parameters like `iam_region`, etc.) pass through the URL query string unchanged as kwargs to the underlying wrapper's `connect()`.

Both the creator-pattern (shown above) and the URL-based path remain supported. Use whichever fits your configuration surface.

## Error handling

Wrapper errors are classified so SQLAlchemy maps them to the correct `sqlalchemy.exc.*` subclass:

| Wrapper error | SQLAlchemy error |
|---|---|
| `AwsConnectError` | `sqlalchemy.exc.OperationalError` |
| `FailoverError`, `FailoverSuccessError`, `FailoverFailedError`, `TransactionResolutionUnknownError` | `sqlalchemy.exc.OperationalError` |
| `QueryTimeoutError` | `sqlalchemy.exc.OperationalError` |
| `ReadWriteSplittingError` | `sqlalchemy.exc.InterfaceError` |
| `UnsupportedOperationError` | `sqlalchemy.exc.NotSupportedError` |
| `AwsWrapperError` (generic) | `sqlalchemy.exc.DBAPIError` |

Applications writing SA retry loops can `except sqlalchemy.exc.OperationalError` and catch failover events naturally. Target-driver exceptions (e.g., `psycopg.errors.*`, `mysql.connector.errors.*`) are not remapped and flow through SA's dialect-specific classification unchanged.

## Resource cleanup

Two things must be torn down at shutdown, in this order:

1. `engine.dispose()` — drains SQLAlchemy's `QueuePool` and closes all pooled DBAPI connections.
2. `aws_advanced_python_wrapper.release_resources()` — tears down the wrapper's own background threads (topology monitor, host monitoring, internal pool cleanup).

They are complementary: `engine.dispose()` does not reach the wrapper's background machinery, and `release_resources()` does not close SA's pool.

## Combining with plugins

Plugins are configured identically to non-SA usage — via the `plugins` connection property and any plugin-specific options. See:

- [Failover Plugin](using-plugins/UsingTheFailoverPlugin.md) / [Failover v2 Plugin](using-plugins/UsingTheFailover2Plugin.md)
- [Read/Write Splitting Plugin](using-plugins/UsingTheReadWriteSplittingPlugin.md)
- [Host Monitoring Plugin (EFM)](using-plugins/UsingTheHostMonitoringPlugin.md)
- [IAM Authentication Plugin](using-plugins/UsingTheIamAuthenticationPlugin.md)
- [AWS Secrets Manager Plugin](using-plugins/UsingTheAwsSecretsManagerPlugin.md)

## Async usage (3.0.0+)

Starting in 3.0.0, the wrapper exposes a native async path. `create_async_engine` works with the URL-based dialect:

```python
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from aws_advanced_python_wrapper.aio import release_resources_async


async def main() -> None:
    engine = create_async_engine(
        "aws_wrapper_postgresql+psycopg_async://john:pwd@"
        "database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/db"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,efm"
    )
    try:
        async with engine.connect() as conn:
            row = await conn.execute(
                text("SELECT pg_catalog.aurora_db_instance_identifier()")
            )
            print(row.scalar_one())
    finally:
        await engine.dispose()
        await release_resources_async()


asyncio.run(main())
```

The async path:

- Drives `psycopg.AsyncConnection` end-to-end (no greenlet hops through the wrapper's own pipeline).
- Supports the same wrapper plugins via `wrapper_plugins`: `failover`, `efm` (host monitoring), `read_write_splitting`, `iam`, `aws_secrets_manager`, plus the minor/observability plugins.
- MySQL async is deferred until an async MySQL driver is formally supported.

At shutdown, call `engine.dispose()` first, then `aws_advanced_python_wrapper.aio.release_resources_async()` to drain async background tasks, then (optionally) the sync `release_resources()` to drain any sync-side threads the app may also have spun up.

## Limitations (current)

- **Async MySQL is not supported in 3.0.0.** Only async PostgreSQL via psycopg v3. An async MySQL driver (aiomysql / asyncmy / a future official mysql-connector async API) can be added later as a ~5-file change per the F3-B master spec (invariant 8a).

## See also

- [Django Support](DjangoSupport.md)
- [Using the AWS Python Wrapper](UsingThePythonWrapper.md)
- Example scripts:
  - [`PGSQLAlchemyFailover.py`](../examples/PGSQLAlchemyFailover.py)
  - [`MySQLSQLAlchemyFailover.py`](../examples/MySQLSQLAlchemyFailover.py)
  - [`PGSQLAlchemyReadWriteSplitting.py`](../examples/PGSQLAlchemyReadWriteSplitting.py)
  - [`MySQLSQLAlchemyReadWriteSplitting.py`](../examples/MySQLSQLAlchemyReadWriteSplitting.py)
