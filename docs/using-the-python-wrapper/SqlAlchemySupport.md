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

## Limitations (current)

- **Sync only.** Async support (`create_async_engine` via `psycopg.AsyncConnection`) is planned for a future release.
- **No custom SA URL-scheme dialect yet.** A planned follow-up release will register `aws-wrapper-postgresql+psycopg://...` and `aws-wrapper-mysql+mysqlconnector://...` dialects so `create_engine` can be driven by URL alone (useful for Alembic / 12-factor config). Until then, the `creator=` pattern shown above is the supported path.

## See also

- [Django Support](DjangoSupport.md)
- [Using the AWS Python Wrapper](UsingThePythonWrapper.md)
- Example scripts:
  - [`PGSQLAlchemyFailover.py`](../examples/PGSQLAlchemyFailover.py)
  - [`MySQLSQLAlchemyFailover.py`](../examples/MySQLSQLAlchemyFailover.py)
  - [`PGSQLAlchemyReadWriteSplitting.py`](../examples/PGSQLAlchemyReadWriteSplitting.py)
  - [`MySQLSQLAlchemyReadWriteSplitting.py`](../examples/MySQLSQLAlchemyReadWriteSplitting.py)
