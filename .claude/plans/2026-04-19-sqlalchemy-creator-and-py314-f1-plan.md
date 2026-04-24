# SQLAlchemy creator-pattern support + Python 3.14 (F1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `aws_advanced_python_wrapper` a PEP 249-complete DBAPI module so SQLAlchemy's `creator=` pattern works for both psycopg v3 and mysql-connector-python, and add Python 3.14 to the supported matrix.

**Architecture:** A new `_dbapi.py` module holds the canonical PEP 249 module surface (exceptions, type ctors/singletons, apilevel/threadsafety/paramstyle) and an `install()` helper. Two new driver-bound submodules (`psycopg.py`, `mysql_connector.py`) call `install()` to become valid DBAPI modules with a no-target-arg `connect()`. `__init__.py` also calls `install()` so the top-level module satisfies `module.Error`/etc. Wrapper exception classes in `errors.py` gain multi-parent inheritance so SA classifies them as the correct `sqlalchemy.exc.*` subclass.

**Tech Stack:** Python 3.10–3.14, poetry, pytest, SQLAlchemy 2.x, psycopg v3, mysql-connector-python.

**Spec:** `.claude/specs/2026-04-19-sqlalchemy-creator-and-py314-f1-design.md`

---

## File Structure (decomposition decisions)

**New:**
- `aws_advanced_python_wrapper/_dbapi.py` — canonical PEP 249 module surface + `install()` helper.
- `aws_advanced_python_wrapper/psycopg.py` — DBAPI submodule bound to psycopg v3.
- `aws_advanced_python_wrapper/mysql_connector.py` — DBAPI submodule bound to mysql-connector-python.
- `tests/unit/test_dbapi_module_contract.py` — parametrized PEP 249 surface checks for all three modules.
- `tests/unit/test_error_hierarchy.py` — isinstance assertions for reparented wrapper errors.
- `tests/unit/test_psycopg_submodule.py` — mock-based submodule behavior.
- `tests/unit/test_mysql_connector_submodule.py` — mock-based submodule behavior.
- `tests/integration/container/test_sqlalchemy.py` — four parametrized Aurora cases (PG/MySQL × Failover/RWS).
- `docs/using-the-python-wrapper/SqlAlchemySupport.md` — user-facing SA guide.
- `docs/examples/PGSQLAlchemyFailover.py`
- `docs/examples/MySQLSQLAlchemyFailover.py`
- `docs/examples/PGSQLAlchemyReadWriteSplitting.py`
- `docs/examples/MySQLSQLAlchemyReadWriteSplitting.py`

**Modified:**
- `aws_advanced_python_wrapper/__init__.py` — replace hand-rolled PEP 249 lines with `_dbapi.install()`.
- `aws_advanced_python_wrapper/errors.py` — reparent wrapper error classes to specific PEP 249 subclasses.
- `pyproject.toml` — add Python 3.14 classifier.
- `.github/workflows/main.yml` — add 3.14 to the build matrix.
- `.github/workflows/integration_tests.yml` — add 3.14 if it has a matrix.
- `docs/development-guide/DevelopmentGuide.md` — 3.14 phrasing update.
- `README.md` — 3.14 phrasing update + one-line SA pointer.
- `docs/README.md` — add `SqlAlchemySupport.md` ToC entry.
- `docs/using-the-python-wrapper/using-plugins/UsingTheReadWriteSplittingPlugin.md` — add cross-links to new SA examples.
- `mypy.ini` — add four new example filenames to the exclude regex.

---

### Task 1: `_dbapi.py` — canonical PEP 249 surface + install() helper

**Files:**
- Create: `aws_advanced_python_wrapper/_dbapi.py`
- Test: `tests/unit/test_dbapi_module_contract.py` (only the `test_install_populates_target_namespace` case in this task; module-coverage parametrization lands in Task 2)

- [ ] **Step 1: Write the failing test for `install()` on a fresh namespace**

Create `tests/unit/test_dbapi_module_contract.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
import pytest

from aws_advanced_python_wrapper import _dbapi


PEP249_NAMES = (
    "Warning", "Error", "InterfaceError", "DatabaseError",
    "DataError", "OperationalError", "IntegrityError",
    "InternalError", "ProgrammingError", "NotSupportedError",
    "Date", "Time", "Timestamp",
    "DateFromTicks", "TimeFromTicks", "TimestampFromTicks",
    "Binary", "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
    "apilevel", "threadsafety", "paramstyle",
)


def test_install_populates_target_namespace_without_connect():
    ns = {}
    _dbapi.install(ns)
    for name in PEP249_NAMES:
        assert name in ns, f"missing {name}"
    assert "connect" not in ns
    assert set(ns["__all__"]) == set(PEP249_NAMES)


def test_install_sets_connect_when_provided():
    ns = {}
    sentinel = lambda *a, **k: None
    _dbapi.install(ns, connect=sentinel)
    assert ns["connect"] is sentinel
    assert "connect" in ns["__all__"]


def test_apilevel_and_paramstyle_values():
    ns = {}
    _dbapi.install(ns)
    assert ns["apilevel"] == "2.0"
    assert ns["threadsafety"] == 2
    assert ns["paramstyle"] == "pyformat"


def test_binary_constructor_returns_bytes():
    ns = {}
    _dbapi.install(ns)
    assert ns["Binary"](b"x") == b"x"
    assert isinstance(ns["Binary"](b"x"), bytes)


def test_date_time_timestamp_aliases():
    import datetime
    ns = {}
    _dbapi.install(ns)
    assert ns["Date"] is datetime.date
    assert ns["Time"] is datetime.time
    assert ns["Timestamp"] is datetime.datetime


def test_type_singletons_compare_equal_to_member_codes():
    ns = {}
    _dbapi.install(ns)
    # At least one OID/FieldType code in each singleton
    assert 25 in ns["STRING"]         # psycopg OID for text
    assert 17 in ns["BINARY"]         # psycopg OID for bytea
    assert 20 in ns["NUMBER"]         # psycopg OID for int8
    assert 1082 in ns["DATETIME"]     # psycopg OID for date
    assert 26 in ns["ROWID"]          # psycopg OID for oid
    # Equality against a contained member returns True (PEP 249 semantics)
    assert ns["STRING"] == 25
    assert ns["STRING"] != 99999


def test_exception_hierarchy_roots():
    ns = {}
    _dbapi.install(ns)
    assert issubclass(ns["Warning"], Exception)
    assert issubclass(ns["Error"], Exception)
    for sub in ("InterfaceError", "DatabaseError"):
        assert issubclass(ns[sub], ns["Error"])
    for sub in ("DataError", "OperationalError", "IntegrityError",
                "InternalError", "ProgrammingError", "NotSupportedError"):
        assert issubclass(ns[sub], ns["DatabaseError"])
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `poetry run python -m pytest tests/unit/test_dbapi_module_contract.py -v`
Expected: `ModuleNotFoundError: No module named 'aws_advanced_python_wrapper._dbapi'` (or similar) on the import line.

- [ ] **Step 3: Create `_dbapi.py`**

Create `aws_advanced_python_wrapper/_dbapi.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Canonical PEP 249 module surface shared by the top-level wrapper module
and the per-driver DBAPI submodules (`aws_advanced_python_wrapper.psycopg`,
`aws_advanced_python_wrapper.mysql_connector`).

Consumers should NOT import from this module directly. The public DBAPI
module surface lives on the top-level module and the per-driver submodules,
populated via `install()`.
"""

from __future__ import annotations

from datetime import date as Date, datetime as Timestamp, time as Time
from time import localtime
from typing import Callable, Optional

from aws_advanced_python_wrapper.pep249 import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


def Binary(data: bytes) -> bytes:
    return bytes(data)


def DateFromTicks(ticks: float) -> Date:
    return Date(*localtime(ticks)[:3])


def TimeFromTicks(ticks: float) -> Time:
    return Time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float) -> Timestamp:
    return Timestamp(*localtime(ticks)[:6])


class _DBAPISet(frozenset):
    """Type-object singleton per PEP 249: compares equal to any contained type code."""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (int, str)):
            return other in self
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return super().__hash__()


# Type-code sources:
#   PG: psycopg.postgres.types (OIDs)
#   MySQL: mysql.connector.FieldType (ints)
# Union both into each singleton.

# PG text-like OIDs: text(25), varchar(1043), bpchar(1042), char(18),
#                    name(19), json(114), jsonb(3802)
# MySQL FieldType string-like: VAR_STRING(253), STRING(254), VARCHAR(15)
STRING = _DBAPISet([25, 1043, 1042, 18, 19, 114, 3802, 253, 254, 15])

# PG binary: bytea(17)
# MySQL FieldType BLOB family: TINY_BLOB(249), MEDIUM_BLOB(250),
#                              LONG_BLOB(251), BLOB(252)
BINARY = _DBAPISet([17, 249, 250, 251, 252])

# PG numeric: int2(21), int4(23), int8(20), float4(700), float8(701),
#             numeric(1700), money(790)
# MySQL FieldType numeric: DECIMAL(0), TINY(1), SHORT(2), LONG(3),
#                          FLOAT(4), DOUBLE(5), LONGLONG(8), INT24(9),
#                          NEWDECIMAL(246)
NUMBER = _DBAPISet([21, 23, 20, 700, 701, 1700, 790, 0, 1, 2, 3, 4, 5, 8, 9, 246])

# PG datetime: date(1082), time(1083), timestamp(1114), timestamptz(1184),
#              timetz(1266), interval(1186)
# MySQL FieldType datetime: DATE(10), TIME(11), DATETIME(12), YEAR(13),
#                           NEWDATE(14), TIMESTAMP(7)
DATETIME = _DBAPISet([1082, 1083, 1114, 1184, 1266, 1186, 10, 11, 12, 13, 14, 7])

# PG rowid: oid(26). MySQL has no ROWID equivalent; left PG-only.
ROWID = _DBAPISet([26])


_PEP249_NAMES = (
    "Warning", "Error", "InterfaceError", "DatabaseError",
    "DataError", "OperationalError", "IntegrityError",
    "InternalError", "ProgrammingError", "NotSupportedError",
    "Date", "Time", "Timestamp",
    "DateFromTicks", "TimeFromTicks", "TimestampFromTicks",
    "Binary", "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
    "apilevel", "threadsafety", "paramstyle",
)


def install(module_ns: dict, connect: Optional[Callable] = None) -> None:
    """Populate `module_ns` with the PEP 249 module surface.

    If `connect` is provided, `module_ns['connect']` is set to it and 'connect'
    is added to `module_ns['__all__']`.
    """
    source = globals()
    for name in _PEP249_NAMES:
        module_ns[name] = source[name]
    if connect is not None:
        module_ns["connect"] = connect
        module_ns["__all__"] = (*_PEP249_NAMES, "connect")
    else:
        module_ns["__all__"] = tuple(_PEP249_NAMES)
```

- [ ] **Step 4: Run the test and confirm it passes**

Run: `poetry run python -m pytest tests/unit/test_dbapi_module_contract.py -v`
Expected: all 7 tests PASS.

- [ ] **Step 5: Run lint + type checks on the new file**

Run: `poetry run flake8 aws_advanced_python_wrapper/_dbapi.py tests/unit/test_dbapi_module_contract.py && poetry run mypy aws_advanced_python_wrapper/_dbapi.py && poetry run isort --check-only aws_advanced_python_wrapper/_dbapi.py tests/unit/test_dbapi_module_contract.py`
Expected: all three report no issues. If isort reports drift, run `poetry run isort aws_advanced_python_wrapper/_dbapi.py tests/unit/test_dbapi_module_contract.py` and re-run `--check-only`.

- [ ] **Step 6: Commit**

```bash
git add aws_advanced_python_wrapper/_dbapi.py tests/unit/test_dbapi_module_contract.py
git commit -m "feat: add PEP 249 canonical surface (_dbapi.install helper)"
```

---

### Task 2: Wire `__init__.py` to use `_dbapi.install()` + module-level contract tests

**Files:**
- Modify: `aws_advanced_python_wrapper/__init__.py`
- Test: `tests/unit/test_dbapi_module_contract.py` (append module-coverage case)

- [ ] **Step 1: Append the failing module-coverage test**

Append to `tests/unit/test_dbapi_module_contract.py`:

```python
import importlib


@pytest.mark.parametrize("module_name", [
    "aws_advanced_python_wrapper",
])
@pytest.mark.parametrize("attr_name", PEP249_NAMES + ("connect",))
def test_module_exports_pep249_attr(module_name, attr_name):
    mod = importlib.import_module(module_name)
    assert hasattr(mod, attr_name), f"{module_name} missing {attr_name}"


def test_top_level_connect_is_awswrapperconnection_connect():
    import aws_advanced_python_wrapper as aaw
    from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection
    assert aaw.connect is AwsWrapperConnection.connect
```

- [ ] **Step 2: Run the new tests and confirm they fail**

Run: `poetry run python -m pytest tests/unit/test_dbapi_module_contract.py -v -k "module_exports or top_level_connect"`
Expected: the parametrized `test_module_exports_pep249_attr` FAILs for the exception/type names (e.g., `aws_advanced_python_wrapper missing Error`). The `test_top_level_connect_is_awswrapperconnection_connect` PASSes (it's already wired).

- [ ] **Step 3: Rewrite `__init__.py` to use `_dbapi.install()`**

Replace the contents of `aws_advanced_python_wrapper/__init__.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import sys
from logging import DEBUG, getLogger

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.cleanup import release_resources
from aws_advanced_python_wrapper.utils.utils import LogUtils
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection

# Populate the full PEP 249 module surface (exceptions, type ctors/singletons,
# apilevel/threadsafety/paramstyle). `connect` stays bound to
# AwsWrapperConnection.connect for back-compat with existing callers.
_dbapi.install(sys.modules[__name__].__dict__, connect=AwsWrapperConnection.connect)


def set_logger(name="aws_advanced_python_wrapper", level=DEBUG, format_string=None):
    LogUtils.setup_logger(getLogger(name), level, format_string)


__all__ = (
    "AwsWrapperConnection",
    "release_resources",
    "set_logger",
    *_dbapi._PEP249_NAMES,
    "connect",
)
```

- [ ] **Step 4: Run the new tests and confirm they pass**

Run: `poetry run python -m pytest tests/unit/test_dbapi_module_contract.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Run the full unit suite to guard against regressions**

Run: `poetry run python -m pytest tests/unit -Werror`
Expected: all pre-existing tests still PASS. If any test imports names from `aws_advanced_python_wrapper` that the old `__init__.py` exposed, they still work.

- [ ] **Step 6: Lint + type check**

Run: `poetry run flake8 aws_advanced_python_wrapper/__init__.py && poetry run mypy aws_advanced_python_wrapper/__init__.py && poetry run isort --check-only aws_advanced_python_wrapper/__init__.py tests/unit/test_dbapi_module_contract.py`
Expected: clean. Fix with `poetry run isort ...` if needed.

- [ ] **Step 7: Commit**

```bash
git add aws_advanced_python_wrapper/__init__.py tests/unit/test_dbapi_module_contract.py
git commit -m "feat: export full PEP 249 surface from aws_advanced_python_wrapper"
```

---

### Task 3: Reparent wrapper exception classes to specific PEP 249 subclasses

**Files:**
- Modify: `aws_advanced_python_wrapper/errors.py`
- Test: `tests/unit/test_error_hierarchy.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_error_hierarchy.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
import pytest

from aws_advanced_python_wrapper import errors, pep249


def test_aws_wrapper_error_is_pep249_error():
    err = errors.AwsWrapperError("x")
    assert isinstance(err, pep249.Error)


def test_aws_connect_error_is_operational_error():
    err = errors.AwsConnectError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.OperationalError)


@pytest.mark.parametrize("cls", [
    errors.FailoverError,
    errors.FailoverSuccessError,
    errors.FailoverFailedError,
    errors.TransactionResolutionUnknownError,
])
def test_failover_errors_are_operational_errors(cls):
    err = cls("x")
    assert isinstance(err, pep249.OperationalError)
    assert isinstance(err, pep249.Error)


def test_query_timeout_is_operational_error():
    err = errors.QueryTimeoutError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.OperationalError)


def test_read_write_splitting_error_is_interface_error():
    err = errors.ReadWriteSplittingError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.InterfaceError)


def test_unsupported_operation_is_not_supported_error():
    err = errors.UnsupportedOperationError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.NotSupportedError)


def test_existing_except_awswrappererror_still_catches_children():
    # Regression guard: any caller using `except AwsWrapperError` must still catch
    # the reparented subclasses.
    for cls in (errors.AwsConnectError, errors.QueryTimeoutError,
                errors.ReadWriteSplittingError, errors.UnsupportedOperationError):
        try:
            raise cls("x")
        except errors.AwsWrapperError:
            pass
        else:
            pytest.fail(f"{cls.__name__} should be caught by except AwsWrapperError")


def test_failover_errors_caught_by_except_error():
    for cls in (errors.FailoverError, errors.FailoverSuccessError,
                errors.FailoverFailedError, errors.TransactionResolutionUnknownError):
        try:
            raise cls("x")
        except pep249.Error:
            pass
        else:
            pytest.fail(f"{cls.__name__} should be caught by except pep249.Error")


def test_mro_shape_has_single_error_ancestor():
    # Verify multi-parent MRO doesn't accidentally add a second Error ancestor.
    for cls in (errors.AwsConnectError, errors.QueryTimeoutError,
                errors.ReadWriteSplittingError, errors.UnsupportedOperationError):
        error_ancestors = [c for c in cls.__mro__ if c is pep249.Error]
        assert len(error_ancestors) == 1, f"{cls.__name__} has >1 Error in MRO"
```

- [ ] **Step 2: Run and confirm failure**

Run: `poetry run python -m pytest tests/unit/test_error_hierarchy.py -v`
Expected: tests for `AwsConnectError`, `FailoverError` (and children), `QueryTimeoutError`, `ReadWriteSplittingError`, `UnsupportedOperationError` FAIL because those classes are not yet subclasses of `OperationalError`/`InterfaceError`/`NotSupportedError`.

- [ ] **Step 3: Reparent the classes in `errors.py`**

Read the current `aws_advanced_python_wrapper/errors.py` first. Then replace the class declarations to add the second parent. Final state of the file (preserving existing `__init__` and attribute signatures):

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from typing import Optional

from .pep249 import (Error, InterfaceError, NotSupportedError,
                     OperationalError)


class AwsWrapperError(Error):
    __module__ = "aws_advanced_python_wrapper"
    driver_error: Optional[Exception]

    def __init__(self, message: str = "", original_error: Optional[Exception] = None):
        super().__init__(message)
        self.driver_error = original_error


class UnsupportedOperationError(AwsWrapperError, NotSupportedError):
    __module__ = "aws_advanced_python_wrapper"


class QueryTimeoutError(AwsWrapperError, OperationalError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverError(Error, OperationalError):
    __module__ = "aws_advanced_python_wrapper"


class TransactionResolutionUnknownError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverFailedError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverSuccessError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class ReadWriteSplittingError(AwsWrapperError, InterfaceError):
    __module__ = "aws_advanced_python_wrapper"


class AwsConnectError(AwsWrapperError, OperationalError):
    __module__ = "aws_advanced_python_wrapper"
```

Preserve the exact `__init__` signature of `AwsWrapperError`. Do not modify methods on the existing classes beyond the parent list.

- [ ] **Step 4: Run the new tests and confirm they pass**

Run: `poetry run python -m pytest tests/unit/test_error_hierarchy.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Run the full unit suite with `-Werror`**

Run: `poetry run python -m pytest tests/unit -Werror`
Expected: all tests PASS. No existing test should break from the added parent — `isinstance` only widens.

- [ ] **Step 6: Lint + type check**

Run: `poetry run flake8 aws_advanced_python_wrapper/errors.py tests/unit/test_error_hierarchy.py && poetry run mypy aws_advanced_python_wrapper/errors.py && poetry run isort --check-only aws_advanced_python_wrapper/errors.py tests/unit/test_error_hierarchy.py`
Expected: clean. Fix with `poetry run isort ...` if needed.

- [ ] **Step 7: Commit**

```bash
git add aws_advanced_python_wrapper/errors.py tests/unit/test_error_hierarchy.py
git commit -m "feat: reparent wrapper errors to specific PEP 249 subclasses"
```

---

### Task 4: `aws_advanced_python_wrapper.psycopg` submodule

**Files:**
- Create: `aws_advanced_python_wrapper/psycopg.py`
- Test: `tests/unit/test_psycopg_submodule.py`
- Modify (append parametrize case): `tests/unit/test_dbapi_module_contract.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_psycopg_submodule.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
from unittest.mock import patch

import pytest

from aws_advanced_python_wrapper import psycopg as wrapper_psycopg
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def test_submodule_connect_routes_through_awswrapperconnection(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel_connection"
    )
    result = wrapper_psycopg.connect(
        "host=h user=u dbname=d", wrapper_dialect="aurora-pg"
    )
    assert result == "sentinel_connection"
    # First positional arg must be psycopg.Connection.connect; second is the DSN.
    import psycopg
    args, kwargs = mock_wrapper_connect.call_args
    assert args[0] is psycopg.Connection.connect
    assert args[1] == "host=h user=u dbname=d"
    assert kwargs == {"wrapper_dialect": "aurora-pg"}


def test_submodule_connect_passes_all_kwargs_through(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel"
    )
    wrapper_psycopg.connect(
        "host=h", wrapper_dialect="aurora-pg", plugins="failover,efm", autocommit=True
    )
    _, kwargs = mock_wrapper_connect.call_args
    assert kwargs == {
        "wrapper_dialect": "aurora-pg",
        "plugins": "failover,efm",
        "autocommit": True,
    }


def test_submodule_has_full_pep249_surface():
    for name in ("Error", "OperationalError", "InterfaceError",
                 "DatabaseError", "Date", "Binary",
                 "STRING", "NUMBER", "apilevel", "paramstyle"):
        assert hasattr(wrapper_psycopg, name), f"missing {name}"
    assert wrapper_psycopg.apilevel == "2.0"
    assert wrapper_psycopg.paramstyle == "pyformat"


def test_submodule_error_is_same_class_as_top_level():
    # Identity: SA resolves module.Error on whichever module it got the conn from;
    # the two identifiers must point to the same class so isinstance works.
    import aws_advanced_python_wrapper as aaw
    assert wrapper_psycopg.Error is aaw.Error
    assert wrapper_psycopg.OperationalError is aaw.OperationalError
```

- [ ] **Step 2: Run and confirm failure**

Run: `poetry run python -m pytest tests/unit/test_psycopg_submodule.py -v`
Expected: `ModuleNotFoundError: No module named 'aws_advanced_python_wrapper.psycopg'`.

- [ ] **Step 3: Create the submodule**

Create `aws_advanced_python_wrapper/psycopg.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""PEP 249 DBAPI module bound to psycopg v3.

Enables SQLAlchemy's creator-pattern:

    from sqlalchemy import create_engine
    from aws_advanced_python_wrapper.psycopg import connect

    engine = create_engine(
        "postgresql+psycopg://",
        creator=lambda: connect(
            "host=... user=... dbname=...",
            wrapper_dialect="aurora-pg",
        ),
    )
"""

from __future__ import annotations

import sys
from typing import Any

from psycopg import Connection as _PGConnection

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def connect(conninfo: str = "", **kwargs: Any) -> AwsWrapperConnection:
    """PEP 249 `connect`, target-driver-bound to psycopg v3.

    Equivalent to::

        AwsWrapperConnection.connect(psycopg.Connection.connect, conninfo, **kwargs)
    """
    return AwsWrapperConnection.connect(_PGConnection.connect, conninfo, **kwargs)


_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
```

- [ ] **Step 4: Run and confirm the submodule tests pass**

Run: `poetry run python -m pytest tests/unit/test_psycopg_submodule.py -v`
Expected: all 4 tests PASS.

- [ ] **Step 5: Extend the module-contract parametrize to cover this submodule**

In `tests/unit/test_dbapi_module_contract.py`, change the `module_name` parametrize in `test_module_exports_pep249_attr` to:

```python
@pytest.mark.parametrize("module_name", [
    "aws_advanced_python_wrapper",
    "aws_advanced_python_wrapper.psycopg",
])
```

- [ ] **Step 6: Run and confirm the parametrized check covers both modules**

Run: `poetry run python -m pytest tests/unit/test_dbapi_module_contract.py::test_module_exports_pep249_attr -v`
Expected: 2 × (24 attrs) = 48 cases, all PASS.

- [ ] **Step 7: Lint + type check**

Run: `poetry run flake8 aws_advanced_python_wrapper/psycopg.py tests/unit/test_psycopg_submodule.py && poetry run mypy aws_advanced_python_wrapper/psycopg.py && poetry run isort --check-only aws_advanced_python_wrapper/psycopg.py tests/unit/test_psycopg_submodule.py tests/unit/test_dbapi_module_contract.py`
Expected: clean. Fix with `poetry run isort ...` if needed.

- [ ] **Step 8: Commit**

```bash
git add aws_advanced_python_wrapper/psycopg.py tests/unit/test_psycopg_submodule.py tests/unit/test_dbapi_module_contract.py
git commit -m "feat: add aws_advanced_python_wrapper.psycopg DBAPI submodule"
```

---

### Task 5: `aws_advanced_python_wrapper.mysql_connector` submodule

**Files:**
- Create: `aws_advanced_python_wrapper/mysql_connector.py`
- Test: `tests/unit/test_mysql_connector_submodule.py`
- Modify (append parametrize case): `tests/unit/test_dbapi_module_contract.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_mysql_connector_submodule.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
import pytest

from aws_advanced_python_wrapper import mysql_connector as wrapper_mysql
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def test_submodule_connect_routes_through_awswrapperconnection(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel_connection"
    )
    result = wrapper_mysql.connect(
        "host=h user=u database=d", wrapper_dialect="aurora-mysql"
    )
    assert result == "sentinel_connection"
    import mysql.connector
    args, kwargs = mock_wrapper_connect.call_args
    assert args[0] is mysql.connector.connect
    assert args[1] == "host=h user=u database=d"
    assert kwargs == {"wrapper_dialect": "aurora-mysql"}


def test_submodule_connect_passes_all_kwargs_through(mocker):
    mock_wrapper_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value="sentinel"
    )
    wrapper_mysql.connect(
        "host=h", wrapper_dialect="aurora-mysql",
        plugins="failover,efm", use_pure=False,
    )
    _, kwargs = mock_wrapper_connect.call_args
    assert kwargs == {
        "wrapper_dialect": "aurora-mysql",
        "plugins": "failover,efm",
        "use_pure": False,
    }


def test_submodule_has_full_pep249_surface():
    for name in ("Error", "OperationalError", "InterfaceError",
                 "DatabaseError", "Date", "Binary",
                 "STRING", "NUMBER", "apilevel", "paramstyle"):
        assert hasattr(wrapper_mysql, name), f"missing {name}"
    assert wrapper_mysql.apilevel == "2.0"
    assert wrapper_mysql.paramstyle == "pyformat"


def test_submodule_error_is_same_class_as_top_level():
    import aws_advanced_python_wrapper as aaw
    assert wrapper_mysql.Error is aaw.Error
    assert wrapper_mysql.OperationalError is aaw.OperationalError
```

- [ ] **Step 2: Run and confirm failure**

Run: `poetry run python -m pytest tests/unit/test_mysql_connector_submodule.py -v`
Expected: `ModuleNotFoundError: No module named 'aws_advanced_python_wrapper.mysql_connector'`.

- [ ] **Step 3: Create the submodule**

Create `aws_advanced_python_wrapper/mysql_connector.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""PEP 249 DBAPI module bound to mysql-connector-python.

Enables SQLAlchemy's creator-pattern:

    from sqlalchemy import create_engine
    from aws_advanced_python_wrapper.mysql_connector import connect

    engine = create_engine(
        "mysql+mysqlconnector://",
        creator=lambda: connect(
            "host=... user=... database=...",
            wrapper_dialect="aurora-mysql",
        ),
    )
"""

from __future__ import annotations

import sys
from typing import Any

from mysql.connector import connect as _mysql_connect

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection


def connect(conninfo: str = "", **kwargs: Any) -> AwsWrapperConnection:
    """PEP 249 `connect`, target-driver-bound to mysql-connector-python.

    Equivalent to::

        AwsWrapperConnection.connect(mysql.connector.connect, conninfo, **kwargs)
    """
    return AwsWrapperConnection.connect(_mysql_connect, conninfo, **kwargs)


_dbapi.install(sys.modules[__name__].__dict__, connect=connect)
```

- [ ] **Step 4: Run and confirm the submodule tests pass**

Run: `poetry run python -m pytest tests/unit/test_mysql_connector_submodule.py -v`
Expected: all 4 tests PASS.

- [ ] **Step 5: Extend the module-contract parametrize to cover this submodule**

In `tests/unit/test_dbapi_module_contract.py`, change the `module_name` parametrize in `test_module_exports_pep249_attr` to:

```python
@pytest.mark.parametrize("module_name", [
    "aws_advanced_python_wrapper",
    "aws_advanced_python_wrapper.psycopg",
    "aws_advanced_python_wrapper.mysql_connector",
])
```

- [ ] **Step 6: Run and confirm parametrized coverage**

Run: `poetry run python -m pytest tests/unit/test_dbapi_module_contract.py::test_module_exports_pep249_attr -v`
Expected: 3 × (24 attrs) = 72 cases, all PASS.

- [ ] **Step 7: Full unit suite with `-Werror`**

Run: `poetry run python -m pytest tests/unit -Werror`
Expected: all tests PASS.

- [ ] **Step 8: Lint + type check**

Run: `poetry run flake8 aws_advanced_python_wrapper/mysql_connector.py tests/unit/test_mysql_connector_submodule.py && poetry run mypy aws_advanced_python_wrapper/mysql_connector.py && poetry run isort --check-only aws_advanced_python_wrapper/mysql_connector.py tests/unit/test_mysql_connector_submodule.py tests/unit/test_dbapi_module_contract.py`
Expected: clean. Fix with `poetry run isort ...` if needed.

- [ ] **Step 9: Commit**

```bash
git add aws_advanced_python_wrapper/mysql_connector.py tests/unit/test_mysql_connector_submodule.py tests/unit/test_dbapi_module_contract.py
git commit -m "feat: add aws_advanced_python_wrapper.mysql_connector DBAPI submodule"
```

---

### Task 6: Add Python 3.14 to classifier + CI matrix + docs phrasing

**Files:**
- Modify: `pyproject.toml`
- Modify: `.github/workflows/main.yml`
- Modify: `.github/workflows/integration_tests.yml` (only if it has a python-version matrix)
- Modify: `docs/development-guide/DevelopmentGuide.md`
- Modify: `README.md` (the "How to Contribute" section only — the SA one-liner lands in Task 8)

- [ ] **Step 1: Add the 3.14 classifier to `pyproject.toml`**

In `pyproject.toml`, locate the `classifiers = [...]` block (around lines 11–23) and add one line after the `"Programming Language :: Python :: 3.13"` entry:

```toml
    "Programming Language :: Python :: 3.13",
    "Programming Language :: Python :: 3.14",
```

- [ ] **Step 2: Add 3.14 to the CI matrix**

In `.github/workflows/main.yml`, change line 35 (or wherever the matrix is):

```yaml
        python-version: ["3.10", "3.11", "3.12", "3.13", "3.14"]
```

- [ ] **Step 3: Check integration_tests.yml for a python matrix**

Read `.github/workflows/integration_tests.yml`. If it contains a `python-version:` matrix listing explicit versions, extend it with `"3.14"` the same way as step 2. If it does not have an explicit version list (e.g., it runs against a single Python via container), skip this step — note in the commit message that integration_tests.yml has no matrix.

- [ ] **Step 4: Update `docs/development-guide/DevelopmentGuide.md`**

Change the line: `Make sure you have Python 3.10 - 3.13 (inclusive) installed, along with your choice of underlying Python driver...`

Replace with: `Make sure you have Python 3.10 - 3.14 (inclusive) installed, along with your choice of underlying Python driver...`

- [ ] **Step 5: Update `README.md` "How to Contribute" section**

Change: `(e.g. do not require Python 3.7 if we are supporting Python 3.10 - 3.13 (inclusive))`

Replace with: `(e.g. do not require Python 3.7 if we are supporting Python 3.10 - 3.14 (inclusive))`

- [ ] **Step 6: Install 3.14 locally and run the suite**

Run:

```bash
poetry env use 3.14
poetry install
poetry run python -m pytest tests/unit -Werror
```

Expected: all tests PASS. If any fail with a deprecation warning new to 3.14, fix the specific call site and commit as a follow-up step inside this task (do not skip to a later task).

If `python3.14` is not installed on the developer's machine, install via pyenv or another method first. Do NOT proceed to step 7 without green 3.14.

- [ ] **Step 7: Lint (on the env you normally use, since `poetry env use 3.14` may have swapped away)**

Run `poetry env use 3.13` (or whatever the developer's default is) and then:

`poetry run flake8 . && poetry run mypy . && poetry run isort --check-only .`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml .github/workflows/main.yml docs/development-guide/DevelopmentGuide.md README.md
# Include integration_tests.yml only if it was modified in step 3.
git commit -m "feat: add Python 3.14 support"
```

---

### Task 7: `SqlAlchemySupport.md` docs page + ToC + README pointer + RWS cross-links

**Files:**
- Create: `docs/using-the-python-wrapper/SqlAlchemySupport.md`
- Modify: `docs/README.md`
- Modify: `README.md`
- Modify: `docs/using-the-python-wrapper/using-plugins/UsingTheReadWriteSplittingPlugin.md`

- [ ] **Step 1: Create `SqlAlchemySupport.md`**

Create `docs/using-the-python-wrapper/SqlAlchemySupport.md`:

```markdown
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

The wrapper's connection options (`wrapper_dialect`, `plugins`, etc.) are passed as kwargs to `connect`; SA's `postgresql+psycopg://` bit tells SA which SQL compiler and type system to use.

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

After F1, wrapper errors are classified so SQLAlchemy maps them to the correct `sqlalchemy.exc.*` subclass:

| Wrapper error | SQLAlchemy error |
|---|---|
| `AwsConnectError` | `sqlalchemy.exc.OperationalError` |
| `FailoverError`, `FailoverSuccessError`, `FailoverFailedError`, `TransactionResolutionUnknownError` | `sqlalchemy.exc.OperationalError` |
| `QueryTimeoutError` | `sqlalchemy.exc.OperationalError` |
| `ReadWriteSplittingError` | `sqlalchemy.exc.InterfaceError` |
| `UnsupportedOperationError` | `sqlalchemy.exc.NotSupportedError` |
| `AwsWrapperError` (generic) | `sqlalchemy.exc.DBAPIError` |

Consumers writing SA retry loops can `except sqlalchemy.exc.OperationalError` and catch failover events naturally. Target-driver exceptions (e.g., `psycopg.errors.*`, `mysql.connector.errors.*`) are not remapped and flow through SA's dialect-specific classification unchanged.

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
```

- [ ] **Step 2: Add ToC entry to `docs/README.md`**

In `docs/README.md`, locate the "Using the AWS Python Wrapper" section (around line 4) and append a bullet right after the main entry (or alongside `DjangoSupport.md`):

Before:
```markdown
- [Using the AWS Python Wrapper](using-the-python-wrapper/UsingThePythonWrapper.md)
```

After (add these two lines immediately following, preserving any existing nested items):
```markdown
- [Using the AWS Python Wrapper](using-the-python-wrapper/UsingThePythonWrapper.md)
  - [SQLAlchemy Support](using-the-python-wrapper/SqlAlchemySupport.md)
  - [Django Support](using-the-python-wrapper/DjangoSupport.md)
```

If `DjangoSupport.md` is already listed elsewhere in the file, leave the existing entry in place and only add the SQLAlchemy line.

- [ ] **Step 3: Add one-line SA pointer to `README.md`**

In `README.md`, locate the "### Using the AWS Advanced Python Wrapper" subsection (around line 141). Add one line at the end of that section:

```markdown
For SQLAlchemy integration (both PostgreSQL and MySQL), see [SQLAlchemy Support](./docs/using-the-python-wrapper/SqlAlchemySupport.md).
```

- [ ] **Step 4: Cross-link from RWS plugin doc**

In `docs/using-the-python-wrapper/using-plugins/UsingTheReadWriteSplittingPlugin.md`, find the existing line (around line 43):

```markdown
The AWS Advanced Python Wrapper currently uses [SqlAlchemy](https://docs.sqlalchemy.org/en/20/core/pooling.html) to create and maintain its internal connection pools. The [Postgres](../../examples/PGReadWriteSplitting.py) or [MySQL](../../examples/MySQLReadWriteSplitting.py) sample code provides a useful example of how to enable this feature. The steps are as follows:
```

Append a follow-up sentence:

```markdown
For consumers already using SQLAlchemy in their application, see the SA-flavored examples: [`PGSQLAlchemyReadWriteSplitting.py`](../../examples/PGSQLAlchemyReadWriteSplitting.py) and [`MySQLSQLAlchemyReadWriteSplitting.py`](../../examples/MySQLSQLAlchemyReadWriteSplitting.py).
```

- [ ] **Step 5: Docs CI sanity — broken-link check**

Run: `find docs -name "*.md" -exec grep -l "\]\(" {} \;` and spot-check the new page's relative links resolve (`../examples/PGSQLAlchemyFailover.py` etc. won't exist until Task 8-11 — that's expected; the markdown-link-check CI job that runs on PRs may flag them. If so, one of two approaches: (a) land this commit together with the example-creation commits in a single push, or (b) temporarily omit the missing-file links and add them back in Task 11's final commit. Prefer (a) — see "Sequencing note" at the end of the plan.

- [ ] **Step 6: Commit**

```bash
git add docs/using-the-python-wrapper/SqlAlchemySupport.md docs/README.md README.md docs/using-the-python-wrapper/using-plugins/UsingTheReadWriteSplittingPlugin.md
git commit -m "docs: add SQLAlchemy support guide and ToC entries"
```

---

### Task 8: `docs/examples/PGSQLAlchemyFailover.py`

**Files:**
- Create: `docs/examples/PGSQLAlchemyFailover.py`

- [ ] **Step 1: Create the example**

Create `docs/examples/PGSQLAlchemyFailover.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""SQLAlchemy + AWS Advanced Python Wrapper: failover on Aurora PostgreSQL.

This example shows how to build an SQLAlchemy Engine that routes through the
AWS Advanced Python Wrapper's failover plugin, then perform a workload that
survives an Aurora failover event by catching sqlalchemy.exc.OperationalError
and retrying the unit of work.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.psycopg import connect


CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_engine(
        "postgresql+psycopg://",
        creator=lambda: connect(
            f"host={CLUSTER_ENDPOINT} dbname={DB_NAME} user={USER} password={PASSWORD}",
            wrapper_dialect="aurora-pg",
            plugins="failover,efm",
        ),
    )


def run_workload(engine, iterations: int = 20) -> None:
    for i in range(iterations):
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT pg_catalog.aurora_db_instance_identifier()")
                ).one()
                print(f"iter {i}: connected to instance {row[0]}")
        except OperationalError as exc:
            # FailoverSuccessError is reclassified as OperationalError by the
            # wrapper; SA wraps it here. The correct response is to retry the
            # unit of work against the new writer.
            print(f"iter {i}: operational error ({type(exc.orig).__name__}); retrying")


def main() -> None:
    engine = build_engine()
    try:
        run_workload(engine)
    finally:
        engine.dispose()
        release_resources()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add docs/examples/PGSQLAlchemyFailover.py
git commit -m "docs: add PGSQLAlchemyFailover example"
```

---

### Task 9: `docs/examples/MySQLSQLAlchemyFailover.py`

**Files:**
- Create: `docs/examples/MySQLSQLAlchemyFailover.py`

- [ ] **Step 1: Create the example**

Create `docs/examples/MySQLSQLAlchemyFailover.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""SQLAlchemy + AWS Advanced Python Wrapper: failover on Aurora MySQL."""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import connect


CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "mysql"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_engine(
        "mysql+mysqlconnector://",
        creator=lambda: connect(
            f"host={CLUSTER_ENDPOINT} database={DB_NAME} user={USER} password={PASSWORD}",
            wrapper_dialect="aurora-mysql",
            plugins="failover,efm",
            use_pure=True,
        ),
    )


def run_workload(engine, iterations: int = 20) -> None:
    for i in range(iterations):
        try:
            with engine.connect() as conn:
                row = conn.execute(text("SELECT @@aurora_server_id")).one()
                print(f"iter {i}: connected to instance {row[0]}")
        except OperationalError as exc:
            print(f"iter {i}: operational error ({type(exc.orig).__name__}); retrying")


def main() -> None:
    engine = build_engine()
    try:
        run_workload(engine)
    finally:
        engine.dispose()
        release_resources()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add docs/examples/MySQLSQLAlchemyFailover.py
git commit -m "docs: add MySQLSQLAlchemyFailover example"
```

---

### Task 10: `docs/examples/PGSQLAlchemyReadWriteSplitting.py`

**Files:**
- Create: `docs/examples/PGSQLAlchemyReadWriteSplitting.py`

- [ ] **Step 1: Create the example**

Create `docs/examples/PGSQLAlchemyReadWriteSplitting.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""SQLAlchemy + AWS Advanced Python Wrapper: read/write splitting on Aurora PostgreSQL.

Demonstrates routing read-only transactions to Aurora replicas via the
readWriteSplitting plugin, by flipping SQLAlchemy's read_only execution option.
"""

from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.psycopg import connect


CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_engine(
        "postgresql+psycopg://",
        creator=lambda: connect(
            f"host={CLUSTER_ENDPOINT} dbname={DB_NAME} user={USER} password={PASSWORD}",
            wrapper_dialect="aurora-pg",
            plugins="readWriteSplitting",
        ),
    )


def instance_id(conn) -> str:
    return conn.execute(
        text("SELECT pg_catalog.aurora_db_instance_identifier()")
    ).scalar_one()


def main() -> None:
    engine = build_engine()
    try:
        # Writer by default.
        with engine.connect() as conn:
            print(f"writer: {instance_id(conn)}")
            conn.commit()

        # Read-only transaction → router switches to a reader.
        with engine.connect().execution_options(postgresql_readonly=True) as conn:
            print(f"reader: {instance_id(conn)}")
            conn.commit()

        # Back to writer on the next non-read-only connection.
        with engine.connect() as conn:
            print(f"writer again: {instance_id(conn)}")
            conn.commit()
    finally:
        engine.dispose()
        release_resources()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add docs/examples/PGSQLAlchemyReadWriteSplitting.py
git commit -m "docs: add PGSQLAlchemyReadWriteSplitting example"
```

---

### Task 11: `docs/examples/MySQLSQLAlchemyReadWriteSplitting.py` + mypy.ini exclude update

**Files:**
- Create: `docs/examples/MySQLSQLAlchemyReadWriteSplitting.py`
- Modify: `mypy.ini`

- [ ] **Step 1: Create the example**

Create `docs/examples/MySQLSQLAlchemyReadWriteSplitting.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""SQLAlchemy + AWS Advanced Python Wrapper: read/write splitting on Aurora MySQL."""

from sqlalchemy import create_engine, text

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import connect


CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "mysql"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_engine(
        "mysql+mysqlconnector://",
        creator=lambda: connect(
            f"host={CLUSTER_ENDPOINT} database={DB_NAME} user={USER} password={PASSWORD}",
            wrapper_dialect="aurora-mysql",
            plugins="readWriteSplitting",
            use_pure=True,
        ),
    )


def instance_id(conn) -> str:
    return conn.execute(text("SELECT @@aurora_server_id")).scalar_one()


def main() -> None:
    engine = build_engine()
    try:
        with engine.connect() as conn:
            print(f"writer: {instance_id(conn)}")
            conn.commit()

        with engine.connect().execution_options(mysql_readonly=True) as conn:
            print(f"reader: {instance_id(conn)}")
            conn.commit()

        with engine.connect() as conn:
            print(f"writer again: {instance_id(conn)}")
            conn.commit()
    finally:
        engine.dispose()
        release_resources()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Update `mypy.ini` to exclude the four new example files**

Read the current `mypy.ini`. The existing `exclude` regex (around lines 2–14) lists filename patterns. Add four new lines matching the existing style. Final state:

```ini
[mypy]
exclude = (?x)(
    debug_integration_.*\.py$
    | MySQLFailover\.py
    | MySQLIamAuthentication\.py
    | MySQLInternalConnectionPoolPasswordWarning\.py
    | MySQLReadWriteSplitting\.py
    | MySQLSecretsManager\.py
    | PGFailover\.py
    | PGIamAuthentication\.py
    | PGInternalConnectionPoolPasswordWarning\.py
    | PGReadWriteSplitting\.py
    | PGSecretsManager\.py
    | PGSQLAlchemyFailover\.py
    | MySQLSQLAlchemyFailover\.py
    | PGSQLAlchemyReadWriteSplitting\.py
    | MySQLSQLAlchemyReadWriteSplitting\.py
    )

[mypy-mysql]
ignore_missing_imports = True

[mypy-parameterized]
ignore_missing_imports = True

[mypy-botocore.config]
ignore_missing_imports = True

[mypy-botocore.exceptions]
ignore_missing_imports = True

[mypy-boto3]
ignore_missing_imports = True

[mypy-ResourceBundle]
ignore_missing_imports = True

[mypy-requests]
ignore_missing_imports = True

[mypy-toml]
ignore_missing_imports = True
```

- [ ] **Step 3: Verify mypy still clean**

Run: `poetry run mypy .`
Expected: no errors.

- [ ] **Step 4: Verify flake8 clean on all four examples**

Run: `poetry run flake8 docs/examples/PGSQLAlchemyFailover.py docs/examples/MySQLSQLAlchemyFailover.py docs/examples/PGSQLAlchemyReadWriteSplitting.py docs/examples/MySQLSQLAlchemyReadWriteSplitting.py`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add docs/examples/MySQLSQLAlchemyReadWriteSplitting.py mypy.ini
git commit -m "docs: add MySQLSQLAlchemyReadWriteSplitting example; exclude from mypy"
```

---

### Task 12: Integration tests — `tests/integration/container/test_sqlalchemy.py`

**Files:**
- Create: `tests/integration/container/test_sqlalchemy.py`

This task touches real Aurora infrastructure. Skip locally; maintainers trigger `integration_tests.yml`. Read the existing conftest in `tests/integration/container/conftest.py` and `tests/integration/container/test_read_write_splitting.py` before writing to understand the fixtures (`test_driver`, `aurora_info`, etc.) and failure-simulation helpers the existing tests use.

- [ ] **Step 1: Create the integration test file**

Create `tests/integration/container/test_sqlalchemy.py`:

```python
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0

"""SQLAlchemy creator-pattern integration tests for Aurora PG and Aurora MySQL.

Proves:
- SA `create_engine(..., creator=lambda: aws_advanced_python_wrapper.<driver>.connect(...))`
  succeeds against real Aurora clusters for both drivers.
- Aurora failover surfaces as sqlalchemy.exc.OperationalError (via the
  OperationalError-classified FailoverSuccessError) and a retry-loop recovers.
- Read/Write Splitting's read_only flip routes to a reader and returns to a writer.

Fixtures follow the existing test_read_write_splitting.py / test_aurora_failover.py
patterns (see conftest.py in this directory).
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import connect as mysql_connect
from aws_advanced_python_wrapper.psycopg import connect as pg_connect


def _pg_engine(dsn, wrapper_dialect, plugins):
    return create_engine(
        "postgresql+psycopg://",
        creator=lambda: pg_connect(dsn, wrapper_dialect=wrapper_dialect, plugins=plugins),
    )


def _mysql_engine(dsn, wrapper_dialect, plugins):
    return create_engine(
        "mysql+mysqlconnector://",
        creator=lambda: mysql_connect(
            dsn, wrapper_dialect=wrapper_dialect, plugins=plugins, use_pure=True
        ),
    )


def _pg_instance_id(conn) -> str:
    return conn.execute(
        text("SELECT pg_catalog.aurora_db_instance_identifier()")
    ).scalar_one()


def _mysql_instance_id(conn) -> str:
    return conn.execute(text("SELECT @@aurora_server_id")).scalar_one()


# ---- Failover ---------------------------------------------------------------

@pytest.mark.parametrize("driver,build_engine,instance_id_fn,dialect_name", [
    ("pg", _pg_engine, _pg_instance_id, "aurora-pg"),
    ("mysql", _mysql_engine, _mysql_instance_id, "aurora-mysql"),
])
def test_sqlalchemy_creator_survives_aurora_failover(
    driver, build_engine, instance_id_fn, dialect_name,
    aurora_info, conn_utils, trigger_failover,
):
    """SA engine recovers from Aurora failover via the OperationalError retry path."""
    dsn = conn_utils.get_dsn_for_driver(driver, aurora_info.writer_endpoint)
    engine = build_engine(dsn, dialect_name, "failover,efm")
    try:
        with engine.connect() as conn:
            initial_writer = instance_id_fn(conn)

        trigger_failover(aurora_info)

        attempts_remaining = 10
        recovered = False
        while attempts_remaining > 0 and not recovered:
            try:
                with engine.connect() as conn:
                    new_writer = instance_id_fn(conn)
                    assert new_writer != initial_writer, (
                        "failover should have promoted a different instance"
                    )
                    recovered = True
            except OperationalError:
                attempts_remaining -= 1
        assert recovered, "SA did not recover from failover within 10 attempts"
    finally:
        engine.dispose()
        release_resources()


# ---- Read/Write Splitting ---------------------------------------------------

@pytest.mark.parametrize("driver,build_engine,instance_id_fn,dialect_name,ro_opt", [
    ("pg", _pg_engine, _pg_instance_id, "aurora-pg", {"postgresql_readonly": True}),
    ("mysql", _mysql_engine, _mysql_instance_id, "aurora-mysql", {"mysql_readonly": True}),
])
def test_sqlalchemy_creator_read_write_splitting(
    driver, build_engine, instance_id_fn, dialect_name, ro_opt,
    aurora_info, conn_utils,
):
    """R/W splitting routes read_only connections to readers."""
    dsn = conn_utils.get_dsn_for_driver(driver, aurora_info.writer_endpoint)
    engine = build_engine(dsn, dialect_name, "readWriteSplitting")
    try:
        with engine.connect() as conn:
            writer_id = instance_id_fn(conn)
            conn.commit()
        with engine.connect().execution_options(**ro_opt) as conn:
            reader_id = instance_id_fn(conn)
            conn.commit()
        assert reader_id != writer_id, (
            f"read-only connection should route to a reader, got {reader_id}"
        )
        assert reader_id in aurora_info.reader_instance_ids

        # Next connection without read_only flip should return to the writer.
        with engine.connect() as conn:
            back_to_writer = instance_id_fn(conn)
            conn.commit()
        assert back_to_writer == writer_id
    finally:
        engine.dispose()
        release_resources()
```

> **Note on fixtures:** the fixture names `aurora_info`, `conn_utils`, `trigger_failover` above are placeholders that MAY NOT match the actual fixture names in `tests/integration/container/conftest.py`. Before writing the file for real, read the existing conftest and `test_aurora_failover.py` / `test_read_write_splitting.py` and rename these parameters to match the actual fixtures (likely something like `test_environment`, `conn_utils`, and an in-test call via `AuroraTestUtility`). The test structure above is correct; only the fixture plumbing needs alignment with the existing harness.

- [ ] **Step 2: Syntax check + flake8 (no test run — requires Aurora)**

Run: `poetry run python -c "import ast; ast.parse(open('tests/integration/container/test_sqlalchemy.py').read())" && poetry run flake8 tests/integration/container/test_sqlalchemy.py`
Expected: both commands succeed silently.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/container/test_sqlalchemy.py
git commit -m "test: add SQLAlchemy creator-pattern integration tests"
```

- [ ] **Step 4: Flag for maintainer trigger**

Add a note to the PR description that `integration_tests.yml` should be triggered to run this suite against Aurora PG and Aurora MySQL before the PR is approved.

---

### Task 13: Final verification and CHANGELOG preparation

**Files:**
- None (checklist task)

- [ ] **Step 1: Run the entire unit suite on the default Python**

Run: `poetry run python -m pytest tests/unit -Werror`
Expected: all tests PASS.

- [ ] **Step 2: Run the entire unit suite on Python 3.14**

Run: `poetry env use 3.14 && poetry install && poetry run python -m pytest tests/unit -Werror`
Expected: all tests PASS.

- [ ] **Step 3: Run full lint + type checks on default env**

Run (after `poetry env use <your-default>`):

```bash
poetry run flake8 . && poetry run mypy . && poetry run isort --check-only .
```

Expected: clean.

- [ ] **Step 4: Draft the PR description with proposed CHANGELOG lines**

Include in the PR body:

```markdown
## Proposed CHANGELOG entries

### :magic_wand: Added
* SQLAlchemy creator-pattern support with dedicated `aws_advanced_python_wrapper.psycopg` and `aws_advanced_python_wrapper.mysql_connector` DBAPI submodules ([PR #XXXX]).
* Python 3.14 support ([PR #XXXX]).
* Examples: `PGSQLAlchemyFailover.py`, `MySQLSQLAlchemyFailover.py`, `PGSQLAlchemyReadWriteSplitting.py`, `MySQLSQLAlchemyReadWriteSplitting.py` ([PR #XXXX]).
* `docs/using-the-python-wrapper/SqlAlchemySupport.md` ([PR #XXXX]).

### :crab: Changed
* Wrapper errors (`AwsConnectError`, `FailoverError` and subclasses, `QueryTimeoutError`, `ReadWriteSplittingError`, `UnsupportedOperationError`) now inherit the appropriate PEP 249 subclass so SQLAlchemy maps them to the correct `sqlalchemy.exc.*` type ([PR #XXXX]).
```

- [ ] **Step 5: Flag the integration-tests workflow for maintainer trigger**

Include in the PR description: `@maintainer please trigger integration_tests.yml to exercise tests/integration/container/test_sqlalchemy.py against Aurora PG and Aurora MySQL.`

- [ ] **Step 6: Verify commit hygiene**

Run: `git log --oneline main..HEAD`
Expected: ~11 focused commits, each matching conventional-commit style (`feat:`, `docs:`, `test:`). No mixed-purpose commits.

---

## Sequencing note

Tasks 7–11 together form the documentation-and-examples body. Task 7's markdown-link-check will flag the example files that don't exist yet. Two ways to avoid that CI noise:

- **Preferred:** push all of Tasks 7–11 together in one push (individual commits still focused), so by the time CI runs on the PR branch all four example files are present.
- **Alternative:** in Task 7's `SqlAlchemySupport.md`, temporarily remove the four example-file links; add them back in a small commit after Task 11.

## Self-review (completed)

- **Spec coverage:** Each section of the spec maps to at least one task. File changes match the spec's File Structure. Error-reparenting rows → Task 3. 3.14 work → Task 6. Examples → Tasks 8–11. Integration tests → Task 12. ✓
- **Placeholder scan:** No TBD/TODO. One deliberate `[PR #XXXX]` in the CHANGELOG lines is intentional — maintainers fill in at release. ✓
- **Type consistency:** `connect(conninfo: str = "", **kwargs: Any) -> AwsWrapperConnection` used identically in `psycopg.py`, `mysql_connector.py`. `_PEP249_NAMES` tuple used identically in `_dbapi.py` and the contract test. ✓
- **Fixture-name mismatch risk flagged** in Task 12 with an explicit "read conftest first" instruction and placeholder names called out. ✓
