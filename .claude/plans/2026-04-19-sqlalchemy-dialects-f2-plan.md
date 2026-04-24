# SQLAlchemy custom dialects (F2) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Register custom SQLAlchemy dialects `aws-wrapper-postgresql[+psycopg]` and `aws-wrapper-mysql[+mysqlconnector]` so `create_engine` can be driven by URL alone.

**Architecture:** Two thin dialect subclasses of SA's standard `PGDialect_psycopg` / `MySQLDialect_mysqlconnector`, each overriding `import_dbapi()` to return F1's per-driver wrapper submodule. Entry-points registered in `pyproject.toml`.

**Tech Stack:** Python 3.10–3.14, poetry, pytest, SQLAlchemy 2.x, psycopg v3, mysql-connector-python.

**Spec:** `.claude/specs/2026-04-19-sqlalchemy-dialects-f2-design.md`

---

## File Structure

**New:**
- `aws_advanced_python_wrapper/sqlalchemy_dialects/__init__.py` — empty package marker.
- `aws_advanced_python_wrapper/sqlalchemy_dialects/pg.py` — `AwsWrapperPGPsycopgDialect`.
- `aws_advanced_python_wrapper/sqlalchemy_dialects/mysql.py` — `AwsWrapperMySQLConnectorDialect`.
- `tests/unit/test_sqlalchemy_dialects.py` — unit tests for dialect registration, subclass shape, `import_dbapi`, URL parsing, kwargs passthrough.

**Modified:**
- `pyproject.toml` — add `[tool.poetry.plugins."sqlalchemy.dialects"]` section with four entries.
- `docs/using-the-python-wrapper/SqlAlchemySupport.md` — add URL-based usage section, remove the "no URL dialect yet" limitation bullet.

---

### Task 1: `sqlalchemy_dialects` package + PG dialect

**Files:**
- Create: `aws_advanced_python_wrapper/sqlalchemy_dialects/__init__.py`
- Create: `aws_advanced_python_wrapper/sqlalchemy_dialects/pg.py`
- Test: `tests/unit/test_sqlalchemy_dialects.py` (PG-only cases in this task; MySQL cases land in Task 2)

- [ ] **Step 1: Write the failing PG dialect test file**

Create `tests/unit/test_sqlalchemy_dialects.py`:

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

import aws_advanced_python_wrapper.psycopg as wrapper_psycopg
from aws_advanced_python_wrapper.sqlalchemy_dialects.pg import \
    AwsWrapperPGPsycopgDialect
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg


def test_pg_dialect_subclasses_pgdialect_psycopg():
    assert issubclass(AwsWrapperPGPsycopgDialect, PGDialect_psycopg)


def test_pg_dialect_import_dbapi_returns_wrapper_submodule():
    assert AwsWrapperPGPsycopgDialect.import_dbapi() is wrapper_psycopg


def test_pg_dialect_driver_attr():
    assert AwsWrapperPGPsycopgDialect.driver == "psycopg"
```

- [ ] **Step 2: Run and confirm failure**

Run: `poetry run python -m pytest tests/unit/test_sqlalchemy_dialects.py -v`
Expected: `ModuleNotFoundError: No module named 'aws_advanced_python_wrapper.sqlalchemy_dialects'`.

- [ ] **Step 3: Create the package marker**

Create `aws_advanced_python_wrapper/sqlalchemy_dialects/__init__.py`:

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

"""Custom SQLAlchemy dialects that swap the DBAPI module to the
AWS Advanced Python Wrapper.

Users should prefer the SA dialect registry
(``create_engine("aws-wrapper-postgresql+psycopg://...")``) over importing
these classes directly.
"""
```

- [ ] **Step 4: Create the PG dialect**

Create `aws_advanced_python_wrapper/sqlalchemy_dialects/pg.py`:

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

"""PostgreSQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``aws-wrapper-postgresql`` / ``aws-wrapper-postgresql+psycopg``
via a pyproject entry-point. Subclasses SA's standard PGDialect_psycopg and
only swaps the DBAPI module to :mod:`aws_advanced_python_wrapper.psycopg`,
which routes connect() through the wrapper's plugin pipeline.
"""

from __future__ import annotations

from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg


class AwsWrapperPGPsycopgDialect(PGDialect_psycopg):
    """SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "psycopg"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.psycopg as dbapi
        return dbapi
```

- [ ] **Step 5: Run the tests and confirm they pass**

Run: `poetry run python -m pytest tests/unit/test_sqlalchemy_dialects.py -v`
Expected: all 3 tests PASS.

- [ ] **Step 6: Lint + type + isort**

Run: `poetry run flake8 aws_advanced_python_wrapper/sqlalchemy_dialects/ tests/unit/test_sqlalchemy_dialects.py && poetry run mypy aws_advanced_python_wrapper/sqlalchemy_dialects/ && poetry run isort --check-only aws_advanced_python_wrapper/sqlalchemy_dialects/ tests/unit/test_sqlalchemy_dialects.py`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add aws_advanced_python_wrapper/sqlalchemy_dialects/__init__.py aws_advanced_python_wrapper/sqlalchemy_dialects/pg.py tests/unit/test_sqlalchemy_dialects.py
git commit -m "feat: add AwsWrapperPGPsycopgDialect for SQLAlchemy URL-based config"
```

---

### Task 2: MySQL dialect

**Files:**
- Create: `aws_advanced_python_wrapper/sqlalchemy_dialects/mysql.py`
- Modify: `tests/unit/test_sqlalchemy_dialects.py` (append MySQL cases)

- [ ] **Step 1: Append the failing MySQL tests**

Append to `tests/unit/test_sqlalchemy_dialects.py`:

```python
import aws_advanced_python_wrapper.mysql_connector as wrapper_mysql
from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql import \
    AwsWrapperMySQLConnectorDialect
from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector


def test_mysql_dialect_subclasses_mysqldialect_mysqlconnector():
    assert issubclass(AwsWrapperMySQLConnectorDialect, MySQLDialect_mysqlconnector)


def test_mysql_dialect_import_dbapi_returns_wrapper_submodule():
    assert AwsWrapperMySQLConnectorDialect.import_dbapi() is wrapper_mysql


def test_mysql_dialect_driver_attr():
    assert AwsWrapperMySQLConnectorDialect.driver == "mysqlconnector"
```

Move all `import` lines to the top of the file so there are no late imports (flake8 E402).

- [ ] **Step 2: Run and confirm failure**

Run: `poetry run python -m pytest tests/unit/test_sqlalchemy_dialects.py -v -k mysql`
Expected: `ModuleNotFoundError: No module named 'aws_advanced_python_wrapper.sqlalchemy_dialects.mysql'`.

- [ ] **Step 3: Create the MySQL dialect**

Create `aws_advanced_python_wrapper/sqlalchemy_dialects/mysql.py`:

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

"""MySQL SQLAlchemy dialect bound to the AWS Advanced Python Wrapper.

Registered as ``aws-wrapper-mysql`` / ``aws-wrapper-mysql+mysqlconnector``
via a pyproject entry-point. Subclasses SA's standard
MySQLDialect_mysqlconnector and only swaps the DBAPI module to
:mod:`aws_advanced_python_wrapper.mysql_connector`, which routes connect()
through the wrapper's plugin pipeline.
"""

from __future__ import annotations

from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector


class AwsWrapperMySQLConnectorDialect(MySQLDialect_mysqlconnector):
    """SQLAlchemy dialect that uses the AWS Advanced Python Wrapper as its DBAPI."""

    driver = "mysqlconnector"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls):
        import aws_advanced_python_wrapper.mysql_connector as dbapi
        return dbapi
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `poetry run python -m pytest tests/unit/test_sqlalchemy_dialects.py -v`
Expected: 6 tests PASS (3 PG + 3 MySQL).

- [ ] **Step 5: Lint + type + isort**

Run: `poetry run flake8 aws_advanced_python_wrapper/sqlalchemy_dialects/mysql.py tests/unit/test_sqlalchemy_dialects.py && poetry run mypy aws_advanced_python_wrapper/sqlalchemy_dialects/mysql.py && poetry run isort --check-only aws_advanced_python_wrapper/sqlalchemy_dialects/mysql.py tests/unit/test_sqlalchemy_dialects.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add aws_advanced_python_wrapper/sqlalchemy_dialects/mysql.py tests/unit/test_sqlalchemy_dialects.py
git commit -m "feat: add AwsWrapperMySQLConnectorDialect for SQLAlchemy URL-based config"
```

---

### Task 3: Entry-point registration + registry tests

**Files:**
- Modify: `pyproject.toml`
- Modify: `tests/unit/test_sqlalchemy_dialects.py` (append registry tests)

- [ ] **Step 1: Append failing registry tests**

Append to `tests/unit/test_sqlalchemy_dialects.py`:

```python
from sqlalchemy.dialects import registry
from sqlalchemy.engine.url import make_url


def test_registry_resolves_aws_wrapper_postgresql():
    cls = registry.load("aws-wrapper-postgresql")
    assert cls is AwsWrapperPGPsycopgDialect


def test_registry_resolves_aws_wrapper_postgresql_psycopg():
    cls = registry.load("aws-wrapper-postgresql.psycopg")
    assert cls is AwsWrapperPGPsycopgDialect


def test_registry_resolves_aws_wrapper_mysql():
    cls = registry.load("aws-wrapper-mysql")
    assert cls is AwsWrapperMySQLConnectorDialect


def test_registry_resolves_aws_wrapper_mysql_mysqlconnector():
    cls = registry.load("aws-wrapper-mysql.mysqlconnector")
    assert cls is AwsWrapperMySQLConnectorDialect


def test_url_get_dialect_pg():
    url = make_url(
        "aws-wrapper-postgresql+psycopg://u:p@h:5432/db?wrapper_dialect=aurora-pg"
    )
    assert url.get_dialect() is AwsWrapperPGPsycopgDialect


def test_url_get_dialect_mysql():
    url = make_url(
        "aws-wrapper-mysql+mysqlconnector://u:p@h:3306/db?wrapper_dialect=aurora-mysql"
    )
    assert url.get_dialect() is AwsWrapperMySQLConnectorDialect
```

- [ ] **Step 2: Run and confirm failure**

Run: `poetry run python -m pytest tests/unit/test_sqlalchemy_dialects.py -v -k registry or url`
Expected: `NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:aws-wrapper-postgresql` (or similar). The dialects aren't registered yet.

- [ ] **Step 3: Add the entry-point section to `pyproject.toml`**

Insert the following block at the end of `pyproject.toml` (before the final newline if the file ends with one):

```toml
[tool.poetry.plugins."sqlalchemy.dialects"]
"aws-wrapper-postgresql" = "aws_advanced_python_wrapper.sqlalchemy_dialects.pg:AwsWrapperPGPsycopgDialect"
"aws-wrapper-postgresql.psycopg" = "aws_advanced_python_wrapper.sqlalchemy_dialects.pg:AwsWrapperPGPsycopgDialect"
"aws-wrapper-mysql" = "aws_advanced_python_wrapper.sqlalchemy_dialects.mysql:AwsWrapperMySQLConnectorDialect"
"aws-wrapper-mysql.mysqlconnector" = "aws_advanced_python_wrapper.sqlalchemy_dialects.mysql:AwsWrapperMySQLConnectorDialect"
```

- [ ] **Step 4: Re-install the package so the new entry-points register**

Run: `poetry install --no-interaction`
Expected: successful install. `pyproject.toml` changes take effect after this.

- [ ] **Step 5: Run the tests and confirm they pass**

Run: `poetry run python -m pytest tests/unit/test_sqlalchemy_dialects.py -v`
Expected: all 12 tests PASS (3 PG + 3 MySQL + 4 registry + 2 URL).

- [ ] **Step 6: Full unit suite regression check**

Run: `poetry run python -m pytest tests/unit -Werror -q`
Expected: no regressions.

- [ ] **Step 7: Lint + type + isort**

Run: `poetry run flake8 . && poetry run mypy . && poetry run isort --check-only . && echo OK`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml tests/unit/test_sqlalchemy_dialects.py
git commit -m "feat: register aws-wrapper SQLAlchemy dialects via pyproject entry-points"
```

---

### Task 4: URL kwargs passthrough verification

**Files:**
- Modify: `tests/unit/test_sqlalchemy_dialects.py` (append kwargs-passthrough test)

- [ ] **Step 1: Append the passthrough test**

Append to `tests/unit/test_sqlalchemy_dialects.py`:

```python
from unittest.mock import patch

from sqlalchemy import create_engine


def test_url_query_args_flow_through_to_dbapi_connect(mocker):
    mock_connect = mocker.patch(
        "aws_advanced_python_wrapper.psycopg.AwsWrapperConnection.connect",
        return_value=mocker.MagicMock(),
    )
    engine = create_engine(
        "aws-wrapper-postgresql+psycopg://u:p@h:5432/db"
        "?wrapper_dialect=aurora-pg&plugins=failover,efm"
    )
    try:
        with engine.connect():
            pass
    except Exception:
        # We only care that AwsWrapperConnection.connect was invoked with the
        # right kwargs. The mock returns a MagicMock connection whose behavior
        # may not satisfy SA's cursor/isolation probes — swallow and check.
        pass

    assert mock_connect.called, "AwsWrapperConnection.connect was never invoked"
    _args, kwargs = mock_connect.call_args
    assert kwargs.get("wrapper_dialect") == "aurora-pg"
    assert kwargs.get("plugins") == "failover,efm"
```

- [ ] **Step 2: Run and confirm the test passes (no new source code needed)**

Run: `poetry run python -m pytest tests/unit/test_sqlalchemy_dialects.py::test_url_query_args_flow_through_to_dbapi_connect -v`
Expected: PASS. If it FAILS because SA didn't pass the query args through, the dialect needs a custom `create_connect_args` override — but the spec's premise is that SA's default behavior handles this. Investigate the failure before adding code.

- [ ] **Step 3: Full suite**

Run: `poetry run python -m pytest tests/unit -Werror -q`
Expected: all tests PASS.

- [ ] **Step 4: Lint clean**

Run: `poetry run flake8 tests/unit/test_sqlalchemy_dialects.py && poetry run isort --check-only tests/unit/test_sqlalchemy_dialects.py`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_sqlalchemy_dialects.py
git commit -m "test: verify URL query args flow through SA dialect to wrapper connect"
```

---

### Task 5: Update `SqlAlchemySupport.md` for URL-based usage

**Files:**
- Modify: `docs/using-the-python-wrapper/SqlAlchemySupport.md`

- [ ] **Step 1: Add the URL-based usage section**

Insert the following block in `docs/using-the-python-wrapper/SqlAlchemySupport.md`, immediately after the existing "Using the wrapper with SQLAlchemy (MySQL)" code block and before the "Error handling" section:

```markdown
## Using the custom SQLAlchemy dialects (URL-based)

Starting in 2.3.0, the wrapper registers two SA dialects so `create_engine` can be driven by URL alone — no `creator=` lambda needed. This is the idiomatic path for Alembic, 12-factor `DATABASE_URL` configs, and framework starters that expect a URL string.

PostgreSQL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "aws-wrapper-postgresql+psycopg://john:pwd@"
    "database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/db"
    "?wrapper_dialect=aurora-pg&plugins=failover,efm"
)
```

MySQL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "aws-wrapper-mysql+mysqlconnector://john:pwd@"
    "database.cluster-xyz.us-east-1.rds.amazonaws.com:3306/db"
    "?wrapper_dialect=aurora-mysql&plugins=failover,efm&use_pure=True"
)
```

The dialect names (`aws-wrapper-postgresql`, `aws-wrapper-mysql`) echo the JDBC wrapper's `jdbc:aws-wrapper:postgresql://` / `jdbc:aws-wrapper:mysql://` URL scheme. The driver slot (`+psycopg`, `+mysqlconnector`) defaults to the wrapper's supported DBAPIs; bare `aws-wrapper-postgresql://` resolves to `+psycopg`.

Every wrapper connection option (`wrapper_dialect`, `plugins`, and any plugin-specific parameter) passes through the URL query string as a kwarg to the underlying wrapper's `connect()`. You do NOT need to register a `creator=` callable.

Both the creator-pattern (shown above) and the URL-based path remain supported. Use whichever fits your configuration surface.
```

- [ ] **Step 2: Remove the stale limitation bullet**

In the "Limitations (current)" section, delete the entire `- **No custom SA URL-scheme dialect yet.**` bullet (it's now shipped). Leave the `Sync only` bullet in place.

- [ ] **Step 3: Verify links in the updated doc**

Run: `poetry run python -c "
import re
with open('docs/using-the-python-wrapper/SqlAlchemySupport.md') as f:
    body = f.read()
# Check all relative markdown links point to existing files
import os
for match in re.finditer(r'\]\((\.\./[^)]+)\)', body):
    path = os.path.normpath(os.path.join('docs/using-the-python-wrapper', match.group(1).split('#')[0]))
    assert os.path.exists(path), f'broken link: {match.group(1)} -> {path}'
print('all relative links resolve')
"`
Expected: `all relative links resolve`.

- [ ] **Step 4: Commit**

```bash
git add docs/using-the-python-wrapper/SqlAlchemySupport.md
git commit -m "docs: document URL-based SQLAlchemy dialect usage (aws-wrapper-postgresql/-mysql)"
```

---

### Task 6: Final verification

**Files:** none (checklist task)

- [ ] **Step 1: Unit suite on py3.13**

Run:
```bash
poetry env use 3.13
poetry run python -m pytest tests/unit -Werror -q 2>&1 | grep -E "passed|failed" | tail -1
```
Expected: all pass, including the 13 new dialect tests.

- [ ] **Step 2: Unit suite on py3.14**

Run:
```bash
poetry env use 3.14
poetry install --no-interaction
poetry run python -m pytest tests/unit -Werror -q 2>&1 | grep -E "passed|failed" | tail -1
```
Expected: all pass.

- [ ] **Step 3: Full lint/type/isort**

Run:
```bash
poetry env use 3.13
poetry run flake8 . && poetry run mypy . && poetry run isort --check-only . && echo OK
```
Expected: clean.

- [ ] **Step 4: Verify commit hygiene**

Run: `git log --oneline main..HEAD | head -20`
Expected: ~5 focused F2 commits on top of the 12 F1 commits, each conventional-commit style.

- [ ] **Step 5: Proposed CHANGELOG lines for PR body**

Include in PR description:

```markdown
### :magic_wand: Added
* Custom SQLAlchemy dialects `aws-wrapper-postgresql[+psycopg]` and `aws-wrapper-mysql[+mysqlconnector]` for URL-based `create_engine` configuration ([PR #XXXX]).

### :crab: Changed
* `docs/using-the-python-wrapper/SqlAlchemySupport.md` — added URL-based dialect usage section; removed the corresponding "Limitations" bullet ([PR #XXXX]).
```

## Self-review (completed inline)

- **Spec coverage:** Every spec section maps to a task. PG dialect → Task 1, MySQL dialect → Task 2, entry-points → Task 3, passthrough verification → Task 4, docs → Task 5, final gates → Task 6. ✓
- **Placeholder scan:** No TBD/TODO. `[PR #XXXX]` in the CHANGELOG block is the deliberate placeholder the maintainer fills at release. ✓
- **Type consistency:** `AwsWrapperPGPsycopgDialect` / `AwsWrapperMySQLConnectorDialect` spelled identically in file and tests. Entry-point paths in Task 3 match the module paths from Tasks 1–2. ✓
- **Scope:** F2 only. No async, no new drivers, no new integration tests. ✓
