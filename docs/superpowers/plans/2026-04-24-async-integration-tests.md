# Async Integration Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add async counterparts to every existing sync integration test so the `feat/async-parity` branch's async wrapper + async SQLAlchemy dialects are exercised end-to-end against real Aurora clusters.

**Architecture:** Additive extension of `tests/integration/`. Sync surface is the invariant — nothing about sync collection or behavior changes. Two new `TestDriver` enum values (`PG_ASYNC` / `MYSQL_ASYNC`) + two new feature flags (`SKIP_SYNC_DRIVER_TESTS` / `SKIP_ASYNC_DRIVER_TESTS`) gate async tests at `conftest.py`'s parametrization layer. Four new Gradle tasks (`test-{pg,mysql}-{aurora,multi-az}-async`) invoke async runs independently per engine × deployment. Sibling `test_*_async.py` files exist for every existing sync `test_*.py`; async tests use `asyncio.run(inner_async())` inside sync `def test_...` functions, matching `tests/unit/test_aio_*.py`'s existing convention (no `pytest-asyncio` dependency).

**Tech Stack:** Python 3.10+ (`asyncio`, `aiomysql >= 0.2`, `psycopg[binary] ^3.3.3` with native async, `SQLAlchemy ^2.0.49` async), Java/JUnit + Gradle Kotlin DSL orchestration, Docker test container, pytest 7.x.

**Spec reference:** `docs/superpowers/specs/2026-04-24-async-integration-tests-design.md`

---

## File Structure

### Python — `tests/integration/container/`

| File | Action | Responsibility |
|---|---|---|
| `utils/test_driver.py` | Modify | Add `PG_ASYNC` / `MYSQL_ASYNC` enum values + `is_async` / `engine` properties |
| `utils/test_environment_features.py` | Modify | Add `SKIP_SYNC_DRIVER_TESTS` / `SKIP_ASYNC_DRIVER_TESTS` |
| `utils/test_environment.py` | Modify | Extend `is_test_driver_allowed` for async variants + two new flags |
| `utils/driver_helper.py` | Modify | Add async branches to `get_connect_func` and `get_connect_params` |
| `utils/async_connection_helpers.py` | **Create** | `pg_connect_async`, `mysql_connect_async`, `create_async_engine_for_driver`, async fixture teardown |
| `test_basic_connectivity_async.py` | **Create** | Async twin of `test_basic_connectivity.py` |
| `test_basic_functionality_async.py` | **Create** | Async twin of `test_basic_functionality.py` |
| `test_sqlalchemy_async.py` | **Create** | Async twin of `test_sqlalchemy.py` |
| `test_aurora_failover_async.py` | **Create** | Async twin of `test_aurora_failover.py` |
| `test_read_write_splitting_async.py` | **Create** | Async twin of `test_read_write_splitting.py` |
| `test_iam_authentication_async.py` | **Create** | Async twin of `test_iam_authentication.py` |
| `test_aws_secrets_manager_async.py` | **Create** | Async twin of `test_aws_secrets_manager.py` |
| `test_host_monitoring_v2_async.py` | **Create** | Async twin of `test_host_monitoring_v2.py` |
| `test_custom_endpoint_async.py` | **Create** | Async twin of `test_custom_endpoint.py` |
| `test_autoscaling_async.py` | **Create** | Async twin of `test_autoscaling.py` |
| `test_blue_green_deployment_async.py` | **Create** | Async twin of `test_blue_green_deployment.py` |
| `test_failover_performance_async.py` | **Create** | Async twin of `test_failover_performance.py` |
| `test_read_write_splitting_performance_async.py` | **Create** | Async twin of `test_read_write_splitting_performance.py` |

### Java — `tests/integration/host/src/test/java/integration/`

| File | Action | Responsibility |
|---|---|---|
| `TestEnvironmentFeatures.java` | Modify | Add `SKIP_SYNC_DRIVER_TESTS` / `SKIP_ASYNC_DRIVER_TESTS` |
| `host/TestEnvironmentConfiguration.java` | Modify | Read `exclude-sync-drivers` / `exclude-async-drivers` system properties |
| `host/TestEnvironmentProvider.java` | Modify | Translate new config flags into features (lines 195–196 area) |

### Gradle — `tests/integration/host/build.gradle.kts`

| Action | Responsibility |
|---|---|
| Modify existing tasks | `test-{pg,mysql}-{aurora,multi-az}` each gain `systemProperty("exclude-async-drivers", "true")` |
| Add new tasks | `test-{pg,mysql}-{aurora,multi-az}-async` — 4 new task registrations |

### Docs

| File | Action |
|---|---|
| `docs/development-guide/IntegrationTests.md` | Modify — document new Gradle tasks + Serverless v2 caveats + SG-allowlist reminder |

---

## Task 0: Step-0 sync baseline (diagnostic, not a commit)

**Goal:** Confirm the async-parity work already on `feat/async-parity` didn't regress sync integration tests before adding any async code.

- [ ] **Step 1: Pre-flight check — confirm SG allowlist**

`REUSE_RDS_DB=true` skips IP-allowlist automation. Verify the test runner's outbound IP is already in both Serverless clusters' security groups. If you're running locally, find your public IP:

```bash
curl -s https://checkip.amazonaws.com
```

Confirm that IP is in both the MySQL and PG Serverless cluster SGs (AWS console → RDS → cluster → VPC security groups → inbound rules → port 3306/5432 from your IP). If not, add it via console before proceeding.

- [ ] **Step 2: Warm both clusters**

Serverless v2 can take 30–60s to scale from 0 ACUs. Warm each with a trivial query before running tests, otherwise the first test may consume the warmup budget:

```bash
psql -h <pg-cluster>.xxx.us-west-2.rds.amazonaws.com -U <user> -d postgres -c 'SELECT 1;'
mysql -h <mysql-cluster>.xxx.us-west-2.rds.amazonaws.com -u <user> -p -e 'SELECT 1;'
```

- [ ] **Step 3: Export required env vars**

```bash
export REUSE_RDS_DB=true
export RDS_DB_REGION=us-west-2                     # or wherever your clusters live
export DB_USERNAME=<user>
export DB_PASSWORD=<password>
export AWS_ACCESS_KEY_ID=<key>
export AWS_SECRET_ACCESS_KEY=<secret>
```

- [ ] **Step 4: Run sync PG integration suite**

```bash
export RDS_DB_NAME=<pg-cluster-id>
export RDS_DB_DOMAIN=<pg-cluster>.xxx.us-west-2.rds.amazonaws.com
./gradlew :integration-testing:test-pg-aurora
```

Expected: suite runs to completion. Failures fall into three triage buckets:
1. **Environmental skip** (multi-instance tests on single-writer topology, IAM tests when IAM not enabled on cluster, BG/Global-DB tests needing specific topology) → note in PR description; not a regression.
2. **Sync regression introduced by async-parity commits on this branch** → STOP. Fix in a separate `fix:` commit before any async-integration commit. File the fix as commit 0a before Task 1's commits.
3. **Transient / infra** (paused cluster, SG mismatch, IAM not configured) → resolve and re-run.

- [ ] **Step 5: Run sync MySQL integration suite**

```bash
export RDS_DB_NAME=<mysql-cluster-id>
export RDS_DB_DOMAIN=<mysql-cluster>.xxx.us-west-2.rds.amazonaws.com
./gradlew :integration-testing:test-mysql-aurora
```

Apply the same triage taxonomy. Only proceed to Task 1 once baseline is green or failures are classified.

- [ ] **Step 6: No commit** — this task is diagnostic only.

---

## Task 1: Verify and adjust async test dependencies

**Files:**
- Modify: `pyproject.toml` (possibly)
- Modify: `poetry.lock` (regenerated if pyproject.toml changed)

**Context:** `aiomysql = ">=0.2"` is in `[tool.poetry.group.dev.dependencies]` (line 65), not in the `test` group. Need to verify the integration-test Docker container installs the dev group.

- [ ] **Step 1: Check how the integration test container installs Python deps**

```bash
grep -rn 'poetry install' tests/integration/ Dockerfile* 2>/dev/null | head
```

Inspect each match. If it uses `poetry install` (no `--only` / `--without` filter), dev deps are included and **no change is needed**. If it uses `poetry install --only test` or similar narrow filter, move `aiomysql` from the dev group to the test group.

- [ ] **Step 2: If no change needed, skip to Task 2**

Mark this task as "no-op, documented in PR description." No commit.

- [ ] **Step 3: If aiomysql needs to move, apply the move**

Remove `aiomysql = ">=0.2"` from `[tool.poetry.group.dev.dependencies]` and add it to `[tool.poetry.group.test.dependencies]` at the appropriate location (alphabetical by dep name in the existing list).

- [ ] **Step 4: Regenerate `poetry.lock`**

```bash
poetry lock --no-update
```

- [ ] **Step 5: Verify the install path works**

```bash
poetry install --only test
poetry run python -c 'import aiomysql; import psycopg; print("ok")'
```

Expected: prints `ok`. If it fails with ImportError, the group move was incorrect.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml poetry.lock
git commit -m "chore(integration): move aiomysql to test group for integration runs

Integration test container installs via 'poetry install --only test'; aiomysql
is required for async MySQL integration tests and must be reachable from that
install path."
```

---

## Task 2: Extend `TestDriver` enum with async variants

**Files:**
- Modify: `tests/integration/container/utils/test_driver.py`

- [ ] **Step 1: Replace the enum definition**

Replace the contents of `tests/integration/container/utils/test_driver.py` (preserving the Apache license header) with:

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

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database_engine import DatabaseEngine


class TestDriver(str, Enum):
    __test__ = False

    PG = "PG"                # psycopg 3 (sync)
    MYSQL = "MYSQL"          # mysql-connector-python (sync)
    PG_ASYNC = "PG_ASYNC"    # psycopg 3 async (psycopg.AsyncConnection)
    MYSQL_ASYNC = "MYSQL_ASYNC"  # aiomysql

    @property
    def is_async(self) -> bool:
        return self in (TestDriver.PG_ASYNC, TestDriver.MYSQL_ASYNC)

    @property
    def engine(self) -> "DatabaseEngine":
        from .database_engine import DatabaseEngine
        if self in (TestDriver.PG, TestDriver.PG_ASYNC):
            return DatabaseEngine.PG
        return DatabaseEngine.MYSQL
```

The `DatabaseEngine` import is local-inside-method to avoid a top-level circular import (conditions.py and test_environment.py both import TestDriver *and* DatabaseEngine).

- [ ] **Step 2: Run type checking**

```bash
poetry run mypy tests/integration/container/utils/test_driver.py
```

Expected: `Success: no issues found`.

- [ ] **Step 3: Run flake8 and isort**

```bash
poetry run flake8 tests/integration/container/utils/test_driver.py
poetry run isort --check-only tests/integration/container/utils/test_driver.py
```

Expected: both succeed silently.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/container/utils/test_driver.py
git commit -m "feat(integration): add async variants to TestDriver enum

Adds PG_ASYNC and MYSQL_ASYNC enum values plus is_async/engine properties
so downstream code can filter and dispatch without hardcoding sync driver
identities. Engine property lets conditions.py keep working with async
drivers via test_driver.engine.

No behavioral change yet — conftest.py still filters via
is_test_driver_allowed, which rejects the new values until Task 3
extends it."
```

---

## Task 3: Extend Python `TestEnvironmentFeatures` + `is_test_driver_allowed`

**Files:**
- Modify: `tests/integration/container/utils/test_environment_features.py`
- Modify: `tests/integration/container/utils/test_environment.py:234-251`

- [ ] **Step 1: Add the two new feature flags**

Edit `tests/integration/container/utils/test_environment_features.py`. Add two lines after `SKIP_PG_DRIVER_TESTS` so the class becomes:

```python
class TestEnvironmentFeatures(Enum):
    __test__ = False

    IAM = "IAM"
    SECRETS_MANAGER = "SECRETS_MANAGER"
    FAILOVER_SUPPORTED = "FAILOVER_SUPPORTED"
    ABORT_CONNECTION_SUPPORTED = "ABORT_CONNECTION_SUPPORTED"
    NETWORK_OUTAGES_ENABLED = "NETWORK_OUTAGES_ENABLED"
    AWS_CREDENTIALS_ENABLED = "AWS_CREDENTIALS_ENABLED"
    PERFORMANCE = "PERFORMANCE"
    RUN_AUTOSCALING_TESTS_ONLY = "RUN_AUTOSCALING_TESTS_ONLY"
    BLUE_GREEN_DEPLOYMENT = "BLUE_GREEN_DEPLOYMENT"
    SKIP_MYSQL_DRIVER_TESTS = "SKIP_MYSQL_DRIVER_TESTS"
    SKIP_PG_DRIVER_TESTS = "SKIP_PG_DRIVER_TESTS"
    SKIP_SYNC_DRIVER_TESTS = "SKIP_SYNC_DRIVER_TESTS"
    SKIP_ASYNC_DRIVER_TESTS = "SKIP_ASYNC_DRIVER_TESTS"
    TELEMETRY_TRACES_ENABLED = "TELEMETRY_TRACES_ENABLED"
    TELEMETRY_METRICS_ENABLED = "TELEMETRY_METRICS_ENABLED"
```

- [ ] **Step 2: Extend `is_test_driver_allowed` in `test_environment.py`**

Replace the body of `is_test_driver_allowed` at `tests/integration/container/utils/test_environment.py:234-251` with:

```python
    def is_test_driver_allowed(self, test_driver: TestDriver) -> bool:
        features = self.get_features()
        database_engine = self.get_engine()

        if test_driver == TestDriver.MYSQL:
            driver_compatible_to_database_engine = database_engine == DatabaseEngine.MYSQL
            disabled_by_feature = (
                TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS in features
                or TestEnvironmentFeatures.SKIP_SYNC_DRIVER_TESTS in features
            )
        elif test_driver == TestDriver.PG:
            driver_compatible_to_database_engine = database_engine == DatabaseEngine.PG
            disabled_by_feature = (
                TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS in features
                or TestEnvironmentFeatures.SKIP_SYNC_DRIVER_TESTS in features
            )
        elif test_driver == TestDriver.MYSQL_ASYNC:
            driver_compatible_to_database_engine = database_engine == DatabaseEngine.MYSQL
            disabled_by_feature = (
                TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS in features
                or TestEnvironmentFeatures.SKIP_ASYNC_DRIVER_TESTS in features
            )
        elif test_driver == TestDriver.PG_ASYNC:
            driver_compatible_to_database_engine = database_engine == DatabaseEngine.PG
            disabled_by_feature = (
                TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS in features
                or TestEnvironmentFeatures.SKIP_ASYNC_DRIVER_TESTS in features
            )
        else:
            raise UnsupportedOperationError(test_driver.value)

        if disabled_by_feature or (not driver_compatible_to_database_engine):
            return False

        return True
```

Note: engine-level `SKIP_*_DRIVER_TESTS` flags gate both sync and async variants of that engine — this mirrors the current semantics where `SKIP_MYSQL_DRIVER_TESTS` means "no MySQL testing at all." Only `SKIP_SYNC_DRIVER_TESTS` / `SKIP_ASYNC_DRIVER_TESTS` discriminate the sync/async axis.

- [ ] **Step 3: Run type checking and linting**

```bash
poetry run mypy tests/integration/container/utils/test_environment.py tests/integration/container/utils/test_environment_features.py
poetry run flake8 tests/integration/container/utils/test_environment.py tests/integration/container/utils/test_environment_features.py
poetry run isort --check-only tests/integration/container/utils/test_environment.py tests/integration/container/utils/test_environment_features.py
```

Expected: all succeed. (These files are in the mypy exclude list if any; check `mypy.ini` — if excluded, mypy won't flag anything.)

- [ ] **Step 4: Write a quick unit-style sanity test that the new gating works**

Add a temporary test (we'll discard after verifying):

```bash
poetry run python - <<'PY'
from tests.integration.container.utils.test_driver import TestDriver
from tests.integration.container.utils.database_engine import DatabaseEngine

# Smoke: properties
assert TestDriver.PG_ASYNC.is_async is True
assert TestDriver.PG.is_async is False
assert TestDriver.PG_ASYNC.engine == DatabaseEngine.PG
assert TestDriver.MYSQL_ASYNC.engine == DatabaseEngine.MYSQL
print("TestDriver extensions: OK")
PY
```

Expected: `TestDriver extensions: OK`. If this fails with ImportError, the circular-import avoidance in Task 2 step 1 is wrong.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/container/utils/test_environment_features.py \
        tests/integration/container/utils/test_environment.py
git commit -m "feat(integration): gate async drivers behind new feature flags

Adds SKIP_SYNC_DRIVER_TESTS and SKIP_ASYNC_DRIVER_TESTS to
TestEnvironmentFeatures. is_test_driver_allowed now covers the async
TestDriver variants and respects both axes — sync/async gating is
independent of the existing engine-level SKIP_MYSQL_DRIVER_TESTS /
SKIP_PG_DRIVER_TESTS.

No Gradle wiring yet — that comes in task 6; the flags are currently
never set by the harness, so collection behavior is unchanged."
```

---

## Task 4: Extend Java `TestEnvironmentFeatures`

**Files:**
- Modify: `tests/integration/host/src/test/java/integration/TestEnvironmentFeatures.java`

- [ ] **Step 1: Add the two enum values**

Open `tests/integration/host/src/test/java/integration/TestEnvironmentFeatures.java` and add `SKIP_SYNC_DRIVER_TESTS` and `SKIP_ASYNC_DRIVER_TESTS` to the enum so it becomes:

```java
package integration;

public enum TestEnvironmentFeatures {
  IAM,
  SECRETS_MANAGER,
  FAILOVER_SUPPORTED,
  ABORT_CONNECTION_SUPPORTED,
  NETWORK_OUTAGES_ENABLED,
  AWS_CREDENTIALS_ENABLED,
  PERFORMANCE,
  SKIP_MYSQL_DRIVER_TESTS,
  SKIP_PG_DRIVER_TESTS,
  SKIP_SYNC_DRIVER_TESTS,
  SKIP_ASYNC_DRIVER_TESTS,
  RUN_AUTOSCALING_TESTS_ONLY,
  TELEMETRY_TRACES_ENABLED,
  TELEMETRY_METRICS_ENABLED,
  BLUE_GREEN_DEPLOYMENT
}
```

- [ ] **Step 2: Verify the project compiles**

```bash
./gradlew :integration-testing:compileTestJava
```

Expected: `BUILD SUCCESSFUL`. If it fails, there's a downstream reference we haven't updated yet — come back after Task 5.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/host/src/test/java/integration/TestEnvironmentFeatures.java
git commit -m "feat(integration): add async driver feature flags to Java host enum

Mirrors the Python TestEnvironmentFeatures additions from the previous
commit. These are consumed by TestEnvironmentProvider in the next task
to translate Gradle system properties into the feature set passed to
the Python container via TEST_ENV_INFO_JSON."
```

---

## Task 5: Java `TestEnvironmentConfiguration` + `TestEnvironmentProvider` translation

**Files:**
- Modify: `tests/integration/host/src/test/java/integration/host/TestEnvironmentConfiguration.java`
- Modify: `tests/integration/host/src/test/java/integration/host/TestEnvironmentProvider.java:195-196`

- [ ] **Step 1: Add two new config booleans**

Open `tests/integration/host/src/test/java/integration/host/TestEnvironmentConfiguration.java`. After the existing `excludePgDriver` declaration (line 43-44), add:

```java
  public boolean excludeSyncDrivers =
      Boolean.parseBoolean(System.getProperty("exclude-sync-drivers", "false"));
  public boolean excludeAsyncDrivers =
      Boolean.parseBoolean(System.getProperty("exclude-async-drivers", "false"));
```

- [ ] **Step 2: Translate the booleans into features**

Open `tests/integration/host/src/test/java/integration/host/TestEnvironmentProvider.java`. Find the block around lines 195–196:

```java
                            config.excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                            config.excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
```

Add two new lines immediately after:

```java
                            config.excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                            config.excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                            config.excludeSyncDrivers ? TestEnvironmentFeatures.SKIP_SYNC_DRIVER_TESTS : null,
                            config.excludeAsyncDrivers ? TestEnvironmentFeatures.SKIP_ASYNC_DRIVER_TESTS : null,
```

- [ ] **Step 3: Compile**

```bash
./gradlew :integration-testing:compileTestJava
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/host/src/test/java/integration/host/TestEnvironmentConfiguration.java \
        tests/integration/host/src/test/java/integration/host/TestEnvironmentProvider.java
git commit -m "feat(integration): translate exclude-{sync,async}-drivers system props to features

TestEnvironmentConfiguration reads the new Gradle system properties and
TestEnvironmentProvider serializes them into TEST_ENV_INFO_JSON as
SKIP_SYNC_DRIVER_TESTS / SKIP_ASYNC_DRIVER_TESTS features consumed by
conftest.py's driver filtering.

No Gradle task sets these properties yet — task 6 wires them."
```

---

## Task 6: Gradle — new async tasks + one-line touch to existing tasks

**Files:**
- Modify: `tests/integration/host/build.gradle.kts`

- [ ] **Step 1: Add `exclude-async-drivers=true` to the four existing sync tasks**

Find these task blocks and add the single line shown:

- `test-pg-aurora` (around line 190) — add `systemProperty("exclude-async-drivers", "true")` as the last `systemProperty` line before the closing `}`.
- `test-mysql-aurora` (around line 206) — same addition.
- `test-pg-multi-az` (around line 231) — same addition.
- `test-mysql-multi-az` (around line 246) — same addition.

After modification, `test-pg-aurora` should read:

```kotlin
tasks.register<Test>("test-pg-aurora") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-async-drivers", "true")
    }
}
```

Apply the same single-line addition to the other three.

- [ ] **Step 2: Add `test-pg-aurora-async`**

Append this task registration after `test-mysql-aurora` (around line 218):

```kotlin
tasks.register<Test>("test-pg-aurora-async") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-sync-drivers", "true")
    }
}
```

- [ ] **Step 3: Add `test-mysql-aurora-async`**

Append after the new `test-pg-aurora-async`:

```kotlin
tasks.register<Test>("test-mysql-aurora-async") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-multi-az-cluster", "true")
        systemProperty("exclude-multi-az-instance", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-sync-drivers", "true")
    }
}
```

- [ ] **Step 4: Add `test-pg-multi-az-async`**

Append after `test-mysql-multi-az` (around line 257):

```kotlin
tasks.register<Test>("test-pg-multi-az-async") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-mysql-driver", "true")
        systemProperty("exclude-mysql-engine", "true")
        systemProperty("exclude-mariadb-driver", "true")
        systemProperty("exclude-mariadb-engine", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-sync-drivers", "true")
    }
}
```

- [ ] **Step 5: Add `test-mysql-multi-az-async`**

Append after the new `test-pg-multi-az-async`:

```kotlin
tasks.register<Test>("test-mysql-multi-az-async") {
    group = "verification"
    filter.includeTestsMatching("integration.host.TestRunner.runTests")
    doFirst {
        systemProperty("exclude-docker", "true")
        systemProperty("exclude-performance", "true")
        systemProperty("exclude-aurora", "true")
        systemProperty("exclude-pg-driver", "true")
        systemProperty("exclude-pg-engine", "true")
        systemProperty("exclude-bg", "true")
        systemProperty("exclude-sync-drivers", "true")
    }
}
```

- [ ] **Step 6: Smoke — verify the tasks exist and sync task collection still passes**

```bash
./gradlew :integration-testing:tasks --group=verification | grep -E 'test-(pg|mysql)-(aurora|multi-az)(-async)?'
```

Expected output includes eight task names: four existing + four new async tasks.

Also run one existing sync task end-to-end (against your PG cluster) to confirm the `exclude-async-drivers=true` touch didn't regress anything:

```bash
# Same env vars as Task 0.
./gradlew :integration-testing:test-pg-aurora --tests 'integration.host.TestRunner.runTests' 2>&1 | tail -30
```

Expected: same passing tests as Task 0's Step 4. No new failures.

- [ ] **Step 7: Smoke — verify the new async tasks collect zero tests and exit clean**

At this point no `*_async.py` files exist yet, so async tasks should find zero tests and exit 0:

```bash
./gradlew :integration-testing:test-pg-aurora-async 2>&1 | tail -30
```

Expected: `BUILD SUCCESSFUL`. The pytest run inside the container reports `collected 0 items` or equivalent. If it reports tests being collected, the gating is wrong.

- [ ] **Step 8: Commit**

```bash
git add tests/integration/host/build.gradle.kts
git commit -m "feat(integration): add async Gradle tasks and preserve sync task behavior

Adds test-{pg,mysql}-{aurora,multi-az}-async, each setting
exclude-sync-drivers=true so only async TestDriver variants parametrize.

Existing sync tasks gain exclude-async-drivers=true — mandatory,
behavior-preserving: without it they would start collecting async
drivers once async test files exist in Task 8+. Deliberate one-line
addition per task."
```

---

## Task 7: Async connection and SQLAlchemy engine helpers

**Files:**
- Modify: `tests/integration/container/utils/driver_helper.py`
- Create: `tests/integration/container/utils/async_connection_helpers.py`

- [ ] **Step 1: Extend `DriverHelper` with async-driver branches**

Open `tests/integration/container/utils/driver_helper.py`. Add imports at the top (with isort ordering preserved):

```python
import aiomysql
import psycopg
```

`psycopg` is already imported. Keep it. Add `aiomysql` in the correct isort position (alphabetical among third-party imports).

Extend `get_connect_func` with async-driver branches:

```python
    @staticmethod
    def get_connect_func(test_driver: TestDriver) -> Callable:
        d: Optional[TestDriver] = test_driver
        if d is None:
            d = TestEnvironment.get_current().get_current_driver()

        if d is None:
            raise Exception(Messages.get("Testing.RequiredTestDriver"))

        if d == TestDriver.PG:
            return psycopg.Connection.connect
        if d == TestDriver.MYSQL:
            return mysql.connector.connect
        if d == TestDriver.PG_ASYNC:
            return psycopg.AsyncConnection.connect
        if d == TestDriver.MYSQL_ASYNC:
            return aiomysql.connect
        raise UnsupportedOperationError(
            Messages.get_formatted("Testing.FunctionNotImplementedForDriver", "get_connect_func", d.value))
```

Note: the original else-branch raised inside `else:`; restructured here to use unconditional `raise` after all `if` branches so mypy narrows correctly. Equivalent control flow.

Extend `get_connect_params` similarly:

```python
    @staticmethod
    def get_connect_params(host, port, user, password, db, test_driver: Optional[TestDriver] = None) -> Dict[str, Any]:
        d: Optional[TestDriver] = test_driver
        if d is None:
            d = TestEnvironment.get_current().get_current_driver()

        if d is None:
            raise Exception(Messages.get("Testing.RequiredTestDriver"))

        if d == TestDriver.PG:
            return {"host": host, "port": port, "dbname": db, "user": user, "password": password}
        if d == TestDriver.MYSQL:
            return {
                "host": host, "port": int(port), "database": db, "user": user, "password": password, "use_pure": True}
        if d == TestDriver.PG_ASYNC:
            # psycopg AsyncConnection.connect takes the same kwargs as sync.
            return {"host": host, "port": port, "dbname": db, "user": user, "password": password}
        if d == TestDriver.MYSQL_ASYNC:
            # aiomysql accepts 'db' not 'database', and no use_pure equivalent.
            return {"host": host, "port": int(port), "db": db, "user": user, "password": password}
        raise UnsupportedOperationError(
            Messages.get_formatted("Testing.FunctionNotImplementedForDriver", "get_connection_string", d.value))
```

- [ ] **Step 2: Create the async helpers file**

Create `tests/integration/container/utils/async_connection_helpers.py`:

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

"""Async integration-test helpers.

Thin, async-aware counterparts to the sync connect/engine helpers used by
integration tests. Mirrors the sync shape so async test files look like
sync files with ``await`` sprinkled in — no new abstractions.

Teardown convention: async tests must ``await release_resources_async()``
to tear down wrapper background tasks cleanly (parallel to the sync
``release_resources()`` convention called out in ``CLAUDE.local.md``).
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

import aiomysql
import psycopg
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from aws_advanced_python_wrapper.aio.cleanup import release_resources_async
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.messages import Messages

from .test_driver import TestDriver

if TYPE_CHECKING:
    from .test_environment import TestEnvironment


async def connect_async(
        test_driver: TestDriver,
        connect_params: Dict[str, Any],
        **wrapper_kwargs: Any) -> AsyncAwsWrapperConnection:
    """Open an AsyncAwsWrapperConnection for the given async TestDriver.

    :param test_driver: must be PG_ASYNC or MYSQL_ASYNC.
    :param connect_params: driver-level connect kwargs (host/port/user/...).
    :param wrapper_kwargs: wrapper-level kwargs (e.g., plugins, wrapper_dialect).
    """
    if test_driver == TestDriver.PG_ASYNC:
        target = psycopg.AsyncConnection.connect
    elif test_driver == TestDriver.MYSQL_ASYNC:
        target = aiomysql.connect
    else:
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "Testing.FunctionNotImplementedForDriver", "connect_async", test_driver.value))

    return await AsyncAwsWrapperConnection.connect(
        target=target,
        **connect_params,
        **wrapper_kwargs,
    )


def create_async_engine_for_driver(
        test_driver: TestDriver,
        user: str,
        password: str,
        host: str,
        port: int,
        dbname: str,
        wrapper_dialect: Optional[str] = None,
        wrapper_plugins: Optional[str] = None,
        **engine_kwargs: Any) -> AsyncEngine:
    """Build an AsyncEngine using the wrapper's registered async dialect.

    Dialect URL schemes registered in pyproject.toml:
      * aws_wrapper_postgresql+psycopg_async  (PG)
      * aws_wrapper_mysql+aiomysql_async      (MySQL)
    """
    if test_driver == TestDriver.PG_ASYNC:
        driver_scheme = "aws_wrapper_postgresql+psycopg_async"
    elif test_driver == TestDriver.MYSQL_ASYNC:
        driver_scheme = "aws_wrapper_mysql+aiomysql_async"
    else:
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "Testing.FunctionNotImplementedForDriver",
                "create_async_engine_for_driver",
                test_driver.value))

    query_parts = []
    if wrapper_dialect is not None:
        query_parts.append(f"wrapper_dialect={wrapper_dialect}")
    if wrapper_plugins is not None:
        query_parts.append(f"wrapper_plugins={wrapper_plugins}")
    query = ("?" + "&".join(query_parts)) if query_parts else ""

    url = f"{driver_scheme}://{user}:{password}@{host}:{port}/{dbname}{query}"
    return create_async_engine(url, **engine_kwargs)


async def cleanup_async() -> None:
    """Teardown to await at the end of every async test fixture.

    Mirrors the sync ``release_resources()`` convention.
    """
    await release_resources_async()
```

- [ ] **Step 3: Run type / lint checks**

```bash
poetry run mypy tests/integration/container/utils/driver_helper.py tests/integration/container/utils/async_connection_helpers.py
poetry run flake8 tests/integration/container/utils/driver_helper.py tests/integration/container/utils/async_connection_helpers.py
poetry run isort --check-only tests/integration/container/utils/driver_helper.py tests/integration/container/utils/async_connection_helpers.py
```

Expected: all succeed. If isort complains, run `poetry run isort tests/integration/container/utils/driver_helper.py tests/integration/container/utils/async_connection_helpers.py` to auto-fix and re-verify.

- [ ] **Step 4: Smoke-import the new module**

```bash
poetry run python -c 'from tests.integration.container.utils.async_connection_helpers import connect_async, create_async_engine_for_driver, cleanup_async; print("async helpers import: OK")'
```

Expected: `async helpers import: OK`.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/container/utils/driver_helper.py \
        tests/integration/container/utils/async_connection_helpers.py
git commit -m "feat(integration): add async connection and engine helpers

Extends DriverHelper with PG_ASYNC / MYSQL_ASYNC branches and introduces
async_connection_helpers.py with:
  * connect_async(test_driver, connect_params, **wrapper_kwargs)
  * create_async_engine_for_driver(test_driver, ...)
  * cleanup_async() -> await release_resources_async()

Teardown convention mirrors the sync release_resources() path so every
async test fixture gets cleanup for free."
```

---

## Task 8: First async test file — `test_basic_connectivity_async.py`

**Files:**
- Create: `tests/integration/container/test_basic_connectivity_async.py`
- Reference (read-only): `tests/integration/container/test_basic_connectivity.py`

**Purpose of this task:** first real async connection against a live Aurora cluster — validates the *entire stack* (Gradle task → Java gating → Python conftest parametrization → async wrapper → real cluster) end-to-end. Before this task, everything is wired but untested against the driver.

- [ ] **Step 1: Open the sync file and familiarize yourself with its tests**

Read `tests/integration/container/test_basic_connectivity.py` in full. Note:
- Class-scoped `rds_utils` and `props` fixtures
- `@disable_on_features(...)` class decorator
- Tests take `(self, test_environment, test_driver, conn_utils)` — all three fixtures parametrize
- Tests use `DriverHelper.get_connect_func(test_driver)` and sync `with conn.cursor() as cur:` patterns
- `release_resources()` called in teardown (or per-test)

- [ ] **Step 2: Create the async twin**

Create `tests/integration/container/test_basic_connectivity_async.py`:

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

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.async_connection_helpers import (
    cleanup_async, connect_async)

from .utils.conditions import disable_on_features
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from .utils.connection_utils import ConnectionUtils
    from .utils.test_driver import TestDriver


@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestBasicConnectivityAsync:

    @pytest.fixture(scope='class')
    def props(self):
        # Mirror sync: skip host_monitoring so tests don't require abort support.
        p: Properties = Properties({
            WrapperProperties.PLUGINS.name: "aurora_connection_tracker,failover",
            "connect_timeout": 3,
            "autocommit": True,
            "cluster_id": "cluster1"})

        if (TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features()
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features()):
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")
        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")
        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    def test_direct_connection_async(
            self,
            test_environment: TestEnvironment,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            props: Properties):
        async def inner() -> None:
            try:
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_connect_params(),
                    **dict(props))
                try:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        row = await cur.fetchone()
                    assert row is not None
                    assert row[0] == 1
                finally:
                    await conn.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    def test_wrapper_connection_via_cluster_endpoint_async(
            self,
            test_environment: TestEnvironment,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            props: Properties):
        """Cluster-endpoint connection smoke — parity with sync test.

        Ports the corresponding ``test_wrapper_connection_via_cluster_endpoint``
        logic from test_basic_connectivity.py; see that sync test for any
        engine-specific branching still required.
        """
        async def inner() -> None:
            try:
                params = conn_utils.get_connect_params(host=conn_utils.writer_cluster_host)
                conn = await connect_async(
                    test_driver=test_driver,
                    connect_params=params,
                    **dict(props))
                try:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        row = await cur.fetchone()
                    assert row is not None
                    assert row[0] == 1
                finally:
                    await conn.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())
```

**Important:** the sync file contains more tests than the two shown above. Read each remaining sync test and port it using the same pattern:
1. Wrap the body in `async def inner(): ...` with `try/finally: await cleanup_async()`.
2. Replace `target_driver_connect = DriverHelper.get_connect_func(test_driver)` + `conn = target_driver_connect(...)` with `conn = await connect_async(test_driver=test_driver, connect_params=..., **wrapper_kwargs)`.
3. Replace sync `with conn.cursor() as cur:` with `async with conn.cursor() as cur:`.
4. Prefix every cursor call with `await`: `await cur.execute(...)`, `await cur.fetchone()`, `await cur.fetchall()`, etc.
5. Replace `conn.close()` with `await conn.close()`.
6. Call the test via `asyncio.run(inner())` at the bottom of the sync wrapper.
7. Test names get `_async` suffix.

- [ ] **Step 3: Lint + type check the new file**

```bash
poetry run mypy tests/integration/container/test_basic_connectivity_async.py
poetry run flake8 tests/integration/container/test_basic_connectivity_async.py
poetry run isort --check-only tests/integration/container/test_basic_connectivity_async.py
```

Expected: all succeed. Auto-fix isort if needed.

- [ ] **Step 4: Run the async suite against PG**

Same env vars as Task 0 Step 3, then:

```bash
export RDS_DB_NAME=<pg-cluster-id>
export RDS_DB_DOMAIN=<pg-cluster>.xxx.us-west-2.rds.amazonaws.com
./gradlew :integration-testing:test-pg-aurora-async 2>&1 | tee /tmp/pg-async.log | tail -60
```

Expected: tests collect (at least `test_direct_connection_async[PG_ASYNC]`), connect to the cluster, pass, `BUILD SUCCESSFUL`.

If a test hangs beyond 600s, the cluster was cold. Warm it (Task 0 Step 2) and re-run.

If you get `AttributeError: 'NoneType' object has no attribute ...` in a type-introspection path, the psycopg-async unwrap fix (commit `2071607`) is not in the branch — verify with `git log --oneline | grep 2071607`.

- [ ] **Step 5: Run the async suite against MySQL**

```bash
export RDS_DB_NAME=<mysql-cluster-id>
export RDS_DB_DOMAIN=<mysql-cluster>.xxx.us-west-2.rds.amazonaws.com
./gradlew :integration-testing:test-mysql-aurora-async 2>&1 | tee /tmp/mysql-async.log | tail -60
```

Expected: same pattern — tests collect with `[MYSQL_ASYNC]`, pass, `BUILD SUCCESSFUL`.

If you get `-Werror`-promoted warnings from aiomysql (e.g. `DeprecationWarning: There is no current event loop`), triage: first check if an aiomysql upgrade is available, else add a targeted `filterwarnings` entry in `pyproject.toml`:

```toml
filterwarnings = [
    'ignore:cache could not write path',
    'ignore:could not create cache path',
    'ignore:Exception during reset or similar:pytest.PytestUnhandledThreadExceptionWarning',
    'ignore:<specific aiomysql warning text>:<WarningClass>:<module>',
]
```

Never use a blanket `ignore::DeprecationWarning` — always target the specific warning source.

- [ ] **Step 6: Commit**

```bash
git add tests/integration/container/test_basic_connectivity_async.py
# If pyproject.toml filterwarnings was touched:
# git add pyproject.toml
git commit -m "test(integration): add async basic connectivity tests

Async twin of test_basic_connectivity.py. First end-to-end validation of
the async integration path: Gradle async task -> Python gating ->
AsyncAwsWrapperConnection -> real Aurora cluster.

Uses asyncio.run(inner()) inside sync def test_... to match the existing
tests/unit/test_aio_*.py convention — no pytest-asyncio dependency."
```

---

## Task 9: `test_sqlalchemy_async.py`

**Files:**
- Create: `tests/integration/container/test_sqlalchemy_async.py`
- Reference (read-only): `tests/integration/container/test_sqlalchemy.py`

- [ ] **Step 1: Read the sync SQLAlchemy test file end to end**

```bash
cat tests/integration/container/test_sqlalchemy.py
```

Note the `mysql_connect` / `pg_connect` creator callables and the `engine = create_engine("aurora-mysql", creator=...)` / `create_engine("aurora-pg", creator=...)` pattern.

- [ ] **Step 2: Write the async twin using `create_async_engine`**

The async path is URL-based rather than creator-based — the wrapper async dialects (registered in `pyproject.toml:31-36`) handle the connection internally.

Create `tests/integration/container/test_sqlalchemy_async.py`. The file header + class + one fully-ported test follows; port each remaining sync test using the same pattern.

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

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from tests.integration.container.utils.async_connection_helpers import (
    cleanup_async, create_async_engine_for_driver)
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment

from .utils.conditions import disable_on_features, enable_on_deployments
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from .utils.connection_utils import ConnectionUtils
    from .utils.test_driver import TestDriver


@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestSqlAlchemyAsync:

    def test_async_engine_basic_select(
            self,
            test_environment: TestEnvironment,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils):
        async def inner() -> None:
            wrapper_dialect = (
                "aurora-pg" if test_driver.engine.name == "PG" else "aurora-mysql")
            engine = create_async_engine_for_driver(
                test_driver=test_driver,
                user=conn_utils.user,
                password=conn_utils.password,
                host=conn_utils.writer_host,
                port=conn_utils.port,
                dbname=conn_utils.dbname,
                wrapper_dialect=wrapper_dialect,
                wrapper_plugins="aurora_connection_tracker,failover",
            )
            try:
                async with engine.connect() as conn:
                    result = await conn.execute(text("SELECT 1"))
                    row = result.fetchone()
                    assert row is not None
                    assert row[0] == 1
            finally:
                await engine.dispose()
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_deployments([DatabaseEngineDeployment.AURORA])
    def test_async_engine_survives_aurora_failover(
            self,
            test_environment: TestEnvironment,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils):
        """Parity port of test_sqlalchemy_creator_survives_aurora_failover.

        Read the sync version for the full failover scenario (trigger
        failover via rds_utils, expect OperationalError, reconnect). Wrap
        each sync call in asyncio.run if required; use engine.begin() /
        engine.connect() as async context managers.
        """
        # Port sync test_sqlalchemy_creator_survives_aurora_failover here,
        # following the translation rules in Task 8 Step 2 notes.
        pytest.skip("Port from sync test_sqlalchemy_creator_survives_aurora_failover")
```

Port the remaining sync tests following the same rules as Task 8 Step 2, plus:
- `create_engine` → `create_async_engine_for_driver(...)`
- `engine.connect()` becomes `async with engine.connect() as conn:` — SA's async engine returns async context managers
- `conn.execute(text(...))` → `await conn.execute(text(...))`
- `engine.dispose()` → `await engine.dispose()` (AsyncEngine)
- Any `session.execute(...)` becomes `await session.execute(...)` inside `async with AsyncSession(engine) as session:` blocks
- `execution_options(postgresql_readonly=True)` / `execution_options(mysql_readonly=True)` still work unchanged — they're SA-level options

Remove the `pytest.skip(...)` placeholder in `test_async_engine_survives_aurora_failover` once ported; the skip is there so the first smoke run in Step 3 below collects cleanly.

- [ ] **Step 3: Lint + type check**

```bash
poetry run mypy tests/integration/container/test_sqlalchemy_async.py
poetry run flake8 tests/integration/container/test_sqlalchemy_async.py
poetry run isort --check-only tests/integration/container/test_sqlalchemy_async.py
```

- [ ] **Step 4: Run against both clusters**

```bash
# PG
export RDS_DB_NAME=<pg-cluster-id>
export RDS_DB_DOMAIN=<pg-cluster>.xxx.us-west-2.rds.amazonaws.com
FILTER="test_sqlalchemy_async" ./gradlew :integration-testing:test-pg-aurora-async 2>&1 | tail -40

# MySQL
export RDS_DB_NAME=<mysql-cluster-id>
export RDS_DB_DOMAIN=<mysql-cluster>.xxx.us-west-2.rds.amazonaws.com
FILTER="test_sqlalchemy_async" ./gradlew :integration-testing:test-mysql-aurora-async 2>&1 | tail -40
```

Expected: both `BUILD SUCCESSFUL`, tests collected and passing (failover test skipped until ported).

- [ ] **Step 5: Commit**

```bash
git add tests/integration/container/test_sqlalchemy_async.py
git commit -m "test(integration): add async SQLAlchemy tests

Async twin of test_sqlalchemy.py using create_async_engine against the
wrapper's registered async URL schemes
(aws_wrapper_postgresql+psycopg_async, aws_wrapper_mysql+aiomysql_async).
Exercises the async engine + async connection lifecycle end-to-end
against real Aurora clusters."
```

---

## Tasks 10-18: Feature-by-feature async test file ports

Each of tasks 10-18 follows the **same shape**. The differences are the source file, the target file, and test-specific quirks documented inline. Translation rules are identical:

**Translation rules (apply to every task below):**
1. Read the sync source file `tests/integration/container/<sync_file>` end-to-end first.
2. Create `tests/integration/container/<sync_file_without_.py>_async.py` with the Apache license header.
3. Copy class decorators (`@disable_on_features`, `@enable_on_deployments`, `@enable_on_num_instances`, etc.) verbatim. They work unchanged on async drivers because conditions.py gates on `test_driver.engine` / deployment / features, not on driver sync/async.
4. Rename the test class from `TestFoo` to `TestFooAsync`.
5. For each `def test_name(self, ...)`:
   - Keep the signature sync.
   - Move the body into `async def inner() -> None:` inside the test function.
   - Wrap in `try: ... finally: await cleanup_async()`.
   - Replace `target_driver_connect = DriverHelper.get_connect_func(test_driver)` + `conn = target_driver_connect(**params)` with `conn = await connect_async(test_driver=test_driver, connect_params=params, **wrapper_kwargs)` (for raw tests) or `engine = create_async_engine_for_driver(...)` (for SA tests).
   - Replace `with conn.cursor() as cur:` with `async with conn.cursor() as cur:`.
   - Add `await` before every cursor method call (`execute`, `fetchone`, `fetchall`, `fetchmany`, `callproc`, `nextset`, `scroll`).
   - Add `await` before `conn.commit()`, `conn.rollback()`, `conn.close()`.
   - For SA: `engine.connect()` → `async with engine.connect() as conn:`, `conn.execute(...)` → `await conn.execute(...)`, `engine.dispose()` → `await engine.dispose()`.
   - End the function body with `asyncio.run(inner())`.
   - Rename the test by appending `_async` to its name.
6. Update imports: import `connect_async` / `create_async_engine_for_driver` / `cleanup_async` from `async_connection_helpers`; drop `DriverHelper`, `AwsWrapperConnection`, `release_resources` if unused.
7. Where the sync test uses `release_resources()` in its teardown, replace with `await cleanup_async()` inside the `finally` block of `inner()`.

**Per-task verification:** Each task's Step 3 runs the newly-added test file only (via `FILTER=test_<name>_async`) against both PG-async and MySQL-async (where applicable per conditions on that file). Any environmental skip is noted in the commit body.

Every task's commit message follows: `test(integration): add async <feature> tests` plus 2-3 lines explaining which sync file it mirrors.

### Task 10: `test_basic_functionality_async.py`

**Files:**
- Create: `tests/integration/container/test_basic_functionality_async.py`
- Reference: `tests/integration/container/test_basic_functionality.py`

- [ ] **Step 1:** Apply the translation rules above to port `test_basic_functionality.py`. No file-specific quirks — exercises basic SQL operations (INSERT / SELECT / transactions / autocommit) which port mechanically.
- [ ] **Step 2:** Lint + type check: `poetry run mypy tests/integration/container/test_basic_functionality_async.py && poetry run flake8 tests/integration/container/test_basic_functionality_async.py && poetry run isort --check-only tests/integration/container/test_basic_functionality_async.py`.
- [ ] **Step 3:** Run: `FILTER="test_basic_functionality_async" ./gradlew :integration-testing:test-pg-aurora-async` and same for MySQL.
- [ ] **Step 4:** Commit: `test(integration): add async basic functionality tests`.

### Task 11: `test_aurora_failover_async.py`

**Files:**
- Create: `tests/integration/container/test_aurora_failover_async.py`
- Reference: `tests/integration/container/test_aurora_failover.py`

**File-specific quirks:**
- Failover tests use `rds_utils.failover_cluster_and_wait_until_writer_changed()` or similar — sync helper methods on the `RdsTestUtility` class. They remain synchronous and can be called directly from the `async def inner()` body without `await` (they block briefly on AWS SDK calls). If you hit an `asyncio` "blocking call in event loop" warning, wrap the call in `await asyncio.get_event_loop().run_in_executor(None, rds_utils.failover_cluster_and_wait_until_writer_changed)`.
- Some failover tests catch `OperationalError` on the old connection and reconnect. On async drivers, the catchable exception from psycopg async / aiomysql may differ slightly (psycopg: `psycopg.OperationalError`; aiomysql: `pymysql.err.OperationalError` or `aiomysql.OperationalError`). If the sync test catches `Exception` or a wrapper-level error, the port is mechanical; if it catches a driver-specific class, branch on `test_driver.engine`.

- [ ] **Step 1:** Port per translation rules + the quirks above.
- [ ] **Step 2:** Lint + type check (same commands, pointing at the new file).
- [ ] **Step 3:** Run against both clusters. If your Serverless v2 cluster is single-writer with no reader autoscaling, failover tests will skip via `@enable_on_num_instances(min_instances=2)` — expected, not a regression.
- [ ] **Step 4:** Commit: `test(integration): add async Aurora failover tests`.

### Task 12: `test_read_write_splitting_async.py`

**Files:**
- Create: `tests/integration/container/test_read_write_splitting_async.py`
- Reference: `tests/integration/container/test_read_write_splitting.py`

**File-specific quirks:**
- RWS tests use `WrapperProperties.PLUGINS.set(p, "read_write_splitting,...")`. Same plugin string works for async — the wrapper's async plugin factory resolves the same plugin codes.
- `execution_options(postgresql_readonly=True)` / `execution_options(mysql_readonly=True)` are SA-level and work with async engines unchanged.
- Raw-driver read-only toggle: sync uses `conn.set_read_only(True)` or similar on the wrapper connection. Async equivalent on `AsyncAwsWrapperConnection` is `await conn.set_read_only(True)` — verify the method is async on the async wrapper (it is — see `aws_advanced_python_wrapper/aio/wrapper.py`).

- [ ] **Step 1:** Port per translation rules + quirks.
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run against both clusters. RWS routing requires a reader endpoint — without one, tests that assert reader-host behavior will skip or fail environmentally.
- [ ] **Step 4:** Commit: `test(integration): add async read/write splitting tests`.

### Task 13: `test_iam_authentication_async.py`

**Files:**
- Create: `tests/integration/container/test_iam_authentication_async.py`
- Reference: `tests/integration/container/test_iam_authentication.py`

**File-specific quirks:**
- IAM plugin is `iam_authentication`. Both sync and async versions support it via `WrapperProperties.PLUGINS`.
- IAM auth tests require `TestEnvironmentFeatures.IAM` in features — gate by existing `@enable_on_features([TestEnvironmentFeatures.IAM])`.
- Cluster-side requirement: the Aurora cluster must have IAM database authentication enabled and `IAM_USER` env var must be set (per `TestEnvironmentConfiguration.java:106`).
- MySQL-async IAM path: aiomysql's password parameter accepts long strings (unlike the MySQL C-extension `use_pure=True` trap called out in CLAUDE.local.md). If the test fails with `int1store requires 0 <= i <= 255` it's an aiomysql version issue — report, don't silence.

- [ ] **Step 1:** Port.
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run. If `IAM_USER` isn't set, tests will skip — not a regression.
- [ ] **Step 4:** Commit: `test(integration): add async IAM authentication tests`.

### Task 14: `test_aws_secrets_manager_async.py`

**Files:**
- Create: `tests/integration/container/test_aws_secrets_manager_async.py`
- Reference: `tests/integration/container/test_aws_secrets_manager.py`

**File-specific quirks:**
- Plugin name: `aws_secrets_manager`. Async version supported.
- Requires `TestEnvironmentFeatures.SECRETS_MANAGER` — tests gated via `@enable_on_features([TestEnvironmentFeatures.SECRETS_MANAGER])`.
- Tests usually create/rotate a secret in Secrets Manager via `boto3` calls (sync). These can stay sync inside `async def inner():` as per Task 11's quirks note.

- [ ] **Step 1:** Port.
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run.
- [ ] **Step 4:** Commit: `test(integration): add async AWS Secrets Manager tests`.

### Task 15: `test_host_monitoring_v2_async.py`

**Files:**
- Create: `tests/integration/container/test_host_monitoring_v2_async.py`
- Reference: `tests/integration/container/test_host_monitoring_v2.py`

**File-specific quirks:**
- EFM/host-monitoring tests typically require `NETWORK_OUTAGES_ENABLED` + Toxiproxy. Against real Aurora clusters without Toxiproxy they skip — expected, full-parity port goal still applies.
- Plugin name: `host_monitoring`.

- [ ] **Step 1:** Port.
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run. Expect environmental skips against real clusters; verify by running `./gradlew :integration-testing:test-docker` async if the Docker test path is available — but that's out of scope for your Serverless run.
- [ ] **Step 4:** Commit: `test(integration): add async host monitoring v2 tests`.

### Task 16: `test_custom_endpoint_async.py`

**Files:**
- Create: `tests/integration/container/test_custom_endpoint_async.py`
- Reference: `tests/integration/container/test_custom_endpoint.py`

**File-specific quirks:**
- Requires an Aurora cluster with a custom endpoint configured. If your Serverless clusters don't have custom endpoints, these tests skip.
- Plugin name: `custom_endpoint`.

- [ ] **Step 1:** Port.
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run.
- [ ] **Step 4:** Commit: `test(integration): add async custom endpoint tests`.

### Task 17: `test_autoscaling_async.py`

**Files:**
- Create: `tests/integration/container/test_autoscaling_async.py`
- Reference: `tests/integration/container/test_autoscaling.py`

**File-specific quirks:**
- Gated by `TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY` — only runs when that feature is on (Gradle `test-autoscaling` task sets it).
- Creates and deletes Aurora reader instances via `boto3` / `RdsTestUtility` — stays sync inside `async def inner()`.

- [ ] **Step 1:** Port.
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run only under the autoscaling Gradle path if you have one set up; otherwise the tests correctly skip.
- [ ] **Step 4:** Commit: `test(integration): add async autoscaling tests`.

### Task 18: `test_blue_green_deployment_async.py`

**Files:**
- Create: `tests/integration/container/test_blue_green_deployment_async.py`
- Reference: `tests/integration/container/test_blue_green_deployment.py`

**File-specific quirks:**
- Gated by `TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT`.
- Requires specific engine versions — see `docs/using-the-python-wrapper/using-plugins/UsingTheBlueGreenPlugin.md` for the matrix. Aurora Serverless v2 may or may not be eligible depending on engine version.

- [ ] **Step 1:** Port.
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run. Expect environmental skip unless the cluster is BG-eligible.
- [ ] **Step 4:** Commit: `test(integration): add async blue/green deployment tests`.

### Task 19: Performance test ports

**Files:**
- Create: `tests/integration/container/test_failover_performance_async.py`
- Create: `tests/integration/container/test_read_write_splitting_performance_async.py`
- References: sync twins of the same names

**File-specific quirks:**
- Gated by `TestEnvironmentFeatures.PERFORMANCE` — only run under the `test-*-performance` Gradle tasks, which are separate from the Aurora tasks.
- Performance tests use `performance_utility.py`. No async-specific adaptation needed beyond the standard translation rules.

- [ ] **Step 1:** Port both files (one at a time).
- [ ] **Step 2:** Lint + type check.
- [ ] **Step 3:** Run via the performance Gradle path if needed — not a blocker for Aurora-async validation.
- [ ] **Step 4:** Commit as one commit: `test(integration): add async performance tests (failover + RWS)`.

---

## Task 20: Update integration-tests documentation

**Files:**
- Modify: `docs/development-guide/IntegrationTests.md`

- [ ] **Step 1: Read the current doc to find the right sections to touch**

```bash
wc -l docs/development-guide/IntegrationTests.md
grep -nE '^##|^###' docs/development-guide/IntegrationTests.md
```

- [ ] **Step 2: Add a section documenting the async Gradle tasks**

Under the "Running Tests" heading (or equivalent), add a new subsection. Template:

````markdown
### Running async integration tests

The wrapper ships async counterparts to every sync integration test file. Async tests exercise `AsyncAwsWrapperConnection` (raw) and `create_async_engine` with the wrapper's async dialects. They are invoked via dedicated Gradle tasks, independent of the sync tasks:

| Deployment | Engine | Sync task | Async task |
|---|---|---|---|
| Aurora | PG | `test-pg-aurora` | `test-pg-aurora-async` |
| Aurora | MySQL | `test-mysql-aurora` | `test-mysql-aurora-async` |
| RDS Multi-AZ | PG | `test-pg-multi-az` | `test-pg-multi-az-async` |
| RDS Multi-AZ | MySQL | `test-mysql-multi-az` | `test-mysql-multi-az-async` |

Environment variables are identical to the sync tasks. Sync and async tasks are disjoint — a single Gradle invocation runs only one axis. Collect both by running both tasks sequentially.

**Aurora Serverless v2 considerations:**
1. The harness does not authorize the test runner's IP when `REUSE_RDS_DB=true`. Ensure your runner IP is already in the cluster security group before invoking a Gradle task.
2. Scale-from-zero can delay the first connection 30–60s; warm the cluster with a trivial query before running timing-sensitive tests.
3. Multi-instance tests (failover, read/write splitting routing) skip on single-writer topologies. This is environmental — not a regression.
````

- [ ] **Step 3: Lint markdown links**

```bash
grep -nE '\[.*\]\(.*\)' docs/development-guide/IntegrationTests.md | head -20
```

Manually verify any new links resolve. CI runs `markdown-link-check` on `docs/` per `.github/workflows/main.yml` — broken links fail CI even for doc-only changes.

- [ ] **Step 4: Commit**

```bash
git add docs/development-guide/IntegrationTests.md
git commit -m "docs(integration): document async test tasks and Serverless v2 caveats

Adds a 'Running async integration tests' section with the Gradle task
matrix and three Aurora Serverless v2 operational notes (SG allowlist,
warmup, single-writer topology implications)."
```

---

## Self-Review

### Spec coverage

| Spec section | Implementing task(s) |
|---|---|
| Four runtime axes table | Tasks 2, 3, 6, 7, 8, 9, 10-19 (test files exercise all four axes per engine) |
| `TestDriver` enum + `is_async` / `engine` | Task 2 |
| Python `TestEnvironmentFeatures` additions | Task 3 |
| `get_allowed_test_drivers` / `is_test_driver_allowed` | Task 3 |
| `asyncio.run(inner)` convention (no pytest-asyncio) | Task 8 (demonstrated); rules codified in Task 10-19 preamble |
| Async connection helpers | Task 7 (`async_connection_helpers.py`) |
| Async SQLAlchemy engine helper | Task 7 (`create_async_engine_for_driver`) |
| Java `TestEnvironmentFeatures` mirror | Task 4 |
| `TestEnvironmentConfiguration` system props | Task 5 |
| `TestEnvironmentProvider` feature translation | Task 5 |
| Four new Gradle tasks | Task 6 |
| One-line touch to existing sync Gradle tasks | Task 6 |
| Dependency verification (aiomysql group) | Task 1 |
| Async `release_resources_async()` discipline | Task 7 (`cleanup_async`); rules in Task 10-19 preamble |
| Per-file async test twins (13 files) | Tasks 8, 9, 10-19 |
| Docs update | Task 20 |
| Step 0 baseline + SG pre-check + warmup | Task 0 |
| `filterwarnings` reactive triage convention | Task 8 Step 5 (and inherited by 10-19) |

### Placeholder scan

- Task 9's `test_async_engine_survives_aurora_failover` has a `pytest.skip(...)` line with the instruction to port sync logic — this is a deliberate scaffold, not a placeholder. Resolved when the engineer ports that test per the translation rules.
- No other "TBD"/"TODO"/"fill in"/"similar to task N" patterns.

### Type / naming consistency

- `connect_async(test_driver, connect_params, **wrapper_kwargs)` is referenced consistently in Task 7 (definition), Task 8 (usage), and the Task 10-19 preamble (translation rule).
- `create_async_engine_for_driver(...)` named consistently in Task 7 (definition) and Task 9 (usage).
- `cleanup_async()` named consistently in Task 7 (definition), Task 8 (usage), Task 10-19 preamble.
- `release_resources_async` module path `aws_advanced_python_wrapper.aio.cleanup.release_resources_async` matches what was verified in the repo.
- Gradle task names `test-{pg,mysql}-{aurora,multi-az}-async` consistent across Task 6, Task 20, and the doc template.
- System property names `exclude-sync-drivers` / `exclude-async-drivers` consistent across Tasks 5, 6, 20.
- Feature-flag names `SKIP_SYNC_DRIVER_TESTS` / `SKIP_ASYNC_DRIVER_TESTS` consistent across Tasks 3, 4, 5.

No inconsistencies found.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-24-async-integration-tests.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration. Best for a plan this size since each task is independently verifiable and context between tasks is intentionally narrow (only the previous task's commit matters).

**2. Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints for review.

Which approach?
