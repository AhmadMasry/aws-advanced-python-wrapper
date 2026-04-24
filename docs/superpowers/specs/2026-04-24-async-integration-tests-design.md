# Async Integration Tests — Design Spec

**Date:** 2026-04-24
**Branch:** `feat/async-parity`
**Status:** Draft — awaiting user review before implementation plan

## Problem

The `feat/async-parity` branch ships wrapper- and SQLAlchemy-level async parity (`aws_advanced_python_wrapper/aio/`, `sqlalchemy_dialects/pg_async.py`, `sqlalchemy_dialects/mysql_async.py`), but `tests/integration/` has no async coverage. `TestDriver` only enumerates `PG` and `MYSQL` (both sync); `test_sqlalchemy.py` uses `create_engine`, not `create_async_engine`. The async code has unit-test coverage but has never been exercised end-to-end against real Aurora clusters.

Goal: extend the existing sync integration harness with a parallel async axis so every sync integration test has an async counterpart that runs against real clusters. Sync surface is the invariant — nothing about its behavior, collection, or parametrization changes.

## Scope

**In scope:**
- Full-parity async ports of every existing `tests/integration/container/test_*.py`.
- Four runtime axes: sync+raw, sync+SQLAlchemy, async+raw, async+SQLAlchemy.
- Gradle task surface to invoke async runs independently per engine × deployment.
- Harness plumbing: `TestDriver` enum variants, conftest gating, Java/system-property mirror, async connection/engine helpers.
- Docs update for the new tasks and axis.

**Out of scope:**
- Parametrizing sync tests over sync/async (explicitly rejected — sync/async differ in API shape, not library; forcing them into one body adds abstraction that harms every test).
- New SQLAlchemy coverage for features that don't currently have sync SQLAlchemy tests (we mirror the existing shape; we don't expand it).
- Alternative async MySQL drivers (`asyncmy`) — wrapper's registered dialect is aiomysql-backed; adding a second driver multiplies surface without validating anything new.
- CHANGELOG entry — handled separately at upstream-PR time.
- `debug-*-async` IDE variants — copy-paste follow-up if needed.

## Architecture

Additive extension. Sync path is untouched.

### Four runtime axes

| Axis | Driver | Test files | Gradle task |
|---|---|---|---|
| sync + raw | psycopg / mysql-connector | existing `test_*.py` | `test-pg-aurora`, `test-mysql-aurora` (unchanged) |
| sync + SQLAlchemy | psycopg / mysql-connector | existing `test_sqlalchemy.py` | same as above |
| async + raw | `psycopg[async]` / `aiomysql` | **new** `test_*_async.py` | **new** `test-{pg,mysql}-{aurora,multi-az}-async` |
| async + SQLAlchemy | `psycopg[async]` / `aiomysql` | **new** `test_sqlalchemy_async.py` | same new tasks |

Sync and async Gradle tasks are disjoint. Each pins its driver variant via the existing system-property-based gating mechanism (`exclude-*-driver`), extended with two new flags: `exclude-async-drivers` and `exclude-sync-drivers`.

### Gating at the conftest layer

Async test files (`test_*_async.py`) are parametrized only over async `TestDriver` variants; sync test files are parametrized only over sync variants. Collecting a sync file when only async drivers are allowed produces zero tests (correctly skipped), and vice versa. No file has to "know" about its sibling.

## Components

### Python — `tests/integration/container/`

**`utils/test_driver.py` — enum additions:**
```python
class TestDriver(str, Enum):
    PG = "PG"
    MYSQL = "MYSQL"
    PG_ASYNC = "PG_ASYNC"
    MYSQL_ASYNC = "MYSQL_ASYNC"

    @property
    def is_async(self) -> bool:
        return self in (TestDriver.PG_ASYNC, TestDriver.MYSQL_ASYNC)

    @property
    def engine(self) -> DatabaseEngine:
        return DatabaseEngine.PG if self in (TestDriver.PG, TestDriver.PG_ASYNC) else DatabaseEngine.MYSQL
```

The `engine` property lets existing conditional decorators and engine checks in `test_environment.py` check `test_driver.engine` instead of hardcoding sync values. No new conditions-layer code required.

**`utils/test_environment.py` — `TestEnvironmentFeatures` additions:**
- `SKIP_SYNC_DRIVER_TESTS`
- `SKIP_ASYNC_DRIVER_TESTS`

Two flags, not four. Sync/async and engine are independent dimensions combined with existing `SKIP_{PG,MYSQL}_DRIVER_TESTS`. `get_allowed_test_drivers()` filters by both.

**`conftest.py`:** no structural change to `pytest_generate_tests`. Behavior changes transparently via `get_allowed_test_drivers()`.

**Test invocation convention — matches existing repo convention:**

Unit tests in `tests/unit/test_aio_*.py` use `asyncio.run(inner_async())` inside sync `def test_...` functions. No `@pytest.mark.asyncio`, no `pytest-asyncio` dependency. Async integration tests mirror this:

```python
def test_basic_async_connect(test_environment, test_driver):
    async def inner():
        async with await pg_connect_async(...) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                row = await cur.fetchone()
                assert row == (1,)
    asyncio.run(inner())
```

**Async helpers (new):**
- `pg_connect_async(host, port, user, password, db, **wrapper_kwargs) -> AsyncAwsWrapperConnection`
- `mysql_connect_async(host, port, user, password, db, **wrapper_kwargs) -> AsyncAwsWrapperConnection`
- `create_async_engine_pg(...)` / `create_async_engine_mysql(...)` — thin wrappers returning a SQLAlchemy `AsyncEngine` with the wrapper URL scheme.

All async helpers codify `await aws_advanced_python_wrapper.aio.cleanup.release_resources_async()` in their `__aexit__` path so every async test inherits the cleanup convention. This is the async counterpart of sync `aws_advanced_python_wrapper.cleanup.release_resources()` that `CLAUDE.local.md` calls out as consumer-critical.

**Test file naming — sibling files only:**

One new file per existing sync file worth porting. Current sync test surface (from `tests/integration/container/`):

| Sync file | Async twin |
|---|---|
| `test_basic_connectivity.py` | `test_basic_connectivity_async.py` |
| `test_basic_functionality.py` | `test_basic_functionality_async.py` |
| `test_sqlalchemy.py` | `test_sqlalchemy_async.py` |
| `test_aurora_failover.py` | `test_aurora_failover_async.py` |
| `test_read_write_splitting.py` | `test_read_write_splitting_async.py` |
| `test_iam_authentication.py` | `test_iam_authentication_async.py` |
| `test_aws_secrets_manager.py` | `test_aws_secrets_manager_async.py` |
| `test_host_monitoring_v2.py` | `test_host_monitoring_v2_async.py` |
| `test_blue_green_deployment.py` | `test_blue_green_deployment_async.py` |
| `test_custom_endpoint.py` | `test_custom_endpoint_async.py` |
| `test_autoscaling.py` | `test_autoscaling_async.py` |
| `test_failover_performance.py` | `test_failover_performance_async.py` |
| `test_read_write_splitting_performance.py` | `test_read_write_splitting_performance_async.py` |

13 sync files → 13 async twins.

### Java — `tests/integration/host/src/test/java/integration/host/`

**`TestEnvironmentFeatures` enum:** mirror the two new Python flags (`SKIP_SYNC_DRIVER_TESTS`, `SKIP_ASYNC_DRIVER_TESTS`).

**`TestEnvironmentConfiguration.java`:** read two new system properties — `exclude-sync-drivers` and `exclude-async-drivers` — and translate them into the corresponding `TestEnvironmentFeatures` values. Mechanical mirror of how `exclude-mysql-driver` → `SKIP_MYSQL_DRIVER_TESTS` works today. The features are serialized into `TEST_ENV_INFO_JSON` consumed by the Python side.

**No changes to `TestRunner.java` or `TestEnvironment.java`.** Driver selection happens via features → Python filtering; cluster provisioning, Docker launch, IP-allowlist handling are reused as-is.

### Gradle — `tests/integration/host/build.gradle.kts`

**Four new tasks:**
- `test-pg-aurora-async`
- `test-mysql-aurora-async`
- `test-pg-multi-az-async`
- `test-mysql-multi-az-async`

Each task mirrors its sync twin's system properties and adds `systemProperty("exclude-sync-drivers", "true")`.

**Existing tasks get one line added, each:** `systemProperty("exclude-async-drivers", "true")`. This preserves current behavior (sync-only) now that the `TestDriver` enum grows. Deliberate behavior-preserving touch.

### Dependencies

Verification pass on `pyproject.toml` (performed 2026-04-24):
- `aiomysql = ">=0.2"` — **present** in `[tool.poetry.group.dev.dependencies]`. Verify during implementation whether the integration-test Docker container installs the dev group; if it doesn't, move aiomysql to the `test` group or add it there.
- `psycopg = "^3.3.3"` — present in both dev and test groups; psycopg 3 includes async natively (`psycopg.AsyncConnection`).
- `SQLAlchemy = "^2.0.49"` — present in dev; `greenlet >= 3` is in test (required for SA async under sync call sites).
- `pytest-asyncio` — **not present** and **not needed** (we use `asyncio.run` inside sync test functions, matching the existing `tests/unit/test_aio_*.py` convention).

**Expected change to `pyproject.toml`:** at most a group move for `aiomysql` if the integration container doesn't install dev deps. No new packages.

### `pyproject.toml` pytest config

Leave `asyncio_mode` unset (default `strict`). No global config change. The existing `filterwarnings` list (lines 100–104) is the extension point if async-specific warnings surface during Step 6-8.

## Data flow

1. **Developer / CI** sets env vars (`REUSE_RDS_DB`, `RDS_DB_DOMAIN`, `AWS_*`, etc.) and invokes `./gradlew test-pg-aurora-async`.
2. **Gradle task** sets system properties including `exclude-sync-drivers=true` and the engine-specific `exclude-*` flags.
3. **Java `TestRunner` / `TestEnvironment`** builds `TestEnvironmentRequest`, reuses cluster per `REUSE_RDS_DB=true`, launches Docker test container.
4. **Java `TestEnvironmentConfiguration`** translates system properties into `TestEnvironmentFeatures` set, serializes cluster info + features into `TEST_ENV_INFO_JSON`.
5. **Docker test container** runs `poetry run pytest -vvvvv tests/integration/container/` (via `ContainerHelper`).
6. **Python `conftest.py` / `TestEnvironment.get_current()`** deserializes `TEST_ENV_INFO_JSON`, computes `get_allowed_test_drivers()` — returns `[PG_ASYNC]` (or `[MYSQL_ASYNC]`) given the active feature flags.
7. **`pytest_generate_tests`** parametrizes every test's `test_driver` fixture over that single value.
8. **Sync test files** collect → match zero async drivers → skip cleanly.
9. **Async test files** collect → run against `PG_ASYNC` / `MYSQL_ASYNC` with `asyncio.run(inner)` invoking `AsyncAwsWrapperConnection` / `create_async_engine` against the user's real Aurora Serverless v2 cluster.
10. **Teardown** — async helper fixtures call the async `release_resources()` path.

## Error handling

- **Connection refused / network unreachable during Step 0 or first async run:** most likely cause is the security-group allowlist (see Risks §6). Not masked — surfaces as a pytest error.
- **Driver-level warnings under `-Werror`:** triage when they appear. Fix at source first; targeted `filterwarnings` entry in `pyproject.toml` is the fallback, not the default.
- **Async teardown leaks:** caught by `-Werror` as `ResourceWarning`. Fixtures must await `release_resources()`-equivalent paths; no GC reliance.
- **psycopg `AsyncAwsWrapperConnection` unwrap requirement:** prerequisite fix is in commit `2071607` on this branch. Regression would manifest as `AttributeError` in PG-async type introspection.

## Testing approach

Testing the test harness itself:
- **Commit 4 smoke:** after Gradle tasks land, running `./gradlew test-pg-aurora-async` with no async test files present should produce zero tests and exit 0. Proves gating works without needing test content.
- **Commit 6 smoke:** first actual async connection against the user's PG cluster. If this passes, the wrapper + psycopg-async + helper + conftest wiring are all working.
- **Per-commit verify:** each feature-port commit (8a–8g) should pass its own Gradle run before the next commit is added. Keeps bisection clean if a later commit regresses an earlier one.

## Baseline and commit plan

### Step 0 — sync baseline (not a commit)

Before any code change, run existing sync integration against user's two Aurora Serverless v2 clusters. Diagnostic only, not a PR artifact.

```bash
export REUSE_RDS_DB=true
export RDS_DB_REGION=us-west-2
export DB_USERNAME=...
export DB_PASSWORD=...
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# PG run
export RDS_DB_NAME=<pg-cluster-id>
export RDS_DB_DOMAIN=<pg-cluster>.xxx.us-west-2.rds.amazonaws.com
(cd tests/integration/host && ./gradlew test-pg-aurora)

# MySQL run
export RDS_DB_NAME=<mysql-cluster-id>
export RDS_DB_DOMAIN=<mysql-cluster>.xxx.us-west-2.rds.amazonaws.com
(cd tests/integration/host && ./gradlew test-mysql-aurora)
```

**⚠️ Pre-Step-0 checklist (mandatory before launching):**
1. Verify the test runner's outbound IP is already in both Serverless clusters' security groups — `REUSE_RDS_DB=true` skips IP allowlist automation.
2. Warm both clusters with a trivial query (e.g., `SELECT 1` via `mysql`/`psql` CLI) before the first Gradle invocation. Serverless v2 cold-start from 0 ACU can exceed a test's 600s `--timeout`.
3. Confirm `DB_USERNAME` has the privileges the integration tests assume (creating tables, etc. — exact list in existing fixtures).

**Triage rules for Step 0 failures:**
- *Environmental skip* (single-writer topology skipping multi-instance tests, IAM auth skipping because cluster isn't IAM-enabled, BG/Global-DB tests skipping due to topology mismatch) → note as environment-specific, not a regression. Document in PR description.
- *Sync regression introduced by async-parity commits* → fix in a separate `fix:` commit at the top of the sequence, before any async-integration commit.
- *Transient / infra* (cluster paused, SG mismatch, IAM not configured) → unblock and re-run.

Only proceed to the commit sequence once Step 0 is green or its failures are classified.

### Commit sequence (inside a single PR)

Each commit is self-contained and leaves the tree in a working state.

1. **`chore(integration): verify and adjust async test deps`** — only if the verification pass discovers aiomysql needs to move from dev to test group (or other small dep adjustment). Skipped if no change is needed.
2. **`feat(integration): extend TestDriver enum with async variants`** — `PG_ASYNC` / `MYSQL_ASYNC` + `is_async` / `engine` properties. Python `TestEnvironmentFeatures` gains `SKIP_SYNC_DRIVER_TESTS` / `SKIP_ASYNC_DRIVER_TESTS`. `is_test_driver_allowed` / `get_allowed_test_drivers` respect new flags.
3. **`feat(integration): mirror async driver gating in Java host`** — Java `TestEnvironmentFeatures` + `TestEnvironmentConfiguration` read `exclude-sync-drivers` / `exclude-async-drivers` system properties and pipe them into `TEST_ENV_INFO_JSON`.
4. **`feat(integration): add async Gradle tasks and preserve sync task behavior`** — 4 new tasks (Aurora + Multi-AZ × PG + MySQL); existing sync tasks gain `exclude-async-drivers=true`. Running the new tasks with no async files still present exits clean.
5. **`feat(integration): add async connection and SQLAlchemy engine helpers`** — `pg_connect_async`, `mysql_connect_async`, async SQLAlchemy engine builders. Async `release_resources()` teardown codified in helpers.
6. **`test(integration): add async basic connectivity tests`** — `test_basic_connectivity_async.py`. First end-to-end async run against real cluster. This is the plumbing validator.
7. **`test(integration): add async SQLAlchemy tests`** — `test_sqlalchemy_async.py`.
8. **Feature-by-feature commits**, roughest-to-most-specific so blast-radius is contained:
   - `test(integration): add async basic functionality tests`
   - `test(integration): add async aurora failover tests`
   - `test(integration): add async read/write splitting tests`
   - `test(integration): add async IAM auth tests`
   - `test(integration): add async AWS Secrets Manager tests`
   - `test(integration): add async host monitoring tests`
   - `test(integration): add async custom endpoint tests`
   - `test(integration): add async autoscaling tests`
   - `test(integration): add async blue/green deployment tests`
   - `test(integration): add async performance tests` (failover + RWS)
9. **`docs(integration): document async test tasks and axis`** — update `docs/development-guide/IntegrationTests.md`: new Gradle tasks, when to use them, Serverless v2 caveats, the SG-allowlist reminder.

## Risks

**1. Serverless v2 topology dictates which async tests execute.** Multi-instance tests (`@enable_on_num_instances(min_instances=2)`, failover) need a reader to promote. Serverless v2 without reader autoscaling skips those. Global DB / Blue-Green tests require specific engine versions or cluster topology. Skipped tests are environmental, not regressions. The PR description should enumerate which async tests ran vs. skipped environmentally.

**2. aiomysql + `-Werror`.** aiomysql is actively maintained (0.3.2 released 2025-10-22, commits through 2025-12). Like any third-party async library, it may emit `DeprecationWarning` / `ResourceWarning` in some paths which `-Werror` would turn into failures. Triage when they appear — fix at source, then targeted `filterwarnings` only as last resort.

**3. Full parity includes Toxiproxy-gated ports.** Every sync test file gets an async twin, including Toxiproxy-gated ones (network-outage failover, some EFM tests). They execute under the Docker harness path identically to sync; against the user's Serverless clusters they skip environmentally via the same `NETWORK_OUTAGES_ENABLED` feature gate sync uses. Ports exist for parity, not Step 0 execution.

**4. psycopg-async unwrap fix is load-bearing.** Commit `2071607` on this branch (unwrapping `AsyncAwsWrapperConnection` before `psycopg.TypeInfo.fetch`) is prerequisite. First validation: commit 6 basic-connectivity-async PG run.

**5. Serverless v2 cold-start + 600s timeout.** Scale-from-zero can add 30–60s to first connection. Warm the cluster in Step 0 before timing-sensitive runs.

**6. SG allowlisting for `REUSE_RDS_DB=true`.** The harness does NOT authorize the runner's IP when reusing clusters. Runner IP must already be in the cluster SG. First failure mode otherwise is connection-refused. **Flagged in the Step 0 pre-checklist.**

**7. Async `release_resources_async()` discipline.** Mirror the sync convention: async teardown calls `aws_advanced_python_wrapper.aio.cleanup.release_resources_async()` via `__aexit__` in helpers. Codified in commit 5 so every subsequent async test inherits it.

## What's deliberately not in the sequence

- No CHANGELOG commit (handled outside this PR).
- No debug-task variants.
- No `asyncmy` or alternative driver support.
- No cross-parametrization of sync tests.
- No new pytest-asyncio dependency.
