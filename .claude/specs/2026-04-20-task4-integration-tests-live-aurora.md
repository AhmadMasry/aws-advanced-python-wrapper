# Task 4 — Integration tests against live Aurora

**Date:** 2026-04-20
**Status:** Draft — post-3.0.0 follow-up.
**Target release:** 3.1.0 or later (landing alongside Task 1 / Task 2 / Task 3 as each ships).
**Branch:** `feat/post-3.0-followups` (off F3-B final).
**Depends on:** F3-B final branch; Tasks 2/3 add more parametrize cases but Task 4 can start with PG-only coverage immediately.

## Goal

End-to-end integration tests for the async path on live Aurora PostgreSQL (and, once Tasks 2–3 ship, Aurora MySQL and asyncpg too). Mirror F1's `tests/integration/container/test_sqlalchemy.py` pattern. Integration tests run maintainer-triggered via `.github/workflows/integration_tests.yml`, not on every PR.

## Non-goals

- Replacing unit tests — they stay as the fast-feedback layer.
- Testing against non-Aurora PostgreSQL / MySQL — out of scope; use existing sync integration tests for those.
- Running integration tests on CI for every push — cost/time prohibitive and not the project's convention.

## Scope

Six test scenarios for the async path, each parametrized across installed drivers:

1. **Basic connect + query** — open engine, run `SELECT 1`, close. Baseline.
2. **Failover** — open engine with `failover` plugin, run workload, trigger writer failover, verify `sqlalchemy.exc.OperationalError` (mapped from `FailoverSuccessError`) on next query, verify retry succeeds against new writer.
3. **EFM / host monitoring** — `failover,efm`. Disable connectivity to a reader via Toxiproxy; verify EFM detects outage and aborts connection within threshold.
4. **Read/write splitting** — `read_write_splitting`. Flip `postgresql_readonly` / `mysql_readonly`; verify reader instance id differs from writer's; flip back and verify writer returned.
5. **IAM authentication** — `iam` plugin. Connect with `user=` but no `password=`; verify token-based connection succeeds.
6. **Custom endpoint** (once Task 1-B ships) — `custom_endpoint,failover`. Connect to a custom endpoint; verify topology filter restricts to custom-endpoint members.

## Test matrix

| Driver | SA URL scheme | Aurora engine | Ships |
|---|---|---|---|
| psycopg async | `aws_wrapper_postgresql+psycopg_async` | Aurora PG | F3-B (already ready for tests) |
| aiomysql | `aws_wrapper_mysql+aiomysql_async` | Aurora MySQL | Task 2 |

Each scenario × each driver = up to 12 parametrize cases. `@pytest.mark.skip_if` gates cases when the corresponding driver isn't installed in the test environment.

asyncpg and asyncmy rows dropped from the original matrix per the roadmap's scope decision.

## Sub-project decomposition

### T4-A — Shared test harness + PG psycopg_async baseline

**What gets built:**
- `tests/integration/container/test_aio_sqlalchemy.py`:
  - Helper factories: `_build_async_engine(driver, plugins)`, `_execute_scenario_query(conn)`.
  - Reuses existing conftest fixtures (`conn_utils`, `aurora_utility`, `test_driver`, `test_environment`) from sync integration tests — they're driver-agnostic at the fixture level.
  - Six scenarios implemented as standalone async test functions.
  - Parametrize currently over one driver: `psycopg_async`.
- `.github/workflows/integration_tests.yml`:
  - New job `test-async-aurora-pg` that installs the wrapper + psycopg + greenlet, runs `pytest tests/integration/container/test_aio_sqlalchemy.py -k "psycopg_async"`.
  - Job guarded by the same AWS credential / cluster setup the sync Aurora integration tests already use.

**Estimated effort:** ~1 commit. Test file ~300 LOC; workflow changes minimal.

### T4-C — Add aiomysql driver (depends on Task 2)

New job `test-async-aurora-mysql` running the same six scenarios against `aiomysql`. Test body identical — only URL scheme + `mysql_readonly` vs `postgresql_readonly` differ. Test harness from T4-A abstracts this.

**Estimated effort:** ~1 commit once Task 2 is green. ~80 LOC test additions + workflow job.

### T4-D — Add Task 1 scenarios

After Task 1-B (custom endpoint) and Task 1-C/D (federated / Okta) ship, add:
- Custom endpoint scenario.
- Federated auth scenario (requires IdP fixture — decide whether real ADFS test infra or mock IdP for integration layer).
- Okta scenario (same question).

**Estimated effort:** ~1 commit once Task 1 is green. Size depends on IdP fixture decisions.

## Cross-cutting decisions

1. **Test runner's Python version**: integration tests run on one version per matrix job. Pick 3.13 for parity with sync integration tests, or include both 3.13 and 3.14 to surface 3.14-specific issues early. Recommendation: 3.13 baseline for matrix economy, add 3.14 separately as a smoke-test job.
2. **Failover trigger mechanism**: reuse existing `aurora_utility.failover_cluster_and_wait_until_writer_changed()` from sync integration tests — driver-agnostic.
3. **Assertion style**: use `sqlalchemy.exc.OperationalError` catches, match F3-B's public contract. Don't reach into `AsyncAwsWrapperConnection` internals except where testing specific plugin-state behavior.
4. **Timeout tuning**: async tests run FAST compared to sync (psycopg's async path doesn't wait on thread-pool scheduling). Halve the sync-side timeout defaults.
5. **Toxiproxy**: sync EFM tests use Toxiproxy for network-outage simulation. Same container works for async tests — no changes needed.

## Running locally

A maintainer / contributor with AWS credentials can run the async integration suite via the same gradle harness the sync tests use:

```bash
./gradlew --no-parallel test-python-3.13-mysql-async
./gradlew --no-parallel test-python-3.13-pg-async
```

New gradle targets `test-python-<ver>-<engine>-async` added alongside the existing sync targets.

## Success criteria

- T4-A integration tests run green against live Aurora PG using psycopg_async.
- T4-B extends the matrix to asyncpg, runs green.
- T4-C extends to aiomysql + asyncmy on Aurora MySQL, runs green.
- T4-D adds custom-endpoint + federated + Okta scenarios once Task 1 lands.
- Workflow jobs documented in `integration_tests.yml` and runnable via gradle target parity with sync.
- Any failover regression in the async path is caught by T4 before shipping a release.

## Sequencing within the post-3.0 stream

T4-A can start **immediately** against F3-B. T4-B, T4-C, T4-D gate on their corresponding feature tasks.

Recommended interleaving:
1. Task 1-A (plugin registry) — user-facing win, independent.
2. T4-A (integration baseline) — validates F3-B's async path is solid before extending.
3. Task 2 (aiomysql) + T4-C folded in.
4. Task 1-B/C/D (custom endpoint + SAML/Okta) + T4-D folded in.

This order ships validation for each new feature as part of the feature itself rather than batching all tests at the end.

## Risks

- **Flakiness**: live-Aurora tests are inherently flakier than mocks. Plan for retry logic in scenario-level test bodies (not at the pytest level — that hides real failures).
- **AWS cost**: each run spins up / tears down Aurora clusters. Budget in the maintainer workflow for cost caps; skip Task 4 runs on fork PRs by default.
- **Credential management**: the workflow's AWS role permissions must cover `rds:FailoverCluster` + `rds:GenerateAuthToken` + Secrets Manager read for T4's IAM and Secrets Manager scenarios. Document the IAM policy in the workflow comments.
