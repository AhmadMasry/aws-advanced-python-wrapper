# Task 1 — Async plugin factory registry + Custom endpoint monitor + Federated/Okta

**Date:** 2026-04-20
**Status:** Draft — post-3.0.0 follow-up.
**Target release:** 3.1.0 (additive minor).
**Branch:** `feat/post-3.0-followups` (off F3-B final).
**Depends on:** F3-B SP-0 through SP-10.

## Goal

Close the remaining gaps between the sync plugin story and 3.0.0's async story:

1. **Plugin factory registry** — let users configure async plugins via the usual `plugins="failover,efm"` connection-property string instead of instantiating plugin classes in code and passing them to `AsyncAwsWrapperConnection.connect(plugins=[...])`.
2. **Async custom endpoint monitor** — port the sync `CustomEndpointMonitor` to `asyncio.Task` so apps using Aurora custom endpoints get member-instance resolution on the async path.
3. **Async federated (SAML) auth plugin** — port `FederatedAuthPlugin`.
4. **Async Okta auth plugin** — port `OktaAuthPlugin`.

## Non-goals

- Async MySQL / asyncpg drivers (Tasks 2 and 3).
- Removing the explicit `plugins=[...]` parameter of `AsyncAwsWrapperConnection.connect` (it stays supported alongside the registry path).
- Auth-plugin UI / secret rotation — plugins call the remote IdP and hand the resulting token to the DBAPI connect.

## Sub-project decomposition

### T1-A — Async plugin factory registry

**What gets built:**
- `AsyncPluginFactory` Protocol mirroring sync `PluginFactory`.
- `AsyncPluginManager.PLUGIN_FACTORIES` dict mapping plugin code strings (`failover`, `efm`, `read_write_splitting`, `iam`, `aws_secrets_manager`, `federated_auth`, `okta`, `custom_endpoint`, `aurora_connection_tracker`, `connect_time`, `execute_time`, `dev`) to concrete factory classes.
- Factory classes for every async plugin shipped in F3-B + new plugins from T1-B/C/D.
- `AsyncAwsWrapperConnection.connect` grows a path that reads the `plugins` connection property, looks up factories, and builds the list — same precedence as sync today (explicit `plugins=[...]` kwarg wins if both provided).
- Auto-sort support mirroring sync `WrapperProperties.AUTO_SORT_PLUGIN_ORDER`.

**Test coverage:** parse `"failover,efm"` → correct plugin instances in correct order; unknown plugin code raises clearly; explicit `plugins=[...]` kwarg takes precedence over the property; auto-sort orders by weight.

**Estimated effort:** ~1 commit (~150 LOC source + ~100 LOC test).

### T1-B — Async custom endpoint monitor

**What gets built:**
- `AsyncCustomEndpointMonitor` — `asyncio.Task` that periodically calls AWS RDS `DescribeDBClusterEndpoints` (via boto3 in `asyncio.to_thread`), parses the member-instance list, updates the plugin service's topology filter.
- `AsyncCustomEndpointPlugin` (replaces the stub from SP-8):
  - Subscribes to `CONNECT`.
  - On initial connect, starts the monitor (if not already running for this cluster).
  - On `CanReleaseResources`, stops the monitor.
- Shares connection properties with sync plugin: `custom_endpoint_monitor_expiration_ms`, `custom_endpoint_info_refresh_rate_ms`, etc.

**Test coverage:** monitor lifecycle (start/stop); member list extraction from `DescribeDBClusterEndpoints` response; plugin pipeline integration; cleanup on `release_resources_async`.

**Estimated effort:** ~1 commit (~200 LOC source + ~150 LOC test).

### T1-C — Async federated authentication plugin

**What gets built:**
- `AsyncFederatedAuthPlugin` subclassing `AsyncAuthPluginBase` from SP-7.
- `_resolve_credentials` flow:
  1. Fetch SAML assertion from IdP (ADFS or generic SAML endpoint) via HTTP. Choice: `aiohttp` (native async) vs `requests`-in-`asyncio.to_thread` (no new dep). T1-C spec picks `aiohttp` — already a transitive dep via some boto ecosystem packages; keeps the event loop unblocked for the full HTTP round-trip including large SAML assertions.
  2. Exchange SAML assertion for AWS STS temporary credentials via `asyncio.to_thread(boto3.client('sts').assume_role_with_saml)`.
  3. Use STS creds to generate an RDS IAM token (same path as `AsyncIamAuthPlugin`).
- Shared connection properties with sync: `db_user`, `idp_endpoint`, `idp_username`, `idp_password`, `iam_role_arn`, `iam_idp_arn`, `iam_region`, `idp_name`, `http_request_connect_timeout`, `ssl_secure`.
- Caches the RDS IAM token like `AsyncIamAuthPlugin` does.

**Test coverage:** mock IdP HTTP response + mock STS + mock RDS token → plugin injects password; caching within token TTL; timeout on IdP unreachable; SSL flag honored.

**Estimated effort:** ~1 commit (~250 LOC source + ~200 LOC test). Most complex of T1.

### T1-D — Async Okta authentication plugin

**What gets built:**
- `AsyncOktaAuthPlugin` subclassing `AsyncAuthPluginBase`.
- `_resolve_credentials` flow:
  1. Authenticate to Okta's `/api/v1/authn` endpoint with username/password. Get session token.
  2. Launch Okta app session with session token → receive SAML assertion via Okta SSO URL.
  3. From here, same `assume_role_with_saml` + RDS IAM token path as T1-C.
- HTTP client choice follows T1-C (aiohttp).
- Shared props: `idp_endpoint`, `app_id`, `okta_*` params identical to sync plugin.

**Test coverage:** mock Okta `/api/v1/authn` → session token; mock app SSO → SAML assertion; mock STS → RDS token; full end-to-end credential resolution.

**Estimated effort:** ~1 commit (~200 LOC source + ~150 LOC test). Smaller than T1-C because the core STS/RDS path is shared.

## Sequencing

Recommended serial order: **T1-A → T1-B → T1-C → T1-D**.

- T1-A unlocks configuration-string usage across all async plugins — biggest user-facing win, independent of the others.
- T1-B is self-contained; can slip to last if users don't need custom endpoints.
- T1-C and T1-D share the STS + RDS token path — doing T1-C first gives T1-D a shared helper.

Parallel execution feasible: T1-B, T1-C, T1-D can all start after T1-A. T1-C and T1-D share a helper — doing them back-to-back is cheaper.

## Cross-cutting decisions to make before implementation

1. **HTTP client choice**: `aiohttp` vs `requests + asyncio.to_thread`. Recommendation: aiohttp — one new dep, keeps the whole auth flow non-blocking, avoids starving the event loop on large SAML responses.
2. **T1-A naming**: `plugins` property stays as-is for string input. Explicit instance list stays as `plugins=[...]` kwarg. Collision resolution: kwarg wins.
3. **T1-B interaction with topology monitor**: Custom endpoint monitor and topology monitor are distinct; they can coexist. Custom endpoint resolves a cluster URL to a **subset of** instances; topology monitor lists all instances in the cluster.

## Success criteria

- `AsyncAwsWrapperConnection.connect(..., plugins="failover,efm,iam")` works end-to-end.
- Custom endpoint monitor drives member-instance filtering and stops cleanly on `release_resources_async`.
- `AsyncFederatedAuthPlugin` generates RDS IAM tokens via ADFS/SAML flow.
- `AsyncOktaAuthPlugin` does the same via Okta's SSO endpoints.
- All four sub-tasks land with unit-test parity against the sync equivalents where a direct test translation is possible.
- No driver-specific imports leak into plugin code (invariant 8a holds).
