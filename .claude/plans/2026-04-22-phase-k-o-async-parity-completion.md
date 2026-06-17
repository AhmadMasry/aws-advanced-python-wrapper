# Phases K–O: Async Parity Completion Plan

**Date:** 2026-04-22
**Branch:** `feat/async-parity`
**Scope:** Close every remaining async-vs-sync gap identified post-Phase J so the upstream PR can claim full feature parity (not just hot-path parity).

## Decisions (signed off 2026-04-22)

1. **Scope:** execute all of K–O in this branch.
2. **BlueGreen:** full port ships in this PR (not feature-flagged, not deferred).
3. **CustomEndpoint wait-for-info contract:** realign with sync — raise on timeout instead of returning a stale-filter connection. This REVERSES the softer contract documented in commit `470584f` and requires updating `UsingTheCustomEndpointPlugin.md` to remove the async-behavior-difference note.
4. Plan persisted here per project convention.

## Sequencing rationale

K first because the `AsyncConnectionProvider` / `AsyncSessionStateService` / IdP-registry abstractions are load-bearing for L, M, and N. L and M are the BlueGreen-heavy phases. N is the wire-up/polish pass that depends on K's abstractions. O is ratification.

## Phase K — Infrastructure foundations

**Goal:** port the three sync abstractions that later phases need. Nothing user-visible lands from K alone — it's plumbing.

### K.1 — AsyncConnectionProvider / AsyncConnectionProviderManager

- Port `connection_provider.ConnectionProvider` protocol to an async variant: `connect`, `accepts_host_info`, `accepts_strategy`, `get_host_info_by_strategy`.
- Port `connection_provider.ConnectionProviderManager` registry: default-provider slot + chain-of-responsibility lookup, accessors that mirror sync (`accepts_strategy`, `accepts_host_info`, `get_host_info_by_strategy`).
- Default provider: `AsyncDriverConnectionProvider` (wraps the existing target-driver-func direct path).
- Plumb through `AsyncPluginService.connect` so fresh connections route via the manager, matching sync.
- Tests: registry semantics, strategy dispatch, provider chaining, fall-through to default.

### K.2 — AsyncSessionStateService

- Port `session_state_service.SessionStateService` to async. Capture/restore set: autocommit, transaction_isolation, read_only, catalog, schema (driver-dependent subset).
- Hook into `AsyncFailoverPlugin` transition points: capture pre-failover, restore post-failover on new connection.
- Hook into `AsyncReadWriteSplittingPlugin` switch points similarly (pre-switch capture, post-switch restore).
- Tests: capture on healthy conn, restore on replacement conn, no-op when disabled, graceful skip when driver doesn't support a given property.

### K.3 — IdP factory registry for async federated auth

- Port sync's IdP factory registry pattern. Replace hardcoded `AsyncAdfsCredentialsProviderFactory` + `AsyncOktaCredentialsProviderFactory` dispatch with registry lookup keyed on `IDP_NAME` property.
- Registry supports user-supplied IdP factory classes (extension point parity with sync).
- Tests: registry dispatch, unknown IdP error path, user-registered factory, default shipped factories resolve.

### K exit criteria

- All 3 new services + managers available under `aws_advanced_python_wrapper/aio/`.
- `AsyncPluginService.connect` routes through `AsyncConnectionProviderManager`.
- Failover/RWS capture & restore session state.
- Federated/Okta plugins resolve their factory via the registry.
- All existing tests still green.

**Estimated scope:** ~1000–1400 LOC + 80–120 new tests. 2–3 sessions.

---

## Phase L — Standing monitors

**Goal:** replace the on-demand fallbacks in Limitless + FastestResponseStrategy, and land the BlueGreen status machinery that M depends on.

### L.1 — AsyncLimitlessRouterMonitor

- Port sync's `limitless_router_monitor.LimitlessRouterMonitor` to async: background `asyncio.Task` refreshing router list on `limitless_router_discovery_interval_ms` cadence.
- Shutdown via `register_shutdown_hook`.
- Integrate with `AsyncLimitlessPlugin`: replace on-demand discovery with cached router list from the monitor.
- Tests: monitor start/stop lifecycle, refresh cadence, router-list caching, plugin uses cache instead of live discovery.

### L.2 — AsyncFastestResponseStrategyMonitor

- Port sync's per-host response-time monitor loop: each host runs an `asyncio.Task` that probes response time on `response_measurement_interval_ms` cadence.
- Cache stores `(host → latest_measurement, timestamp)`.
- Replace the Random-selector fallback path in `AsyncFastestResponseStrategyPlugin` — cache miss now waits briefly for first measurement, then falls through.
- Tests: per-host task lifecycle, cache population, selector returns fastest-measured host, shutdown cleanup.

### L.3 — AsyncBlueGreenStatusMonitor + AsyncBlueGreenStatusProvider

- Port sync's `blue_green_status_monitor.BlueGreenStatusMonitor`: state machine polling Aurora BG metadata view (or equivalent endpoint) on configurable cadence, transitioning through BG phases (`NOT_CREATED`, `CREATED`, `PREPARATION`, `IN_PROGRESS`, `POST`, `COMPLETED`).
- Port `BlueGreenStatusProvider`: singleton per cluster keyed on cluster identifier, exposes `get_status()` to plugins.
- Hook into `AsyncBlueGreenPlugin.connect` to start/subscribe on initial connect.
- Shutdown via `register_shutdown_hook`.
- Tests: phase transitions from mocked metadata responses, singleton-per-cluster semantics, subscriber notification, monitor teardown.

### L exit criteria

- Three standing monitors live, feature-flag parity with sync.
- On-demand fallbacks removed or demoted to cache-miss path.
- Plugins consume monitor output.

**Estimated scope:** ~1400–2000 LOC + 120–180 new tests. 3–5 sessions. The BG status monitor is the biggest slice (~800–1200 LOC alone).

---

## Phase M — BlueGreen routing implementations

**Goal:** replace the four `NotImplementedError` routing classes in `AsyncBlueGreenPlugin` with real logic, completing the BG port.

### M.1 — ConnectRouting implementations

- Port sync's `blue_green_plugin.SubstituteConnectRoute` (BG redirects connect to substitute host during certain phases).
- Port sync's `IamHostConnectRoute` if present (IAM token regen for new host).
- Port sync's `SuspendConnectRoute` (blocks connect during IN_PROGRESS phase up to timeout).

### M.2 — ExecuteRouting implementations

- Port sync's `SuspendExecuteRoute` (blocks execute during IN_PROGRESS phase up to timeout).
- Port any remaining execute routings (e.g. rerouting to new endpoint post-switch).

### M.3 — Dispatcher polish

- Confirm `AsyncBlueGreenPlugin` dispatcher picks the right routing for each phase (match sync's routing table).
- Integration test: full phase-sweep (NOT_CREATED → CREATED → … → COMPLETED) with mocked status monitor, verify connect + execute behavior at each phase.

### M exit criteria

- No `NotImplementedError` stubs remain in `AsyncBlueGreenPlugin`.
- Phase-by-phase routing matches sync.
- BG integration tests pass end-to-end against mocked monitor.

**Estimated scope:** ~600–900 LOC + 80–120 new tests. 2–3 sessions.

---

## Phase N — Auto-wire + polish

**Goal:** engage everything that K/L/M built, and clean up remaining divergences.

### N.1 — MultiAz + GlobalAurora auto-detection

- In `AsyncAwsWrapperConnection.connect` (or dialect resolution site), pick `AsyncMultiAzHostListProvider` or `AsyncGlobalAuroraHostListProvider` based on resolved `DatabaseDialect`, matching sync's dispatch.
- Remove the "wire manually" caveat from the gap list.

### N.2 — Panic-mode auto-wire

- In `AsyncAwsWrapperConnection.connect`, when initial topology is unknown/stale, engage `ClusterTopologyMonitor.force_refresh_with_connection` + `_writer_found_event` + probe-host path.
- Timeout via existing `CLUSTER_TOPOLOGY_HIGH_REFRESH_INTERVAL_MS` or equivalent.

### N.3 — Pool-aware RWS via ConnectionProviderManager

- Replace `AsyncReadWriteSplittingPlugin._is_pool_connection`'s module-string heuristic with a query against `AsyncConnectionProviderManager` (K.1).
- Matches sync's behavior.

### N.4 — Multi-alias invalidation in AsyncOpenedConnectionTracker

- Change tracker key from canonical-alias to `HostInfo.as_aliases()` frozenset (or maintain an alias→canonical index so any alias lookup finds the tracked connections).
- Tests: invalidation triggered via any alias, not just canonical.

### N.5 — CustomEndpoint contract realignment

- `AsyncCustomEndpointPlugin.connect`: on `wait_for_info` timeout, **raise** `AwsWrapperError` (match sync) instead of returning the connection with stale filter.
- Remove the "Async wrapper behavior difference" section from `docs/using-the-python-wrapper/using-plugins/UsingTheCustomEndpointPlugin.md` (reverts commit `470584f`).
- CHANGELOG: note this under `### :crab: Breaking Changes` since it changes a currently-shipped (v3.x) async behavior. If we consider async still pre-1.0, downgrade to `### :crab: Changed`; flag for you at implementation time.

### N.6 — Telemetry counter completeness audit

- Walk each async plugin, cross-reference sync for counter/gauge emissions, fill in any gaps.
- Common gaps from prior review: topology refresh counters, provider-dispatch counters, session-state capture/restore counters.

### N exit criteria

- `AsyncAwsWrapperConnection.connect` picks the right host-list provider and engages panic mode automatically.
- RWS pool detection uses the provider manager.
- Tracker invalidation works for all aliases.
- CustomEndpoint matches sync on timeout.
- Telemetry surface equivalent to sync on a counter-by-counter basis.

**Estimated scope:** ~500–800 LOC + 80–120 new tests. 2 sessions.

---

## Phase O — Verification + docs

**Goal:** prove parity and prepare the upstream PR.

### O.1 — aiomysql end-to-end

- Wire aiomysql through the existing integration-test harness (or skeleton one if none exists for async). Run against a real MySQL cluster.
- Target tests: failover, RWS, IAM (with `use_pure` trade-off surfaced per README), custom endpoint, EFM.
- Note: IAM + MySQL constraint per `CLAUDE.local.md` — surface in test documentation.

### O.2 — Re-run P0/P1/P2 audit

- Execute the same audit method used pre-Phase-A against the post-N codebase. Expectation: zero remaining gaps.
- If new gaps surface (likely — implementation always uncovers something), append to this plan under "Phase P deferred" or address inline if trivial.

### O.3 — Documentation pass

- Every new async feature gets a doc note in the corresponding `docs/using-the-python-wrapper/using-plugins/UsingThe<Name>Plugin.md`, matching the sync doc style.
- Update `docs/using-the-python-wrapper/UsingThePythonWrapper.md` async section with the final feature matrix.
- `CHANGELOG.md`: batch all K–O entries under the right emoji categories, linking PR URL (TBD at PR-creation time).

### O.4 — PR preparation

- Final `git rebase origin/main`, squash-merge readiness check.
- PR body drafts the squashed commit message per `.github/PULL_REQUEST_TEMPLATE.md`.
- Call out in PR description: "Delivers async parity with sync for all shipped v3.x features."

### O exit criteria

- aiomysql integration tests green.
- Audit clean.
- Docs complete.
- PR body ready.

**Estimated scope:** ~300–500 LOC (mostly tests + docs) + integration-test infra. 1–2 sessions.

---

## Totals

- **LOC:** ~3800–5600 across phases.
- **Tests:** ~450–650 new unit tests + aiomysql integration suite.
- **Sessions:** 10–15 depending on depth, iteration on reviews, and discovered complexity in BlueGreen.

## Risk register

| Risk | Mitigation |
|------|-----------|
| BlueGreen status monitor complexity exceeds estimate | Fall back to feature-flagged BG ship (revisit decision 2) — but this is not the current plan. |
| CustomEndpoint realignment breaks consumers relying on current async behavior | Low risk: async contract is ~8 weeks old, not yet in a tagged release. Flag in CHANGELOG under Breaking Changes or Changed depending on release status at PR time. |
| aiomysql E2E surfaces latent bugs | Expected; budget a half-session for fixes. Don't scope-creep into MySQL-specific async improvements. |
| Session-state service conflicts with existing failover capture | Prototype K.2 against an AsyncFailoverPlugin mock first. |
| Upstream review bounces on PR size | Pre-warn with a brief issue before opening the PR; the PR body explicitly enumerates the feature matrix. |

## Tracking

- Per-phase plans (K, L, M, N, O) will be spun out into dedicated `.claude/plans/2026-04-NN-phase-X-*.md` files as they start, mirroring the phase-A-through-J convention.
- Each phase follows the session pattern: TDD red-green-commit per task, subagent dispatch (implementer + spec reviewer + code quality reviewer), squash-friendly commit history.
- Memory updates when decisions or surprises worth persisting come up.
