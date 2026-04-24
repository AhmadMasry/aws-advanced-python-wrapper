# Async-vs-Sync Parity Audit — 2026-04-22

**Method:** file-by-file diff between `aws_advanced_python_wrapper/` (sync) and `aws_advanced_python_wrapper/aio/` (async). LOC from `wc -l`. Verified all called-out known-incomplete items by reading the actual code.

## Summary

| Category | Count | LOC estimate |
|---|---|---|
| Missing files entirely | 5 | ~1,030 |
| Missing classes/methods within existing files | ~25 symbols | ~2,200 |
| Behavioral divergences | 5 | ~150 |
| Known skeletons (confirmed) | 3 | ~2,200 |
| **Grand total to close all gaps** | | **~5,580 LOC** |

Split by severity:
- **Must-fix (crashes or silent divergence):** BlueGreen routing stubs + CustomEndpoint realignment ≈ 400 LOC
- **Parity with sync (every item below exists in sync today; missing in async is a feature regression vs sync):** everything else ≈ 5,180 LOC

Note: there is no "nice to have" tier. Every gap below is a sync feature. If the PR is scoped for parity, all 5,580 LOC is in scope. SqlAlchemy provider/dialect are the only items that are intentionally sync-only (SQLAlchemy is a sync ORM).

---

## 1. Missing files entirely

| Sync file | Sync LOC | Status | Notes |
|---|---:|---|---|
| `connection_provider.py` | 247 | MISSING | `ConnectionProvider` protocol + `DriverConnectionProvider` + `ConnectionProviderManager`. Async uses target-driver-func directly; no provider registry. |
| `states/session_state.py` | 97 | MISSING | `SessionState` + `SessionStateField[T]` dataclasses. |
| `states/session_state_service.py` | 221 | MISSING | `SessionStateService` protocol + `SessionStateTransferHandlers` + `SessionStateServiceImpl`. Async plugin_service has no session_state_service property. |
| `sql_alchemy_connection_provider.py` | 207 | MISSING | SQLAlchemy-specific provider. Likely not blocking for async; SQLAlchemy is sync-only consumer. Mark as N/A. |
| `sqlalchemy_driver_dialect.py` | 127 | MISSING | Same as above — SQLAlchemy driver dialect. N/A for async. |
| `reader_failover_handler.py` | 258 | N/A | Used only by sync `failover_plugin.py` (v1). Async uses v2 approach, inline. OK. |
| `writer_failover_handler.py` | 350 | N/A | Same as above. OK. |
| `allowed_and_blocked_hosts.py` | 29 | UNUSED | `AllowedAndBlockedHosts` dataclass exists in sync, unreferenced in async. Async plugin_service lacks the property. Fix: expose the property, import the shared dataclass. ~15 LOC. |

**Net missing (real gaps):** `connection_provider.py` + `session_state.py` + `session_state_service.py` + `allowed_and_blocked_hosts` property = ~580 LOC.

---

## 2. Within-file gaps

### 2.1 `blue_green_plugin.py` → `aio/blue_green_plugin.py`

Sync: 1,958 LOC, 21 classes. Async: 361 LOC, 16 classes.

Missing from async:
- **`BlueGreenStatusMonitor`** (414 LOC, sync line 796–1209). State machine polling Aurora BG metadata, phase transitions.
- **`BlueGreenDbStatusInfo`** (8 LOC). Data holder used by monitor.
- **`BlueGreenStatusProvider`** (737 LOC, sync line 1218–1954). Singleton per cluster, subscriber notifications, phase lookup.
- **`PhaseTimeInfo`** (3 LOC).
- **4 routing classes raise `NotImplementedError`** at async lines 194, 208, 221, 246: `SubstituteConnectRouting`, `SuspendConnectRouting`, `SuspendUntilCorrespondingHostFoundConnectRouting`, `SuspendExecuteRouting`.

Port estimate: ~1,200 LOC for status monitor/provider + ~400 LOC for the 4 routing class bodies + integration wiring = **~1,600 LOC**.

### 2.2 `limitless_plugin.py` → `aio/limitless_plugin.py`

Sync: 546 LOC, 7 classes. Async: 257 LOC, 1 class.

Missing: `LimitlessRouters`, `LimitlessRouterMonitor` (123 LOC), `LimitlessQueryHelper`, `LimitlessContext`, `LimitlessRouterService`.

Port estimate: **~290 LOC**.

### 2.3 `fastest_response_strategy_plugin.py` → `aio/fastest_response_strategy_plugin.py`

Sync: 367 LOC, 5 classes. Async: 286 LOC, 1 class.

Missing: `ResponseTimeHolder`, `HostResponseTimeMonitor` (158 LOC), `HostResponseTimeService` (53 LOC). `FastestResponseStrategyPluginFactory` is centralized in async `plugin_factory.py` — not a gap.

Port estimate: **~220 LOC**.

### 2.4 `host_monitoring_v2_plugin.py` → `aio/host_monitoring_plugin.py`

Sync: 535 LOC, 5 classes. Async: 275 LOC, 1 class (with `AsyncHostMonitoringV2Plugin` alias).

Missing: `MonitoringContext`, `HostMonitorV2`, `MonitorServiceV2`. Async consolidated these into a single standing-task-per-plugin design. Behaviorally equivalent for single-connection async usage; sync's multi-context design is for thread-pool reuse that doesn't apply to asyncio.

Port estimate: N/A as-is — async design is intentional. Could add a brief note in the plugin docstring confirming the architectural divergence is intentional.

### 2.5 `plugin_service.py` → `aio/plugin_service.py`

Sync: 1,224 LOC, 5 classes. Async: 525 LOC, 2 classes.

Missing methods on `AsyncPluginService` (Protocol + Impl):
- `allowed_and_blocked_hosts` property + setter (~20 LOC).
- `session_state_service` property (~5 LOC, depends on gap #1).
- `is_in_transaction` property (~10 LOC).
- `update_in_transaction` method (~15 LOC).
- `update_driver_dialect` method (~10 LOC).
- `identify_connection` method (~30 LOC).
- `get_connection_provider_manager` method (~10 LOC, depends on `connection_provider.py` port).
- `PluginServiceManagerContainer` holder class (~25 LOC, may or may not be needed if DI is direct).
- `_unwrap` helper (~20 LOC, optional).

Port estimate (methods only, excluding provider-manager dependency): **~100 LOC**.

### 2.6 `custom_endpoint_plugin.py` / `aio/custom_endpoint_monitor.py`

Sync: 365 LOC, 5 classes. Async: 292 LOC, 2 classes.

Missing: `CustomEndpointRoleType` enum (13 LOC), `CustomEndpointInfo` dataclass (48 LOC). Sync uses these for richer endpoint metadata (role-filtered topology). Async only tracks the instance list.

Port estimate: **~65 LOC** + topology-integration logic.

### 2.7 `aurora_connection_tracker_plugin.py` → `aio/aurora_connection_tracker.py`

Sync: 355 LOC, 3 classes. Async: 325 LOC, 2 classes (factory centralized).

Behaviorally similar. Gap flagged earlier (canonical-key-only vs multi-alias) is ~30 LOC fix.

### 2.8 `read_write_splitting_plugin.py` → `aio/read_write_splitting_plugin.py`

Sync: 701 LOC, 5 classes. Async: 301 LOC, 1 class.

Async flattens sync's split of `ReadWriteSplittingConnectionManager` + `ReadWriteConnectionHandler` + `TopologyBasedConnectionHandler` into a single class. Not a bug per se, but pool-aware detection uses module-string heuristic (gap #3.2) instead of querying `ConnectionProviderManager`.

Port estimate to realign: **~40 LOC** (once connection_provider exists).

### 2.9 `federated_plugin.py` / `okta_plugin.py` → `aio/federated_auth_plugins.py`

Sync: 323 + 257 LOC, multiple factories. Async: 420 LOC.

Missing: IdP factory registry pattern. Async hardcodes ADFS + Okta dispatch; sync uses a registry that consumers can extend.

Port estimate: **~80 LOC**.

---

## 3. Behavioral divergences

| Symbol | Divergence | Fix LOC |
|---|---|---:|
| `AsyncCustomEndpointPlugin.connect` | Timeout returns connection with stale filter; sync raises. | ~15 |
| `AsyncReadWriteSplittingPlugin._is_pool_connection` | Module-string heuristic vs sync's `ConnectionProviderManager` query. | ~40 (needs #1) |
| `AsyncAwsWrapperConnection.connect` | Panic-mode machinery present in topology monitor but not engaged on initial connect. | ~50 |
| `AsyncAwsWrapperConnection.connect` | `MultiAz` / `GlobalAurora` host-list-providers exist but not auto-selected from `DatabaseDialect`. | ~30 |
| `AsyncOpenedConnectionTracker._canonical_key` | Keys on a single canonical alias; sync tracks all `HostInfo.as_aliases()`. | ~30 |

**Total:** ~165 LOC.

---

## 4. Infrastructure gaps (subset of §1, elevated)

The load-bearing abstractions the async side does not yet have:

1. **`AsyncConnectionProvider` / `AsyncConnectionProviderManager`** — required for pluggable providers (internal pool, user-registered providers). Unblocks the RWS pool-aware fix. ~250 LOC.
2. **`AsyncSessionStateService`** — capture/restore session state across failover + RWS switches. Failover/RWS currently drop session state silently. ~300 LOC + hooks.
3. **IdP factory registry** — extract the sync registry pattern, wire into async auth plugins. ~80 LOC.

---

## 5. Known skeletons (confirmed incomplete)

| Item | Location | State | LOC to complete |
|---|---|---|---:|
| BlueGreen routing stubs | `aio/blue_green_plugin.py:194,208,221,246` | `raise NotImplementedError` in 4 classes | ~400 |
| BlueGreenStatusMonitor + Provider | `aio/blue_green_plugin.py` | absent | ~1,200 |
| Limitless standing monitor | `aio/limitless_plugin.py` | on-demand only | ~290 |
| FastestResponseStrategy standing monitors | `aio/fastest_response_strategy_plugin.py` | on-demand with Random fallback | ~220 |

---

## 6. Items NOT gaps (intentional or N/A)

- **`reader_failover_handler.py` + `writer_failover_handler.py`**: sync v1-only. Async uses v2 inline design.
- **`sql_alchemy_connection_provider.py` + `sqlalchemy_driver_dialect.py`**: SQLAlchemy integration is sync-only by design.
- **`host_selector.py`**: async `default_plugin.py` reuses sync's `DriverConnectionProvider.accepted_strategies()` registry — all 4 selectors (Random, RoundRobin, WeightedRandom, HighestWeight) are reachable.
- **`utils/*`**: Properties, RdsUtils, IamUtils, RegionUtils, exception handlers, telemetry factories are pure / shared — no async port needed.
- **`host_monitoring_v2_plugin.py` MonitoringContext/MonitorServiceV2**: async design is single-task-per-plugin, which is the correct asyncio analog.
- **`aurora_connection_tracker_plugin.py` factory class**: async centralizes factories in `plugin_factory.py`.
- **`allowed_and_blocked_hosts.py`** as a file: the dataclass is shared-ready; the gap is the missing property on `AsyncPluginService`.

---

## 7. Recommended scope for upstream PR

### Must-fix (blocks "usable async" claim): ~400 LOC
1. Replace 4 BlueGreen `NotImplementedError` routing stubs with real implementations — OR guard the whole `AsyncBlueGreenPlugin` with a clear "not yet supported in async" error + feature flag.
2. Realign `AsyncCustomEndpointPlugin` timeout to raise (sync parity).

### Should-fix to claim "parity": ~1,500 LOC
3. Port `connection_provider.py` → `aio/connection_provider.py`.
4. Port `states/session_state*.py` → `aio/session_state.py` + wire into failover/RWS.
5. Add missing `AsyncPluginService` methods (§2.5): `allowed_and_blocked_hosts`, `is_in_transaction`, `update_in_transaction`, `update_driver_dialect`, `identify_connection`, `get_connection_provider_manager`, `session_state_service`.
6. IdP factory registry.
7. Auto-wire MultiAz/GlobalAurora in connect.
8. Auto-wire panic mode in connect.
9. Multi-alias tracker fix.
10. Pool-aware RWS (once #3 lands).
11. CustomEndpointRoleType + CustomEndpointInfo.

### Remaining sync features missing in async (deferring means shipping a PR that is NOT at parity): ~3,700 LOC
12. BlueGreenStatusMonitor + Provider full port — exists in sync `blue_green_plugin.py:796–1954`.
13. Limitless standing router monitor — exists in sync `limitless_plugin.py:116` (`LimitlessRouterMonitor`).
14. FastestResponseStrategy standing monitors — exists in sync `fastest_response_strategy_plugin.py:156` (`HostResponseTimeMonitor`) + `:314` (`HostResponseTimeService`).

These are not new features. Deferring them means the PR cannot honestly claim "parity with sync"; it can only claim "parity for the hot paths." Choose framing accordingly when writing the PR description.

---

## 8. Honest assessment

This audit found gaps my prior A–J audits missed: **`connection_provider.py`, `states/`, `allowed_and_blocked_hosts` property, and ~7 methods on `AsyncPluginService`**. Combined with items I knew were deferred (BlueGreen skeleton, Limitless/FRS minimal) plus confirmed divergences (CustomEndpoint softer timeout, RWS heuristic, tracker single-alias), the real outstanding scope is **~5,600 LOC**, not the ~4,000–5,000 I estimated earlier.

The K–O plan I drafted 2026-04-22 is **directionally right** but undersized for the session-state / plugin-service-method gaps. Before executing, the plan needs a phase covering §2.5 (plugin service method backfill) and needs explicit mention of `allowed_and_blocked_hosts`.

No further hidden gaps expected after this audit. If new gaps surface during execution, I'll add them to this document rather than letting them accumulate silently.
