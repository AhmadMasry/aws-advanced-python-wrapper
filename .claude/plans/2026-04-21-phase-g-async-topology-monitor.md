# Phase G — Async Cluster Topology Monitor

**Goal:** Close the tractable parts of audit P0 #11 on `AsyncClusterTopologyMonitor`. Add a high-frequency refresh mode after writer change, the ignore-request window after writer found, and the `force_refresh_with_connection(conn)` API variant.

**Scope note:** The full sync panic mode spawns per-host probe threads via a `ThreadPoolExecutor` to find a new writer in parallel when the monitoring connection goes bad. That machinery is substantial (sync has ~500 lines for `ClusterTopologyMonitorImpl`). Phase G ships the **tractable subset** — rate switching, ignore window, connection-specific refresh API. Full panic-mode parallel probing is deferred as a follow-up; the audit's core concern (detecting writer change quickly + not hammering callers during the discovery window) is addressed.

---

## File structure

**Modify:**
- `aws_advanced_python_wrapper/aio/cluster_topology_monitor.py`
- `tests/unit/test_aio_host_list_provider.py` (existing topology monitor tests live here)

---

## Task G.1: High-frequency refresh after writer change

Add `high_refresh_rate_sec` (default 1.0). When the monitor detects a writer change (new topology's writer host differs from the previous cached writer), switch the tick interval to `high_refresh_rate_sec` for `HIGH_REFRESH_PERIOD_SEC` (30s). Then revert.

Sync reference: `cluster_topology_monitor.py:86, :121-122, :192-210`.

**Commit:** `feat(aio): cluster topology monitor high-freq refresh after writer change (phase G.1)` — partial close of P0 #11.

---

## Task G.2: Ignore-request window after writer found

Add `IGNORE_REQUEST_SEC = 10`. After a refresh successfully confirms the writer, suppress any subsequent `force_refresh_with_connection` / external refresh request for 10s — the topology is known, don't hammer the DB. Store a `_ignore_requests_until_ns` timestamp.

Sync reference: `cluster_topology_monitor.py:87, :136-141, :273-277`.

**Commit:** `feat(aio): cluster topology monitor ignore-request window after writer found (phase G.2)` — partial close of P0 #11.

---

## Task G.3: `force_refresh_with_connection(conn, timeout_sec)` API

Sync exposes a second refresh API that uses the caller's connection instead of the monitor's getter. Useful during failover when the monitor's own connection is dead but the caller has a fresh one.

Port: `async def force_refresh_with_connection(conn, timeout_sec=5.0) -> Topology`. Bypasses the ignore window; uses the provided conn directly.

Sync reference: `cluster_topology_monitor.py:150-153`.

**Commit:** `feat(aio): cluster topology monitor force_refresh_with_connection API (phase G.3)` — closes P0 #11 for the connection-specific refresh gap.

---

## Task G.4: Final `/verify`

---

## Out of scope (documented follow-ups)

- Parallel-probe panic mode via multiple asyncio.Tasks when monitoring connection dies.
- Verified-writer state machine (`_is_verified_writer_connection` flag).
- Submitted-hosts deduplication.
- `MonitorResetEvent` equivalent.

These require either an async port of `ConnectionProvider` (to open new connections outside the pipeline) or a significant asyncio refactor of the monitor's state management. Deferred as Phase G.x follow-ups.
