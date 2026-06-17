# Phase F — Async RWS Parity

**Goal:** Close audit P0 #9, #10 and P2 #24 on `AsyncReadWriteSplittingPlugin`. Honor `READER_HOST_SELECTOR_STRATEGY`, refuse reader swap mid-transaction, cycle through reader candidates on failure, verify initial host role, validate cached reader/writer before reuse, check hosts are still in topology.

**Architecture:** RWS stays as a `set_read_only`-triggered swap plugin. Changes are localized to `_switch_to_reader` / `_switch_to_writer` + adding a pre-flight `is_in_transaction` gate in `execute`. No new external dependencies; reuses `plugin_service.get_host_info_by_strategy` (A.4), `driver_dialect.is_in_transaction` (existing), `AsyncHostListProvider.refresh` (existing).

---

## File structure

**Modify:**
- `aws_advanced_python_wrapper/aio/read_write_splitting_plugin.py`
- `tests/unit/test_aio_read_write_splitting.py` (existing)

---

## Task F.1: Honor `READER_HOST_SELECTOR_STRATEGY`

Swap `next((h for h in topology if h.role == READER), None)` with:
```python
strategy = WrapperProperties.READER_HOST_SELECTOR_STRATEGY.get(self._props) or "random"
reader = self._plugin_service.get_host_info_by_strategy(
    HostRole.READER, strategy, [h for h in topology if h.role == HostRole.READER])
```

Sync reference: `read_write_splitting_plugin.py:540-548, :578-593`.

**Commit:** `fix(aio): RWS honors READER_HOST_SELECTOR_STRATEGY (phase F.1)` — closes P0 #9.

---

## Task F.2: Transaction gating

Add pre-flight check in `execute` before any swap: if `is_read_only=True` AND `await plugin_service.driver_dialect.is_in_transaction(current_connection)`, raise `ReadWriteSplittingError`. Writers can always swap (no txn risk there).

Sync reference: `read_write_splitting_plugin.py:244-265`.

**Commit:** `fix(aio): RWS refuses mid-txn reader switch (phase F.2)` — closes P0 #10.

---

## Task F.3: Reader retry loop with candidate cycling

Currently `_switch_to_reader` tries one reader. If it fails, raises. Sync tries up to `2 * len(hosts)` reader candidates, removing failed ones. Port the loop.

Sync reference: `read_write_splitting_plugin.py:578-593`.

**Commit:** `fix(aio): RWS retries reader candidates on connect failure (phase F.3)` — partial close P2 #24.

---

## Task F.4: Connection usability + topology validation before reuse

Before reusing `_reader_conn` / `_writer_conn`:
1. Check `driver_dialect.is_closed(conn)` (already done on reader side — add on writer side, ensure both paths).
2. Validate the cached HostInfo is still in the current topology; if not, discard cache and reopen.

Sync reference: `read_write_splitting_plugin.py:300-310, :640-642`.

**Commit:** `fix(aio): RWS validates cached conns against current topology (phase F.4)` — partial close P2 #24.

---

## Task F.5: Final `/verify`

---

## Out of scope

- Connection-provider pool awareness (P2 #24 remainder) — requires async port of `ConnectionProviderManager`. Sync uses `_POOL_PROVIDER_CLASS_NAME` string match. Defer until Phase F-followup or Phase H.
- Initial host role verification via DB query (P2 #24 remainder) — requires `plugin_service.get_host_role` which isn't in the async PluginService surface yet. Defer.
- Telemetry — Phase I.
