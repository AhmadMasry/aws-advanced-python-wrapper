# Phase B — Failover v2 Parity

> **For agentic workers:** Use superpowers:subagent-driven-development to implement task-by-task.

**Goal:** Close the four P0 audit findings on `AsyncFailoverPlugin` so it behaves like `failover_v2_plugin.py` under real fault conditions — dialect-aware exception classification, transaction-aware failover signaling, dead-host tracking, and strategy-based reader retry.

**Architecture:** The async failover plugin already owns the pipeline entry points (`execute` wrapping, `_do_failover` reconnect). Phase B replaces four internal behaviors with calls to the `AsyncPluginService` surface added in Phase A:
- String-match `_should_failover` → `plugin_service.is_network_exception` + `is_read_only_connection_exception`
- Single `FailoverSuccessError` raise → mid-transaction gets `TransactionResolutionUnknownError`
- Silent retry of dead host → `plugin_service.set_availability(UNAVAILABLE)`
- First-reader pick → reader-retry loop honoring `FAILOVER_READER_HOST_SELECTOR_STRATEGY` + original-writer fallback + `initial_connection_host_info` last-resort

No new files. No new `AsyncPluginService` surface needed (all A.1–A.7 work covers it).

**Tech Stack:** Python 3.10–3.14. Reuses: sync `TransactionResolutionUnknownError` (no async port — it's an exception class), `WrapperProperties.FAILOVER_READER_HOST_SELECTOR_STRATEGY`, `HostAvailability`. Tests via plain pytest + `asyncio.run()`.

---

## File structure

**Modify:**
- `aws_advanced_python_wrapper/aio/failover_plugin.py` — all 6 behavior fixes
- `tests/unit/test_aio_failover_plugin.py` — extend with new tests (file already exists)

**No new files.** Plan phases are commit-grouped logically; each task is one commit.

---

## Task B.1: Replace string-match exception heuristic with dialect-aware classification

Swap `_should_failover`'s `"operational"/"interface"/"connection"` substring check for `plugin_service.is_network_exception(exc)`. For `FailoverMode.STRICT_WRITER`, also trigger on `is_read_only_connection_exception(exc)` (mirrors sync v2 `_should_exception_trigger_connection_switch` at `failover_v2_plugin.py:416-427`).

**Files:** `aws_advanced_python_wrapper/aio/failover_plugin.py`, `tests/unit/test_aio_failover_plugin.py`.

### TDD steps

- [ ] **B.1.1: Write failing tests** — append to `tests/unit/test_aio_failover_plugin.py`:

```python
def test_should_failover_uses_plugin_service_is_network_exception():
    """Replaces the string-match heuristic with is_network_exception."""
    # Arrange: plugin_service classifies the error as network via dialect
    ps = _make_plugin_service()  # helper from existing tests
    ps.is_network_exception = MagicMock(return_value=True)
    ps.is_read_only_connection_exception = MagicMock(return_value=False)
    plugin = AsyncFailoverPlugin(ps, _mk_host_list_provider(), _enable_failover_props())

    # Act/Assert
    assert plugin._should_failover(Exception("arbitrary")) is True
    ps.is_network_exception.assert_called_once()


def test_should_failover_strict_writer_triggers_on_read_only_exception():
    """STRICT_WRITER mode treats read-only-connection exception as failover trigger."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=False)
    ps.is_read_only_connection_exception = MagicMock(return_value=True)
    props = _enable_failover_props()
    props[WrapperProperties.FAILOVER_MODE.name] = "strict-writer"
    plugin = AsyncFailoverPlugin(ps, _mk_host_list_provider(), props)

    assert plugin._should_failover(Exception("read only")) is True


def test_should_failover_strict_reader_does_not_trigger_on_read_only():
    """STRICT_READER mode does NOT treat read-only as failover trigger."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=False)
    ps.is_read_only_connection_exception = MagicMock(return_value=True)
    props = _enable_failover_props()
    props[WrapperProperties.FAILOVER_MODE.name] = "strict-reader"
    plugin = AsyncFailoverPlugin(ps, _mk_host_list_provider(), props)

    assert plugin._should_failover(Exception("read only")) is False


def test_should_not_failover_on_self_raised_signals():
    """FailoverSuccessError / FailoverFailedError must not re-enter failover."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=True)
    ps.is_read_only_connection_exception = MagicMock(return_value=False)
    plugin = AsyncFailoverPlugin(ps, _mk_host_list_provider(), _enable_failover_props())

    assert plugin._should_failover(FailoverSuccessError("noop")) is False
    assert plugin._should_failover(FailoverFailedError("noop")) is False
```

Test helpers `_make_plugin_service`, `_mk_host_list_provider`, `_enable_failover_props` may need to be added if they don't exist — check the existing test file first.

- [ ] **B.1.2: Run — verify fail** — `poetry run python -m pytest tests/unit/test_aio_failover_plugin.py -v -k should_failover` — expect FAIL.

- [ ] **B.1.3: Replace `_should_failover`** in `aws_advanced_python_wrapper/aio/failover_plugin.py`:

```python
    def _should_failover(self, exc: Exception) -> bool:
        """Decide whether ``exc`` indicates a failover-worthy error.

        Mirrors sync v2 _should_exception_trigger_connection_switch
        (failover_v2_plugin.py:416-427): delegate to the dialect-aware
        ExceptionHandler through the plugin service, with a STRICT_WRITER
        escape hatch for read-only-connection exceptions.
        """
        # Avoid catching our own failover signals.
        if isinstance(exc, (FailoverSuccessError, FailoverFailedError)):
            return False
        if self._plugin_service.is_network_exception(error=exc):
            return True
        # STRICT_WRITER mode: promote read-only-connection errors to
        # failover triggers so clients pinned to the writer fail over when
        # their connection becomes read-only after an Aurora swap.
        return (self._mode == FailoverMode.STRICT_WRITER
                and self._plugin_service.is_read_only_connection_exception(error=exc))
```

- [ ] **B.1.4: Run — verify pass** — all 4 new tests pass; existing tests still pass; full suite no regressions.

- [ ] **B.1.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/failover_plugin.py tests/unit/test_aio_failover_plugin.py
git commit -m "fix(aio): replace string-match exception heuristic in failover (phase B.1)

AsyncFailoverPlugin._should_failover now delegates to
plugin_service.is_network_exception (dialect-aware, SQLSTATE/errno-based)
instead of matching exception class names for 'operational', 'interface',
or 'connection'. STRICT_WRITER mode additionally triggers on
is_read_only_connection_exception, matching sync v2 at
failover_v2_plugin.py:416-427.

Closes audit P0 #1."
```

---

## Task B.2: Mid-transaction `TransactionResolutionUnknownError`

When failover succeeds mid-transaction, raise `TransactionResolutionUnknownError` instead of `FailoverSuccessError`. Caller can't safely retry because the transaction's fate is unknown. Sync v2: `failover_v2_plugin.py:312-321` (`_throw_failover_success_exception`).

**Files:** `aws_advanced_python_wrapper/aio/failover_plugin.py`, `tests/unit/test_aio_failover_plugin.py`.

### TDD steps

- [ ] **B.2.1: Write failing tests** — append:

```python
from aws_advanced_python_wrapper.errors import TransactionResolutionUnknownError


def test_failover_raises_transaction_unknown_when_mid_transaction():
    """Mid-transaction failover -> TransactionResolutionUnknownError,
    not plain FailoverSuccessError."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=True)
    # Simulate: driver dialect reports we're mid-txn
    ps.driver_dialect.is_in_transaction = AsyncMock(return_value=True)
    # Stub _do_failover to succeed silently
    plugin = AsyncFailoverPlugin(ps, _mk_host_list_provider(), _enable_failover_props())
    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(TransactionResolutionUnknownError):
        asyncio.run(plugin.execute(MagicMock(), "execute", _raising))


def test_failover_raises_failover_success_when_not_in_transaction():
    """Outside a transaction -> FailoverSuccessError, caller can retry cleanly."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=True)
    ps.driver_dialect.is_in_transaction = AsyncMock(return_value=False)
    plugin = AsyncFailoverPlugin(ps, _mk_host_list_provider(), _enable_failover_props())
    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "execute", _raising))
```

- [ ] **B.2.2: Run — verify fail**.

- [ ] **B.2.3: Add `_raise_failover_success_or_txn_unknown` helper in `failover_plugin.py`**:

```python
    async def _raise_failover_success_or_txn_unknown(
            self, original_exc: Exception) -> None:
        """Signal successful failover. If the caller was mid-transaction,
        raise TransactionResolutionUnknownError so they know the txn's
        state is ambiguous; otherwise raise FailoverSuccessError so they
        can retry the unit of work cleanly.

        Mirrors sync v2 _throw_failover_success_exception at
        failover_v2_plugin.py:312-321.
        """
        in_txn = False
        current = self._plugin_service.current_connection
        if current is not None:
            try:
                in_txn = await self._plugin_service.driver_dialect.is_in_transaction(current)
            except Exception:  # noqa: BLE001 - probe best-effort
                in_txn = False
        if in_txn:
            raise TransactionResolutionUnknownError(
                "Failover succeeded mid-transaction; transaction state is unknown."
            ) from original_exc
        raise FailoverSuccessError(
            "Connection was replaced as part of failover; please retry the transaction."
        ) from original_exc
```

Update `execute`:

```python
    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if not self._enabled:
            return await execute_func()
        try:
            return await execute_func()
        except Exception as exc:
            if not self._should_failover(exc):
                raise
            await self._do_failover(driver_dialect=self._plugin_service.driver_dialect)
            await self._raise_failover_success_or_txn_unknown(exc)
```

Add import: `from aws_advanced_python_wrapper.errors import ..., TransactionResolutionUnknownError`.

- [ ] **B.2.4: Run — verify pass**.

- [ ] **B.2.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/failover_plugin.py tests/unit/test_aio_failover_plugin.py
git commit -m "fix(aio): raise TransactionResolutionUnknownError on mid-txn failover (phase B.2)

AsyncFailoverPlugin now probes driver_dialect.is_in_transaction after a
successful reconnect and raises TransactionResolutionUnknownError when
the caller was mid-transaction -- their txn state is ambiguous and they
cannot blindly retry.

Outside a transaction, raises FailoverSuccessError as before.

Mirrors sync v2 _throw_failover_success_exception at
failover_v2_plugin.py:312-321. Closes audit P0 #2."
```

---

## Task B.3: Mark failed hosts UNAVAILABLE

After a connect attempt during failover raises an exception, mark the target host's aliases UNAVAILABLE so later failover attempts (or host selectors) skip it. Sync sets availability on `HostInfo.set_availability()` AND the plugin service's `set_availability()` cache.

**Files:** `aws_advanced_python_wrapper/aio/failover_plugin.py`, `tests/unit/test_aio_failover_plugin.py`.

### TDD steps

- [ ] **B.3.1: Write failing test** — append:

```python
def test_failover_marks_failed_host_unavailable():
    """Connect failure during failover -> host marked UNAVAILABLE."""
    from aws_advanced_python_wrapper.host_availability import HostAvailability

    ps = _make_plugin_service()
    ps.set_availability = MagicMock()
    ps.is_network_exception = MagicMock(return_value=True)

    # Simulate: first target fails, second succeeds
    hlp = _mk_host_list_provider()
    dead = HostInfo(host="dead-reader", port=5432, role=HostRole.READER)
    good = HostInfo(host="good-reader", port=5432, role=HostRole.READER)
    hlp._topology = (dead, good)

    plugin = AsyncFailoverPlugin(ps, hlp, _enable_failover_props())

    async def _fake_open(target, driver_dialect):
        if target is dead:
            raise OSError("refused")
        return MagicMock()

    plugin._open_connection = _fake_open  # type: ignore[method-assign]
    # Force strict-reader mode so reader loop is exercised
    plugin._mode = FailoverMode.STRICT_READER

    asyncio.run(plugin._do_failover(
        driver_dialect=ps.driver_dialect))

    ps.set_availability.assert_any_call(
        frozenset({dead.as_alias()}), HostAvailability.UNAVAILABLE)
```

`HostInfo.as_alias()` is the canonical alias format; verify it exists via `grep -n 'def as_alias' aws_advanced_python_wrapper/hostinfo.py`. If absent, use `frozenset({dead.host})`.

- [ ] **B.3.2: Run — verify fail**.

- [ ] **B.3.3: Modify `_do_failover`** to call `set_availability(UNAVAILABLE)` when a candidate fails. Also call `set_availability(AVAILABLE)` when a candidate succeeds (so a host recovering from a blip goes back in the pool).

Update the retry loop body (conceptually — actual implementation merges with B.4/B.5):

```python
    # Inside the per-target try/except:
    try:
        new_conn = await self._open_connection(target, driver_dialect)
        self._plugin_service.set_availability(
            frozenset({target.as_alias()}), HostAvailability.AVAILABLE)
        await self._plugin_service.set_current_connection(new_conn, target)
        return
    except Exception as e:
        self._plugin_service.set_availability(
            frozenset({target.as_alias()}), HostAvailability.UNAVAILABLE)
        last_error = e
```

Import `HostAvailability` at top of `failover_plugin.py`.

- [ ] **B.3.4: Run — verify pass**.

- [ ] **B.3.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/failover_plugin.py tests/unit/test_aio_failover_plugin.py
git commit -m "fix(aio): mark failed hosts UNAVAILABLE during failover (phase B.3)

AsyncFailoverPlugin's reconnect loop now marks each target host
AVAILABLE on success and UNAVAILABLE on connect failure, via the new
plugin_service.set_availability hook from phase A.3.

Prevents the retry loop from hammering a known-dead host every iteration.
Caches for 5 minutes (HostAvailability TTL) so a transient blip doesn't
permanently blacklist the host. Closes audit P0 #3."
```

---

## Task B.4: Reader retry loop with strategy + original-writer fallback

Rewrite `_do_failover` to mirror sync v2's `_get_reader_failover_connection` (`failover_v2_plugin.py:254-310`):

1. In `STRICT_READER` / `READER_OR_WRITER`, build a reader-candidates list from the fresh topology.
2. Loop: pick a candidate via `plugin_service.get_host_info_by_strategy(READER, strategy, remaining)`; if it connects, return. If it fails or role is wrong, remove from remaining.
3. When readers exhausted, fall back to the original writer (if topology still lists one).
4. In `STRICT_WRITER`, do the sync-v2 `_failover_writer` flow (just pick the writer from topology).

Honor `FAILOVER_READER_HOST_SELECTOR_STRATEGY` prop (defaults to `"random"`).

**Files:** `aws_advanced_python_wrapper/aio/failover_plugin.py`, `tests/unit/test_aio_failover_plugin.py`.

### TDD steps

- [ ] **B.4.1: Write failing tests** — append:

```python
def test_reader_failover_uses_configured_strategy():
    """Reader failover picks via plugin_service.get_host_info_by_strategy
    with the configured strategy name."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=True)
    reader = HostInfo(host="r1", port=5432, role=HostRole.READER)
    ps.get_host_info_by_strategy = MagicMock(return_value=reader)

    hlp = _mk_host_list_provider()
    hlp._topology = (reader,)

    props = _enable_failover_props()
    props[WrapperProperties.FAILOVER_MODE.name] = "strict-reader"
    props[WrapperProperties.FAILOVER_READER_HOST_SELECTOR_STRATEGY.name] = "round_robin"

    plugin = AsyncFailoverPlugin(ps, hlp, props)
    plugin._open_connection = AsyncMock(return_value=MagicMock())

    asyncio.run(plugin._do_failover(driver_dialect=ps.driver_dialect))

    ps.get_host_info_by_strategy.assert_called()
    (_role, strategy, _candidates), _ = ps.get_host_info_by_strategy.call_args_list[0]
    assert strategy == "round_robin"


def test_reader_failover_cycles_through_candidates():
    """When a reader fails, the next one is tried. Sync v2 parity."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=True)

    r1 = HostInfo(host="r1", port=5432, role=HostRole.READER)
    r2 = HostInfo(host="r2", port=5432, role=HostRole.READER)

    # Strategy returns r1 first, then r2
    ps.get_host_info_by_strategy = MagicMock(side_effect=[r1, r2])

    hlp = _mk_host_list_provider()
    hlp._topology = (r1, r2)

    props = _enable_failover_props()
    props[WrapperProperties.FAILOVER_MODE.name] = "strict-reader"
    plugin = AsyncFailoverPlugin(ps, hlp, props)

    attempts = []

    async def _open(target, _):
        attempts.append(target.host)
        if target is r1:
            raise OSError("r1 down")
        return MagicMock()

    plugin._open_connection = _open  # type: ignore[method-assign]

    asyncio.run(plugin._do_failover(driver_dialect=ps.driver_dialect))

    assert attempts == ["r1", "r2"]


def test_reader_failover_falls_back_to_original_writer_when_readers_exhausted():
    """After all readers fail, try the original writer (READER_OR_WRITER mode)."""
    ps = _make_plugin_service()
    ps.is_network_exception = MagicMock(return_value=True)

    r1 = HostInfo(host="r1", port=5432, role=HostRole.READER)
    w = HostInfo(host="w1", port=5432, role=HostRole.WRITER)
    ps.get_host_info_by_strategy = MagicMock(side_effect=[r1, None])

    hlp = _mk_host_list_provider()
    hlp._topology = (r1, w)

    props = _enable_failover_props()
    props[WrapperProperties.FAILOVER_MODE.name] = "reader-or-writer"
    plugin = AsyncFailoverPlugin(ps, hlp, props)

    attempts = []

    async def _open(target, _):
        attempts.append(target.host)
        if target is r1:
            raise OSError("r1 down")
        return MagicMock()

    plugin._open_connection = _open  # type: ignore[method-assign]

    asyncio.run(plugin._do_failover(driver_dialect=ps.driver_dialect))

    assert attempts == ["r1", "w1"]
```

- [ ] **B.4.2: Run — verify fail**.

- [ ] **B.4.3: Rewrite `_do_failover`** to branch on mode. Add `_failover_reader` and `_failover_writer` helpers. Honor `FAILOVER_READER_HOST_SELECTOR_STRATEGY`.

Read current `_do_failover` and replace with:

```python
    async def _do_failover(self, driver_dialect: AsyncDriverDialect) -> None:
        """Probe topology, then reader-loop or writer-pick per failover_mode.

        Mirrors sync v2 _failover_reader (failover_v2_plugin.py:254-310)
        and _failover_writer (failover_v2_plugin.py:323-390).
        """
        deadline = asyncio.get_event_loop().time() + self._failover_timeout_sec
        last_error: Optional[BaseException] = None

        try:
            topology = await self._host_list_provider.force_refresh(
                self._plugin_service.current_connection)
        except Exception as e:
            topology = ()
            last_error = e

        # Fall back to initial-connect host if topology is empty.
        if not topology and self._plugin_service.initial_connection_host_info is not None:
            topology = (self._plugin_service.initial_connection_host_info,)

        if self._mode == FailoverMode.STRICT_WRITER:
            await self._failover_writer(topology, driver_dialect, deadline, last_error)
        else:
            await self._failover_reader(topology, driver_dialect, deadline, last_error)

    async def _failover_reader(
            self,
            topology: Topology,
            driver_dialect: AsyncDriverDialect,
            deadline: float,
            last_error: Optional[BaseException]) -> None:
        reader_candidates = [h for h in topology if h.role == HostRole.READER]
        original_writer = next((h for h in topology if h.role == HostRole.WRITER), None)
        strategy = WrapperProperties.FAILOVER_READER_HOST_SELECTOR_STRATEGY.get(self._props) or "random"

        while asyncio.get_event_loop().time() < deadline:
            remaining = list(reader_candidates)
            while remaining and asyncio.get_event_loop().time() < deadline:
                try:
                    candidate = self._plugin_service.get_host_info_by_strategy(
                        HostRole.READER, strategy, remaining)
                except Exception as e:
                    last_error = e
                    break
                if candidate is None:
                    break

                try:
                    new_conn = await self._open_connection(candidate, driver_dialect)
                    self._plugin_service.set_availability(
                        frozenset({candidate.as_alias()}), HostAvailability.AVAILABLE)
                    await self._plugin_service.set_current_connection(new_conn, candidate)
                    return
                except Exception as e:
                    self._plugin_service.set_availability(
                        frozenset({candidate.as_alias()}), HostAvailability.UNAVAILABLE)
                    last_error = e
                    remaining.remove(candidate)

            # Readers exhausted; try original writer if mode allows
            if (original_writer is not None
                    and self._mode != FailoverMode.STRICT_READER
                    and asyncio.get_event_loop().time() < deadline):
                try:
                    new_conn = await self._open_connection(original_writer, driver_dialect)
                    self._plugin_service.set_availability(
                        frozenset({original_writer.as_alias()}),
                        HostAvailability.AVAILABLE)
                    await self._plugin_service.set_current_connection(
                        new_conn, original_writer)
                    return
                except Exception as e:
                    self._plugin_service.set_availability(
                        frozenset({original_writer.as_alias()}),
                        HostAvailability.UNAVAILABLE)
                    last_error = e

            await asyncio.sleep(1.0)

        raise FailoverFailedError(
            f"Failover could not establish a new reader within {self._failover_timeout_sec}s"
        ) from last_error

    async def _failover_writer(
            self,
            topology: Topology,
            driver_dialect: AsyncDriverDialect,
            deadline: float,
            last_error: Optional[BaseException]) -> None:
        while asyncio.get_event_loop().time() < deadline:
            writer = next((h for h in topology if h.role == HostRole.WRITER), None)
            if writer is not None:
                try:
                    new_conn = await self._open_connection(writer, driver_dialect)
                    self._plugin_service.set_availability(
                        frozenset({writer.as_alias()}), HostAvailability.AVAILABLE)
                    await self._plugin_service.set_current_connection(new_conn, writer)
                    return
                except Exception as e:
                    self._plugin_service.set_availability(
                        frozenset({writer.as_alias()}), HostAvailability.UNAVAILABLE)
                    last_error = e

            # Re-refresh topology for the next iteration
            await asyncio.sleep(1.0)
            try:
                topology = await self._host_list_provider.force_refresh(
                    self._plugin_service.current_connection)
            except Exception as e:
                last_error = e

        raise FailoverFailedError(
            f"Failover could not establish a new writer within {self._failover_timeout_sec}s"
        ) from last_error
```

- [ ] **B.4.4: Run — verify pass** — all 3 new tests pass; existing tests still pass.

- [ ] **B.4.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/failover_plugin.py tests/unit/test_aio_failover_plugin.py
git commit -m "fix(aio): reader retry loop + strategy + original-writer fallback (phase B.4)

AsyncFailoverPlugin._do_failover now branches on failover_mode:

- STRICT_WRITER: repeatedly probe topology, pick the writer, connect.
- STRICT_READER / READER_OR_WRITER: loop through readers via
  plugin_service.get_host_info_by_strategy honoring
  FAILOVER_READER_HOST_SELECTOR_STRATEGY (defaults to 'random').
  Dead candidates marked UNAVAILABLE and removed from the pool.
  When readers are exhausted, READER_OR_WRITER falls back to the
  original writer.

Also falls back to initial_connection_host_info when topology
force_refresh returns empty (phase A.6 hook).

Mirrors sync v2 _failover_reader / _failover_writer at
failover_v2_plugin.py:254-390. Closes audit P0 #4."
```

---

## Task B.5: Final `/verify`

- [ ] **B.5.1:** `poetry run mypy .` — clean.
- [ ] **B.5.2:** `poetry run flake8 .` — clean.
- [ ] **B.5.3:** `poetry run isort --check-only .` — clean.
- [ ] **B.5.4:** `poetry run python -m pytest tests/unit -Werror` — all pass, no new warnings.
- [ ] **B.5.5:** Spot-check: review `aws_advanced_python_wrapper/aio/failover_plugin.py` diff from Phase A baseline (`e2aa83f..HEAD`) — confirm all 4 audit P0 findings addressed, no stray changes.

No commit needed; commits happen inside B.1–B.4.

---

## Out of scope (for Phase B)

- Telemetry counters (`writer_failover.*.count` etc.) — land in Phase I.
- `FAILOVER_WRITER_RECONNECT_INTERVAL_SEC`, `FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_SEC`, `FAILOVER_READER_CONNECT_TIMEOUT_SEC` fine-grained timeout knobs — sync v1 honors them; v2 simplifies. Async mirrors v2, so deferred unless a consumer specifically needs them.
- Stale-DNS helper — sync v1 uses `StaleDnsHelper` for cluster-endpoint verification. Not in v2; async mirrors v2.
- `force_monitoring_refresh_host_list` (sync writer-failover pattern) — requires topology monitor panic mode (Phase G).
- FailoverV2 plugin factory registration — Phase H.

## Self-review

- [x] All 4 audit P0 items for failover mapped to a B.x task (P0 #1 → B.1, P0 #2 → B.2, P0 #3 → B.3, P0 #4 → B.4).
- [x] Every task cites sync reference with file:line.
- [x] Commit messages reference audit IDs.
- [x] No cross-phase forward references (Phase G/H/I items explicitly deferred).
- [x] Python 3.10-compatible syntax throughout.
- [x] `as_alias` usage assumes `HostInfo.as_alias()` exists — implementer must verify before committing.
