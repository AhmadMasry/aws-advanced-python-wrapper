# Phase A — AsyncPluginService Foundation

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand `AsyncPluginService` with the surface needed by async plugin parity work — DatabaseDialect resolution, exception classification, host availability tracking, host selection strategies, host list provider binding, and resource cleanup.

**Architecture:** Async reuses sync's dialect-agnostic infrastructure verbatim — `ExceptionManager`, `HostSelector` registry, `HostAvailability`, `CacheMap` are CPU-bound, no I/O, safe to call from async code. New work: (1) detect+stash `DatabaseDialect` on `AsyncPluginServiceImpl`, (2) expose sync services via new async Protocol methods, (3) wire `AsyncAwsWrapperConnection.connect()` to resolve the dialect. No behavior change is user-visible after this phase; it unlocks B/C/D/F/G.

**Tech Stack:** Python 3.10–3.14, reuses sync `exception_handling.ExceptionManager`, `host_selector.*`, `host_availability.HostAvailability`, `utils.cache_map.CacheMap`. Tests via pytest + bare `asyncio.run()` (no `pytest-asyncio` dep). Verify via `/verify` after the final commit.

---

## File structure

**Modify:**
- `aws_advanced_python_wrapper/aio/plugin_service.py` — expand `AsyncPluginService` Protocol + `AsyncPluginServiceImpl`
- `aws_advanced_python_wrapper/aio/plugin_manager.py` — add `accepts_strategy` / `get_host_info_by_strategy` iteration
- `aws_advanced_python_wrapper/aio/default_plugin.py` — add `accepts_strategy` / `get_host_info_by_strategy` backed by sync host selectors
- `aws_advanced_python_wrapper/aio/plugin.py` — add `accepts_strategy` / `get_host_info_by_strategy` default no-op on `AsyncPlugin`
- `aws_advanced_python_wrapper/aio/wrapper.py` — resolve `DatabaseDialect` on connect, pass to `AsyncPluginServiceImpl`

**Create:**
- `tests/unit/test_aio_plugin_service_expansion.py` — new surface coverage

**No `mypy.ini` changes expected.** All new code type-checks under existing rules.

**Commit strategy:** one commit per task group (see below). Commit messages use conventional-commit style with `refactor(aio):` prefix since these add surface but don't change user-observable behavior yet.

---

## Task 1: Add `database_dialect` property

Exposes a `DatabaseDialect` instance on `AsyncPluginService`. `DatabaseDialect` comes from sync (`aws_advanced_python_wrapper.database_dialect`); the Protocol is pure Python, no async ops — safe to share. `AsyncPluginServiceImpl` stores it as an optional instance field set by the wrapper on connect (populated in Task 8).

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/plugin_service.py`
- Test: `tests/unit/test_aio_plugin_service_expansion.py`

- [ ] **Step 1.1: Write the failing test**

```python
# tests/unit/test_aio_plugin_service_expansion.py
from __future__ import annotations

from typing import Optional
from unittest.mock import MagicMock

from aws_advanced_python_wrapper.aio.driver_dialect.base import AsyncDriverDialect
from aws_advanced_python_wrapper.aio.plugin_service import AsyncPluginServiceImpl
from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
from aws_advanced_python_wrapper.utils.properties import Properties


def _make_service(database_dialect: Optional[DatabaseDialect] = None) -> AsyncPluginServiceImpl:
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    if database_dialect is not None:
        svc.database_dialect = database_dialect
    return svc


def test_database_dialect_defaults_to_none():
    svc = _make_service()
    assert svc.database_dialect is None


def test_database_dialect_is_settable():
    dialect = MagicMock(spec=DatabaseDialect)
    svc = _make_service(database_dialect=dialect)
    assert svc.database_dialect is dialect
```

- [ ] **Step 1.2: Run test to verify it fails**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py::test_database_dialect_defaults_to_none -v`
Expected: FAIL with `AttributeError: 'AsyncPluginServiceImpl' object has no attribute 'database_dialect'`

- [ ] **Step 1.3: Add to the Protocol and Impl**

In `aws_advanced_python_wrapper/aio/plugin_service.py`, add these imports near the existing `TYPE_CHECKING` block:

```python
if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties
```

Add property on the `AsyncPluginService` Protocol (after `driver_dialect`):

```python
    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        """The resolved :class:`DatabaseDialect`, or ``None`` before connect."""
        ...

    @database_dialect.setter
    def database_dialect(self, value: Optional[DatabaseDialect]) -> None:
        ...
```

In `AsyncPluginServiceImpl.__init__`, add after `self._driver_dialect = driver_dialect`:

```python
        self._database_dialect: Optional[DatabaseDialect] = None
```

Add property getter/setter on `AsyncPluginServiceImpl`:

```python
    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        return self._database_dialect

    @database_dialect.setter
    def database_dialect(self, value: Optional[DatabaseDialect]) -> None:
        self._database_dialect = value
```

- [ ] **Step 1.4: Run test to verify it passes**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v`
Expected: PASS for both `test_database_dialect_defaults_to_none` and `test_database_dialect_is_settable`.

- [ ] **Step 1.5: Commit**

```bash
git add aws_advanced_python_wrapper/aio/plugin_service.py tests/unit/test_aio_plugin_service_expansion.py
git commit -m "$(cat <<'EOF'
refactor(aio): expose database_dialect on AsyncPluginService (phase A.1)

Phase A foundation commit. Adds a settable database_dialect slot on
AsyncPluginServiceImpl so plugins can reach dialect-specific services
(exception classification, host id query, etc.). Slot is populated by
AsyncAwsWrapperConnection.connect in phase A.8; left None here.

Cites audit: P0 #1 #2 prerequisite.
EOF
)"
```

---

## Task 2: Add exception classification (`is_network_exception`, `is_login_exception`)

Plugins (failover, auth) need to classify driver exceptions. Sync uses `ExceptionManager` which takes a `DatabaseDialect`. `ExceptionManager` is pure sync (no I/O); reuse as-is. The async service delegates.

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/plugin_service.py`
- Test: `tests/unit/test_aio_plugin_service_expansion.py` (append)

- [ ] **Step 2.1: Write the failing tests**

Append to `tests/unit/test_aio_plugin_service_expansion.py`:

```python
def test_is_network_exception_returns_false_when_no_dialect():
    svc = _make_service()
    assert svc.is_network_exception(error=Exception("boom")) is False


def test_is_network_exception_delegates_to_dialect_handler():
    dialect = MagicMock(spec=DatabaseDialect)
    handler = MagicMock()
    handler.is_network_exception.return_value = True
    dialect.exception_handler = handler
    svc = _make_service(database_dialect=dialect)
    err = Exception("connection reset")
    assert svc.is_network_exception(error=err) is True
    handler.is_network_exception.assert_called_once_with(error=err, sql_state=None)


def test_is_login_exception_delegates_to_dialect_handler():
    dialect = MagicMock(spec=DatabaseDialect)
    handler = MagicMock()
    handler.is_login_exception.return_value = True
    dialect.exception_handler = handler
    svc = _make_service(database_dialect=dialect)
    err = Exception("auth failed")
    assert svc.is_login_exception(error=err, sql_state="28000") is True
    handler.is_login_exception.assert_called_once_with(error=err, sql_state="28000")
```

- [ ] **Step 2.2: Run — verify fail**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k exception`
Expected: FAIL (method not defined).

- [ ] **Step 2.3: Implement delegation**

Add to `aws_advanced_python_wrapper/aio/plugin_service.py` at top-level imports:

```python
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
```

Add to the `AsyncPluginService` Protocol:

```python
    def is_network_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        ...

    def is_login_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        ...
```

In `AsyncPluginServiceImpl.__init__`, add after `self._database_dialect = None`:

```python
        self._exception_manager: ExceptionManager = ExceptionManager()
```

Add methods to `AsyncPluginServiceImpl`:

```python
    def is_network_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_network_exception(
            self._database_dialect, error=error, sql_state=sql_state)

    def is_login_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_login_exception(
            self._database_dialect, error=error, sql_state=sql_state)
```

- [ ] **Step 2.4: Run — verify pass**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k exception`
Expected: PASS (3 tests).

- [ ] **Step 2.5: Commit**

```bash
git add aws_advanced_python_wrapper/aio/plugin_service.py tests/unit/test_aio_plugin_service_expansion.py
git commit -m "$(cat <<'EOF'
refactor(aio): add is_network/is_login exception delegation (phase A.2)

AsyncPluginService delegates exception classification to the sync
ExceptionManager, which itself looks up a per-dialect ExceptionHandler.
No async port needed -- classification is CPU-bound and pure Python.

Unblocks the failover plugin replacing its string-match heuristic
(audit P0 #1) with dialect-aware is_network_exception in phase B.
EOF
)"
```

---

## Task 3: Add host availability tracking (`set_availability`, `get_availability`)

Failover and other plugins mark hosts `UNAVAILABLE` so future connect attempts skip known-dead nodes. Sync uses a class-level `CacheMap[str, HostAvailability]` with TTL.

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/plugin_service.py`
- Test: `tests/unit/test_aio_plugin_service_expansion.py` (append)

- [ ] **Step 3.1: Write the failing tests**

Append:

```python
from aws_advanced_python_wrapper.host_availability import HostAvailability


def test_get_availability_returns_none_when_unset():
    svc = _make_service()
    assert svc.get_availability("host-1.cluster.example") is None


def test_set_then_get_availability():
    svc = _make_service()
    svc.set_availability(frozenset({"host-1.cluster.example"}), HostAvailability.UNAVAILABLE)
    assert svc.get_availability("host-1.cluster.example") == HostAvailability.UNAVAILABLE


def test_set_availability_covers_all_aliases():
    svc = _make_service()
    aliases = frozenset({"h1.example", "h1-alias.example"})
    svc.set_availability(aliases, HostAvailability.UNAVAILABLE)
    for alias in aliases:
        assert svc.get_availability(alias) == HostAvailability.UNAVAILABLE
```

- [ ] **Step 3.2: Run — verify fail**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k availability`
Expected: FAIL.

- [ ] **Step 3.3: Implement**

Add imports at top of `aio/plugin_service.py`:

```python
from typing import ClassVar, FrozenSet
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
```

Add to the `AsyncPluginService` Protocol:

```python
    def set_availability(
            self,
            host_aliases: FrozenSet[str],
            availability: HostAvailability) -> None:
        ...

    def get_availability(self, host_url: str) -> Optional[HostAvailability]:
        ...
```

In `AsyncPluginServiceImpl`, add a class-level cache (matches sync at `plugin_service.py:322`):

```python
    _host_availability_expiring_cache: ClassVar[CacheMap[str, HostAvailability]] = CacheMap()
    _HOST_AVAILABILITY_EXPIRATION_NANO: ClassVar[int] = 5 * 60 * 1_000_000_000  # 5 min (matches sync)
```

Add methods on `AsyncPluginServiceImpl`:

```python
    def set_availability(
            self,
            host_aliases: FrozenSet[str],
            availability: HostAvailability) -> None:
        for alias in host_aliases:
            AsyncPluginServiceImpl._host_availability_expiring_cache.put(
                alias,
                availability,
                AsyncPluginServiceImpl._HOST_AVAILABILITY_EXPIRATION_NANO,
            )

    def get_availability(self, host_url: str) -> Optional[HostAvailability]:
        return AsyncPluginServiceImpl._host_availability_expiring_cache.get(host_url)
```

- [ ] **Step 3.4: Run — verify pass**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k availability`
Expected: PASS (3).

- [ ] **Step 3.5: Commit**

```bash
git add aws_advanced_python_wrapper/aio/plugin_service.py tests/unit/test_aio_plugin_service_expansion.py
git commit -m "$(cat <<'EOF'
refactor(aio): add host availability tracking (phase A.3)

AsyncPluginService now exposes set_availability/get_availability backed
by a class-level CacheMap with 5-minute TTL, matching sync parity at
plugin_service.py:322. Unblocks failover marking failed hosts
UNAVAILABLE so retries skip known-dead nodes (audit P0 #3).
EOF
)"
```

---

## Task 4: Add host selection strategies (`accepts_strategy`, `get_host_info_by_strategy`)

Failover-reader and RWS need a named strategy to pick hosts ("random", "round_robin", "highest_weight"). Sync iterates through plugins, asking each if it supports the strategy; `DefaultPlugin` delegates to `ConnectionProviderManager`. For Phase A, async mirrors the iteration shape; `AsyncDefaultPlugin` answers via a sync `HostSelector` lookup. `AsyncPluginService` delegates to `AsyncPluginManager`, which iterates plugins.

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/plugin.py` (no-op default on `AsyncPlugin`)
- Modify: `aws_advanced_python_wrapper/aio/default_plugin.py` (impl via sync HostSelector)
- Modify: `aws_advanced_python_wrapper/aio/plugin_manager.py` (iteration)
- Modify: `aws_advanced_python_wrapper/aio/plugin_service.py` (delegation)
- Test: `tests/unit/test_aio_plugin_service_expansion.py` (append)

- [ ] **Step 4.1: Write the failing tests**

Append:

```python
import asyncio

from aws_advanced_python_wrapper.aio.default_plugin import AsyncDefaultPlugin
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.aio.plugin_manager import AsyncPluginManager
from aws_advanced_python_wrapper.host_selector import RANDOM_STRATEGY
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole


def _hosts(*tuples):
    # tuples: (url, role)
    return tuple(
        HostInfo(host=url, port=5432, role=role) for url, role in tuples
    )


def test_default_plugin_accepts_random_strategy():
    plugin = AsyncDefaultPlugin()
    assert plugin.accepts_strategy(HostRole.READER, RANDOM_STRATEGY) is True


def test_default_plugin_rejects_unknown_strategy():
    plugin = AsyncDefaultPlugin()
    assert plugin.accepts_strategy(HostRole.READER, "bogus_strategy") is False


def test_default_plugin_get_host_info_by_strategy_returns_matching_role():
    plugin = AsyncDefaultPlugin()
    reader = HostInfo(host="reader-1", port=5432, role=HostRole.READER)
    writer = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    chosen = plugin.get_host_info_by_strategy(
        HostRole.READER, RANDOM_STRATEGY, [reader, writer]
    )
    assert chosen is reader


def test_plugin_service_delegates_strategy_through_manager():
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    manager = AsyncPluginManager(svc, [AsyncDefaultPlugin()])
    svc.plugin_manager = manager
    assert svc.accepts_strategy(HostRole.READER, RANDOM_STRATEGY) is True
    reader = HostInfo(host="reader-1", port=5432, role=HostRole.READER)
    got = svc.get_host_info_by_strategy(HostRole.READER, RANDOM_STRATEGY, [reader])
    assert got is reader
```

- [ ] **Step 4.2: Run — verify fail**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k strategy`
Expected: FAIL.

- [ ] **Step 4.3: Add default no-op on `AsyncPlugin`**

In `aws_advanced_python_wrapper/aio/plugin.py`, add to `AsyncPlugin` class:

```python
    def accepts_strategy(self, role: "HostRole", strategy: str) -> bool:
        """Default: plugin does not support any strategy."""
        return False

    def get_host_info_by_strategy(
            self,
            role: "HostRole",
            strategy: str,
            host_list: Optional[List["HostInfo"]] = None) -> Optional["HostInfo"]:
        """Default: plugin does not participate in strategy-based selection."""
        return None
```

Add `TYPE_CHECKING` imports for `HostRole`, `HostInfo`, `List` if not already present.

- [ ] **Step 4.4: Implement `AsyncDefaultPlugin`**

In `aws_advanced_python_wrapper/aio/default_plugin.py`, add import:

```python
from aws_advanced_python_wrapper.host_selector import (
    HIGHEST_WEIGHT_STRATEGY, RANDOM_STRATEGY, ROUND_ROBIN_STRATEGY,
    WEIGHTED_RANDOM_STRATEGY, HighestWeightHostSelector,
    RandomHostSelector, RoundRobinHostSelector, WeightedRandomHostSelector,
)
from aws_advanced_python_wrapper.hostinfo import HostRole
from typing import List, Optional
```

Add to `AsyncDefaultPlugin`:

```python
    _SELECTORS = {
        RANDOM_STRATEGY: RandomHostSelector(),
        ROUND_ROBIN_STRATEGY: RoundRobinHostSelector(),
        HIGHEST_WEIGHT_STRATEGY: HighestWeightHostSelector(),
        WEIGHTED_RANDOM_STRATEGY: WeightedRandomHostSelector(),
    }

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        if role == HostRole.UNKNOWN:
            return False
        return strategy in self._SELECTORS

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        selector = self._SELECTORS.get(strategy)
        if selector is None or host_list is None:
            return None
        return selector.get_host(tuple(host_list), role)
```

Note: if the sync `host_selector` module does not export the strategy name constants, look up the names used in `host_selector.py:HIGHEST_WEIGHT_STRATEGY` etc. (they exist as module-level names — no import changes needed).

- [ ] **Step 4.5: Add iteration on `AsyncPluginManager`**

In `aws_advanced_python_wrapper/aio/plugin_manager.py`, add:

```python
    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        for plugin in self._plugins:
            if plugin.accepts_strategy(role, strategy):
                return True
        return False

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        for plugin in self._plugins:
            host = plugin.get_host_info_by_strategy(role, strategy, host_list)
            if host is not None:
                return host
        return None
```

Add any missing imports at the top (`HostRole`, `HostInfo`, `List`, `Optional`).

- [ ] **Step 4.6: Delegate from `AsyncPluginService`**

In `aws_advanced_python_wrapper/aio/plugin_service.py`, add a `plugin_manager` slot (settable by wrapper after construction — avoids circular init):

```python
    @property
    def plugin_manager(self) -> Optional["AsyncPluginManager"]:
        ...

    @plugin_manager.setter
    def plugin_manager(self, value: "AsyncPluginManager") -> None:
        ...
```

In the impl:

```python
        self._plugin_manager: Optional[AsyncPluginManager] = None

    @property
    def plugin_manager(self) -> Optional[AsyncPluginManager]:
        return self._plugin_manager

    @plugin_manager.setter
    def plugin_manager(self, value: AsyncPluginManager) -> None:
        self._plugin_manager = value

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        if self._plugin_manager is None:
            return False
        return self._plugin_manager.accepts_strategy(role, strategy)

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        if self._plugin_manager is None:
            return None
        return self._plugin_manager.get_host_info_by_strategy(role, strategy, host_list)
```

Add Protocol equivalents on `AsyncPluginService`.

Add imports for `HostRole`, `HostInfo`, `List`, `AsyncPluginManager` under `TYPE_CHECKING`.

- [ ] **Step 4.7: Run — verify pass**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k strategy`
Expected: PASS (4).

- [ ] **Step 4.8: Commit**

```bash
git add aws_advanced_python_wrapper/aio/plugin.py aws_advanced_python_wrapper/aio/default_plugin.py aws_advanced_python_wrapper/aio/plugin_manager.py aws_advanced_python_wrapper/aio/plugin_service.py tests/unit/test_aio_plugin_service_expansion.py
git commit -m "$(cat <<'EOF'
refactor(aio): add strategy-based host selection (phase A.4)

Mirrors sync plugin_service:1155-1182. AsyncPluginManager iterates
plugins; AsyncDefaultPlugin answers via the sync HostSelector registry.
Unblocks failover honoring FAILOVER_READER_HOST_SELECTOR_STRATEGY and
RWS honoring READER_HOST_SELECTOR_STRATEGY (audit P0 #4, #9).
EOF
)"
```

---

## Task 5: Bind host list provider (`host_list_provider`, `refresh_host_list`, `force_refresh_host_list`)

Failover, RWS, topology monitor all need to refresh topology. Sync's `PluginServiceImpl` owns a `HostListProvider`. Async has `AsyncHostListProvider` already (SP-3); we just need to plumb it through `AsyncPluginService`.

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/plugin_service.py`
- Test: `tests/unit/test_aio_plugin_service_expansion.py` (append)

- [ ] **Step 5.1: Write the failing tests**

Append:

```python
from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncHostListProvider


class _FakeHostListProvider:
    def __init__(self):
        self.refresh_calls = 0
        self.force_refresh_calls = 0
        self._topology = (HostInfo(host="writer-1", port=5432, role=HostRole.WRITER),)

    async def refresh(self, connection):
        self.refresh_calls += 1
        return self._topology

    async def force_refresh(self, connection):
        self.force_refresh_calls += 1
        return self._topology

    def get_cluster_id(self) -> str:
        return "test-cluster"


def test_host_list_provider_defaults_to_none():
    svc = _make_service()
    assert svc.host_list_provider is None


def test_host_list_provider_is_settable():
    svc = _make_service()
    hlp = _FakeHostListProvider()
    svc.host_list_provider = hlp  # type: ignore[assignment]
    assert svc.host_list_provider is hlp


def test_refresh_host_list_delegates_to_provider():
    svc = _make_service()
    hlp = _FakeHostListProvider()
    svc.host_list_provider = hlp  # type: ignore[assignment]
    topology = asyncio.run(svc.refresh_host_list())
    assert topology == hlp._topology
    assert hlp.refresh_calls == 1


def test_force_refresh_host_list_delegates_to_provider():
    svc = _make_service()
    hlp = _FakeHostListProvider()
    svc.host_list_provider = hlp  # type: ignore[assignment]
    topology = asyncio.run(svc.force_refresh_host_list())
    assert topology == hlp._topology
    assert hlp.force_refresh_calls == 1


def test_refresh_raises_when_no_provider():
    svc = _make_service()
    import pytest
    from aws_advanced_python_wrapper.errors import AwsWrapperError
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.refresh_host_list())
```

- [ ] **Step 5.2: Run — verify fail**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k host_list`
Expected: FAIL.

- [ ] **Step 5.3: Implement**

In `aws_advanced_python_wrapper/aio/plugin_service.py`, add Protocol:

```python
    @property
    def host_list_provider(self) -> Optional["AsyncHostListProvider"]:
        ...

    @host_list_provider.setter
    def host_list_provider(self, value: "AsyncHostListProvider") -> None:
        ...

    async def refresh_host_list(
            self,
            connection: Optional[Any] = None) -> Tuple["HostInfo", ...]:
        ...

    async def force_refresh_host_list(
            self,
            connection: Optional[Any] = None) -> Tuple["HostInfo", ...]:
        ...
```

In the Impl (add after `plugin_manager` slot):

```python
        self._host_list_provider: Optional[AsyncHostListProvider] = None

    @property
    def host_list_provider(self) -> Optional[AsyncHostListProvider]:
        return self._host_list_provider

    @host_list_provider.setter
    def host_list_provider(self, value: AsyncHostListProvider) -> None:
        self._host_list_provider = value

    async def refresh_host_list(
            self,
            connection: Optional[Any] = None) -> Tuple[HostInfo, ...]:
        if self._host_list_provider is None:
            from aws_advanced_python_wrapper.errors import AwsWrapperError
            raise AwsWrapperError("AsyncPluginService.host_list_provider is not set")
        conn = connection if connection is not None else self._current_connection
        return await self._host_list_provider.refresh(conn)

    async def force_refresh_host_list(
            self,
            connection: Optional[Any] = None) -> Tuple[HostInfo, ...]:
        if self._host_list_provider is None:
            from aws_advanced_python_wrapper.errors import AwsWrapperError
            raise AwsWrapperError("AsyncPluginService.host_list_provider is not set")
        conn = connection if connection is not None else self._current_connection
        return await self._host_list_provider.force_refresh(conn)
```

Add `Tuple` to typing import at top of file.

- [ ] **Step 5.4: Run — verify pass**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k host_list`
Expected: PASS (5).

- [ ] **Step 5.5: Commit**

```bash
git add aws_advanced_python_wrapper/aio/plugin_service.py tests/unit/test_aio_plugin_service_expansion.py
git commit -m "$(cat <<'EOF'
refactor(aio): bind host list provider via AsyncPluginService (phase A.5)

Mirrors sync plugin_service:579-591. AsyncHostListProvider lives on the
plugin service so failover, RWS, topology monitor can all refresh
without holding private references to the provider. Raises
AwsWrapperError if caller forgets to set the provider (boundary
condition; not a silent no-op).
EOF
)"
```

---

## Task 6: Add `initial_connection_host_info` property

Sync records the first `HostInfo` the user connected to so plugins can reason about "was this the originally-requested host". Async currently has no such slot. Failover retry uses this to fall back to the original writer when the topology refresh fails.

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/plugin_service.py`
- Test: `tests/unit/test_aio_plugin_service_expansion.py` (append)

- [ ] **Step 6.1: Write the failing tests**

Append:

```python
def test_initial_connection_host_info_defaults_to_none():
    svc = _make_service()
    assert svc.initial_connection_host_info is None


def test_initial_connection_host_info_is_settable():
    svc = _make_service()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    svc.initial_connection_host_info = host
    assert svc.initial_connection_host_info is host
```

- [ ] **Step 6.2: Run — verify fail**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k initial_connection`
Expected: FAIL.

- [ ] **Step 6.3: Implement**

Add to Protocol and Impl, following the `database_dialect` pattern from Task 1 (property + setter backed by `Optional[HostInfo]` instance field initialized to `None` in `__init__`).

- [ ] **Step 6.4: Run — verify pass**

Expected: PASS (2).

- [ ] **Step 6.5: Commit**

```bash
git add aws_advanced_python_wrapper/aio/plugin_service.py tests/unit/test_aio_plugin_service_expansion.py
git commit -m "$(cat <<'EOF'
refactor(aio): add initial_connection_host_info slot (phase A.6)

Tracks the first host the user connected to, so failover can fall back
to the original writer when topology refresh fails (audit P0 #4
prerequisite). Slot populated by AsyncAwsWrapperConnection.connect in
phase A.8.
EOF
)"
```

---

## Task 7: Add `release_resources()` cleanup hook

Plugins that spawn background tasks (topology monitor, EFM watchdog) need a hook to shut down cleanly. Sync uses `release_resources()` in `plugin_service.py:779`. Async already has `aio/cleanup.py:register_shutdown_hook`; we just need a `release_resources()` on the plugin service that closes the current connection and delegates to the host list provider if it's `CanReleaseResources`-like.

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/plugin_service.py`
- Test: `tests/unit/test_aio_plugin_service_expansion.py` (append)

- [ ] **Step 7.1: Write the failing test**

Append:

```python
class _ReleasableHostListProvider(_FakeHostListProvider):
    def __init__(self):
        super().__init__()
        self.released = False

    async def release_resources(self):
        self.released = True


def test_release_resources_closes_connection_and_provider():
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()

    async def _close(conn):
        conn.closed = True

    driver_dialect.abort_connection = MagicMock(side_effect=_close)
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    conn = MagicMock()
    conn.closed = False
    svc._current_connection = conn
    hlp = _ReleasableHostListProvider()
    svc.host_list_provider = hlp  # type: ignore[assignment]
    asyncio.run(svc.release_resources())
    assert hlp.released is True


def test_release_resources_survives_errors():
    svc = _make_service()
    driver_dialect = svc.driver_dialect
    driver_dialect.abort_connection = MagicMock(side_effect=RuntimeError("boom"))
    conn = MagicMock()
    conn.closed = False
    svc._current_connection = conn  # type: ignore[attr-defined]
    # Must not raise
    asyncio.run(svc.release_resources())
```

- [ ] **Step 7.2: Run — verify fail**

Run: `poetry run python -m pytest tests/unit/test_aio_plugin_service_expansion.py -v -k release_resources`
Expected: FAIL.

- [ ] **Step 7.3: Implement**

Add to Protocol:

```python
    async def release_resources(self) -> None:
        ...
```

Impl:

```python
    async def release_resources(self) -> None:
        # Close current connection best-effort -- do not raise.
        conn = self._current_connection
        if conn is not None:
            try:
                await self._driver_dialect.abort_connection(conn)
            except Exception:  # noqa: BLE001 - intentional best-effort teardown
                pass

        hlp = self._host_list_provider
        if hlp is not None and hasattr(hlp, "release_resources"):
            try:
                await hlp.release_resources()
            except Exception:  # noqa: BLE001
                pass
```

- [ ] **Step 7.4: Run — verify pass**

Expected: PASS (2).

- [ ] **Step 7.5: Commit**

```bash
git add aws_advanced_python_wrapper/aio/plugin_service.py tests/unit/test_aio_plugin_service_expansion.py
git commit -m "$(cat <<'EOF'
refactor(aio): add release_resources hook (phase A.7)

Matches sync plugin_service:779. Idempotent, best-effort teardown:
aborts current connection, calls host list provider's release hook if
present. Safe to call multiple times. Plugins can register their own
async shutdown via aio.cleanup.register_shutdown_hook as today.
EOF
)"
```

---

## Task 8: Wire `AsyncAwsWrapperConnection.connect()` to resolve `DatabaseDialect`

With all slots in place, populate them when the user calls `connect()`. For Phase A we use a minimal detection: honor the `wrapper_dialect` connection prop if provided; otherwise default to a driver-dialect-derived dialect (e.g., `PgDatabaseDialect` when the driver is psycopg, `MysqlDatabaseDialect` when aiomysql). Full auto-upgrade (querying the DB to detect Aurora vs stock) lands in a later phase.

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/wrapper.py`
- Modify: `aws_advanced_python_wrapper/aio/plugin_manager.py` (if it needs to accept and forward the dialect)
- Test: `tests/unit/test_aio_wrapper.py` (extend existing tests)

- [ ] **Step 8.1: Inspect the current connect flow**

Read `aws_advanced_python_wrapper/aio/wrapper.py` lines 240–300 (the `connect` classmethod) to locate where `AsyncPluginServiceImpl` is constructed. Note the variable name holding the `AsyncPluginServiceImpl` instance (likely `plugin_service`).

- [ ] **Step 8.2: Write the failing test**

In `tests/unit/test_aio_wrapper.py`, add:

```python
def test_connect_resolves_database_dialect_from_driver_dialect():
    from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
    from aws_advanced_python_wrapper.database_dialect import PgDatabaseDialect
    import asyncio

    async def _fake_target(**kwargs):
        return MagicMock(spec=["close", "cursor"])

    conn = asyncio.run(
        AsyncAwsWrapperConnection.connect(
            target=_fake_target,
            host="localhost",
            dbname="test",
            user="u",
            password="p",
        )
    )
    assert isinstance(conn._plugin_service.database_dialect, PgDatabaseDialect)
```

Adjust import paths to match the existing test file's style.

- [ ] **Step 8.3: Run — verify fail**

Run: `poetry run python -m pytest tests/unit/test_aio_wrapper.py::test_connect_resolves_database_dialect_from_driver_dialect -v`
Expected: FAIL.

- [ ] **Step 8.4: Add dialect resolution helper**

Add to `aws_advanced_python_wrapper/aio/wrapper.py`:

```python
def _resolve_database_dialect(
        driver_dialect: AsyncDriverDialect,
        props: Properties) -> DatabaseDialect:
    # Explicit override wins.
    explicit = WrapperProperties.DATABASE_DIALECT.get(props)
    if explicit:
        from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
        return DatabaseDialectManager(props).get_dialect(explicit, props)

    # Fall back to the driver dialect's default database dialect.
    dialect_code = driver_dialect.dialect_code
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
    return DatabaseDialectManager(props).get_dialect(dialect_code, props)
```

Call this helper in `connect()` right after the driver dialect is instantiated, and populate the plugin service:

```python
    database_dialect = _resolve_database_dialect(driver_dialect, props)
    plugin_service.database_dialect = database_dialect
```

Also set `plugin_service.host_list_provider = host_list_provider` (previously held as a local) and `plugin_service.plugin_manager = plugin_manager`.

If the initial connect succeeds, also populate `initial_connection_host_info`:

```python
    if plugin_service.initial_connection_host_info is None and plugin_service.current_host_info is not None:
        plugin_service.initial_connection_host_info = plugin_service.current_host_info
```

- [ ] **Step 8.5: Run — verify pass**

Run: `poetry run python -m pytest tests/unit/test_aio_wrapper.py -v`
Expected: new test PASS; existing tests still PASS.

- [ ] **Step 8.6: Commit**

```bash
git add aws_advanced_python_wrapper/aio/wrapper.py tests/unit/test_aio_wrapper.py
git commit -m "$(cat <<'EOF'
refactor(aio): wire AsyncAwsWrapperConnection.connect to resolve
DatabaseDialect and populate plugin service slots (phase A.8)

Populates database_dialect, host_list_provider, plugin_manager, and
initial_connection_host_info on AsyncPluginServiceImpl during connect,
so plugins can reach services added in phases A.1-A.7.

Dialect resolution is intentionally minimal for Phase A: honor explicit
wrapper_dialect prop, else fall back to driver-dialect default. Full
auto-upgrade (Aurora-vs-stock detection via DB query) lands in a later
phase.
EOF
)"
```

---

## Task 9: Final `/verify` and phase wrap-up

- [ ] **Step 9.1: Run the full local check suite**

Run: `poetry run mypy . && poetry run flake8 . && poetry run isort --check-only . && poetry run python -m pytest ./tests/unit -Werror`
Expected: all green.

- [ ] **Step 9.2: Verify no behavior regression**

Run: `poetry run python -m pytest tests/unit -v 2>&1 | tail -20`
Check: all previously-green tests still pass; new test file reports PASS for every test.

- [ ] **Step 9.3: Spot-check module imports work**

Run: `poetry run python -c "from aws_advanced_python_wrapper.aio.plugin_service import AsyncPluginService; print(sorted([m for m in dir(AsyncPluginService) if not m.startswith('_')]))"`
Expected: output includes `accepts_strategy`, `database_dialect`, `force_refresh_host_list`, `get_availability`, `get_host_info_by_strategy`, `host_list_provider`, `initial_connection_host_info`, `is_login_exception`, `is_network_exception`, `plugin_manager`, `refresh_host_list`, `release_resources`, `set_availability`.

- [ ] **Step 9.4: No commit needed** — all commits happen inside tasks 1–8.

---

## Out of scope (for Phase A)

- `session_state_service`: lands with Phase F (RWS) — the SessionStateService port is substantial on its own and RWS is the first consumer.
- `get_telemetry_factory`: lands in Phase I (OpenTelemetry plumbing).
- `connection_provider_manager`: lands with Phase F or a dedicated later phase — RWS's pool awareness is the first real consumer.
- `get_status` / `set_status` / `is_plugin_in_use`: not required by any of the 25 gaps; defer until a concrete need surfaces.
- Full DatabaseDialect auto-detection (Aurora-vs-stock by DB query): minimal code-based resolution in Task 8 is enough until Phase G, where the cluster topology monitor needs it.

## Self-review checklist (run before executing)

- [x] Every step has concrete code — no "TBD" or "similar to above"
- [x] File paths are absolute-relative to repo root
- [x] Every test cites a specific assertion, not just "test the method"
- [x] Every commit message cites the audit reference from the overview
- [x] No forward references to Phase B/C/D types or functions
- [x] Python syntax stays 3.10-compatible throughout (no `Self`, no PEP 695)
- [x] flake8 max-line-length=150 respected in all code blocks
