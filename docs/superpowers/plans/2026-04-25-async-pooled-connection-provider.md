# AsyncPooledConnectionProvider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `AsyncPooledConnectionProvider` to the wrapper's `aio/` submodule — a per-host, per-user async connection pool that gives async users feature parity with the sync `SqlAlchemyPooledConnectionProvider`, including all five host-selector strategies, the `pool_configurator` / `pool_mapping` / `accept_url_func` hooks, sliding-expiration cache with async disposal, and `release_resources()` teardown.

**Architecture:** Three new components in `aws_advanced_python_wrapper/aio/`:
1. `AsyncSlidingExpirationCache` — async-disposal-aware sibling of the sync sliding cache, in `aio/storage/sliding_expiration_cache_async.py`.
2. `_AsyncPool` (private) — asyncio-primitive pool over `asyncio.Queue` + `asyncio.Semaphore` + `asyncio.Lock`, in `aio/pooled_connection_provider.py`.
3. `AsyncPooledConnectionProvider` — public class implementing `AsyncConnectionProvider` + `AsyncCanReleaseResources` protocols, alongside `_PooledAsyncConnectionProxy` for release-on-close semantics.

The `AsyncCanReleaseResources` protocol already exists at `aws_advanced_python_wrapper/aio/plugin.py:120` (no need to add). `PoolKey` is reused from the sync `SqlAlchemyPooledConnectionProvider`.

**Tech Stack:** Python 3.10–3.14, asyncio (Queue, Semaphore, Lock), pytest 7.x, `pytest-mock` (already in dev deps), `unittest.mock.AsyncMock`.

**Spec reference:** `docs/superpowers/specs/2026-04-25-async-pooled-connection-provider-design.md`

---

## File Structure

### Files to create

| File | Lines (est.) | Responsibility |
|---|---|---|
| `aws_advanced_python_wrapper/aio/storage/__init__.py` | 1 | Empty `__init__.py` for the new submodule |
| `aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py` | ~140 | `AsyncSlidingExpirationCache[K, V]` with async disposal callbacks |
| `aws_advanced_python_wrapper/aio/pooled_connection_provider.py` | ~320 | `_AsyncPool`, `_PooledAsyncConnectionProxy`, `AsyncPooledConnectionProvider`; re-exports `PoolKey` |
| `tests/unit/test_aio_sliding_expiration_cache.py` | ~250 | Cache unit tests |
| `tests/unit/test_aio_pooled_connection_provider.py` | ~600 | Provider, pool, and proxy unit tests |

### Files to modify

| File | Lines | What |
|---|---|---|
| `aws_advanced_python_wrapper/resources/aws_advanced_python_wrapper_messages.properties` | +3 | Three new `AsyncPooledConnectionProvider.*` entries |
| `aws_advanced_python_wrapper/aio/__init__.py` | +2 | Export `AsyncPooledConnectionProvider` |
| `tests/integration/container/test_read_write_splitting_async.py` | replace 2-3 lines | Drop `# type: ignore[arg-type]`; switch from sync `SqlAlchemyPooledConnectionProvider` to `AsyncPooledConnectionProvider` |
| `tests/integration/container/test_autoscaling_async.py` | replace 2-3 lines | Same as above |

---

## Task 1: Add Messages entries

**Files:**
- Modify: `aws_advanced_python_wrapper/resources/aws_advanced_python_wrapper_messages.properties` — add 3 lines after line 431 (after the existing `SqlAlchemyPooledConnectionProvider.UnableToCreateDefaultKey` entry)

- [ ] **Step 1: Add the three new message keys**

Open `aws_advanced_python_wrapper/resources/aws_advanced_python_wrapper_messages.properties`. Find these existing lines (around line 430–431):

```properties
SqlAlchemyPooledConnectionProvider.PoolNone=[SqlAlchemyPooledConnectionProvider] Attempted to find or create a pool for '{}' but the result of the attempt evaluated to None.
SqlAlchemyPooledConnectionProvider.UnableToCreateDefaultKey=[SqlAlchemyPooledConnectionProvider] Unable to create a default key for internal connection pools. By default, the user parameter is used, but the given user evaluated to None or the empty string (""). Please ensure you have passed a valid user in the connection properties.
```

Insert these three new lines immediately after, before the blank line that separates groups:

```properties
AsyncPooledConnectionProvider.PoolNone=[AsyncPooledConnectionProvider] Attempted to find or create a pool for '{}' but the result of the attempt evaluated to None.
AsyncPooledConnectionProvider.UnableToCreateDefaultKey=[AsyncPooledConnectionProvider] Unable to create a default key for internal connection pools. By default, the user parameter is used, but the given user evaluated to None or the empty string (""). Please ensure you have passed a valid user in the connection properties.
AsyncPooledConnectionProvider.PoolDisposed=[AsyncPooledConnectionProvider] Cannot acquire a connection from the pool for '{}' because the pool is being disposed.
```

- [ ] **Step 2: Verify the keys are loadable**

```bash
poetry run python -c "from aws_advanced_python_wrapper.utils.messages import Messages; print(Messages.get_formatted('AsyncPooledConnectionProvider.PoolNone', 'host:5432')); print(Messages.get('AsyncPooledConnectionProvider.UnableToCreateDefaultKey')); print(Messages.get_formatted('AsyncPooledConnectionProvider.PoolDisposed', 'host:5432'))"
```

Expected: prints all three formatted messages.

- [ ] **Step 3: Commit**

```bash
git add aws_advanced_python_wrapper/resources/aws_advanced_python_wrapper_messages.properties
git commit -m "$(cat <<'EOF'
feat(aio): add AsyncPooledConnectionProvider message keys

Three message keys mirroring the sync SqlAlchemyPooledConnectionProvider
entries plus PoolDisposed (a state the async pool exposes that sync did
not need): PoolNone, UnableToCreateDefaultKey, PoolDisposed.
EOF
)"
```

---

## Task 2: Create `AsyncSlidingExpirationCache` storage submodule + cache class

**Files:**
- Create: `aws_advanced_python_wrapper/aio/storage/__init__.py`
- Create: `aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py`
- Create: `tests/unit/test_aio_sliding_expiration_cache.py`

### Step 1: Create the empty submodule

- [ ] **Step 1a: Create `aws_advanced_python_wrapper/aio/storage/__init__.py`** with only a license header:

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
```

### Step 2: Write the failing tests first (TDD)

- [ ] **Step 2a: Create `tests/unit/test_aio_sliding_expiration_cache.py`** with the full test suite:

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
import time

from aws_advanced_python_wrapper.aio.storage.sliding_expiration_cache_async import (
    AsyncSlidingExpirationCache, CacheItem)


def test_compute_if_absent_creates_when_missing():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()
        calls = []

        async def factory(key):
            calls.append(key)
            return 42

        value = await cache.compute_if_absent("k", factory, item_expiration_ns=10**12)
        assert value == 42
        assert calls == ["k"]

    asyncio.run(inner())


def test_compute_if_absent_returns_existing_without_recomputing():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()
        calls = []

        async def factory(key):
            calls.append(key)
            return 99

        first = await cache.compute_if_absent("k", factory, 10**12)
        second = await cache.compute_if_absent("k", factory, 10**12)
        assert first == 99
        assert second == 99
        assert calls == ["k"]  # factory called only once

    asyncio.run(inner())


def test_compute_if_absent_extends_expiration_on_access():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()
        async def factory(key):
            return 1
        await cache.compute_if_absent("k", factory, item_expiration_ns=10**12)
        items = cache.items()
        first_expiration = items[0][1].expiration_time
        # Access again — expiration should advance
        await cache.compute_if_absent("k", factory, item_expiration_ns=2 * 10**12)
        items_after = cache.items()
        assert items_after[0][1].expiration_time > first_expiration

    asyncio.run(inner())


def test_remove_invokes_async_disposal_callback():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            item_disposal_func=disposal,
        )

        async def factory(key):
            return 7

        await cache.compute_if_absent("k", factory, 10**12)
        await cache.remove("k")
        assert disposed == [7]
        assert "k" not in cache

    asyncio.run(inner())


def test_clear_invokes_disposal_for_each_item():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            item_disposal_func=disposal,
        )

        async def factory(key):
            return ord(key)

        await cache.compute_if_absent("a", factory, 10**12)
        await cache.compute_if_absent("b", factory, 10**12)
        await cache.clear()
        assert sorted(disposed) == [ord("a"), ord("b")]
        assert len(cache) == 0

    asyncio.run(inner())


def test_cleanup_disposes_expired_items():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            cleanup_interval_ns=0,  # always cleanup
            item_disposal_func=disposal,
        )

        async def factory(key):
            return 5

        # Use a very short expiration so the entry is expired immediately
        await cache.compute_if_absent("k", factory, item_expiration_ns=1)
        # Sleep enough to surely exceed 1 ns
        time.sleep(0.001)
        # Trigger cleanup by inserting another key (cleanup runs inline)
        await cache.compute_if_absent("k2", factory, item_expiration_ns=10**12)
        assert disposed == [5]
        assert "k" not in cache
        assert "k2" in cache

    asyncio.run(inner())


def test_cleanup_skips_when_should_dispose_returns_false():
    async def inner():
        disposed = []

        async def disposal(value):
            disposed.append(value)

        def should_dispose(value):
            return False  # never dispose

        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache(
            cleanup_interval_ns=0,
            should_dispose_func=should_dispose,
            item_disposal_func=disposal,
        )

        async def factory(key):
            return 3

        await cache.compute_if_absent("k", factory, item_expiration_ns=1)
        time.sleep(0.001)
        await cache.compute_if_absent("k2", factory, item_expiration_ns=10**12)
        assert disposed == []
        assert "k" in cache  # not disposed because should_dispose returned False

    asyncio.run(inner())


def test_get_returns_existing_value():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()
        async def factory(key):
            return 11
        await cache.compute_if_absent("k", factory, 10**12)
        assert cache.get("k") == 11
        assert cache.get("missing") is None

    asyncio.run(inner())


def test_keys_and_items_match():
    async def inner():
        cache: AsyncSlidingExpirationCache[str, int] = AsyncSlidingExpirationCache()
        async def factory(key):
            return ord(key)
        await cache.compute_if_absent("a", factory, 10**12)
        await cache.compute_if_absent("b", factory, 10**12)
        assert sorted(cache.keys()) == ["a", "b"]
        assert {k for k, _ in cache.items()} == {"a", "b"}

    asyncio.run(inner())


def test_cache_item_dataclass_holds_value_and_expiration():
    item = CacheItem(item="x", expiration_time=12345)
    assert item.item == "x"
    assert item.expiration_time == 12345
```

- [ ] **Step 2b: Run the tests to confirm they fail**

```bash
poetry run pytest tests/unit/test_aio_sliding_expiration_cache.py -v 2>&1 | tail -20
```

Expected: every test errors with `ModuleNotFoundError: No module named 'aws_advanced_python_wrapper.aio.storage.sliding_expiration_cache_async'` — module doesn't exist yet.

### Step 3: Implement `AsyncSlidingExpirationCache`

- [ ] **Step 3a: Create `aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py`**:

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

"""Async-disposal-aware sliding-expiration cache.

Mirrors :class:`aws_advanced_python_wrapper.utils.storage.sliding_expiration_cache.SlidingExpirationCache`
in shape and semantics. Differences:

* ``compute_if_absent`` is ``async def`` because the mapping function
  returns an awaitable.
* ``remove`` / ``clear`` / ``cleanup`` are ``async def`` because the
  disposal callback is async (closes pooled async connections).
* ``should_dispose_func`` stays sync because it's a quick predicate.

Cross-loop usage is callers' responsibility — the cache itself uses
``asyncio.Lock`` for in-loop write serialization.
"""

from __future__ import annotations

import asyncio
from time import perf_counter_ns
from typing import (Awaitable, Callable, Generic, List, Optional, Tuple,
                    TypeVar)

from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.log import Logger

K = TypeVar("K")
V = TypeVar("V")
logger = Logger(__name__)


class CacheItem(Generic[V]):
    def __init__(self, item: V, expiration_time: int):
        self.item = item
        self.expiration_time = expiration_time

    def __str__(self) -> str:
        return f"CacheItem [item={self.item!s}, expiration_time={self.expiration_time}]"

    def update_expiration(self, expiration_interval_ns: int) -> "CacheItem[V]":
        self.expiration_time = perf_counter_ns() + expiration_interval_ns
        return self


class AsyncSlidingExpirationCache(Generic[K, V]):
    def __init__(
            self,
            cleanup_interval_ns: int = 10 * 60_000_000_000,  # 10 minutes
            should_dispose_func: Optional[Callable[[V], bool]] = None,
            item_disposal_func: Optional[Callable[[V], Awaitable[None]]] = None,
    ):
        self._cleanup_interval_ns = cleanup_interval_ns
        self._should_dispose_func = should_dispose_func
        self._item_disposal_func = item_disposal_func

        self._cdict: ConcurrentDict[K, CacheItem[V]] = ConcurrentDict()
        self._cleanup_time_ns: AtomicInt = AtomicInt(perf_counter_ns() + cleanup_interval_ns)
        self._write_lock: asyncio.Lock = asyncio.Lock()

    def __len__(self) -> int:
        return len(self._cdict)

    def __contains__(self, key: K) -> bool:
        return key in self._cdict

    def set_cleanup_interval_ns(self, interval_ns: int) -> None:
        self._cleanup_interval_ns = interval_ns

    def keys(self) -> List[K]:
        return self._cdict.keys()

    def items(self) -> List[Tuple[K, CacheItem[V]]]:
        return self._cdict.items()

    def get(self, key: K) -> Optional[V]:
        cache_item = self._cdict.get(key)
        return cache_item.item if cache_item is not None else None

    async def compute_if_absent(
            self,
            key: K,
            mapping_func: Callable[[K], Awaitable[V]],
            item_expiration_ns: int,
    ) -> Optional[V]:
        await self.cleanup()
        async with self._write_lock:
            existing = self._cdict.get(key)
            if existing is not None:
                return existing.update_expiration(item_expiration_ns).item
            new_value: V = await mapping_func(key)
            new_item = CacheItem(new_value, perf_counter_ns() + item_expiration_ns)
            self._cdict.put(key, new_item)
            return new_value

    async def put(self, key: K, value: V, item_expiration_ns: int) -> None:
        await self.cleanup()
        async with self._write_lock:
            old = self._cdict.remove(key)
            if old is not None and self._item_disposal_func is not None:
                await self._item_disposal_func(old.item)
            self._cdict.put(key, CacheItem(value, perf_counter_ns() + item_expiration_ns))

    async def remove(self, key: K) -> None:
        async with self._write_lock:
            cache_item = self._cdict.remove(key)
        if cache_item is not None and self._item_disposal_func is not None:
            await self._item_disposal_func(cache_item.item)
        await self.cleanup()

    async def clear(self) -> None:
        async with self._write_lock:
            items = self._cdict.items()
            self._cdict.clear()
        if self._item_disposal_func is not None:
            for _, cache_item in items:
                try:
                    await self._item_disposal_func(cache_item.item)
                except Exception:  # noqa: BLE001 - best-effort teardown
                    pass

    async def cleanup(self) -> None:
        current_time = perf_counter_ns()
        if self._cleanup_time_ns.get() > current_time:
            return
        self._cleanup_time_ns.set(current_time + self._cleanup_interval_ns)

        to_dispose: List[V] = []
        async with self._write_lock:
            for key in self._cdict.keys():
                cache_item = self._cdict.remove_key_if(key, self._should_cleanup_item)
                if cache_item is not None:
                    to_dispose.append(cache_item.item)
        for item in to_dispose:
            if self._item_disposal_func is not None:
                try:
                    await self._item_disposal_func(item)
                except Exception:  # noqa: BLE001 - best-effort teardown
                    pass

    def _should_cleanup_item(self, cache_item: "CacheItem[V]") -> bool:
        if self._should_dispose_func is not None:
            return perf_counter_ns() > cache_item.expiration_time and self._should_dispose_func(cache_item.item)
        return perf_counter_ns() > cache_item.expiration_time
```

- [ ] **Step 3b: Run the tests to confirm they pass**

```bash
poetry run pytest tests/unit/test_aio_sliding_expiration_cache.py -v 2>&1 | tail -20
```

Expected: all 10 tests pass.

- [ ] **Step 3c: Lint + type check**

```bash
poetry run mypy aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py tests/unit/test_aio_sliding_expiration_cache.py
poetry run flake8 aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py tests/unit/test_aio_sliding_expiration_cache.py
poetry run isort --check-only aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py tests/unit/test_aio_sliding_expiration_cache.py
```

All must succeed. Auto-fix isort with `poetry run isort <files>` if needed.

- [ ] **Step 3d: Commit**

```bash
git add aws_advanced_python_wrapper/aio/storage/__init__.py \
        aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py \
        tests/unit/test_aio_sliding_expiration_cache.py
git commit -m "$(cat <<'EOF'
feat(aio): add AsyncSlidingExpirationCache

Async-disposal-aware sibling of utils.storage.SlidingExpirationCache
for caching items whose teardown is async (e.g., async connection pools).
Reuses the sync ConcurrentDict + AtomicInt primitives. asyncio.Lock
serializes writes within a single event loop. Cross-loop usage is the
caller's responsibility.

Includes 10 unit tests covering compute_if_absent, expiration, async
disposal in remove/clear, sliding cleanup, and should-dispose gating.
EOF
)"
```

---

## Task 3: Implement `_AsyncPool` (private pool primitive)

**Files:**
- Create (initial): `aws_advanced_python_wrapper/aio/pooled_connection_provider.py`
- Create: `tests/unit/test_aio_pooled_connection_provider.py` (will grow over Tasks 3–5)

This task introduces the file with the `_AsyncPool` class only. Subsequent tasks add the proxy and the public provider.

### Step 1: Write the failing tests

- [ ] **Step 1a: Create `tests/unit/test_aio_pooled_connection_provider.py`** with the pool tests:

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
from unittest.mock import AsyncMock

import pytest

from aws_advanced_python_wrapper.aio.pooled_connection_provider import \
    _AsyncPool
from aws_advanced_python_wrapper.errors import AwsWrapperError


# ---------- _AsyncPool tests ----------

def test_pool_acquire_invokes_creator_when_idle_empty():
    async def inner():
        creator = AsyncMock(return_value="conn-1")
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        assert conn == "conn-1"
        assert pool.checkedout() == 1
        creator.assert_awaited_once()

    asyncio.run(inner())


def test_pool_acquire_reuses_idle_conn():
    async def inner():
        creator = AsyncMock(side_effect=["conn-A", "conn-B"])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        c1 = await pool.acquire()
        await pool.release(c1)  # back to idle
        c2 = await pool.acquire()
        assert c2 == "conn-A"  # reused
        assert creator.await_count == 1  # not called again

    asyncio.run(inner())


def test_pool_release_invalidated_actually_closes():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        await pool.release(conn, invalidated=True)
        raw.close.assert_awaited_once()
        assert pool.checkedout() == 0

    asyncio.run(inner())


def test_pool_dispose_closes_idle_conns():
    async def inner():
        raw_a, raw_b = AsyncMock(), AsyncMock()
        raw_a.close = AsyncMock()
        raw_b.close = AsyncMock()
        creator = AsyncMock(side_effect=[raw_a, raw_b])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        await pool.release(c1)
        await pool.release(c2)
        assert pool.checkedout() == 0
        await pool.dispose()
        raw_a.close.assert_awaited_once()
        raw_b.close.assert_awaited_once()
        assert pool.is_disposing()

    asyncio.run(inner())


def test_pool_acquire_after_dispose_raises():
    async def inner():
        creator = AsyncMock(return_value="c")
        pool = _AsyncPool(creator=creator, max_size=1, max_overflow=0)
        await pool.dispose()
        with pytest.raises(AwsWrapperError, match="PoolDisposed"):
            await pool.acquire()

    asyncio.run(inner())


def test_pool_release_when_disposing_actually_closes():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        # Mark for disposal (do not await dispose itself — release must observe state)
        pool._disposing = True  # internal state inspection for test
        await pool.release(conn)
        raw.close.assert_awaited_once()

    asyncio.run(inner())


def test_pool_acquire_blocks_at_max_when_overflow_zero():
    async def inner():
        creator = AsyncMock(side_effect=["a", "b", "c"])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0, timeout_seconds=0.05)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        # Third acquire should time out because semaphore is full
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.1)
        # Cleanup
        await pool.release(c1)
        await pool.release(c2)

    asyncio.run(inner())


def test_pool_acquire_uses_overflow_when_configured():
    async def inner():
        creator = AsyncMock(side_effect=["a", "b", "c"])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=1, timeout_seconds=0.1)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        c3 = await pool.acquire()  # overflow permits one more
        assert pool.checkedout() == 3
        # 4th would block / timeout
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.1)
        await pool.release(c1)
        await pool.release(c2)
        await pool.release(c3)

    asyncio.run(inner())


def test_pool_concurrent_acquire_release_keeps_checkedout_correct():
    async def inner():
        n = 50
        creator = AsyncMock(side_effect=[f"c{i}" for i in range(n)])
        pool = _AsyncPool(creator=creator, max_size=10, max_overflow=10)

        async def worker():
            conn = await pool.acquire()
            await asyncio.sleep(0.001)
            await pool.release(conn)

        await asyncio.gather(*(worker() for _ in range(n)))
        assert pool.checkedout() == 0

    asyncio.run(inner())
```

- [ ] **Step 1b: Run tests to verify they fail**

```bash
poetry run pytest tests/unit/test_aio_pooled_connection_provider.py -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name '_AsyncPool' from 'aws_advanced_python_wrapper.aio.pooled_connection_provider'` (file doesn't exist yet).

### Step 2: Create the file with `_AsyncPool` only

- [ ] **Step 2a: Create `aws_advanced_python_wrapper/aio/pooled_connection_provider.py`**:

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

"""Per-host, per-user async connection pool registry.

:class:`AsyncPooledConnectionProvider` is the async counterpart of
:class:`aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider`,
implemented over asyncio primitives instead of SQLAlchemy's pool. The
underlying pool (:class:`_AsyncPool`) holds raw async connections
(``psycopg.AsyncConnection`` / ``aiomysql.Connection``); the proxy
(:class:`_PooledAsyncConnectionProxy`) routes ``close()`` /
``__aexit__`` to release-to-pool semantics.
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Optional)

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    pass

logger = Logger(__name__)


class _AsyncPool:
    """Bounded async connection pool over asyncio primitives.

    The pool's max concurrency is ``max_size + max_overflow``; ``acquire``
    blocks (with a timeout) when the cap is reached. Idle connections are
    held in an ``asyncio.Queue``; new ones are created on demand via the
    async ``creator`` callable.

    Disposal is one-way: once :meth:`dispose` is called the pool refuses
    new acquires and closes idle conns immediately; in-flight conns close
    on the next :meth:`release` instead of returning to the queue.
    """

    def __init__(
            self,
            creator: Callable[[], Awaitable[Any]],
            max_size: int = 5,
            max_overflow: int = 10,
            timeout_seconds: float = 30.0,
    ):
        self._creator = creator
        self._max_size = max_size
        self._max_overflow = max_overflow
        self._timeout_seconds = timeout_seconds
        self._idle: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_size + max_overflow)
        self._semaphore = asyncio.Semaphore(max_size + max_overflow)
        self._checkedout: int = 0
        self._disposing: bool = False

    def checkedout(self) -> int:
        return self._checkedout

    def is_disposing(self) -> bool:
        return self._disposing

    async def acquire(self) -> Any:
        if self._disposing:
            raise AwsWrapperError(
                Messages.get_formatted("AsyncPooledConnectionProvider.PoolDisposed", "<pool>"))
        await asyncio.wait_for(self._semaphore.acquire(), timeout=self._timeout_seconds)
        try:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                conn = await self._creator()
            self._checkedout += 1
            return conn
        except BaseException:
            self._semaphore.release()
            raise

    async def release(self, conn: Any, invalidated: bool = False) -> None:
        try:
            self._checkedout -= 1
            if invalidated or self._disposing:
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
                return
            try:
                self._idle.put_nowait(conn)
            except asyncio.QueueFull:
                # Should not normally happen — semaphore should have prevented it.
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
        finally:
            self._semaphore.release()

    async def dispose(self) -> None:
        self._disposing = True
        # Drain idle queue and close each conn.
        while True:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await conn.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass
```

- [ ] **Step 2b: Run the tests to confirm they pass**

```bash
poetry run pytest tests/unit/test_aio_pooled_connection_provider.py -v 2>&1 | tail -20
```

Expected: all 9 pool tests pass.

- [ ] **Step 2c: Lint + type check**

```bash
poetry run mypy aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
poetry run flake8 aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
poetry run isort --check-only aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
```

All must succeed.

- [ ] **Step 2d: Commit**

```bash
git add aws_advanced_python_wrapper/aio/pooled_connection_provider.py \
        tests/unit/test_aio_pooled_connection_provider.py
git commit -m "$(cat <<'EOF'
feat(aio): add _AsyncPool primitive for async connection pooling

Bounded async connection pool over asyncio.Queue + asyncio.Semaphore.
Acquire/release with timeout; one-way dispose that closes idle conns
immediately and routes in-flight returns to close instead of queue.

This is the underlying primitive for AsyncPooledConnectionProvider; it
holds raw async connections and is wrapped by a proxy in the next
commit for release-on-close semantics.
EOF
)"
```

---

## Task 4: Add `_PooledAsyncConnectionProxy`

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/pooled_connection_provider.py` — append the proxy class
- Modify: `tests/unit/test_aio_pooled_connection_provider.py` — append proxy tests

### Step 1: Write the failing tests

- [ ] **Step 1a: Append to `tests/unit/test_aio_pooled_connection_provider.py`** the proxy tests:

```python
# ---------- _PooledAsyncConnectionProxy tests ----------

from aws_advanced_python_wrapper.aio.pooled_connection_provider import \
    _PooledAsyncConnectionProxy


def test_proxy_close_returns_to_pool_not_actual_close():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        await proxy.close()
        # raw conn was NOT actually closed — returned to pool
        raw.close.assert_not_awaited()
        assert pool.checkedout() == 0
        # Re-acquire returns the same raw
        again = await pool.acquire()
        assert again is raw

    asyncio.run(inner())


def test_proxy_aexit_returns_to_pool():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        async with proxy:
            pass  # __aexit__ should release
        raw.close.assert_not_awaited()
        assert pool.checkedout() == 0

    asyncio.run(inner())


def test_proxy_invalidate_forces_actual_close_on_release():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        proxy.invalidate()
        await proxy.close()
        raw.close.assert_awaited_once()
        assert pool.checkedout() == 0

    asyncio.run(inner())


def test_proxy_delegates_unknown_attrs_to_raw_conn():
    async def inner():
        raw = AsyncMock()
        raw.cursor = AsyncMock(return_value="cursor-obj")
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        # Access an attribute not defined on the proxy itself
        result = await proxy.cursor()
        assert result == "cursor-obj"
        await proxy.close()

    asyncio.run(inner())
```

- [ ] **Step 1b: Run to verify failure**

```bash
poetry run pytest tests/unit/test_aio_pooled_connection_provider.py::test_proxy_close_returns_to_pool_not_actual_close -v 2>&1 | tail -10
```

Expected: `ImportError: cannot import name '_PooledAsyncConnectionProxy'`.

### Step 2: Implement the proxy

- [ ] **Step 2a: Append to `aws_advanced_python_wrapper/aio/pooled_connection_provider.py`** (after the `_AsyncPool` class):

```python
class _PooledAsyncConnectionProxy:
    """Wraps a raw async connection acquired from a :class:`_AsyncPool`.

    ``close()`` and ``__aexit__`` route to ``pool.release(...)`` instead
    of closing the underlying connection. Use :meth:`invalidate` before
    closing to force an actual close (e.g., after a SQL error left the
    connection in a bad state).

    All other attributes are delegated to the underlying connection via
    ``__getattr__``, so cursor / transaction / driver-specific methods
    work transparently.
    """

    def __init__(self, raw_conn: Any, pool: "_AsyncPool"):
        # Set as instance attributes so __getattr__ doesn't fall through to raw_conn.
        self._raw = raw_conn
        self._pool = pool
        self._invalidated = False

    def invalidate(self) -> None:
        self._invalidated = True

    async def close(self) -> None:
        await self._pool.release(self._raw, invalidated=self._invalidated)

    async def __aenter__(self) -> "_PooledAsyncConnectionProxy":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        # Only fires for attributes not on `self`. The `_raw` / `_pool` /
        # `_invalidated` attributes set in __init__ shadow any equivalents
        # on the underlying conn.
        return getattr(self._raw, name)
```

- [ ] **Step 2b: Run the proxy tests to confirm they pass**

```bash
poetry run pytest tests/unit/test_aio_pooled_connection_provider.py -v 2>&1 | tail -20
```

Expected: all 13 tests (9 pool + 4 proxy) pass.

- [ ] **Step 2c: Lint + type check**

```bash
poetry run mypy aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
poetry run flake8 aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
poetry run isort --check-only aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
```

- [ ] **Step 2d: Commit**

```bash
git add aws_advanced_python_wrapper/aio/pooled_connection_provider.py \
        tests/unit/test_aio_pooled_connection_provider.py
git commit -m "$(cat <<'EOF'
feat(aio): add _PooledAsyncConnectionProxy

Wraps a raw async connection acquired from _AsyncPool. close() and
__aexit__ route to pool.release(...); invalidate() forces actual
close on next release (for connections left in a bad state by SQL
errors). Other attributes delegate to the underlying conn via
__getattr__ so cursor / transaction / driver methods pass through
transparently.
EOF
)"
```

---

## Task 5: Add `AsyncPooledConnectionProvider` (public class)

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/pooled_connection_provider.py` — append imports + the public provider class
- Modify: `tests/unit/test_aio_pooled_connection_provider.py` — append provider tests

### Step 1: Write the failing tests

- [ ] **Step 1a: Append to `tests/unit/test_aio_pooled_connection_provider.py`** the provider tests:

```python
# ---------- AsyncPooledConnectionProvider tests ----------

from unittest.mock import MagicMock

from aws_advanced_python_wrapper.aio.pooled_connection_provider import (
    AsyncPooledConnectionProvider, PoolKey)
from aws_advanced_python_wrapper.host_selector import (HostRole,
                                                       RandomHostSelector)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType


def _make_host_info(host: str = "instance-1.cluster-xyz.us-west-2.rds.amazonaws.com",
                    port: int = 5432, role: HostRole = HostRole.WRITER) -> HostInfo:
    return HostInfo(host=host, port=port, role=role)


def _make_props(user: str = "admin", password: str = "secret") -> Properties:
    p = Properties()
    p[WrapperProperties.USER.name] = user
    p[WrapperProperties.PASSWORD.name] = password
    return p


def _reset_class_state():
    """Clear class-level pool cache between tests."""
    AsyncPooledConnectionProvider._database_pools = type(
        AsyncPooledConnectionProvider._database_pools
    )(
        cleanup_interval_ns=10 * 60_000_000_000,
        should_dispose_func=lambda pool: pool.checkedout() == 0,
        item_disposal_func=lambda pool: pool.dispose(),
    )


def test_accepts_host_info_default_filters_to_rds_instance():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    instance_host = _make_host_info()
    cluster_host = _make_host_info(host="my-cluster.cluster-xyz.us-west-2.rds.amazonaws.com")
    assert provider.accepts_host_info(instance_host, _make_props()) is True
    assert provider.accepts_host_info(cluster_host, _make_props()) is False


def test_accepts_host_info_uses_custom_accept_url_func():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider(
        accept_url_func=lambda host_info, props: host_info.host == "allow-me",
    )
    assert provider.accepts_host_info(_make_host_info(host="allow-me"), _make_props()) is True
    assert provider.accepts_host_info(_make_host_info(host="deny-me"), _make_props()) is False


def test_accepts_strategy_default_strategies():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    for strat in ("random", "round_robin", "weighted_random", "highest_weight"):
        assert provider.accepts_strategy(HostRole.WRITER, strat) is True


def test_accepts_strategy_least_connections():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    assert provider.accepts_strategy(HostRole.WRITER, "least_connections") is True


def test_accepts_strategy_unknown_returns_false():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    assert provider.accepts_strategy(HostRole.WRITER, "totally_made_up") is False


def test_get_host_info_by_strategy_least_connections_picks_lowest_checkedout():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        h1 = _make_host_info(host="h1")
        h2 = _make_host_info(host="h2")
        # Pre-seed pools with known checkout counts via direct manipulation
        from aws_advanced_python_wrapper.aio.pooled_connection_provider import \
            _AsyncPool
        pool_h1 = _AsyncPool(creator=AsyncMock(return_value="c"))
        pool_h2 = _AsyncPool(creator=AsyncMock(return_value="c"))
        pool_h1._checkedout = 5
        pool_h2._checkedout = 1
        await AsyncPooledConnectionProvider._database_pools.put(
            PoolKey(h1.url, "user"), pool_h1, 10**12)
        await AsyncPooledConnectionProvider._database_pools.put(
            PoolKey(h2.url, "user"), pool_h2, 10**12)
        chosen = provider.get_host_info_by_strategy(
            (h1, h2), HostRole.WRITER, "least_connections", _make_props())
        assert chosen is h2

    asyncio.run(inner())


def test_get_host_info_by_strategy_random_invokes_selector():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    h1 = _make_host_info(host="h1")
    h2 = _make_host_info(host="h2")
    chosen = provider.get_host_info_by_strategy(
        (h1, h2), HostRole.WRITER, "random", _make_props())
    assert chosen in (h1, h2)


def test_get_host_info_by_strategy_unsupported_raises():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    with pytest.raises(AwsWrapperError):
        provider.get_host_info_by_strategy(
            (_make_host_info(),), HostRole.WRITER, "totally_made_up", _make_props())


def test_connect_creates_pool_on_first_call_then_reuses():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        host = _make_host_info()
        props = _make_props()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(return_value=props)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        proxy1 = await provider.connect(target, driver_dialect, database_dialect, host, props)
        await proxy1.close()
        proxy2 = await provider.connect(target, driver_dialect, database_dialect, host, props)
        await proxy2.close()
        # Same pool key → only one pool created
        assert provider.num_pools == 1
        # Connection reused
        assert target.await_count == 1

    asyncio.run(inner())


def test_connect_creates_separate_pool_per_user():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        host = _make_host_info()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="alice"))
        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="bob"))
        assert provider.num_pools == 2

    asyncio.run(inner())


def test_connect_creates_separate_pool_per_url():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        h1 = _make_host_info(host="h1")
        h2 = _make_host_info(host="h2")
        await provider.connect(target, driver_dialect, database_dialect, h1, _make_props())
        await provider.connect(target, driver_dialect, database_dialect, h2, _make_props())
        assert provider.num_pools == 2

    asyncio.run(inner())


def test_pool_mapping_overrides_default_user_key():
    async def inner():
        _reset_class_state()
        # Use host:port as the pool-mapping key — NOT user
        provider = AsyncPooledConnectionProvider(
            pool_mapping=lambda host_info, props: f"{host_info.host}:{host_info.port}",
        )
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        host = _make_host_info()
        # Different users — should still hit ONE pool because mapping ignores user
        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="alice"))
        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="bob"))
        assert provider.num_pools == 1

    asyncio.run(inner())


def test_pool_configurator_kwargs_applied():
    async def inner():
        _reset_class_state()
        captured_kwargs = {}

        def configurator(host_info, props):
            return {"max_size": 7, "max_overflow": 3, "timeout_seconds": 1.5}

        provider = AsyncPooledConnectionProvider(pool_configurator=configurator)
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        await provider.connect(target, driver_dialect, database_dialect, _make_host_info(), _make_props())
        # Inspect the created pool
        items = list(AsyncPooledConnectionProvider._database_pools.items())
        assert len(items) == 1
        pool = items[0][1].item
        assert pool._max_size == 7
        assert pool._max_overflow == 3
        assert pool._timeout_seconds == 1.5

    asyncio.run(inner())


def test_default_pool_key_missing_user_raises():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        # Props without "user"
        empty = Properties()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        with pytest.raises(AwsWrapperError, match="UnableToCreateDefaultKey"):
            await provider.connect(target, driver_dialect, database_dialect, _make_host_info(), empty)

    asyncio.run(inner())


def test_release_resources_disposes_all_pools():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        target = AsyncMock(return_value=AsyncMock(close=AsyncMock()))
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        await provider.connect(target, driver_dialect, database_dialect, _make_host_info(host="h1"), _make_props())
        await provider.connect(target, driver_dialect, database_dialect, _make_host_info(host="h2"), _make_props())
        assert provider.num_pools == 2

        await provider.release_resources()
        assert provider.num_pools == 0

    asyncio.run(inner())
```

- [ ] **Step 1b: Run to verify failure**

```bash
poetry run pytest tests/unit/test_aio_pooled_connection_provider.py -v 2>&1 | tail -10
```

Expected: tests fail because `AsyncPooledConnectionProvider` and `PoolKey` aren't exported from `pooled_connection_provider.py` yet.

### Step 2: Implement the public provider

- [ ] **Step 2a: Modify `aws_advanced_python_wrapper/aio/pooled_connection_provider.py`** — replace its top-level imports and append the new classes. The full file should now read (replace the existing content with this — preserve license header):

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

"""Per-host, per-user async connection pool registry.

:class:`AsyncPooledConnectionProvider` is the async counterpart of
:class:`aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider`,
implemented over asyncio primitives instead of SQLAlchemy's pool. The
underlying pool (:class:`_AsyncPool`) holds raw async connections
(``psycopg.AsyncConnection`` / ``aiomysql.Connection``); the proxy
(:class:`_PooledAsyncConnectionProxy`) routes ``close()`` /
``__aexit__`` to release-to-pool semantics.
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    List, Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncCanReleaseResources
from aws_advanced_python_wrapper.aio.storage.sliding_expiration_cache_async import \
    AsyncSlidingExpirationCache
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import (
    HighestWeightHostSelector, HostSelector, RandomHostSelector,
    RoundRobinHostSelector, WeightedRandomHostSelector)
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    PoolKey
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.connection_provider import \
        AsyncConnectionProvider  # noqa: F401  (Protocol the class satisfies)
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole

logger = Logger(__name__)


class _AsyncPool:
    """Bounded async connection pool over asyncio primitives.

    The pool's max concurrency is ``max_size + max_overflow``; ``acquire``
    blocks (with a timeout) when the cap is reached. Idle connections are
    held in an ``asyncio.Queue``; new ones are created on demand via the
    async ``creator`` callable.

    Disposal is one-way: once :meth:`dispose` is called the pool refuses
    new acquires and closes idle conns immediately; in-flight conns close
    on the next :meth:`release` instead of returning to the queue.
    """

    def __init__(
            self,
            creator: Callable[[], Awaitable[Any]],
            max_size: int = 5,
            max_overflow: int = 10,
            timeout_seconds: float = 30.0,
    ):
        self._creator = creator
        self._max_size = max_size
        self._max_overflow = max_overflow
        self._timeout_seconds = timeout_seconds
        self._idle: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_size + max_overflow)
        self._semaphore = asyncio.Semaphore(max_size + max_overflow)
        self._checkedout: int = 0
        self._disposing: bool = False

    def checkedout(self) -> int:
        return self._checkedout

    def is_disposing(self) -> bool:
        return self._disposing

    async def acquire(self) -> Any:
        if self._disposing:
            raise AwsWrapperError(
                Messages.get_formatted("AsyncPooledConnectionProvider.PoolDisposed", "<pool>"))
        await asyncio.wait_for(self._semaphore.acquire(), timeout=self._timeout_seconds)
        try:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                conn = await self._creator()
            self._checkedout += 1
            return conn
        except BaseException:
            self._semaphore.release()
            raise

    async def release(self, conn: Any, invalidated: bool = False) -> None:
        try:
            self._checkedout -= 1
            if invalidated or self._disposing:
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
                return
            try:
                self._idle.put_nowait(conn)
            except asyncio.QueueFull:
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
        finally:
            self._semaphore.release()

    async def dispose(self) -> None:
        self._disposing = True
        while True:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await conn.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass


class _PooledAsyncConnectionProxy:
    """Wraps a raw async connection acquired from a :class:`_AsyncPool`.

    ``close()`` and ``__aexit__`` route to ``pool.release(...)`` instead
    of closing the underlying connection. Use :meth:`invalidate` before
    closing to force an actual close (e.g., after a SQL error left the
    connection in a bad state).

    All other attributes are delegated to the underlying connection via
    ``__getattr__``, so cursor / transaction / driver-specific methods
    work transparently.
    """

    def __init__(self, raw_conn: Any, pool: "_AsyncPool"):
        self._raw = raw_conn
        self._pool = pool
        self._invalidated = False

    def invalidate(self) -> None:
        self._invalidated = True

    async def close(self) -> None:
        await self._pool.release(self._raw, invalidated=self._invalidated)

    async def __aenter__(self) -> "_PooledAsyncConnectionProxy":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)


class AsyncPooledConnectionProvider(AsyncCanReleaseResources):
    """Async counterpart of :class:`SqlAlchemyPooledConnectionProvider`.

    Per-host pooling keyed by ``PoolKey(host.url, extra_key)``. Pools
    expire after a sliding window of inactivity (default 30 minutes).

    Configuration callables mirror the sync class:

      * ``pool_configurator(host_info, props) -> dict`` -- kwargs forwarded
        to ``_AsyncPool.__init__`` (``max_size``, ``max_overflow``,
        ``timeout_seconds``). Defaults if omitted.
      * ``pool_mapping(host_info, props) -> str`` -- override the default
        "user" extra-key for pool-cache lookup.
      * ``accept_url_func(host_info, props) -> bool`` -- override the
        default RDS-instance-only filter.

    Five host-selector strategies supported: ``random``, ``round_robin``,
    ``weighted_random``, ``highest_weight``, and ``least_connections``
    (which counts active checked-out conns per host).
    """

    _POOL_EXPIRATION_CHECK_NS: ClassVar[int] = 30 * 60_000_000_000  # 30 minutes
    _LEAST_CONNECTIONS: ClassVar[str] = "least_connections"

    _accepted_strategies: ClassVar[Dict[str, HostSelector]] = {
        "random": RandomHostSelector(),
        "round_robin": RoundRobinHostSelector(),
        "weighted_random": WeightedRandomHostSelector(),
        "highest_weight": HighestWeightHostSelector(),
    }
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()
    _database_pools: ClassVar[AsyncSlidingExpirationCache[PoolKey, "_AsyncPool"]] = (
        AsyncSlidingExpirationCache(
            should_dispose_func=lambda pool: pool.checkedout() == 0,
            item_disposal_func=lambda pool: pool.dispose(),
        )
    )

    def __init__(
            self,
            pool_configurator: Optional[Callable[[Any, Properties], Dict[str, Any]]] = None,
            pool_mapping: Optional[Callable[[Any, Properties], str]] = None,
            accept_url_func: Optional[Callable[[Any, Properties], bool]] = None,
            pool_expiration_check_ns: int = -1,
            pool_cleanup_interval_ns: int = -1,
    ):
        self._pool_configurator = pool_configurator
        self._pool_mapping = pool_mapping
        self._accept_url_func = accept_url_func

        if pool_expiration_check_ns > -1:
            AsyncPooledConnectionProvider._POOL_EXPIRATION_CHECK_NS = pool_expiration_check_ns
        if pool_cleanup_interval_ns > -1:
            AsyncPooledConnectionProvider._database_pools.set_cleanup_interval_ns(
                pool_cleanup_interval_ns)

    @property
    def num_pools(self) -> int:
        return len(AsyncPooledConnectionProvider._database_pools)

    @property
    def pool_urls(self) -> Set[str]:
        return {pool_key.url for pool_key, _ in AsyncPooledConnectionProvider._database_pools.items()}

    def keys(self) -> List[PoolKey]:
        return AsyncPooledConnectionProvider._database_pools.keys()

    def accepts_host_info(self, host_info: "HostInfo", props: Properties) -> bool:
        if self._accept_url_func is not None:
            return self._accept_url_func(host_info, props)
        url_type = AsyncPooledConnectionProvider._rds_utils.identify_rds_type(host_info.host)
        return url_type == RdsUrlType.RDS_INSTANCE

    def accepts_strategy(self, role: "HostRole", strategy: str) -> bool:
        return (strategy == AsyncPooledConnectionProvider._LEAST_CONNECTIONS
                or strategy in self._accepted_strategies)

    def get_host_info_by_strategy(
            self,
            hosts: Tuple["HostInfo", ...],
            role: "HostRole",
            strategy: str,
            props: Optional[Properties],
    ) -> "HostInfo":
        if not self.accepts_strategy(role, strategy):
            raise AwsWrapperError(Messages.get_formatted(
                "ConnectionProvider.UnsupportedHostSelectorStrategy",
                strategy, type(self).__name__))

        if strategy == AsyncPooledConnectionProvider._LEAST_CONNECTIONS:
            valid_hosts = [host for host in hosts if host.role == role]
            valid_hosts.sort(key=lambda host: self._num_connections(host))
            if len(valid_hosts) == 0:
                raise AwsWrapperError(Messages.get_formatted("HostSelector.NoHostsMatchingRole", role))
            return valid_hosts[0]

        return self._accepted_strategies[strategy].get_host(hosts, role, props)

    def _num_connections(self, host_info: "HostInfo") -> int:
        total = 0
        for pool_key, cache_item in AsyncPooledConnectionProvider._database_pools.items():
            if pool_key.url == host_info.url:
                total += cache_item.item.checkedout()
        return total

    async def connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect: "AsyncDriverDialect",
            database_dialect: "DatabaseDialect",
            host_info: "HostInfo",
            props: Properties,
    ) -> _PooledAsyncConnectionProxy:
        pool_key = PoolKey(host_info.url, self._get_extra_key(host_info, props))

        async def _create(_key: PoolKey) -> _AsyncPool:
            return await self._create_pool(target_func, driver_dialect, database_dialect, host_info, props)

        pool: Optional[_AsyncPool] = await AsyncPooledConnectionProvider._database_pools.compute_if_absent(
            pool_key, _create, AsyncPooledConnectionProvider._POOL_EXPIRATION_CHECK_NS)
        if pool is None:
            raise AwsWrapperError(
                Messages.get_formatted("AsyncPooledConnectionProvider.PoolNone", host_info.url))

        raw_conn = await pool.acquire()
        return _PooledAsyncConnectionProxy(raw_conn, pool)

    def _get_extra_key(self, host_info: "HostInfo", props: Properties) -> str:
        if self._pool_mapping is not None:
            return self._pool_mapping(host_info, props)
        user = props.get(WrapperProperties.USER.name, None)
        if user is None or user == "":
            raise AwsWrapperError(Messages.get("AsyncPooledConnectionProvider.UnableToCreateDefaultKey"))
        return user

    async def _create_pool(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect: "AsyncDriverDialect",
            database_dialect: "DatabaseDialect",
            host_info: "HostInfo",
            props: Properties,
    ) -> _AsyncPool:
        kwargs: Dict[str, Any] = (
            {} if self._pool_configurator is None else self._pool_configurator(host_info, props))
        prepared = driver_dialect.prepare_connect_info(host_info, props)
        database_dialect.prepare_conn_props(prepared)
        creator = self._get_connection_func(target_func, prepared)
        return _AsyncPool(creator=creator, **kwargs)

    def _get_connection_func(
            self,
            target_connect_func: Callable[..., Awaitable[Any]],
            props: Properties,
    ) -> Callable[[], Awaitable[Any]]:
        async def _creator() -> Any:
            return await target_connect_func(**props)
        return _creator

    async def release_resources(self) -> None:
        items = list(AsyncPooledConnectionProvider._database_pools.items())
        for _, cache_item in items:
            try:
                await cache_item.item.dispose()
            except Exception:  # noqa: BLE001 - best-effort teardown; conns may already be dead
                pass
        await AsyncPooledConnectionProvider._database_pools.clear()


__all__ = [
    "AsyncPooledConnectionProvider",
    "PoolKey",
    # Private but re-exported for tests:
    "_AsyncPool",
    "_PooledAsyncConnectionProxy",
]
```

- [ ] **Step 2b: Run tests to verify they pass**

```bash
poetry run pytest tests/unit/test_aio_pooled_connection_provider.py -v 2>&1 | tail -30
```

Expected: all tests pass (~25 tests total now: 9 pool + 4 proxy + 14 provider).

- [ ] **Step 2c: Lint + type check**

```bash
poetry run mypy aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
poetry run flake8 aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
poetry run isort --check-only aws_advanced_python_wrapper/aio/pooled_connection_provider.py tests/unit/test_aio_pooled_connection_provider.py
```

All must succeed.

- [ ] **Step 2d: Run the full unit test suite once to make sure nothing else broke**

```bash
poetry run pytest tests/unit -x 2>&1 | tail -10
```

Expected: all tests pass (no regressions).

- [ ] **Step 2e: Commit**

```bash
git add aws_advanced_python_wrapper/aio/pooled_connection_provider.py \
        tests/unit/test_aio_pooled_connection_provider.py
git commit -m "$(cat <<'EOF'
feat(aio): add AsyncPooledConnectionProvider

Per-host, per-user async connection pool registry — async parity with
SqlAlchemyPooledConnectionProvider. Implements AsyncConnectionProvider
protocol + AsyncCanReleaseResources for manager-driven teardown.

Surface mirrors sync line-for-line: pool_configurator / pool_mapping /
accept_url_func hooks, all five host-selector strategies including
least_connections (counts active checkouts across per-host pools),
sliding-expiration cache with async disposal.

Built on _AsyncPool + _PooledAsyncConnectionProxy from the previous
two commits and AsyncSlidingExpirationCache. Reuses sync PoolKey via
re-export. No SQLAlchemy dependency despite the conceptual parallel —
SA's AsyncAdaptedQueuePool is a greenlet-bridged sync pool whose
returned _ConnectionFairy proxy doesn't fit the wrapper's async-native
plugin pipeline.
EOF
)"
```

---

## Task 6: Export from `aio/__init__.py`

**Files:**
- Modify: `aws_advanced_python_wrapper/aio/__init__.py`

- [ ] **Step 1: Update the imports + `__all__`**

Replace the existing import + `__all__` block (current file is small; preserve everything else verbatim). The final file should read:

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

"""Async counterparts of the sync wrapper classes.

Structure:
  - ``aio.wrapper`` -- ``AsyncAwsWrapperConnection`` (async connection facade)
  - ``aio.plugin`` -- ``AsyncPlugin`` / ``AsyncConnectionProvider`` ABCs
  - ``aio.driver_dialect`` -- the sole driver-specific interface
    (``AsyncDriverDialect`` ABC + per-driver concrete dialects)
  - ``aio.pooled_connection_provider`` -- ``AsyncPooledConnectionProvider``
    (per-host, per-user async pool with sliding expiration)

Typical usage (once SP-2 lands the implementation)::

    from aws_advanced_python_wrapper.aio import AsyncAwsWrapperConnection

    conn = await AsyncAwsWrapperConnection.connect(...)

See the F3-B master spec for the overall design.
"""

from aws_advanced_python_wrapper.aio.cleanup import (register_shutdown_hook,
                                                     release_resources_async)
from aws_advanced_python_wrapper.aio.pooled_connection_provider import \
    AsyncPooledConnectionProvider
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection

__all__ = [
    "AsyncAwsWrapperConnection",
    "AsyncPooledConnectionProvider",
    "release_resources_async",
    "register_shutdown_hook",
]
```

- [ ] **Step 2: Smoke-import**

```bash
poetry run python -c 'from aws_advanced_python_wrapper.aio import AsyncPooledConnectionProvider; print("ok:", AsyncPooledConnectionProvider.__name__)'
```

Expected: `ok: AsyncPooledConnectionProvider`.

- [ ] **Step 3: Lint + type check**

```bash
poetry run mypy aws_advanced_python_wrapper/aio/__init__.py
poetry run flake8 aws_advanced_python_wrapper/aio/__init__.py
poetry run isort --check-only aws_advanced_python_wrapper/aio/__init__.py
```

- [ ] **Step 4: Commit**

```bash
git add aws_advanced_python_wrapper/aio/__init__.py
git commit -m "$(cat <<'EOF'
feat(aio): export AsyncPooledConnectionProvider from aio package

Adds the new public class to the top-level aio package import surface
and updates the package docstring's structure overview.
EOF
)"
```

---

## Task 7: Replace sync provider workarounds in async integration tests

**Files:**
- Modify: `tests/integration/container/test_read_write_splitting_async.py` — replace `SqlAlchemyPooledConnectionProvider` with `AsyncPooledConnectionProvider`; drop `# type: ignore[arg-type]` markers
- Modify: `tests/integration/container/test_autoscaling_async.py` — same

### Step 1: Find the workarounds

- [ ] **Step 1a: Locate the call sites**

```bash
grep -nE 'SqlAlchemyPooledConnectionProvider|# type: ignore\[arg-type\]' tests/integration/container/test_read_write_splitting_async.py tests/integration/container/test_autoscaling_async.py
```

Expected: a handful of lines per file showing `SqlAlchemyPooledConnectionProvider(...)` constructions and `# type: ignore[arg-type]` markers on calls passing them to `AsyncConnectionProviderManager.set_connection_provider(...)`.

### Step 2: Update each file

- [ ] **Step 2a: For each match in `test_read_write_splitting_async.py`:**

Replace `from aws_advanced_python_wrapper.sql_alchemy_connection_provider import SqlAlchemyPooledConnectionProvider` (or equivalent) with:

```python
from aws_advanced_python_wrapper.aio.pooled_connection_provider import \
    AsyncPooledConnectionProvider
```

Replace each `SqlAlchemyPooledConnectionProvider(...)` constructor call with `AsyncPooledConnectionProvider(...)` with the same arguments.

Remove the `# type: ignore[arg-type]` marker from each line that previously needed it (these were marking the type mismatch that no longer exists).

- [ ] **Step 2b: Same for `test_autoscaling_async.py`**

Apply the same three substitutions.

- [ ] **Step 2c: Lint + type check**

```bash
poetry run mypy tests/integration/container/test_read_write_splitting_async.py tests/integration/container/test_autoscaling_async.py
poetry run flake8 tests/integration/container/test_read_write_splitting_async.py tests/integration/container/test_autoscaling_async.py
poetry run isort --check-only tests/integration/container/test_read_write_splitting_async.py tests/integration/container/test_autoscaling_async.py
```

All must succeed. The mypy run should now report success without needing the `# type: ignore` markers.

- [ ] **Step 2d: Smoke-import**

```bash
poetry run python -c 'import importlib; importlib.import_module("tests.integration.container.test_read_write_splitting_async"); importlib.import_module("tests.integration.container.test_autoscaling_async"); print("imports: OK")' 2>&1 | tail -3
```

Expected: `imports: OK`, OR the expected `TEST_ENV_INFO_JSON is required` error (that's the harness gating, not an import error).

- [ ] **Step 3: Commit**

```bash
git add tests/integration/container/test_read_write_splitting_async.py \
        tests/integration/container/test_autoscaling_async.py
git commit -m "$(cat <<'EOF'
refactor(test): use AsyncPooledConnectionProvider in async integration tests

Replaces the sync SqlAlchemyPooledConnectionProvider + type-ignore
workaround in test_read_write_splitting_async.py and
test_autoscaling_async.py with the now-real AsyncPooledConnectionProvider.

Drops the # type: ignore[arg-type] markers — the type mismatch that
required them is gone.
EOF
)"
```

---

## Task 8: Final verification

- [ ] **Step 1: Run the full unit test suite**

```bash
poetry run pytest tests/unit -v 2>&1 | tail -10
```

Expected: all tests pass, including the new ~25 tests in `test_aio_pooled_connection_provider.py` and ~10 in `test_aio_sliding_expiration_cache.py`.

- [ ] **Step 2: Run the project's quality gates**

```bash
poetry run mypy .
poetry run flake8 .
poetry run isort --check-only .
```

All must succeed. If isort reports issues elsewhere in the repo not related to this work, leave them alone — they're pre-existing.

- [ ] **Step 3: Verify git log**

```bash
git log --oneline -10
```

Expected (top-down):
- `refactor(test): use AsyncPooledConnectionProvider in async integration tests`
- `feat(aio): export AsyncPooledConnectionProvider from aio package`
- `feat(aio): add AsyncPooledConnectionProvider`
- `feat(aio): add _PooledAsyncConnectionProxy`
- `feat(aio): add _AsyncPool primitive for async connection pooling`
- `feat(aio): add AsyncSlidingExpirationCache`
- `feat(aio): add AsyncPooledConnectionProvider message keys`
- (prior session work continues below)

- [ ] **Step 4: No additional commit** — this is verification only.

---

## Self-Review

### Spec coverage

| Spec section | Implementing task(s) |
|---|---|
| 3 new Messages entries | Task 1 |
| `AsyncSlidingExpirationCache` | Task 2 |
| `_AsyncPool` | Task 3 |
| `_PooledAsyncConnectionProxy` | Task 4 |
| `AsyncPooledConnectionProvider` (full surface) | Task 5 |
| `aio/__init__.py` export | Task 6 |
| `AsyncCanReleaseResources` protocol | **Already exists** at `aio/plugin.py:120` — no task needed |
| Integration test cleanup | Task 7 |
| Unit tests (cache + pool + proxy + provider) | Tasks 2, 3, 4, 5 (TDD throughout) |
| `release_resources_async` integration | Task 5 — provider's `release_resources()` matches `AsyncCanReleaseResources` protocol |

### Placeholder scan

No "TBD" / "TODO" / "implement later" / "similar to Task N" patterns. Every step has the exact code or exact command. The integration test cleanup in Task 7 uses a "find then replace" pattern (grep first, then substitute) because the sync workaround's exact line numbers depend on prior commits — which is concrete enough to act on.

### Type / naming consistency

- Class name `AsyncPooledConnectionProvider` consistent across spec, plan, all task code, all imports.
- `_AsyncPool`, `_PooledAsyncConnectionProxy` private with leading underscore consistent.
- `AsyncSlidingExpirationCache[K, V]` generic signature consistent.
- Method signatures: `acquire() -> Any`, `release(conn, invalidated=False) -> None`, `dispose() -> None`, `checkedout() -> int` consistent across pool implementation and tests.
- `compute_if_absent(key, mapping_func, item_expiration_ns)` signature consistent across cache implementation, tests, and provider usage.
- Messages keys spelled consistently: `AsyncPooledConnectionProvider.PoolNone`, `.UnableToCreateDefaultKey`, `.PoolDisposed`.

No inconsistencies found.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-25-async-pooled-connection-provider.md`.

Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, with two-stage review between tasks. Best for this plan because each task is independently verifiable via the bundled unit tests (no real-cluster dependency until Task 7's smoke import).

**2. Inline Execution** — I run tasks in this session with checkpoints for batch review.

Which approach?
