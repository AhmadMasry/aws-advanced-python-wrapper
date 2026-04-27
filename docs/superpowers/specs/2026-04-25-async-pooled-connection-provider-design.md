# Async Pooled Connection Provider — Design Spec

**Date:** 2026-04-25
**Branch:** `feat/async-parity`
**Status:** Draft — awaiting user review before implementation plan
**Related:** `docs/superpowers/specs/2026-04-24-async-integration-tests-design.md` (which surfaced this gap)

## Problem

The `feat/async-parity` branch's async wrapper has no equivalent of `aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider` — the per-host, per-user connection pool registry that several wrapper plugins depend on for correctness and performance. Without it, async users of the wrapper's `failover`, `read_write_splitting`, `iam_authentication` (multi-tenant), `least_connections` host-selection, or cluster topology-refresh paths cannot get host-aware pooling. SQLAlchemy's built-in `AsyncAdaptedQueuePool` does not substitute: it pools per-engine, not per-host, and has no concept of per-user keying or per-host invalidation.

Two integration tests already added on `feat/async-parity` (`test_read_write_splitting_async.py`, `test_autoscaling_async.py`) currently bolt the **sync** `SqlAlchemyPooledConnectionProvider` onto `AsyncConnectionProviderManager` with `# type: ignore[arg-type]` markers. Those tests will fail at runtime against real clusters until the async provider exists.

This spec defines `AsyncPooledConnectionProvider` — a full feature-parity async equivalent that uses asyncio primitives (no SQLAlchemy pool) so the async plugin pipeline can consume real `psycopg.AsyncConnection` / `aiomysql.Connection` instances natively.

## Why not use SQLAlchemy's pool

SA's `AsyncAdaptedQueuePool` is a sync pool with greenlet bridging for async callers. Its creator is sync; the returned object is a `_ConnectionFairy` proxy; native async access requires `greenlet_spawn(pool.connect)` and unwrapping the fairy via private SA attributes. The wrapper's async plugin pipeline is written `async def` / `await` end-to-end and went out of its way to avoid greenlet bridges on hot paths. Forcing SA's pool into that pipeline would either propagate `greenlet_spawn` calls through every plugin boundary or rely on SA private internals to unwrap fairies — both worse than rolling ~150 lines of asyncio-primitive pool code.

## Why not skip the custom provider entirely

Without a custom provider, async users lose pooling for any scenario the wrapper's plugins control:

| Plugin / feature | What needs host-aware pooling | SA's per-engine pool |
|---|---|---|
| `failover` | Discard one host's pool atomically on writer promotion | Cannot — pool spans topology changes |
| `read_write_splitting` | Route reader queries to a reader-host pool, writer queries to a writer-host pool, under one engine | Cannot — one pool per engine, no per-role |
| `iam_authentication` multi-tenant | Per-user pools when IAM tokens differ across users | Cannot — credentials baked into engine URL |
| `least_connections` host strategy | Compare active checkout counts across hosts | Cannot — no per-host metric |
| Topology refresh | Drop just the dropped host's pool | Cannot — would flush whole engine pool |

These are precisely the wrapper's load-bearing plugins. Shipping async parity without host-aware pooling would mean async users running these plugins re-open connections on every plugin-driven host change. That is a meaningful regression vs. the sync side and erodes the wrapper's value proposition for high-traffic async users.

## Scope

**In scope — full parity with `SqlAlchemyPooledConnectionProvider`:**
- `AsyncConnectionProvider` protocol implementation.
- Per-`PoolKey(url, extra_key)` pool registry.
- `pool_configurator`, `pool_mapping`, `accept_url_func` callables.
- All five host-selector strategies, including `least_connections`.
- Sliding-expiration cache with disposal callbacks (async).
- `release_resources()` async teardown.
- New `Messages` entries for the three error paths sync surfaces.

**Out of scope:**
- Replacing or modifying `SqlAlchemyPooledConnectionProvider` (sync) — untouched.
- Adding new selector strategies the sync side doesn't have.
- Changes to `AsyncConnectionProviderManager` — already supports custom providers; only its consumers change.
- Changes to async plugin pipeline.
- A `SlidingExpirationCache` API change — we add an async variant alongside it.

## Architecture

Three private classes + one public class, all in `aws_advanced_python_wrapper/aio/pooled_connection_provider.py`. One supporting class in `aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py`.

```
AsyncPooledConnectionProvider
    implements AsyncConnectionProvider protocol
    class-level: _database_pools: AsyncSlidingExpirationCache[PoolKey, _AsyncPool]
    connect(target_func, dialects, host_info, props):
        → cache.compute_if_absent(PoolKey(host.url, extra_key), _create_pool)
        → await pool.acquire()
        → return _PooledAsyncConnectionProxy(raw_conn, pool)

_AsyncPool
    creator: async () → raw async DB-API connection
    asyncio.Semaphore for max concurrency (size + overflow)
    asyncio.Queue for idle conns
    asyncio.Lock for teardown
    acquire(timeout) → raw async conn (newly created or reused from queue)
    release(conn, was_invalidated) → returns conn to queue or closes it
    dispose() → closes all queued + signals in-flight to close on release
    checkedout() → int (for least_connections strategy)

_PooledAsyncConnectionProxy
    wraps raw async conn (psycopg.AsyncConnection / aiomysql.Connection)
    overrides close() / __aexit__: returns conn to pool instead of closing
    delegates everything else via __getattr__

AsyncSlidingExpirationCache[K, V]
    mirrors sync SlidingExpirationCache shape
    compute_if_absent / clear / remove / cleanup are async (so disposal callbacks can await)
    reuses ConcurrentDict + AtomicInt from sync utils
```

**Loop-binding stance:** the cache is process-global and thread-safe via `threading.Lock`. Connections remain bound to the event loop that opened them — psycopg/aiomysql invariant we don't fight. Multi-loop consumers must manage their own scoping; the provider does not enforce or detect cross-loop misuse. Documented prominently.

## Components

### 1. `aws_advanced_python_wrapper/aio/storage/__init__.py`

Empty `__init__.py` for the new submodule.

### 2. `aws_advanced_python_wrapper/aio/storage/sliding_expiration_cache_async.py` (new, ~140 lines)

```python
class AsyncSlidingExpirationCache(Generic[K, V]):
    def __init__(
            self,
            cleanup_interval_ns: int = 10 * 60_000_000_000,
            should_dispose_func: Optional[Callable[[V], bool]] = None,
            item_disposal_func: Optional[Callable[[V], Awaitable[None]]] = None,
    ): ...

    def __len__(self) -> int: ...
    def __contains__(self, key: K) -> bool: ...
    def set_cleanup_interval_ns(self, interval_ns: int) -> None: ...
    def keys(self) -> List[K]: ...
    def items(self) -> List[Tuple[K, CacheItem[V]]]: ...

    async def compute_if_absent(
            self,
            key: K,
            mapping_func: Callable[[K], Awaitable[V]],
            item_expiration_ns: int,
    ) -> Optional[V]: ...

    async def put(self, key: K, value: V, item_expiration_ns: int) -> None: ...
    def get(self, key: K) -> Optional[V]: ...
    async def remove(self, key: K) -> None: ...
    async def clear(self) -> None: ...
    async def cleanup(self) -> None: ...
```

Uses the same `ConcurrentDict` + `AtomicInt` primitives as the sync cache. The disposal callback is async; expired-item disposal happens outside the lock (matching sync) but with `await` instead of synchronous call.

`should_dispose_func` is sync (a quick predicate), but `item_disposal_func` is async (does real work — closes conns).

### 3. `aws_advanced_python_wrapper/aio/pooled_connection_provider.py` (new, ~320 lines)

**`PoolKey`** — re-exported from sync (`from aws_advanced_python_wrapper.sql_alchemy_connection_provider import PoolKey`). Identical semantics: `(url, extra_key)`, hashable, equality.

**`_AsyncPool`** (private):

```python
class _AsyncPool:
    def __init__(
            self,
            creator: Callable[[], Awaitable[Any]],
            max_size: int = 5,
            max_overflow: int = 10,
            timeout_seconds: float = 30.0,
            pre_ping: bool = False,
    ): ...

    async def acquire(self) -> Any: ...
    async def release(self, conn: Any, invalidated: bool = False) -> None: ...
    async def dispose(self) -> None: ...
    def checkedout(self) -> int: ...
    def is_disposing(self) -> bool: ...
```

Internal state:
- `_idle: asyncio.Queue[Any]` — reusable conns
- `_semaphore: asyncio.Semaphore` — limits concurrency to `max_size + max_overflow`
- `_dispose_lock: asyncio.Lock` — guards disposal
- `_checkedout: int` — tracks in-flight conns (incremented on acquire, decremented on release)
- `_disposing: bool` — flips to True on `dispose()`, blocks new `acquire()`s

`acquire()` flow:
1. If `_disposing`: raise `AwsWrapperError("AsyncPooledConnectionProvider.PoolDisposed")`.
2. `await self._semaphore.acquire()` (with timeout).
3. Try `self._idle.get_nowait()`; if empty, `await self._creator()`.
4. Increment `_checkedout`.
5. Return raw conn.

`release(conn, invalidated)`:
- Decrement `_checkedout`.
- If `invalidated` or `_disposing` or conn detected as closed: `await conn.close()`, release semaphore.
- Else: `self._idle.put_nowait(conn)`, release semaphore.

`dispose()`:
- Set `_disposing = True`.
- Drain `_idle`: `await conn.close()` for each queued.
- In-flight conns close on release (handled by `release`'s `_disposing` branch).

**`_PooledAsyncConnectionProxy`** (private):

```python
class _PooledAsyncConnectionProxy:
    def __init__(self, raw_conn: Any, pool: _AsyncPool): ...

    async def close(self) -> None:
        await self._pool.release(self._raw, invalidated=self._invalidated)

    async def __aenter__(self) -> "_PooledAsyncConnectionProxy": ...
    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def invalidate(self) -> None:
        self._invalidated = True

    def __getattr__(self, name): return getattr(self._raw, name)
```

The proxy wraps the raw async connection. `close()` and `__aexit__` route to `pool.release(...)`; everything else delegates to the raw conn. `invalidate()` flag tells `release` to actually close the underlying conn (e.g., after a SQL error that left the conn in a bad state).

**`AsyncPooledConnectionProvider`** (public):

```python
class AsyncPooledConnectionProvider(AsyncConnectionProvider, AsyncCanReleaseResources):
    _POOL_EXPIRATION_CHECK_NS: ClassVar[int] = 30 * 60_000_000_000
    _LEAST_CONNECTIONS: ClassVar[str] = "least_connections"
    _accepted_strategies: ClassVar[Dict[str, HostSelector]] = { ... }
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()
    _database_pools: ClassVar[AsyncSlidingExpirationCache[PoolKey, _AsyncPool]] = ...

    def __init__(
            self,
            pool_configurator: Optional[Callable] = None,
            pool_mapping: Optional[Callable] = None,
            accept_url_func: Optional[Callable] = None,
            pool_expiration_check_ns: int = -1,
            pool_cleanup_interval_ns: int = -1,
    ): ...

    @property
    def num_pools(self) -> int: ...
    @property
    def pool_urls(self) -> Set[str]: ...
    def keys(self) -> List[PoolKey]: ...

    def accepts_host_info(self, host_info, props) -> bool: ...      # sync
    def accepts_strategy(self, role, strategy) -> bool: ...          # sync
    def get_host_info_by_strategy(self, hosts, role, strategy, props) -> HostInfo: ...  # sync

    async def connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect: AsyncDriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties,
    ) -> _PooledAsyncConnectionProxy: ...

    async def release_resources(self) -> None: ...
```

Surface mirrors the sync class line-for-line. The `_get_extra_key`, `_create_pool`, `_get_connection_func` private methods mirror sync exactly with their bodies adapted for async.

`_get_connection_func` returns an `async` callable:
```python
def _get_connection_func(self, target_connect_func, props):
    async def _creator():
        return await target_connect_func(**props)
    return _creator
```

`_num_connections` (powers `least_connections`) iterates the cache and sums `pool.checkedout()` per matching URL — same as sync.

### 4. `AsyncCanReleaseResources` protocol

The wrapper has a sync `CanReleaseResources` protocol consumed by the manager's `release_resources()`. Add an async sibling at `aws_advanced_python_wrapper/aio/plugin.py`:

```python
class AsyncCanReleaseResources(Protocol):
    async def release_resources(self) -> None: ...
```

`AsyncConnectionProviderManager.release_resources()` already uses duck-typing (`getattr(provider, "release_resources", None)` + `hasattr(result, "__await__")`), so it works without changes — but the protocol gives type-checking surface for callers that want it.

### 5. New `Messages` entries

Add to whichever message bundle the existing `SqlAlchemyPooledConnectionProvider.*` messages live in (likely `aws_advanced_python_wrapper/resources/aws_advanced_python_wrapper_messages.properties` or similar — verify during implementation):

- `AsyncPooledConnectionProvider.PoolNone` — "Failed to create or retrieve connection pool for {0}."
- `AsyncPooledConnectionProvider.PoolDisposed` — "Cannot acquire connection: pool for {0} is being disposed."
- `AsyncPooledConnectionProvider.UnableToCreateDefaultKey` — "Cannot create default pool key: 'user' property is missing or empty."

### 6. `aws_advanced_python_wrapper/aio/__init__.py` exports

Add `AsyncPooledConnectionProvider` to the `__all__` list (whatever shape that file currently has).

### 7. Tests

#### `tests/unit/test_aio_pooled_connection_provider.py` (new, ~600 lines)

Mirrors `tests/unit/test_sql_alchemy_connection_provider.py` (if it exists) or builds out from scratch. Test functions are sync `def test_...` invoking `asyncio.run(inner())` for async portions, matching repo convention.

Coverage:
- `test_accepts_host_info_default_filters_to_rds_instance`
- `test_accepts_host_info_uses_custom_accept_url_func`
- `test_accepts_strategy_default_strategies`
- `test_accepts_strategy_least_connections`
- `test_accepts_strategy_unknown_returns_false`
- `test_get_host_info_by_strategy_least_connections_picks_lowest_checkedout`
- `test_get_host_info_by_strategy_random_invokes_random_selector`
- `test_get_host_info_by_strategy_unsupported_raises`
- `test_connect_creates_pool_on_first_call`
- `test_connect_reuses_pool_for_same_pool_key`
- `test_connect_creates_separate_pool_per_user`
- `test_connect_creates_separate_pool_per_url`
- `test_pool_mapping_overrides_default_user_key`
- `test_pool_configurator_kwargs_applied`
- `test_proxy_close_returns_to_pool_not_actual_close`
- `test_proxy_aexit_returns_to_pool`
- `test_proxy_close_actually_closes_when_pool_disposing`
- `test_proxy_invalidate_forces_actual_close`
- `test_proxy_delegates_unknown_attrs_to_raw_conn`
- `test_release_resources_disposes_all_pools`
- `test_pool_expiration_disposes_idle_pool`
- `test_pool_not_expired_while_checkedout`
- `test_pool_concurrent_acquire_blocks_at_max_size`
- `test_pool_overflow_allows_excess_up_to_overflow_limit`
- `test_pool_acquire_after_dispose_raises`
- `test_pool_release_invalidated_actually_closes`
- `test_pool_release_dead_conn_actually_closes`
- `test_default_pool_key_uses_user_property`
- `test_default_pool_key_missing_user_raises`

#### `tests/unit/test_aio_sliding_expiration_cache.py` (new, ~250 lines)

- `test_compute_if_absent_creates_when_missing`
- `test_compute_if_absent_returns_existing`
- `test_compute_if_absent_extends_expiration_on_access`
- `test_remove_invokes_disposal_callback`
- `test_clear_invokes_disposal_for_each_item`
- `test_cleanup_disposes_expired_items`
- `test_cleanup_skips_should_dispose_false`
- `test_cleanup_runs_only_after_interval`

#### Integration test cleanup

After this feature lands, in a follow-up commit on the same branch:
- Replace `SqlAlchemyPooledConnectionProvider` + `# type: ignore[arg-type]` markers in `test_read_write_splitting_async.py` and `test_autoscaling_async.py` with `AsyncPooledConnectionProvider`. Drops the type-ignore markers.
- Re-run those integration tests against real Aurora clusters to validate.

## Data flow

### Connection acquisition

```
async plugin pipeline
  → manager.get_connection_provider(host_info, props)
  → provider.accepts_host_info(host_info, props)         # sync filter
  → await provider.connect(target_func, dialects, host_info, props)
       └─ pool_key = PoolKey(host_info.url, self._get_extra_key(host_info, props))
       └─ pool = await cache.compute_if_absent(pool_key, _create_pool)
            └─ on miss: await self._create_pool(target_func, dialects, host_info, props)
                  └─ kwargs = self._pool_configurator(host_info, props) or {}
                  └─ prepared = driver_dialect.prepare_connect_info(host_info, props)
                  └─ database_dialect.prepare_conn_props(prepared)
                  └─ creator = self._get_connection_func(target_func, prepared)
                  └─ return _AsyncPool(creator=creator, **kwargs)
       └─ raw_conn = await pool.acquire()
       └─ return _PooledAsyncConnectionProxy(raw_conn, pool)
  → plugin pipeline uses proxy as if it were AsyncAwsWrapperConnection
```

### Connection release

```
proxy.close()
  → await pool.release(raw_conn, invalidated=self._invalidated)
       └─ if pool._disposing or invalidated or raw_conn.closed:
            await raw_conn.close()  # actually close
          else:
            pool._idle.put_nowait(raw_conn)  # back into pool
       └─ pool._semaphore.release()
       └─ pool._checkedout -= 1
```

### Pool expiration (sliding TTL)

```
provider.connect(...) → cache.compute_if_absent(...)
  → cache.cleanup()  (inline, mirrors sync)
       └─ for each key whose expiration is past:
            if should_dispose_func(pool):  # e.g., pool.checkedout() == 0
              keys_to_remove.append(key)
       └─ outside the lock:
            for key in keys_to_remove:
              await item_disposal_func(pool)  # await pool.dispose()
              cache._cdict.remove(key)
```

### `release_resources()`

```
manager.release_resources()
  → if custom provider installed:
       await provider.release_resources()
            └─ for key, cache_item in cache.items():
                 await cache_item.item.dispose()
            └─ await cache.clear()
```

## Error handling

- **Creator failure during `acquire()`:** semaphore permit released in `finally`; exception propagates. No leaked permits.
- **Dead conn in idle queue:** `release` checks driver-level `closed` / `is_closed`; if dead, actually close + don't return to queue.
- **`acquire()` after `dispose()` started:** raises `AwsWrapperError(Messages.get_formatted("AsyncPooledConnectionProvider.PoolDisposed", host))`.
- **`acquire()` semaphore timeout:** raises `AwsWrapperError` with timeout message; semaphore not modified.
- **Cross-loop conn use:** driver raises its own error; we don't swallow.
- **`release_resources()` per-pool exception:** logged, swallowed (matches sync convention: "connections may already be dead"). Other pools still get disposed.
- **Default-key derivation when `user` property missing:** raises `AwsWrapperError(Messages.get("AsyncPooledConnectionProvider.UnableToCreateDefaultKey"))`.

## Testing approach

Three-tier testing:

1. **Unit tests** for `_AsyncPool` and `AsyncSlidingExpirationCache` in isolation — covers bounds, lifecycle, concurrency, error paths, with `AsyncMock` creators.
2. **Unit tests** for `AsyncPooledConnectionProvider` end-to-end — exercises every public method, every selector strategy, every constructor option, and the proxy lifecycle.
3. **Integration validation** by re-running `test_read_write_splitting_async.py` and `test_autoscaling_async.py` against real clusters after replacing the sync provider workaround. (User runs these; not part of the unit-test commit set.)

Concurrency tests use `asyncio.gather(...)` to drive parallel acquires and verify semaphore bounds.

## Risks & known caveats

1. **Loop binding not enforced.** Same policy as the rest of the async wrapper. A connection acquired in event loop A and used in loop B will fail at the driver level; we don't detect or report this in our layer.
2. **`AsyncSlidingExpirationCache` is new shared surface.** Will likely become a dependency for other future async features. Reasonable; reuses sync's `ConcurrentDict` + `AtomicInt`.
3. **Pool proxy `__getattr__` fallback delegates everything not on `self`.** `__getattr__` only fires for attribute *misses* on the proxy, so the proxy's own `_raw` / `_pool` / `_invalidated` (set in `__init__`) take precedence — no actual collision risk for them. The real risk is forgetting to override a method that **must** behave differently on the proxy (e.g., a future driver-level `terminate()` or `cancel()` that should release-to-pool semantically); those would silently delegate to the raw conn and behave wrong. Mitigation: enumerate the methods that need proxy-level behavior in the test suite and assert proxy-level routing for each.
4. **`least_connections` count under concurrent acquires/releases.** `_checkedout` is mutated under the semaphore — should be atomic relative to it. Verified by a unit test that runs 50 concurrent acquires + releases and asserts `_checkedout` returns to zero.
5. **Pool disposal while in-flight conns exist.** New `acquire()` after `dispose()` raises immediately; in-flight conns close on next release (not killed mid-use). This is intentional — matches SA pool semantics. Documented.
6. **Default sliding cache TTL of 30 minutes** is preserved from sync. If integration tests need shorter TTL for verification, they pass `pool_expiration_check_ns` explicitly via the constructor.
7. **No `SqlAlchemyPooledConnectionProvider` modification.** Sync side is fully untouched. The two classes share `PoolKey` via re-export but are otherwise independent.

## Dependencies on prior work

- Built on the async wrapper that already exists at `aws_advanced_python_wrapper/aio/` (this branch).
- Uses `AsyncConnectionProvider` protocol + `AsyncConnectionProviderManager` (already present at `aws_advanced_python_wrapper/aio/connection_provider.py`).
- Reuses sync `PoolKey`, `ConcurrentDict`, `AtomicInt`, `RdsUtils`, host-selector classes — no changes to sync code.

## What's deliberately not in this design

- No new selector strategies (parity only).
- No new wrapper plugin features (this is a provider; not a plugin).
- No CHANGELOG commit (per the user's "I'll handle upstream PRs" stance on the parent async-parity branch).
- No replacement of `# type: ignore[arg-type]` in the existing async integration tests in this PR — that cleanup happens as a follow-up commit on the same branch after the provider lands and is unit-tested.
- No event-loop affinity tracking / multi-loop guard. Documented and accepted.
