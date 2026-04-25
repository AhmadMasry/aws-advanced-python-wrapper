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
