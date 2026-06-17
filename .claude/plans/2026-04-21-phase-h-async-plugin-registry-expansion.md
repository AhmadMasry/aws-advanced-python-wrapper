# Phase H — Plugin Registry Expansion

**Goal:** Close audit P1 #12 by registering the missing async plugin codes. Users configuring `plugins="bg,stale_dns,limitless"` (etc.) in connection props will no longer fail with "unknown plugin code" — the codes resolve to pass-through stubs that log a "not fully implemented in async 3.0.0" warning on first use.

**Scope cut:** P1 #13 (MultiAz + GlobalAurora host list providers) deferred. Those require porting sync's `TopologyUtils` strategy pattern (`MultiAzTopologyUtils` + `GlobalAuroraTopologyMonitor`), which is substantial new async infrastructure. Tracked as Phase H.2 follow-up when a consumer actually exercises MultiAz/GlobalAurora against the async wrapper.

---

## File structure

**Create:**
- `aws_advanced_python_wrapper/aio/stub_plugins.py` — 6 stub plugins + factories.
- `tests/unit/test_aio_stub_plugins.py` — coverage.

**Modify:**
- `aws_advanced_python_wrapper/aio/plugin_factory.py` — register 8 new codes (2 aliases + 6 stubs).

---

## Task H.1: Aliases for v2 plugin codes

In `aws_advanced_python_wrapper/aio/plugin_factory.py`, add to `PLUGIN_FACTORIES`:

```python
    "host_monitoring_v2": _HostMonitoringFactory(),   # alias -- async EFM already v2-shaped
    "failover_v2": _FailoverFactory(),                # alias -- async failover already v2-shaped
```

No new factory classes. Share instances with the non-v2 codes.

## Task H.2: Stub plugins for unimplemented codes

6 codes get a pass-through stub that:
- Subscribes to nothing (`set()` — pipeline bypasses them).
- Logs a single `logger.warning(...)` on construction: `"Plugin '{code}' is not implemented in the async wrapper; connections using it will behave as if the plugin were absent."`
- Returns inherited default async-plugin behavior (no-op for connect/execute/notify).

Codes + canonical sync class names (for docstrings):

| Code | Sync class |
|---|---|
| `srw` | `SimpleReadWriteSplittingPlugin` |
| `stale_dns` | `StaleDnsPlugin` |
| `initial_connection` | `AuroraInitialConnectionStrategyPlugin` |
| `limitless` | `LimitlessPlugin` |
| `bg` | `BlueGreenPlugin` |
| `fastest_response_strategy` | `FastestResponseStrategyPlugin` |

### Sketch

`aws_advanced_python_wrapper/aio/stub_plugins.py`:

```python
"""Pass-through stubs for plugins not yet ported to async.

Registered in aio/plugin_factory.py so users can include these codes in
``plugins="..."`` config strings without tripping unknown-plugin errors.
Each stub logs a single warning on construction to surface the gap at
runtime; no pipeline hooks fire.

Actual async ports land as separate phases when consumers exercise
these features against the async wrapper.
"""

from __future__ import annotations

from typing import Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class _AsyncStubPlugin(AsyncPlugin):
    """Shared base for async plugin stubs."""

    _STUB_NAME: str = "unknown"

    def __init__(self) -> None:
        logger.warning(
            "Plugin '%s' is not implemented in the async wrapper; "
            "connections using it will behave as if the plugin were absent.",
            self._STUB_NAME,
        )

    @property
    def subscribed_methods(self) -> Set[str]:
        return set()


class AsyncSimpleReadWriteSplittingStubPlugin(_AsyncStubPlugin):
    """Stub for sync SimpleReadWriteSplittingPlugin ('srw')."""
    _STUB_NAME = "srw"


class AsyncStaleDnsStubPlugin(_AsyncStubPlugin):
    """Stub for sync StaleDnsPlugin ('stale_dns')."""
    _STUB_NAME = "stale_dns"


class AsyncAuroraInitialConnectionStrategyStubPlugin(_AsyncStubPlugin):
    """Stub for sync AuroraInitialConnectionStrategyPlugin ('initial_connection')."""
    _STUB_NAME = "initial_connection"


class AsyncLimitlessStubPlugin(_AsyncStubPlugin):
    """Stub for sync LimitlessPlugin ('limitless')."""
    _STUB_NAME = "limitless"


class AsyncBlueGreenStubPlugin(_AsyncStubPlugin):
    """Stub for sync BlueGreenPlugin ('bg')."""
    _STUB_NAME = "bg"


class AsyncFastestResponseStrategyStubPlugin(_AsyncStubPlugin):
    """Stub for sync FastestResponseStrategyPlugin ('fastest_response_strategy')."""
    _STUB_NAME = "fastest_response_strategy"
```

Factories in `plugin_factory.py`:

```python
class _AsyncStubFactory:
    """Factory that instantiates a stub plugin."""
    def __init__(self, stub_cls):
        self._cls = stub_cls
    def get_instance(self, plugin_service, props):
        return self._cls()

_SimpleReadWriteSplittingFactory = _AsyncStubFactory(AsyncSimpleReadWriteSplittingStubPlugin)
_StaleDnsFactory = _AsyncStubFactory(AsyncStaleDnsStubPlugin)
_InitialConnectionFactory = _AsyncStubFactory(AsyncAuroraInitialConnectionStrategyStubPlugin)
_LimitlessFactory = _AsyncStubFactory(AsyncLimitlessStubPlugin)
_BlueGreenFactory = _AsyncStubFactory(AsyncBlueGreenStubPlugin)
_FastestResponseFactory = _AsyncStubFactory(AsyncFastestResponseStrategyStubPlugin)
```

Add to `PLUGIN_FACTORIES` dict:

```python
    "srw": _SimpleReadWriteSplittingFactory,
    "stale_dns": _StaleDnsFactory,
    "initial_connection": _InitialConnectionFactory,
    "limitless": _LimitlessFactory,
    "bg": _BlueGreenFactory,
    "fastest_response_strategy": _FastestResponseFactory,
    "host_monitoring_v2": _HostMonitoringFactory(),  # alias
    "failover_v2": _FailoverFactory(),               # alias
```

Also add to the `_PLUGIN_WEIGHTS` dict (if one exists) so plugin ordering is stable. Use the sync weight for each code (look up sync at `plugin_service.py` if a weight table exists).

## Task H.3: Final /verify

---

## Out of scope

- Full async implementations of the 6 stubbed plugins. Each is substantial — e.g. BlueGreenPlugin is 800+ lines in sync. Port when a consumer exercises it.
- MultiAz / GlobalAurora host list providers. Require porting sync's `TopologyUtils` strategy + `GlobalAuroraTopologyMonitor`. Tracked as future Phase H.2.
