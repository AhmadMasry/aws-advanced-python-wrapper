# Phase I — Cursor PEP 249 Surface + Dev Parity + OTel + Cleanups

**Goal:** Close audit P1 #15 (cursor PEP 249), P1 #18 (Developer injection modes), P1 #17 (OTel plumbing), P2 #20-23 (cleanups).

---

## Scope summary

| Task | Audit | Scope |
|---|---|---|
| I.1 | P1 #15 cursor | Add `lastrowid`, `scroll`, `callproc`, `nextset`, `setinputsizes`, `setoutputsize` to `AsyncAwsWrapperCursor` (pass-through to target_cursor). |
| I.2 | P1 #15 conn | Add `autocommit` (getter+setter) and `isolation_level` (getter+setter) properties on `AsyncAwsWrapperConnection` routed through the plugin pipeline. |
| I.3 | P1 #18 | Port sync DeveloperPlugin's 4 injection modes (connect_exception, connect_callback, method_exception, method_callback) to `AsyncDeveloperPlugin`. |
| I.4 | P1 #17 | Async telemetry factory stub on `AsyncPluginService.get_telemetry_factory()`. Counter/gauge interfaces match sync; default no-op impl + `AwsXRayTelemetry` integration deferred. |
| I.5 | P2 #20 | Verify Okta SAML regex actually handles Okta's HTML (covered in Phase E.4; confirm; add a test if missing). |
| I.6 | P2 #21 | Replace `asyncio.get_event_loop().time()` with `time.monotonic_ns()` or `time.perf_counter_ns()` for clock precision in minor plugins (ConnectTime, ExecuteTime). |
| I.7 | P2 #25 | aiohttp proxy support — honor `HTTP_PROXY` / `HTTPS_PROXY` env vars via `trust_env=True` on aiohttp sessions in federated/okta plugins. |

Each task is its own commit (or bundled logically).

---

## Out of scope

- Full OTel integration (real X-Ray exporter, attribute propagation): requires significant async plumbing — Phase I ships the no-op factory interface so plugins can start emitting; actual backend exporter is a follow-up.
- Iteration protocol (`__iter__`/`__next__` / `__aiter__`/`__anext__`) on cursor — `__getattr__` forwarding already handles it via the target cursor; explicit implementation only needed if typed consumers demand it.
