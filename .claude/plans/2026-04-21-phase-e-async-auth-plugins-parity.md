# Phase E — Async Auth Plugins Parity

**Goal:** Close audit P0 #7 + #8 + P1 #14 across all four async auth plugins (IAM, SecretsManager, FederatedAuth, Okta). Add retry-on-login-exception with cache invalidation; honor `*_EXPIRATION` configurable TTLs; subscribe to `FORCE_CONNECT`; add endpoint override + region discovery.

**Architecture:** Lift the retry-on-401 pattern into `AsyncAuthPluginBase.connect` so all subclasses inherit it. `_resolve_credentials` returns a 3-tuple `(user, password, was_cached)` so the base knows whether to retry (fresh credentials already failed — don't spin). Subclasses expose `_invalidate_cache(host_info, props)` to drop stale entries on login failure. Per-plugin TTLs flow through `WrapperProperties.*_EXPIRATION.get_int(props)` with sensible defaults.

**Tech Stack:** Python 3.10–3.14. Reuses `plugin_service.is_login_exception` (A.2), `RegionUtils.get_region` (existing sync util), `IamAuthUtils.get_iam_host` / `get_cache_key` / `get_port` (existing sync utils), `aiohttp` (federated/okta).

---

## File structure

**Modify:**
- `aws_advanced_python_wrapper/aio/auth_plugins.py` — base class + IAM + Secrets Manager changes
- `aws_advanced_python_wrapper/aio/federated_auth_plugins.py` — federated + Okta retry + SAML regex
- `tests/unit/test_aio_auth_plugins.py` — extend (existing file)
- `tests/unit/test_aio_federated_okta.py` — extend (existing file)

**No new files.**

---

## Task E.1: `AsyncAuthPluginBase` — FORCE_CONNECT + retry-on-login + _invalidate_cache hook

Expand the base class to:
1. Subscribe to both `CONNECT` and `FORCE_CONNECT`.
2. Change `_resolve_credentials` signature to return `(user, password, was_cached)`.
3. Add `_invalidate_cache(host_info, props)` abstract (default no-op so existing subclasses don't break during porting).
4. Wrap `connect_func()` in try/except: on `plugin_service.is_login_exception(exc)` AND `was_cached`, call `_invalidate_cache`, re-resolve (now uncached), inject, retry once.
5. Mirror the same logic in `force_connect`.

### TDD

- [ ] **E.1.1: Write failing tests** in `tests/unit/test_aio_auth_plugins.py` (append):

```python
def test_base_plugin_subscribes_to_connect_and_force_connect():
    from aws_advanced_python_wrapper.aio.auth_plugins import AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "p", False)
        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = MagicMock()
    plugin = _Stub(svc, props)
    assert DbApiMethod.CONNECT.method_name in plugin.subscribed_methods
    assert DbApiMethod.FORCE_CONNECT.method_name in plugin.subscribed_methods


def test_base_plugin_retries_on_login_exception_when_cached():
    """Cached credentials + login exception -> invalidate + re-resolve + retry."""
    from aws_advanced_python_wrapper.aio.auth_plugins import AsyncAuthPluginBase

    resolve_calls = []
    invalidate_calls = []

    class _Stub(AsyncAuthPluginBase):
        _next_was_cached = True
        async def _resolve_credentials(self, host_info, props):
            resolve_calls.append(self._next_was_cached)
            was_cached = self._next_was_cached
            self._next_was_cached = False  # next call is fresh
            return ("u", f"token-{len(resolve_calls)}", was_cached)
        def _invalidate_cache(self, host_info, props):
            invalidate_calls.append(1)

    svc = MagicMock()
    svc.is_login_exception = MagicMock(return_value=True)
    svc.is_network_exception = MagicMock(return_value=False)
    props = Properties({"host": "h", "port": "5432"})
    plugin = _Stub(svc, props)

    attempt = [0]
    async def _connect_func():
        attempt[0] += 1
        if attempt[0] == 1:
            raise Exception("auth failed")
        return MagicMock(name="conn")

    host = HostInfo(host="h", port=5432)
    result = asyncio.run(plugin.connect(
        MagicMock(), MagicMock(), host, props, True, _connect_func))

    assert result is not None
    assert invalidate_calls == [1]
    assert len(resolve_calls) == 2  # first resolve returned cached, second returned fresh
    assert attempt[0] == 2


def test_base_plugin_does_not_retry_when_credentials_were_fresh():
    """Fresh credentials + login exception -> propagate (no retry spin)."""
    from aws_advanced_python_wrapper.aio.auth_plugins import AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "fresh-token", False)  # was_cached=False
        def _invalidate_cache(self, host_info, props):
            pass

    svc = MagicMock()
    svc.is_login_exception = MagicMock(return_value=True)
    props = Properties({"host": "h", "port": "5432"})
    plugin = _Stub(svc, props)

    async def _connect_func():
        raise Exception("auth failed")

    host = HostInfo(host="h", port=5432)
    with pytest.raises(Exception, match="auth failed"):
        asyncio.run(plugin.connect(
            MagicMock(), MagicMock(), host, props, True, _connect_func))


def test_base_plugin_does_not_retry_on_non_login_exceptions():
    from aws_advanced_python_wrapper.aio.auth_plugins import AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "p", True)  # cached
        def _invalidate_cache(self, host_info, props):
            pass

    svc = MagicMock()
    svc.is_login_exception = MagicMock(return_value=False)
    props = Properties({"host": "h", "port": "5432"})
    plugin = _Stub(svc, props)

    async def _connect_func():
        raise Exception("network boom")

    host = HostInfo(host="h", port=5432)
    with pytest.raises(Exception, match="network boom"):
        asyncio.run(plugin.connect(
            MagicMock(), MagicMock(), host, props, True, _connect_func))


def test_base_plugin_force_connect_uses_same_retry_flow():
    from aws_advanced_python_wrapper.aio.auth_plugins import AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        _cached = True
        async def _resolve_credentials(self, host_info, props):
            was = self._cached
            self._cached = False
            return ("u", "t", was)
        def _invalidate_cache(self, host_info, props):
            pass

    svc = MagicMock()
    svc.is_login_exception = MagicMock(return_value=True)
    props = Properties({"host": "h", "port": "5432"})
    plugin = _Stub(svc, props)

    attempt = [0]
    async def _fc():
        attempt[0] += 1
        if attempt[0] == 1:
            raise Exception("auth failed")
        return MagicMock()

    asyncio.run(plugin.force_connect(
        MagicMock(), MagicMock(), HostInfo(host="h", port=5432), props, True, _fc))
    assert attempt[0] == 2
```

Existing tests that call `await _resolve_credentials(...)` expecting a 2-tuple WILL BREAK after the signature change. Update them by adapting to the 3-tuple return OR by restructuring them to test via the public `connect` path.

- [ ] **E.1.2: Run — verify fail**.

- [ ] **E.1.3: Implement `AsyncAuthPluginBase`**:

```python
class AsyncAuthPluginBase(AsyncPlugin):
    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }

    def __init__(self, plugin_service, props):
        self._plugin_service = plugin_service
        self._props = props

    @property
    def subscribed_methods(self):
        return set(self._SUBSCRIBED)

    async def connect(self, target_driver_func, driver_dialect, host_info, props,
                      is_initial_connection, connect_func):
        return await self._connect_with_retry(host_info, props, connect_func)

    async def force_connect(self, target_driver_func, driver_dialect, host_info, props,
                            is_initial_connection, force_connect_func):
        return await self._connect_with_retry(host_info, props, force_connect_func)

    async def _connect_with_retry(self, host_info, props, connect_func):
        user, password, was_cached = await self._resolve_credentials(host_info, props)
        self._inject(props, user, password)
        try:
            return await connect_func()
        except Exception as exc:
            if not was_cached:
                raise
            if not self._plugin_service.is_login_exception(error=exc):
                raise
            # Cached credentials failed auth -- invalidate, re-resolve, retry once.
            self._invalidate_cache(host_info, props)
            user, password, _ = await self._resolve_credentials(host_info, props)
            self._inject(props, user, password)
            return await connect_func()

    @staticmethod
    def _inject(props, user, password):
        if user is not None:
            props["user"] = user
        if password is not None:
            props["password"] = password

    async def _resolve_credentials(self, host_info, props):
        raise NotImplementedError

    def _invalidate_cache(self, host_info, props):
        """Subclasses override to drop stale cache entries on login failure."""
```

- [ ] **E.1.4: Run — verify pass** (5 new + all existing still pass after signature migration).

- [ ] **E.1.5: Commit**:

```bash
git add aws_advanced_python_wrapper/aio/auth_plugins.py tests/unit/test_aio_auth_plugins.py
git commit -m "refactor(aio): AsyncAuthPluginBase retry-on-login + FORCE_CONNECT (phase E.1)

- _SUBSCRIBED now includes FORCE_CONNECT (audit P1 #14).
- _resolve_credentials returns (user, password, was_cached). Base class
  wraps connect_func in try/except: on is_login_exception AND was_cached,
  invokes _invalidate_cache, re-resolves, retries once (audit P0 #7).
  Fresh credentials that fail auth propagate immediately (no retry spin).
- Adds _invalidate_cache hook; default no-op so subclasses opt in.
- force_connect reuses the same retry path via _connect_with_retry.

Closes audit P0 #7 (base), P1 #14 (base). IAM / Secrets Manager /
Federated / Okta get the concrete was_cached/invalidate wiring in
E.2-E.4."
```

---

## Task E.2: `AsyncIamAuthPlugin` — IAM_EXPIRATION + region discovery + was_cached/invalidate

### Changes
1. `_resolve_credentials` returns 3-tuple. `was_cached` is True when cache hit.
2. `_invalidate_cache` drops the cache entry for this (host, port, user, region) key.
3. Replace hardcoded `_DEFAULT_TOKEN_EXPIRATION_SEC = 15*60` with `WrapperProperties.IAM_EXPIRATION.get_int(props)` (default stays 15 min if prop is absent — matches sync).
4. Replace direct `IAM_REGION` read with sync's `RegionUtils.get_region(props, name, host, host_info)` flow so RDS hosts auto-resolve region. Use `RegionUtils` or `GdbRegionUtils` based on `rds_type`.
5. Use `IamAuthUtils.get_iam_host(props, host_info)` / `get_port` / `get_cache_key` for parity with sync cache key shape.

### TDD

- [ ] **E.2.1: Failing tests** — append:

```python
def test_iam_plugin_uses_iam_expiration_property():
    """Token TTL honors IAM_EXPIRATION."""
    props = Properties({"host": "db-instance.abc.us-west-2.rds.amazonaws.com",
                        "port": "5432", "user": "testuser",
                        "iam_expiration": "30"})  # 30 seconds
    svc = _mk_plugin_service()
    plugin = AsyncIamAuthPlugin(svc, props)

    # Arrange: call twice quickly; second call should hit cache
    with patch.object(plugin, "_generate_token_blocking", return_value="tok-1") as gen:
        asyncio.run(plugin._resolve_credentials(
            HostInfo(host="db-instance.abc.us-west-2.rds.amazonaws.com", port=5432), props))
        asyncio.run(plugin._resolve_credentials(
            HostInfo(host="db-instance.abc.us-west-2.rds.amazonaws.com", port=5432), props))
    assert gen.call_count == 1  # second call cached


def test_iam_plugin_auto_discovers_region_from_rds_host():
    """Region auto-discovered from RDS hostname when IAM_REGION not set."""
    props = Properties({"host": "db.cluster-xyz.us-west-2.rds.amazonaws.com",
                        "port": "5432", "user": "testuser"})
    svc = _mk_plugin_service()
    plugin = AsyncIamAuthPlugin(svc, props)
    with patch.object(plugin, "_generate_token_blocking", return_value="tok") as gen:
        asyncio.run(plugin._resolve_credentials(
            HostInfo(host="db.cluster-xyz.us-west-2.rds.amazonaws.com", port=5432), props))
    _, kwargs = gen.call_args
    # Region argument should be "us-west-2" extracted from hostname
    assert any("us-west-2" in str(a) for a in gen.call_args[0]) or kwargs.get("region") == "us-west-2"


def test_iam_plugin_invalidate_cache_drops_entry():
    props = Properties({"host": "h", "port": "5432", "user": "u",
                        "iam_region": "us-west-2"})
    svc = _mk_plugin_service()
    plugin = AsyncIamAuthPlugin(svc, props)
    with patch.object(plugin, "_generate_token_blocking", return_value="tok-1"):
        asyncio.run(plugin._resolve_credentials(HostInfo(host="h", port=5432), props))

    plugin._invalidate_cache(HostInfo(host="h", port=5432), props)
    assert not plugin._token_cache


def test_iam_plugin_resolve_credentials_returns_was_cached_flag():
    props = Properties({"host": "h", "port": "5432", "user": "u",
                        "iam_region": "us-west-2"})
    svc = _mk_plugin_service()
    plugin = AsyncIamAuthPlugin(svc, props)
    with patch.object(plugin, "_generate_token_blocking", return_value="tok"):
        user1, pw1, was_cached1 = asyncio.run(
            plugin._resolve_credentials(HostInfo(host="h", port=5432), props))
        user2, pw2, was_cached2 = asyncio.run(
            plugin._resolve_credentials(HostInfo(host="h", port=5432), props))
    assert was_cached1 is False
    assert was_cached2 is True
```

`_mk_plugin_service` helper should be visible; if not, add it or use MagicMock with needed attrs. Test file should already have similar helpers.

- [ ] **E.2.2-4: Implement, verify, commit**:

```python
# auth_plugins.py, AsyncIamAuthPlugin changes:

    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60  # keep as fallback
    _TOKEN_REGEN_GRACE_SEC = 60

    async def _resolve_credentials(self, host_info, props):
        user = WrapperProperties.USER.get(props)
        if not user:
            raise AwsWrapperError("IAM auth requires a 'user' connection property")

        # Sync parity: use IamAuthUtils for host/port/cache-key
        from aws_advanced_python_wrapper.utils.iam_auth_utils import IamAuthUtils
        from aws_advanced_python_wrapper.utils.region_utils import \
            GdbRegionUtils, RegionUtils
        from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
        from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType

        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, 5432)

        rds_type = RdsUtils().identify_rds_type(host)
        region_utils = (GdbRegionUtils()
                        if rds_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
                        else RegionUtils())
        region = region_utils.get_region(
            props, WrapperProperties.IAM_REGION.name, host, host_info)
        if not region:
            raise AwsWrapperError(
                f"Could not resolve region from '{host}'. Set IAM_REGION explicitly.")

        cache_key = IamAuthUtils.get_cache_key(user, host, port, region)
        ttl_sec = WrapperProperties.IAM_EXPIRATION.get_int(props)
        if not ttl_sec:
            ttl_sec = self._DEFAULT_TOKEN_EXPIRATION_SEC

        now = asyncio.get_event_loop().time()
        cached = self._token_cache.get(cache_key)
        if cached is not None:
            token, expires_at = cached
            if now < expires_at - self._TOKEN_REGEN_GRACE_SEC:
                return user, token, True  # was_cached=True

        token = await asyncio.to_thread(
            self._generate_token_blocking, host, int(port), user, region)
        self._token_cache[cache_key] = (token, now + ttl_sec)
        return user, token, False

    def _invalidate_cache(self, host_info, props):
        user = WrapperProperties.USER.get(props)
        from aws_advanced_python_wrapper.utils.iam_auth_utils import IamAuthUtils
        from aws_advanced_python_wrapper.utils.region_utils import RegionUtils
        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, 5432)
        region = (WrapperProperties.IAM_REGION.get(props)
                  or RegionUtils().get_region(props, WrapperProperties.IAM_REGION.name, host, host_info))
        if user is None:
            return
        cache_key = IamAuthUtils.get_cache_key(user, host, port, region)
        self._token_cache.pop(cache_key, None)
```

Commit message: `fix(aio): IAM honors IAM_EXPIRATION + auto-detects region + retry-on-login (phase E.2) -- closes audit P0 #8 for IAM.`

---

## Task E.3: `AsyncAwsSecretsManagerPlugin` — SECRETS_MANAGER_EXPIRATION + endpoint + ARN region

### Changes
1. `_resolve_credentials` returns 3-tuple.
2. `_invalidate_cache` drops entry.
3. Per-entry TTL using `SECRETS_MANAGER_EXPIRATION.get_int(props)` (default: long — matches sync's "negative fallback to 1-year").
4. Honor `SECRETS_MANAGER_ENDPOINT.get(props)` — pass as `endpoint_url=` to boto3 client.
5. ARN parsing: if `secret_id` is an ARN and `region` is None, extract region from ARN (sync `secrets_manager_plugin.py:223-238`).
6. Cache stores `(user, password, expires_at)`; cache hit requires `now < expires_at`.

### TDD

- [ ] Similar 4 tests: TTL honored, endpoint override forwarded, ARN region extraction, was_cached flag.

- [ ] Implement + commit: `fix(aio): Secrets Manager honors TTL + endpoint + ARN region + retry-on-login (phase E.3) -- closes audit P0 #8 for Secrets Manager.`

---

## Task E.4: Federated + Okta — retry inherited; Okta SAML regex fix

### Changes
1. Both already subclass `AsyncAuthPluginBase`; their `_resolve_credentials` needs to return 3-tuple.
2. `_invalidate_cache` drops token from the mixin's cache.
3. Okta's `_extract_saml_assertion` should use sync Okta's regex (`"SAMLResponse" .* value="..."`, allowing whitespace between attributes) — NOT ADFS's stricter regex.
4. Honor `HTTP_REQUEST_TIMEOUT` (already done; verify no changes needed).

### TDD

- [ ] Tests for: was_cached flag on both, retry on 401 via base class, Okta regex matches Okta's HTML format.

- [ ] Implement + commit: `fix(aio): Federated/Okta return was_cached + Okta regex parity (phase E.4) -- closes audit P0 #7 for SAML auth.`

---

## Task E.5: Final `/verify`

- [ ] mypy + flake8 + isort + pytest -Werror clean.
- [ ] Confirm P0 #7, #8, P1 #14 closed across all 4 auth plugins.

---

## Out of scope

- aiohttp proxy support (P2 #25) — Phase I cleanup.
- Telemetry counters — Phase I.
- IdP factory registry (P1 federated) — would need sync-side refactor; defer.

## Self-review

- [x] All 4 P0/P1 auth findings mapped: retry-on-401 (E.1 base), expiration (E.2/E.3), FORCE_CONNECT (E.1), endpoint/region discovery (E.2/E.3), Okta regex (E.4).
- [x] Sync reference lines cited.
- [x] No forward refs to Phase F-J.
