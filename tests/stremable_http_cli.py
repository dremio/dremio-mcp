"""
MCP HTTP Streamable Client Example using Python SDK

This example demonstrates how to create an MCP client that connects to a server
using the Streamable HTTP transport protocol.
"""

import asyncio
import contextlib
import functools
import json
import random
import sys
import threading
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Awaitable, AsyncGenerator, Callable, Dict, Optional
from urllib.parse import urlparse

import httpx
import jwt
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.shared.auth import OAuthMetadata
import requests
import uvicorn
import yaml
from rich import print as pp, print_json as pj
from rich.markdown import Markdown
from rich.table import Table
from typer import Typer, Option
from datetime import datetime

from dremioai import log
from dremioai.api.oauth2 import get_oauth2_tokens, OAuth2Redirect
from dremioai.api.transport import AsyncHttpClient
from dremioai.config import settings
from dremioai.config.feature_flags import FeatureFlagManager
from dremioai.config.tools import ToolType
from dremioai.servers.mcp import FastMCPServerWithAuthToken, init, Transports
from pydantic import BaseModel, ConfigDict, Field, RootModel


def async_command(func: Callable) -> Callable:
    """Decorator to run async functions in Typer commands."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(func(*args, **kwargs))

    return wrapper


# ---------------------------------------------------------------------------
# Auth-cache helpers
# ---------------------------------------------------------------------------

_AUTH_CACHE_PATH = Path.home() / ".config" / "dremioai" / ".auth.yaml"


class TokenExpiredError(Exception):
    """Raised when a 401 Unauthorized is detected from the MCP server."""


class AuthCache(BaseModel):
    """Per-URI token entry stored in ``~/.config/dremioai/.auth.yaml``."""

    token: str | None = None
    refresh_token: str | None = Field(None, alias="refresh-token")
    client_id: str | None = Field(None, alias="client-id")

    model_config = ConfigDict(populate_by_name=True)


class AuthCacheStore(RootModel[dict[str, AuthCache]]):
    """Auth cache file — a mapping of MCP server URI → :class:`AuthCache`.

    YAML on disk::

        https://mcp.dremio.cloud/mcp/<project-id>:
          token: <access_token>
          refresh-token: <refresh_token>
          client-id: <oauth_client_id>
        https://mcp.eu.dremio.cloud/mcp/<other-project>:
          token: ...
    """

    root: dict[str, AuthCache] = Field(default_factory=dict)

    def get(self, url: str) -> AuthCache:
        return self.root.get(url, AuthCache())

    def set(self, url: str, cache: AuthCache) -> "AuthCacheStore":
        self.root[url] = cache
        return self


def _read_auth_store() -> AuthCacheStore:
    """Load the full auth cache store from disk."""
    if not _AUTH_CACHE_PATH.exists():
        return AuthCacheStore()
    try:
        data = yaml.safe_load(_AUTH_CACHE_PATH.read_text())
        if isinstance(data, dict):
            return AuthCacheStore.model_validate(data)
    except Exception:
        pass
    return AuthCacheStore()


def _write_auth_store(store: AuthCacheStore) -> None:
    """Persist the full auth cache store to disk."""
    _AUTH_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _AUTH_CACHE_PATH.write_text(
        yaml.safe_dump(store.model_dump(by_alias=True, exclude_none=True))
    )


def _read_auth_cache(url: str) -> AuthCache:
    """Return the :class:`AuthCache` entry for *url*; empty if not found."""
    return _read_auth_store().get(url)


def _write_auth_cache(url: str, cache: AuthCache) -> None:
    """Write/update the :class:`AuthCache` entry for *url* in the store."""
    _write_auth_store(_read_auth_store().set(url, cache))


def _do_token_refresh(
    token_endpoint: str, client_id: str, refresh_token: str
) -> tuple[str | None, str | None]:
    """POST to *token_endpoint* to exchange *refresh_token* for a new access token.

    Returns ``(new_access_token, new_refresh_token)``.  The new refresh token
    may be ``None`` when the server does not rotate refresh tokens.
    """
    try:
        resp = requests.post(
            token_endpoint,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            body = resp.json()
            return body.get("access_token"), body.get("refresh_token")
        pp(f"[yellow]Token refresh returned HTTP {resp.status_code}[/yellow]")
    except Exception as exc:
        pp(f"[yellow]Token refresh request failed: {exc}[/yellow]")
    return None, None


def _resolve_token(
    explicit_token: str | None,
    url: str,
    client_id: str | None,
    redirect_port: int,
    redirect_path: str,
) -> tuple[str, AuthCache]:
    """Return the bearer token to use, in priority order:

    1. *explicit_token* (``--token`` flag) — cache is bypassed entirely.
    2. ``~/.config/dremioai/.auth.yaml`` ``token`` field.
    3. Full OAuth PKCE flow (requires *client_id*); writes result to cache.

    Returns ``(token, AuthCache)``.  The cache is empty when the caller supplied
    an explicit token.
    """
    url = _normalize_url(url)
    if explicit_token is not None:
        return explicit_token, AuthCache()

    cache = _read_auth_cache(url)
    if cache.token:
        pp(f"[dim]Using cached token for {url}[/dim]", file=sys.stderr)
        return cache.token, cache

    # No cached token — try the OAuth flow.
    if not client_id:
        pp(
            "[red]No token available.[/red]  "
            "Provide --token, or use --client-id to authenticate via OAuth."
        )
        raise SystemExit(1)

    pp(f"No cached token for {url}. Starting OAuth flow..")
    oauth_meta = get_oauth_config(url)
    oauth = get_oauth2_tokens(
        client_id,
        str(oauth_meta.authorization_endpoint),
        str(oauth_meta.token_endpoint),
        redirect_port,
        redirect_path,
    )
    if not oauth.access_token:
        pp("[red]OAuth flow did not return a token.[/red]")
        raise SystemExit(1)

    new_cache = AuthCache(
        token=oauth.access_token, refresh_token=oauth.refresh_token, client_id=client_id
    )
    _write_auth_cache(url, new_cache)
    pp(f"[green]Authenticated.[/green]  Token cached for {url}")
    return oauth.access_token, new_cache


def _handle_token_expired(url: str, cache: AuthCache) -> str | None:
    """Attempt a token refresh using the cached refresh token.

    On success writes the new token to cache and returns it.
    Returns ``None`` if the cache lacks a refresh token or the refresh fails.
    """
    url = _normalize_url(url)
    if not (cache.refresh_token and cache.client_id):
        return None
    try:
        oauth_meta = get_oauth_config(url)
        new_token, new_refresh = _do_token_refresh(
            str(oauth_meta.token_endpoint), cache.client_id, cache.refresh_token
        )
        if new_token:
            _write_auth_cache(
                url,
                AuthCache(
                    token=new_token,
                    refresh_token=new_refresh or cache.refresh_token,
                    client_id=cache.client_id,
                ),
            )
            return new_token
    except Exception as exc:
        pp(f"[yellow]Token refresh failed: {exc}[/yellow]")
    return None


def with_auth(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Decorator: resolve bearer token (explicit → cache → OAuth) and retry once on 401.

    The wrapped function's ``token`` kwarg is replaced with the resolved token
    before the call.  The function must accept ``url``, ``token``, and optionally
    ``client_id``, ``redirect_port``, ``redirect_path``::

        @cli.command("list-tools")
        @async_command
        @with_auth
        async def list_tools(url=..., token=None, client_id=None):
            async with mcp_client_session(url, token) as session:
                ...  # token is already resolved here
    """

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        url = kwargs.get("url") or "http://127.0.0.1:8000/mcp"
        redirect_port = kwargs.get("redirect_port", 8976)
        redirect_path = kwargs.get("redirect_path", "/")

        resolved, cache = _resolve_token(
            kwargs.get("token"),
            url,
            kwargs.get("client_id"),
            redirect_port,
            redirect_path,
        )
        try:
            return await fn(*args, **{**kwargs, "token": resolved})
        except TokenExpiredError:
            pp("[yellow]Token expired — refreshing...[/yellow]")
            new_token = _handle_token_expired(url, cache)
            if not new_token:
                pp(
                    "[red]Token refresh failed.[/red]  "
                    "Re-authenticate with --client-id or supply a new --token."
                )
                raise SystemExit(1)
            pp("[green]Token refreshed — retrying...[/green]")
            return await fn(*args, **{**kwargs, "token": new_token})

    return wrapper


# ---------------------------------------------------------------------------
# Typer apps
# ---------------------------------------------------------------------------

app = Typer(
    no_args_is_help=True,
    name="mcp-client",
    help="Run simple mcp client",
    context_settings=dict(help_option_names=["-h", "--help"]),
)

auth = Typer(
    no_args_is_help=True,
    name="auth",
    help="Auth related sub commands",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


def _normalize_url(url: str) -> str:
    """Canonicalize *url* so it is always usable as an unambiguous cache key.

    * Prepends ``https://`` when no scheme is present.
    * Strips a trailing ``/`` to avoid ``/mcp`` vs ``/mcp/`` mismatches.
    """
    if url and not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    return url.rstrip("/")


def get_oauth_config(url: str) -> OAuthMetadata:
    u = urlparse(url)
    u = u._replace(path="/.well-known/oauth-authorization-server")
    log.logger("auth").info(f"Checking auth for {u.geturl()}")
    r = requests.get(u.geturl())
    if r.status_code != 200:
        pp(f"Cannot get oauth config: {u.geturl()}")
        r.raise_for_status()
    return OAuthMetadata.model_validate(r.json())


def _redact_value(v: Any, keep: int = 12) -> str:
    if v is None:
        return ""
    s = str(v)
    if len(s) <= keep:
        return s
    return f"{s[:keep]}..."


class JWK(BaseModel):
    kid: Optional[str] = None
    kty: Optional[str] = None
    alg: Optional[str] = None
    use: Optional[str] = None
    n: Optional[str] = None
    e: Optional[str] = None


class JWKS(BaseModel):
    keys: list[JWK] = Field(default_factory=list)


@auth.command("list")
def list_auth(
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
):
    pp(get_oauth_config(url))


@auth.command("check")
def check_auth(
    client_id: Annotated[str, Option(help="The client id to check")],
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
    redirect_port: Annotated[
        int, Option(help="Local port for OAuth redirect listener")
    ] = 8976,
    redirect_path: Annotated[
        str, Option(help="Path for OAuth redirect (e.g. /Callback)")
    ] = "/",
) -> OAuth2Redirect:
    url = _normalize_url(url or "http://127.0.0.1:8000/mcp")
    md = get_oauth_config(url)
    oauth = get_oauth2_tokens(
        client_id,
        str(md.authorization_endpoint),
        str(md.token_endpoint),
        redirect_port,
        redirect_path,
    )
    if oauth.access_token:
        cache_update(
            url=url,
            token=oauth.access_token,
            refresh_token=oauth.refresh_token,
            client_id=client_id,
        )
    pp(oauth.access_token)
    return oauth


@auth.command("verify-token")
@async_command
async def verify_token(
    token: Annotated[str, Option(help="Bearer token/JWT to verify")],
    jwks_uri: Annotated[
        Optional[str],
        Option(
            help="JWKS endpoint URL. When set, validates token signature+exp via JWKS."
        ),
    ] = None,
    extract_org_id_from_jwt: Annotated[
        bool,
        Option(help="Enable JWT aud extraction path when JWKS is not configured."),
    ] = False,
    strict: Annotated[
        bool,
        Option(
            help="Exit non-zero when --jwks-uri is set but token is not cryptographically verified."
        ),
    ] = False,
    debug: Annotated[
        bool,
        Option(
            help="Print debug diagnostics, including JWKS payload when --jwks-uri is set."
        ),
    ] = False,
):
    overrides: Dict[str, Any] = {
        "dremio.extract_org_id_from_jwt": extract_org_id_from_jwt,
    }
    if jwks_uri is not None:
        overrides["dremio.jwks_uri"] = jwks_uri

    async def _run_verify(tok: str):
        FeatureFlagManager.set_org_id(None)
        verifier = FastMCPServerWithAuthToken.DelegatingTokenVerifier()
        result = await verifier.verify_token(tok)
        org_id = FeatureFlagManager.get_org_id() if extract_org_id_from_jwt else None
        return result, org_id

    result, org_id = await settings.run_with(
        _run_verify,
        overrides=overrides,
        args=[token],
    )

    if result is None:
        pp({"accepted": False, "verified": False, "reason": "rejected"})
        raise SystemExit(1)

    try:
        hdr = jwt.get_unverified_header(token)
    except Exception as e:
        hdr = {"error": str(e)}

    claims = None
    try:
        c = jwt.decode(token, options={"verify_signature": False})
        claims = {k: c.get(k) for k in ("iss", "aud", "sub", "exp", "iat")}
    except Exception as e:
        claims = {"error": str(e)}

    aud_claim = claims.get("aud") if isinstance(claims, dict) else None
    aud_claim_value = (
        aud_claim[0] if isinstance(aud_claim, list) and aud_claim else aud_claim
    )

    verified = "jwt_verified" in result.scopes
    if debug:
        jwt_tbl = Table(title="JWT Debug")
        jwt_tbl.add_column("Field")
        jwt_tbl.add_column("Value")
        jwt_tbl.add_row("token_bytes", str(len(token)))
        jwt_tbl.add_row(
            "header.alg", str(hdr.get("alg")) if isinstance(hdr, dict) else ""
        )
        kid = str(hdr.get("kid"))
        jwt_tbl.add_row("header.kid", kid)
        jwt_tbl.add_row("claims.iss", str(claims.get("iss")))
        jwt_tbl.add_row("claims.aud", str(claims.get("aud")))
        jwt_tbl.add_row("claims.sub", str(claims.get("sub")))
        if claims.get("exp"):
            exp = str(datetime.fromtimestamp(claims.get("exp")))
        else:
            exp = ""
        jwt_tbl.add_row("claims.exp", exp)
        pp(jwt_tbl)

        if jwks_uri is not None:
            try:
                parsed_jwks = urlparse(jwks_uri)
                jwks_client = AsyncHttpClient(
                    uri=f"{parsed_jwks.scheme}://{parsed_jwks.netloc}", token=""
                )
                jwks_endpoint = parsed_jwks.path or "/"
                if parsed_jwks.query:
                    jwks_endpoint = f"{jwks_endpoint}?{parsed_jwks.query}"
                jwks: JWKS = await jwks_client.get(jwks_endpoint, deser=JWKS)
                kid = hdr.get("kid") if isinstance(hdr, dict) else None
                key_ids = [k.kid for k in jwks.keys if k.kid]
                jwks_tbl = Table(title="JWKS Keys")
                jwks_tbl.add_column("kid")
                jwks_tbl.add_column("kty")
                jwks_tbl.add_column("alg")
                for k in jwks.keys:
                    x = k.kid
                    if k.kid == kid:
                        x = f"[bold green]{k.kid}[/bold green]"
                    jwks_tbl.add_row(str(x), str(k.kty), str(k.alg))
                pp(f"JWKS URI: {jwks_uri}")
                pp(jwks_tbl)
                pp(
                    f"header_kid={kid} kid_found_in_jwks={kid in key_ids if kid else False}"
                )
            except Exception as e:
                pp({"debug": {"jwks_uri": jwks_uri, "jwks_fetch_error": str(e)}})

    if strict and jwks_uri is not None and not verified:
        pp(
            {
                "accepted": True,
                "verified": False,
                "reason": "strict_mode_verification_failed",
            }
        )
        raise SystemExit(1)

    result_tbl = Table(title="Verification Result")
    result_tbl.add_column("Field")
    result_tbl.add_column("Value")
    verified_text = "[green]true[/green]" if verified else "[red]false[/red]"
    expires_at_text = "None"
    if result.expires_at is not None:
        exp_dt = datetime.fromtimestamp(result.expires_at, tz=timezone.utc)
        expired = exp_dt < datetime.now(timezone.utc)
        expires_at_text = exp_dt.isoformat()
        if expired:
            expires_at_text = f"[red]{expires_at_text} (EXPIRED)[/red]"
    org_id_text = f"[cyan]{org_id}[/cyan]" if org_id else "[dim]not found[/dim]"
    if org_id and aud_claim_value:
        org_aud_match_text = (
            "[green]✓ match[/green]"
            if str(org_id) == str(aud_claim_value)
            else f"[red]✗ mismatch[/red] (aud={aud_claim_value})"
        )
    elif org_id and not aud_claim_value:
        org_aud_match_text = "[red]✗ no aud claim[/red]"
    else:
        org_aud_match_text = "[dim]n/a[/dim]"
    result_tbl.add_row("accepted", "true")
    result_tbl.add_row("verified", verified_text)
    result_tbl.add_row("expires_at", expires_at_text)
    result_tbl.add_row("scopes", ", ".join(result.scopes))
    result_tbl.add_row("client_id", str(result.client_id))
    result_tbl.add_row("org_id", org_id_text)
    result_tbl.add_row("org_id vs claims.aud", org_aud_match_text)
    result_tbl.add_row("jwks_enabled", str(jwks_uri is not None).lower())
    result_tbl.add_row("extract_org_id_from_jwt", str(extract_org_id_from_jwt).lower())
    pp(result_tbl)


@auth.command("cache-update")
def cache_update(
    url: Annotated[str, Option(help="MCP server URL — used as the cache key")],
    token: Annotated[str, Option(help="Bearer / access token to store")],
    refresh_token: Annotated[
        Optional[str], Option("--refresh-token", help="Refresh token (optional)")
    ] = None,
    client_id: Annotated[
        Optional[str],
        Option("--client-id", help="OAuth client ID (needed for future refresh)"),
    ] = None,
):
    """Write or overwrite the cached credentials for a given MCP server URL.

    Useful when you have a token from another source and want to inject it
    so that ``list-tools``, ``call-tool``, and ``test`` pick it up automatically::

        auth cache-update --url https://mcp.dremio.cloud/mcp/<proj> \\
                          --token eyJ... \\
                          --refresh-token rrt_... \\
                          --client-id my-client-id
    """
    url = _normalize_url(url)
    _write_auth_cache(
        url,
        AuthCache(token=token, refresh_token=refresh_token, client_id=client_id),
    )
    pp(f"[green]Cache updated for[/green] {url}")


@auth.command("cache-show")
def cache_show(
    url: Annotated[
        Optional[str], Option(help="Show only the entry for this URL (omit for all)")
    ] = None,
    token_only: Annotated[
        Optional[bool], Option(help="Show only the entry for this URL")
    ] = False,
):
    """Display the current auth cache contents (tokens are redacted)."""
    if url:
        url = _normalize_url(url)
    store = _read_auth_store()
    if not store.root:
        pp("[dim]Auth cache is empty.[/dim]")
        return
    entries = {url: store.root[url]} if url and url in store.root else store.root
    if url and url not in entries:
        pp(f"[yellow]No cache entry found for {url}[/yellow]")
        return

    if token_only:
        print(entries[url].token)
        return

    tbl = Table(title="Auth Cache (~/.config/dremioai/.auth.yaml)")
    tbl.add_column("URL")
    tbl.add_column("token")
    tbl.add_column("refresh-token")
    tbl.add_column("client-id")
    for entry_url, entry in entries.items():
        tbl.add_row(
            entry_url,
            _redact_value(entry.token),
            _redact_value(entry.refresh_token),
            entry.client_id or "",
        )
    pp(tbl)


cli = Typer(
    no_args_is_help=True,
    name="cli",
    help="MCP client session related sub commands",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@asynccontextmanager
async def mcp_client_session(
    url: str, token: Optional[str] = None
) -> AsyncGenerator[ClientSession, None]:
    """Open a ``ClientSession`` against *url*, converting 401 responses to
    :class:`TokenExpiredError` so that :func:`with_auth` can refresh and retry.

    ``streamablehttp_client`` runs its read/write streams inside an anyio
    ``TaskGroup``, so ``httpx.HTTPStatusError`` arrives wrapped in a
    ``BaseExceptionGroup``.  The ``except*`` syntax (PEP 654) handles both the
    bare and the group-wrapped case transparently.
    """
    headers = {"Authorization": f"Bearer {token}"} if token is not None else None
    try:
        async with streamablehttp_client(url=url, headers=headers) as (
            read_stream,
            write_stream,
            gid,
        ):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                yield session
    except* httpx.HTTPStatusError as eg:
        for exc in eg.exceptions:
            if exc.response.status_code == 401:
                raise TokenExpiredError(str(exc)) from exc
        raise  # non-401 HTTP errors propagate unchanged


@cli.command("list-tools")
@async_command
@with_auth
async def list_tools(
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
    token: Annotated[
        Optional[str], Option(help="The authorization token to use")
    ] = None,
    client_id: Annotated[
        Optional[str],
        Option(help="OAuth client ID (used when no --token or cached token)"),
    ] = None,
    redirect_port: Annotated[
        int, Option("--redirect-port", help="Local port for OAuth redirect listener")
    ] = 8976,
    redirect_path: Annotated[
        str, Option("--redirect-path", help="Path for OAuth redirect (e.g. /Callback)")
    ] = "/",
):
    async with mcp_client_session(url, token) as session:
        result = await session.list_tools()
        tools = result.tools

    tbl = Table(
        title=f"Tools — {url}", show_header=True, header_style="bold", show_lines=True
    )
    tbl.add_column("Tool", style="bold cyan", no_wrap=True)
    tbl.add_column("Description")
    for tool in tools:
        tbl.add_row(tool.name, Markdown(tool.description or ""))
    pp(tbl)


@cli.command("call-tool")
@async_command
@with_auth
async def call_tool(
    tool: Annotated[str, Option(help="The tool to call")],
    url: Annotated[
        Optional[str], Option(help="The URL of the MCP server")
    ] = "http://127.0.0.1:8000/mcp",
    token: Annotated[
        Optional[str], Option(help="The authorization token to use")
    ] = None,
    client_id: Annotated[
        Optional[str],
        Option(help="OAuth client ID (used when no --token or cached token)"),
    ] = None,
    redirect_port: Annotated[
        int, Option("--redirect-port", help="Local port for OAuth redirect listener")
    ] = 8976,
    redirect_path: Annotated[
        str, Option("--redirect-path", help="Path for OAuth redirect (e.g. /Callback)")
    ] = "/",
    args: Annotated[
        Optional[str], Option(help="The arguments to pass to the tool as a JSON")
    ] = None,
):
    async with mcp_client_session(url, token) as session:
        result = await session.call_tool(tool, json.loads(args) if args else None)
        if result.isError:
            pp("[red]Error[/red]")
            pp(result.content)
            return
        if result.structuredContent is not None:
            pj(data=result.structuredContent.get("result", result.structuredContent))
        else:
            pj(result.content[0].text if result.content else "null")


def _assert(condition: bool, msg: str):
    if not condition:
        pp(f"[red]FAIL[/red] {msg}")
        raise SystemExit(1)


def _derive_dremio_api_uri(mcp_url: str) -> str:
    """Derive the Dremio API URI from an MCP server URL.

    Replaces 'mcp.' with 'api.' in the hostname and returns the base URL.
    E.g. https://mcp.dremio.cloud/mcp/proj-id -> https://api.dremio.cloud
         https://mcp.eu.dremio.cloud/... -> https://api.eu.dremio.cloud
    """
    parsed = urlparse(mcp_url)
    hostname = parsed.hostname or ""
    if hostname.startswith("mcp."):
        api_host = "api." + hostname[4:]
        scheme = parsed.scheme
        port_str = (
            f":{parsed.port}" if parsed.port and parsed.port not in (80, 443) else ""
        )
        return f"{scheme}://{api_host}{port_str}"
    return hostname


def _extract_project_id(mcp_url: str) -> Optional[str]:
    """Extract project ID from MCP URL path (e.g. /mcp/<project-id>)."""
    parsed = urlparse(mcp_url)
    path_parts = [p for p in parsed.path.split("/") if p and p != "mcp"]
    return path_parts[0] if path_parts else None


@contextlib.contextmanager
def _local_mcp_server(dremio_uri: str, port: int = 8989, ld_sdk_key: str | None = None):
    """Start a local MCP server configured to proxy to the given Dremio URI."""
    old = settings.instance()
    try:
        overrides = {
            "dremio.uri": dremio_uri,
            "dremio.raw_project_id": "DREMIO_DYNAMIC",
            "dremio.enable_search": True,
            "tools.server_mode": ToolType.FOR_DATA_PATTERNS.name,
        }
        if ld_sdk_key:
            overrides["launchdarkly.sdk_key"] = ld_sdk_key
        configured_settings = old.model_copy(deep=True).with_overrides(overrides)
        settings.set_base_settings(configured_settings)
        mcp_server = init(
            transport=Transports.streamable_http,
            port=port,
            host="127.0.0.1",
            mode=configured_settings.tools.server_mode,
            support_project_id_endpoints=True,
        )

        def _run():
            a = mcp_server.streamable_http_app()
            c = uvicorn.Config(app=a, host="127.0.0.1", port=port, log_level="warning")
            s = uvicorn.Server(c)
            asyncio.new_event_loop().run_until_complete(s.serve())

        t = threading.Thread(target=_run, daemon=True)
        t.start()

        # Wait for server to be ready
        for _ in range(30):
            try:
                requests.get(f"http://127.0.0.1:{port}/healthz", timeout=1)
                break
            except Exception:
                time.sleep(0.5)
        else:
            raise RuntimeError("Local MCP server did not start in time")

        yield port
    finally:
        settings.set_base_settings(old)


@app.command("test", help="Run a quick smoketest for a deployed MCP server")
@async_command
async def run_test(
    client_id: Annotated[
        Optional[str],
        Option(help="The OAuth client id (skipped when --token is provided)"),
    ] = None,
    token: Annotated[
        Optional[str], Option(help="Bearer token to use directly (skips OAuth)")
    ] = None,
    url: Annotated[
        Optional[str],
        Option(
            help="The URL of the MCP server (e.g. https://mcp.dremio.cloud/mcp/<project-id>)"
        ),
    ] = "http://127.0.0.1:8000/mcp",
    check_annotations: Annotated[
        bool, Option(help="Check MCP tool annotations")
    ] = True,
    check_new_contract: Annotated[
        bool,
        Option(
            help="Check new-contract changes (param rename, input validation, etc.)"
        ),
    ] = True,
    check_remote_tools: Annotated[
        bool,
        Option(
            help="Verify remote Dremio tools appear in tools/list and can be called directly"
        ),
    ] = False,
    local: Annotated[
        bool, Option(help="Start a local MCP server for running the tests")
    ] = False,
    ld_sdk_key: Annotated[
        Optional[str],
        Option(help="LaunchDarkly SDK key for flag evaluation check"),
    ] = None,
    ld_flag: Annotated[
        Optional[str],
        Option(help="LD flag name to evaluate (e.g. dremio.allow_dml)"),
    ] = None,
    ld_expected: Annotated[
        Optional[str],
        Option(
            help="Expected flag value (parsed: true/false→bool, numeric→int/float, else str)"
        ),
    ] = None,
    redirect_port: Annotated[
        int, Option(help="Local port for OAuth redirect listener")
    ] = 8976,
    redirect_path: Annotated[
        str, Option(help="Path for OAuth redirect (e.g. /Callback)")
    ] = "/",
):
    if not local:
        # Remote mode — resolve token then run smoketests, retry once on 401.
        async def _do_remote(tok: str) -> None:
            await _run_smoketests(
                url,
                tok,
                check_annotations,
                check_new_contract,
                check_remote_tools=check_remote_tools,
                ld_sdk_key=ld_sdk_key,
                ld_flag=ld_flag,
                ld_expected=ld_expected,
            )

        resolved, cache = _resolve_token(
            token, url, client_id, redirect_port, redirect_path
        )
        try:
            await _do_remote(resolved)
        except TokenExpiredError:
            pp("[yellow]Token expired — refreshing...[/yellow]")
            new_token = _handle_token_expired(url, cache)
            if not new_token:
                pp(
                    "[red]Token refresh failed.[/red]  Re-authenticate with --client-id or supply a new --token."
                )
                raise SystemExit(1)
            pp("[green]Token refreshed — retrying...[/green]")
            await _do_remote(new_token)
        return

    # Local mode: start a local MCP server and test against it
    dremio_api_uri = _derive_dremio_api_uri(url)
    project_id = _extract_project_id(url)

    if token is None:
        # No token — do OAuth against the local server (after it starts)
        if client_id is None:
            pp("[red]FAIL[/red] --local without --token requires --client-id for OAuth")
            raise SystemExit(1)

    pp(
        f"Starting local MCP server (dremio.uri={dremio_api_uri}, project_id=DREMIO_DYNAMIC).."
    )
    local_port = random.randrange(9000, 12000)
    with _local_mcp_server(dremio_api_uri, port=local_port, ld_sdk_key=ld_sdk_key):
        local_url = (
            f"http://127.0.0.1:{local_port}/mcp/{project_id}/"
            if project_id
            else f"http://127.0.0.1:{local_port}/mcp/"
        )
        pp(f"Local server ready at {local_url}")

        if token is None:
            pp("Checking auth against local server..", end=" ")
            a = check_auth(client_id, local_url, redirect_port, redirect_path)
            token = a.access_token
            pp("[green]OK[/green]")

        await _run_smoketests(
            local_url,
            token,
            check_annotations,
            check_new_contract,
            check_remote_tools=check_remote_tools,
            ld_sdk_key=ld_sdk_key,
            ld_flag=ld_flag,
            ld_expected=ld_expected,
        )


def _parse_flag_value(value: str) -> bool | int | float | str:
    """Parse a CLI string to a typed value for LD flag comparison."""
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


async def _run_smoketests(
    url: str,
    token: str,
    check_annotations: bool,
    check_new_contract: bool,
    check_remote_tools: bool = False,
    ld_sdk_key: str | None = None,
    ld_flag: str | None = None,
    ld_expected: str | None = None,
):
    # Determine the SQL query parameter name based on contract version
    sql_param = "query" if check_new_contract else "s"

    pp("Connecting to server..")
    async with mcp_client_session(url, token) as session:
        tools_result = await session.list_tools()
        tool_names = [t.name for t in tools_result.tools]
        pp(tool_names)

        # ------------------------------------------------------------------
        # 1. MCP tool annotations: every tool must have annotations;
        #    RunSqlQuery may have destructiveHint=True if allow_dml is on
        # ------------------------------------------------------------------
        if check_annotations:
            pp("Checking tool annotations..", end=" ")
            for t in tools_result.tools:
                _assert(
                    t.annotations is not None,
                    f"Tool {t.name} is missing annotations",
                )
                if t.name == "RunSqlQuery":
                    # RunSqlQuery: readOnlyHint and destructiveHint depend on
                    # the server's allow_dml setting — just verify they are
                    # consistent (mutually exclusive).
                    _assert(
                        t.annotations.readOnlyHint != t.annotations.destructiveHint,
                        f"RunSqlQuery readOnlyHint={t.annotations.readOnlyHint} and "
                        f"destructiveHint={t.annotations.destructiveHint} should be opposites",
                    )
                    pp(
                        f"\n  RunSqlQuery: readOnly={t.annotations.readOnlyHint}, "
                        f"destructive={t.annotations.destructiveHint}"
                    )
                else:
                    _assert(
                        t.annotations.readOnlyHint is True,
                        f"Tool {t.name} does not have readOnlyHint=True",
                    )
                    _assert(
                        t.annotations.destructiveHint is False,
                        f"Tool {t.name} does not have destructiveHint=False",
                    )
            pp("[green]OK[/green]")
        else:
            pp("Skipping tool annotations check")

        # ------------------------------------------------------------------
        # 2. RunSqlQuery: parameter renamed from 's' to 'query'
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking RunSqlQuery parameter name..", end=" ")
            sql_tool = next(t for t in tools_result.tools if t.name == "RunSqlQuery")
            params = sql_tool.inputSchema.get("properties", {})
            _assert("query" in params, "RunSqlQuery should have a 'query' parameter")
            _assert("s" not in params, "RunSqlQuery should NOT have an 's' parameter")
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 3. RunSqlQuery: basic SELECT works
        # ------------------------------------------------------------------
        pp("Checking RunSqlQuery SELECT..", end=" ")
        n = int(time.time())
        query = f"SELECT {n} as n"
        result = await session.call_tool("RunSqlQuery", {sql_param: query})
        _assert(not result.isError, f"RunSqlQuery SELECT failed: {result.content}")
        _assert(
            result.structuredContent is not None,
            "RunSqlQuery returned no structured content",
        )
        pp(result.structuredContent["result"]["result"])
        pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 4. RunSqlQuery: DML is rejected with clean error
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking RunSqlQuery DML rejection..", end=" ")
            result = await session.call_tool(
                "RunSqlQuery", {sql_param: "DROP TABLE foo"}
            )
            _assert(
                result.structuredContent is not None
                and "error" in result.structuredContent["result"],
                "RunSqlQuery should return an error dict for DML",
            )
            _assert(
                "SELECT" in result.structuredContent["result"]["error"],
                "DML error should mention that only SELECT is allowed",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 5. RunSqlQuery: verify job tracking via jobs_recent
        # ------------------------------------------------------------------
        pp("Checking RunSqlQuery job tracking..", end=" ")
        query2 = f"""
        SELECT query
        FROM   sys.project.jobs_recent
        WHERE query_type = 'REST' and  submitted_ts > CURRENT_TIMESTAMP() - INTERVAL '1' minute
        and query like '/* dremioai: submitter=RunS%' and query like '%SELECT {n} as n';
        """
        result = await session.call_tool("RunSqlQuery", {sql_param: query2})
        _assert(not result.isError, f"Job tracking query failed: {result.content}")
        rows = result.structuredContent["result"]["result"]
        pp(rows)
        _assert(len(rows) == 1, f"Expected 1 job tracking row, got {len(rows)}")
        pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 6. GetUsefulSystemTableNames: should return multiple entries
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetUsefulSystemTableNames..", end=" ")
            result = await session.call_tool("GetUsefulSystemTableNames", {})
            _assert(
                not result.isError,
                f"GetUsefulSystemTableNames failed: {result.content}",
            )
            table_names = result.structuredContent["result"]
            _assert(
                len(table_names) > 1,
                f"Expected multiple system tables, got {len(table_names)}",
            )
            expected_tables = [
                'INFORMATION_SCHEMA."TABLES"',
                "sys.project.jobs_recent",
                'INFORMATION_SCHEMA."COLUMNS"',
            ]
            for et in expected_tables:
                _assert(et in table_names, f"Missing expected system table: {et}")
            pp(f"{len(table_names)} tables returned")
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 7. GetSchemaOfTable: empty string returns validation error
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetSchemaOfTable empty input validation..", end=" ")
            result = await session.call_tool("GetSchemaOfTable", {"table_name": ""})
            _assert(
                not result.isError,
                f"GetSchemaOfTable empty input crashed: {result.content}",
            )
            schema_result = result.structuredContent["result"]
            _assert(
                "error" in schema_result,
                "GetSchemaOfTable('') should return an error dict",
            )
            _assert(
                "empty" in schema_result["error"].lower(),
                f"Error should mention 'empty', got: {schema_result['error']}",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 8. GetSchemaOfTable: valid table returns schema
        # ------------------------------------------------------------------
        pp("Checking GetSchemaOfTable with valid table..", end=" ")
        result = await session.call_tool(
            "GetSchemaOfTable",
            {"table_name": 'INFORMATION_SCHEMA."TABLES"'},
        )
        _assert(not result.isError, f"GetSchemaOfTable failed: {result.content}")
        schema_result = result.structuredContent["result"]
        _assert(
            "fields" in schema_result,
            f"Expected 'fields' in schema result, got keys: {list(schema_result.keys())}",
        )
        pp(f"{len(schema_result['fields'])} fields returned")
        pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 9. GetSchemaOfTable: parameter docstring includes format examples
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetSchemaOfTable parameter docs..", end=" ")
            schema_tool = next(
                t for t in tools_result.tools if t.name == "GetSchemaOfTable"
            )
            desc = schema_tool.description or ""
            _assert(
                "dot-separated" in desc or "list of path" in desc,
                "GetSchemaOfTable description should document input formats",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 10. GetDescriptionOfTableOrSchema: should work (auth fix)
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetDescriptionOfTableOrSchema auth..", end=" ")
            result = await session.call_tool(
                "GetDescriptionOfTableOrSchema",
                {"name": 'INFORMATION_SCHEMA."TABLES"'},
            )
            _assert(
                not result.isError,
                f"GetDescriptionOfTableOrSchema failed (auth issue?): {result.content}",
            )
            _assert(
                result.structuredContent is not None,
                "GetDescriptionOfTableOrSchema returned no structured content",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 11. GetTableOrViewLineage: bad table returns sanitized error
        # ------------------------------------------------------------------
        if check_new_contract:
            pp("Checking GetTableOrViewLineage error sanitization..", end=" ")
            result = await session.call_tool(
                "GetTableOrViewLineage",
                {"table_name": "nonexistent.table.name"},
            )
            _assert(
                not result.isError,
                f"GetTableOrViewLineage crashed instead of returning error dict: {result.content}",
            )
            lineage_result = result.structuredContent["result"]
            if "error" in lineage_result:
                _assert(
                    "api.dremio.cloud" not in lineage_result["error"],
                    "Lineage error should not expose internal API URLs",
                )
                _assert(
                    "project" not in lineage_result["error"].lower()
                    or "project id" not in lineage_result["error"].lower(),
                    "Lineage error should not expose project IDs",
                )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 12. Remote tools: verify they appear in tools/list (optional)
        # ------------------------------------------------------------------
        if check_remote_tools:
            pp("Checking remote tools in tools/list..", end=" ")
            static_tool_names = {
                "RunSqlQuery",
                "GetUsefulSystemTableNames",
                "GetSchemaOfTable",
                "GetDescriptionOfTableOrSchema",
                "GetTableOrViewLineage",
                "SearchTableAndViews",
                "DiscoverDynamicTools",
                "CallDynamicTool",
            }
            remote_tools = [
                t for t in tools_result.tools if t.name not in static_tool_names
            ]
            _assert(
                len(remote_tools) > 0,
                f"No remote tools found in tools/list (got {[t.name for t in tools_result.tools]})",
            )
            pp(
                f"{len(remote_tools)} remote tool(s) found: {[t.name for t in remote_tools]}"
            )

            # Call the first remote tool with no arguments to verify routing works
            first_remote = remote_tools[0]
            pp(f"Calling remote tool '{first_remote.name}'..", end=" ")
            result = await session.call_tool(first_remote.name, {})
            _assert(
                not result.isError
                and "error" not in (result.structuredContent or {}),
                f"Remote tool call failed: {result.content}",
            )
            pp("[green]OK[/green]")

        # ------------------------------------------------------------------
        # 13. LaunchDarkly flag evaluation (optional, requires --ld-sdk-key)
        # ------------------------------------------------------------------
        if ld_sdk_key and ld_flag:
            pp(f"Checking LD flag '{ld_flag}'..", end=" ")

            project_id = _extract_project_id(url)
            org_id = None
            if token:
                try:
                    claims = jwt.decode(token, options={"verify_signature": False})
                    aud = claims.get("aud")
                    org_id = aud[0] if isinstance(aud, list) else aud
                except Exception:
                    pass

            pp(
                f"\n  LD context: application=mcp-server, projectId={project_id}, orgId={org_id}"
            )
            _assert(
                org_id is not None,
                "JWT 'aud' claim must contain an orgId for org-based LD targeting "
                "(token may be opaque or missing 'aud')",
            )

            try:
                FeatureFlagManager.initialize(ld_sdk_key)
                fm = FeatureFlagManager.instance()

                # Wait for LD client to initialize (up to 5s)
                for _ in range(10):
                    if fm.is_enabled():
                        break
                    time.sleep(0.5)
                _assert(
                    fm.is_enabled(),
                    "LaunchDarkly client failed to initialize within 5s",
                )

                if project_id:
                    FeatureFlagManager.set_project_id(project_id)
                if org_id:
                    FeatureFlagManager.set_org_id(org_id)

                value = fm.get_flag(ld_flag, None)
                pp(f"  Flag '{ld_flag}' = {value!r}")

                if ld_expected is not None:
                    expected = _parse_flag_value(ld_expected)
                    _assert(
                        value == expected,
                        f"Expected {expected!r} ({type(expected).__name__}), got {value!r} ({type(value).__name__})",
                    )
                    pp(f"  Matches expected value {expected!r}")
                pp("[green]OK[/green]")
            finally:
                FeatureFlagManager.reset()

    pp("\n[green]All smoketests passed![/green]")


# Add the CLI subcommand to the main app
app.add_typer(cli)
app.add_typer(auth)


def main():
    log.configure(enable_json_logging=False, to_file=False)
    app()


if __name__ == "__main__":
    main()
