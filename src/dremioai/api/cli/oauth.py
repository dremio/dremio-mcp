from enum import StrEnum, auto
import asyncio
from typing import Annotated, Optional, Tuple
from urllib.parse import urlparse

from typer import Option, Typer
from rich import print as pp

from dremioai.api.oauth2 import get_oauth2_tokens
from dremioai.api.oauth_metadata import OAuthMetadataRFC8414
from dremioai.api.transport import AsyncHttpClient
from dremioai.config import settings

app = Typer(
    no_args_is_help=True,
    name="oauth",
    help="Run commands related to oauth",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


class PredefinedApp(StrEnum):
    """Predefined OAuth app names registered in Dremio DaaS."""

    CLAUDE = auto()
    CHATGPT = auto()

    @property
    def global_identifier(self) -> str:
        return f"https://connectors.dremio.app/{self.value}"


def resolve_oauth_endpoints(
    oauth_uri: Optional[str],
    mcp_uri: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Return (auth_url, token_url) from --oauth-uri or --mcp-uri.

    --oauth-uri: used directly as base → {base}/oauth/authorize, {base}/oauth/token
    --mcp-uri:   fetches /.well-known/oauth-authorization-server and extracts endpoints
    """
    if oauth_uri:
        base = oauth_uri.rstrip("/")
        return f"{base}/oauth/authorize", f"{base}/oauth/token"

    if mcp_uri:
        parsed = urlparse(mcp_uri)
        endpoint = "/.well-known/oauth-authorization-server"
        well_known = parsed._replace(
            path=endpoint, params="", query="", fragment=""
        ).geturl()
        pp(f"Fetching OAuth metadata from {well_known}")
        client = AsyncHttpClient(uri=f"{parsed.scheme}://{parsed.netloc}", token="")
        md = asyncio.run(client.get(endpoint, deser=OAuthMetadataRFC8414))
        return str(md.authorization_endpoint), str(md.token_endpoint)
    return None, None


@app.command("login")
def login(
    predefined_app: Annotated[
        Optional[PredefinedApp],
        Option(
            "--app",
            help="Predefined OAuth app name (uses its global identifier as client_id)",
        ),
    ] = PredefinedApp.CLAUDE,
    client_id: Annotated[
        Optional[str],
        Option(help="Explicit client_id (overrides --app)"),
    ] = None,
    redirect_port: Annotated[
        int, Option(help="Local port for OAuth redirect listener")
    ] = 8080,
    redirect_path: Annotated[
        str, Option(help="Path for OAuth redirect (e.g. /Callback)")
    ] = "/",
    oauth_uri: Annotated[
        Optional[str],
        Option(
            help="OAuth base URI (e.g. https://login.dremio.cloud). "
            "Derives /oauth/authorize and /oauth/token from it"
        ),
    ] = None,
    mcp_uri: Annotated[
        Optional[str],
        Option(
            help="MCP server URI (e.g. https://mcp.dremio.cloud/mcp/<project-id>). "
            "Discovers OAuth endpoints via .well-known/oauth-authorization-server"
        ),
    ] = None,
):
    auth_url, token_url = resolve_oauth_endpoints(oauth_uri, mcp_uri)
    resolved_client_id = client_id or predefined_app.global_identifier

    if auth_url is None:
        # Fall back to settings-based flow
        if not settings.instance().dremio.oauth_supported:
            raise RuntimeError(
                "OAuth is not supported for this Dremio instance. "
                "Provide --oauth-uri or --mcp-uri to specify endpoints directly."
            )

    if resolved_client_id is not None:
        if settings.instance().dremio.oauth_configured:
            settings.instance().dremio.oauth2.client_id = resolved_client_id
        else:
            settings.instance().dremio.oauth2 = settings.OAuth2.model_validate(
                {"client_id": resolved_client_id}
            )

    pp(f"Using client_id: {resolved_client_id}")
    oauth = get_oauth2_tokens(
        client_id=resolved_client_id,
        auth_url=auth_url,
        token_url=token_url,
        redirect_port=redirect_port,
        redirect_path=redirect_path,
    )
    pp(
        f"Access token: {oauth.access_token}"
        if oauth.access_token
        else "No token received"
    )
    pp(f"User: {oauth.user}")
    pp(f"Expires in: {oauth.expiry}s")


@app.command("status")
def status():
    if not settings.instance().dremio.oauth_supported:
        pp(
            f"OAuth is supported only for this Dremio cloud (uri={settings.instance().dremio.uri})"
        )
        return

    if not settings.instance().dremio.oauth_configured:
        pp("OAuth is not configured for this Dremio instance")
        return

    tok = (
        f"{settings.instance().dremio.pat[:4]}..."
        if settings.instance().dremio.pat
        else "<not set>"
    )
    exp = (
        str(settings.instance().dremio.oauth2.expiry)
        if settings.instance().dremio.oauth2.expiry
        else ""
    )
    if settings.instance().dremio.oauth2.has_expired:
        exp += f":(EXPIRED)"
    pp(
        {
            "token": tok,
            "expiry": exp,
            "user": (
                settings.instance().dremio.oauth2.dremio_user_identifier
                if settings.instance().dremio.oauth2.dremio_user_identifier
                else ""
            ),
        }
    )
