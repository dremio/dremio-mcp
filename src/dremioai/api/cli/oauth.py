from dremioai.api.oauth2 import get_oauth2_tokens
from dremioai.config import settings
from typing import Annotated
from typer import Option, Typer
from rich import print as pp

app = Typer(
    no_args_is_help=True,
    name="oauth",
    help="Run commands related to oauth",
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@app.command("login")
def login(
    client_id: Annotated[str, Option(help="The client id for the OAuth app")] = None,
):
    if not settings.instance().dremio.oauth_supported:
        raise RuntimeError("OAuth is not supported for this Dremio instance")

    if client_id is not None:
        if settings.instance().dremio.oauth_configured:
            settings.instance().dremio.oauth2.client_id = client_id
        else:
            settings.instance().dremio.oauth2 = settings.OAuth2.model_validate(
                {"client_id": client_id}
            )
    oauth = get_oauth2_tokens()
    pp(
        settings.instance().dremio.model_dump(
            exclude_none=True, mode="json", by_alias=True, exclude_unset=True
        )
    )


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
