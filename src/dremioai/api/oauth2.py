from typing import Optional

from aiohttp import ClientSession, web
from threading import Thread
from secrets import token_urlsafe
from hashlib import sha256
from base64 import urlsafe_b64encode
from urllib.parse import urlencode, urlparse
from dremioai.config import settings
from datetime import datetime, timedelta
from importlib.resources import files
from dremioai.log import logger
import webbrowser
import asyncio


class OAuth2Redirect:
    def __init__(
        self, client_id, code_verifier, code_challenge, token_url, redirect_port
    ):
        self.client_id = client_id
        self.code_verifier = code_verifier
        self.code_challenge = code_challenge
        self.token_url = token_url
        self.redirect_port = redirect_port
        self.stop = asyncio.Event()
        self.token = {}

    async def auth_redirect(self, request: web.Request):
        print(f"auth_redirect: {request}")
        redirect_uri = f"http://localhost:{self.redirect_port}"
        params = {
            "client_id": self.client_id,
            "code_verifier": self.code_verifier,
            "code": request.query["code"],
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }

        async with ClientSession() as session:
            async with session.post(self.token_url, data=params) as response:
                if response.status != 200:
                    print(f"Failed to get token: {await response.text()}")
                else:
                    self.token = await response.json()
        self.stop.set()
        auth_html = files("dremioai.resources") / "auth_redirect.html"
        return web.Response(text=auth_html.read_text(), content_type="text/html")

    @property
    def access_token(self) -> str:
        return self.token.get("access_token")

    @property
    def refresh_token(self) -> str:
        return self.token.get("refresh_token")

    @property
    def user(self) -> str:
        return self.token.get("dremio_user_identifier")

    @property
    def expiry(self) -> int:
        return self.token.get("expires_in")

    async def start_server(self):
        app = web.Application()
        app.router.add_get("/", self.auth_redirect)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", self.redirect_port)
        await site.start()
        await self.stop.wait()

    def update_settings(self):
        expiry = datetime.now() + timedelta(seconds=self.expiry - 10)
        settings.instance().dremio.pat = self.access_token
        settings.instance().dremio.oauth2 = settings.OAuth2.model_validate(
            {
                "client_id": self.client_id,
                "refresh_token": self.refresh_token,
                "dremio_user_identifier": self.user,
                "expiry": expiry,
            }
        )
        settings.write_settings()


def run_server(oauth: OAuth2Redirect):
    print("Starting server")
    asyncio.run(oauth.start_server())


def get_pkce_pair(length=96):
    length = max(min(length, 128), 43)
    code_verifier = token_urlsafe(length)
    code_challenge = (
        urlsafe_b64encode(sha256(code_verifier.encode()).digest()).rstrip(b"=").decode()
    )
    return code_verifier, code_challenge


class OAuth2:
    def __init__(
        self,
        client_id: Optional[str] = None,
        auth_url: Optional[str] = None,
        token_url: Optional[str] = None,
    ):
        if not auth_url:
            if settings.instance().dremio.oauth2.client_id is None:
                raise RuntimeError("oauth_client_id is not set in the config file")

            base = urlparse(settings.instance().dremio.uri)
            if base.netloc.startswith("api."):
                base = base._replace(netloc=f"login.{base.netloc[4:]}")
            url = base.geturl()

            self.client_id = settings.instance().dremio.oauth2.client_id
            self.authorize_url = f"{url}/oauth/authorize"
            self.access_token_url = f"{url}/oauth/token"
        else:
            self.authorize_url = auth_url
            self.access_token_url = token_url
            self.client_id = client_id
            if self.client_id is None:
                raise RuntimeError("client_id is required")
        self.redirect_port = 8976
        self.scope = "dremio.all offline_access"
        self.code_verifier, self.code_challenge = get_pkce_pair()
        self.init_params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": f"http://localhost:{self.redirect_port}",
            "scope": self.scope,
            "code_challenge": self.code_challenge,
            "code_challenge_method": "S256",
        }
        self.oauth_redirect = OAuth2Redirect(
            self.client_id,
            self.code_verifier,
            self.code_challenge,
            self.access_token_url,
            self.redirect_port,
        )


def get_oauth2_tokens(
    client_id: Optional[str] = None,
    auth_url: Optional[str] = None,
    token_url: Optional[str] = None,
) -> OAuth2Redirect:
    # client_id = "311658a1-19ae-4851-b6a6-911c794312e2",
    # client_id = "a3743893-d849-4c8a-893b-533dd457aac4"
    oauth = OAuth2(client_id, auth_url, token_url)
    server_thread = Thread(
        target=run_server,
        daemon=True,
        args=(oauth.oauth_redirect,),
    )
    server_thread.start()

    url = f"{oauth.authorize_url}?{urlencode(oauth.init_params)}"
    print(f"Opening browser to {url}")
    webbrowser.open(url)
    server_thread.join()
    logger("oauth2").debug(
        f"Access token: {oauth.oauth_redirect.access_token}\n"
        f"Refresh token: {oauth.oauth_redirect.refresh_token}\n"
        f"Expiry: {oauth.oauth_redirect.expiry}\n"
    )
    return oauth.oauth_redirect
