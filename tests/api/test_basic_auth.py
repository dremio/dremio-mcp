#
#  Copyright (C) 2017-2025 Dremio Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from datetime import datetime, timedelta
from io import BytesIO
from json import dumps, loads
from unittest.mock import patch

import pytest

from dremioai.api.basic_auth import get_session_token
from dremioai.config import settings


@pytest.fixture
def software_settings_with_basic_auth(mock_config_dir):
    settings.configure(force=True)
    settings.instance().dremio = settings.Dremio.model_validate(
        {
            "uri": "http://dremio.example.com:9047",
            "basic_auth": {"username": "alice", "password": "s3cret"},
        }
    )
    yield settings.instance()
    settings.configure(force=True)


class _FakeLoginResponse:
    def __init__(self, payload: dict):
        self._body = BytesIO(dumps(payload).encode("utf-8"))

    def read(self):
        return self._body.read()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_basic_auth_settings_parse(software_settings_with_basic_auth):
    dremio = settings.instance().dremio
    assert dremio.basic_auth_configured
    assert dremio.basic_auth.username == "alice"
    assert dremio.basic_auth.password == "s3cret"
    assert not dremio.basic_auth.has_expired
    assert dremio.pat is None


def test_basic_auth_password_file_resolution(
    software_settings_with_basic_auth, tmp_path
):
    password_file = tmp_path / "dremio-password"
    password_file.write_text("from-a-file\n")
    dremio = settings.instance().dremio
    dremio.basic_auth = settings.BasicAuth.model_validate(
        {"username": "alice", "password": f"@{password_file}"}
    )
    assert dremio.basic_auth.password == "from-a-file"
    # serialization must keep the reference, not the resolved secret
    assert dremio.basic_auth.model_dump()["raw_password"] == f"@{password_file}"


def test_basic_auth_not_configured_for_cloud(software_settings_with_basic_auth):
    dremio = settings.instance().dremio
    dremio.project_id = "01234567-89ab-cdef-0123-456789abcdef"
    assert dremio.is_cloud
    assert not dremio.basic_auth_configured


def test_get_session_token_logs_in_and_updates_settings(
    software_settings_with_basic_auth,
):
    expires_ms = int((datetime.now() + timedelta(hours=30)).timestamp() * 1000)
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["body"] = loads(request.data.decode("utf-8"))
        return _FakeLoginResponse({"token": "session-token-123", "expires": expires_ms})

    with patch("dremioai.api.basic_auth.urlopen", side_effect=fake_urlopen):
        session = get_session_token()
    session.update_settings()

    assert captured["url"] == "http://dremio.example.com:9047/apiv2/login"
    assert captured["body"] == {"userName": "alice", "password": "s3cret"}

    dremio = settings.instance().dremio
    assert dremio.pat == "session-token-123"
    assert dremio.basic_auth.expiry is not None
    assert not dremio.basic_auth.has_expired


def test_expired_session_is_detected(software_settings_with_basic_auth):
    dremio = settings.instance().dremio
    dremio.basic_auth.expiry = datetime.now() - timedelta(minutes=1)
    assert dremio.basic_auth.has_expired
