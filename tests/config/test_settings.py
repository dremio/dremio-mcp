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

import os
import uuid

import pydantic
import pytest
import yaml
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pydantic_core import ValidationError

from dremioai.config import settings
from dremioai.config.tools import ToolType


def test_configure_with_no_file_works(mock_config_dir):
    s = settings.instance()
    assert settings.instance() is not None
    settings.configure(force=True)
    assert settings.instance() is not None
    assert settings.instance() is not s


def test_configure_creates_default_config(mock_config_dir):
    """Test that configure creates the default config file if it doesn't exist"""
    default_path = mock_config_dir / "dremioai" / "config.yaml"
    assert default_path == settings.default_config()
    assert not default_path.exists()
    # Call configure with no arguments (should use default path)
    settings.configure()
    # Check that the default config file was created
    assert default_path.exists()
    assert settings.instance() is not None and settings.instance().dremio is None


def test_create_default_config(mock_config_dir):
    uri = settings.DremioCloudUri.PRODEMEA.value
    pat = "test-pat"
    project_id = uuid.uuid4()
    mode = ToolType.FOR_DATA_PATTERNS
    settings.configure(force=True)
    settings._settings.set(
        settings.instance().model_validate(
            {
                "dremio": {
                    "uri": uri,
                    "pat": pat,
                    "project_id": project_id,
                },
                "tools": {"server_mode": mode.name},
            }
        )
    )
    settings.write_settings()
    assert settings.default_config().exists()
    settings.configure(force=True)
    dremio = settings.instance().dremio
    assert (
        dremio.uri == "https://api.eu.dremio.cloud"
        and dremio.pat == pat
        and dremio.project_id == str(project_id)
    )
    tools = settings.instance().tools
    assert tools.server_mode == mode


@pytest.mark.parametrize(
    "name,value",
    [
        (name, value)
        for name in ("enable_search", "enable_experimental")
        for value in (True, False)
    ],
)
def test_experimental_rename(name: str, value: bool):
    d = settings.Dremio.model_validate(
        {name: value, "uri": "https://foo", "pat": "bar"}
    )
    assert d.enable_search == value


@pytest.mark.parametrize(
    "project_id,error",
    [
        pytest.param(str(uuid.uuid4()), False, id="valid project id"),
        pytest.param(None, False, id="no project id"),
        pytest.param("asdfsa safsa", True, id="invalid project id"),
        pytest.param(str(uuid.uuid4())[:-1] + "a", True, id="invalid project id"),
        pytest.param("DREMIO_DYNAMIC", False, id="dynamic project id"),
    ],
)
def test_projects(project_id: str | None, error: bool):
    val = {"uri": "https://foo", "project_id": project_id}
    if error:
        try:
            settings.Dremio.model_validate(val)
            assert False
        except:
            pass
    else:
        d = settings.Dremio.model_validate(val)
        assert d.project_id == project_id or d.project_id is None and project_id is None


def test_env_file(mock_config_dir):
    try:
        os.environ["DREMIOAI_DREMIO__URI"] = "https://foo"
        os.environ["DREMIOAI_DREMIO__PAT"] = "bar"
        os.environ["DREMIOAI_TOOLS__SERVER_MODE"] = "FOR_DATA_PATTERNS"
        settings.configure(force=True)
        assert settings.instance().dremio.uri == "https://foo"
        assert settings.instance().dremio.pat == "bar"
        assert settings.instance().tools.server_mode == ToolType.FOR_DATA_PATTERNS
    finally:
        os.environ.pop("DREMIOAI_DREMIO_URI", None)
        os.environ.pop("DREMIOAI_DREMIO_PAT", None)


@pytest.mark.parametrize(
    "uri,project_id,issuer,error,iss_override",
    [
        pytest.param(
            uri,
            project_id,
            iss,
            project_id is None,
            iss_override,
            id=f"{label} with {plabel}",
        )
        for uri, iss, label in (
            ("https://foo", "https://foo", "custom-uri"),
            ("https://api.dremio.cloud", "https://login.dremio.cloud", "prod"),
            (
                "https://api.eu.dremio.cloud",
                "https://login.eu.dremio.cloud",
                "prodemea",
            ),
            ("https://api.dev.dremio.site", "https://login.dev.dremio.site", "dev"),
        )
        for project_id, plabel in (
            (None, "no-project-id"),
            ("DREMIO_DYNAMIC", "dynamic-project-id"),
            (str(uuid.uuid4()), "project-id"),
        )
        for iss_override in (None, "https://my-override")
    ],
)
def test_auth_urls(
    uri: str, project_id: str | None, issuer: str, error: bool, iss_override: str | None
):
    d = settings.Dremio.model_validate(
        {
            "uri": uri,
            "project_id": project_id,
            "auth_issuer_uri_override": (
                iss_override if iss_override and not error else None
            ),
        }
    )
    if iss_override:
        issuer = iss_override
    auth = (f"{issuer}/oauth/authorize", f"{issuer}/oauth/token") if not error else None
    issuer = issuer if not error else None
    assert d.auth_issuer_uri == issuer
    assert d.auth_endpoints == auth


def test_launchdarkly_sdk_key_from_env_with_yaml_config(monkeypatch):
    """Test that LaunchDarkly SDK key can be set via env var while other settings are in YAML."""
    # Set SDK key via environment variable
    monkeypatch.setenv("DREMIOAI_DREMIO__LAUNCHDARKLY__SDK_KEY", "sdk-env-key-12345")

    # Create settings with LaunchDarkly config in YAML (but no SDK key)
    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat",
            "launchdarkly": {}
        }
    })

    # Verify that SDK key from env var is picked up
    assert s.dremio.launchdarkly is not None
    assert s.dremio.launchdarkly.sdk_key == "sdk-env-key-12345"
    # enabled property should be True when sdk_key is set
    assert s.dremio.launchdarkly.enabled is True


def test_launchdarkly_all_from_env(monkeypatch):
    """Test that LaunchDarkly SDK key can be set via environment variable."""
    monkeypatch.setenv("DREMIOAI_DREMIO__LAUNCHDARKLY__SDK_KEY", "sdk-env-key-67890")

    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat"
        }
    })

    assert s.dremio.launchdarkly is not None
    assert s.dremio.launchdarkly.sdk_key == "sdk-env-key-67890"
    # enabled property should be True when sdk_key is set
    assert s.dremio.launchdarkly.enabled is True


def test_launchdarkly_sdk_key_from_file(tmp_path):
    """Test that LaunchDarkly SDK key can be loaded from a file."""
    # Create a temporary file with SDK key
    sdk_key_file = tmp_path / "sdk_key.txt"
    sdk_key_file.write_text("sdk-file-key-abcdef")

    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat",
            "launchdarkly": {
                "sdk_key": f"@{sdk_key_file}"
            }
        }
    })

    assert s.dremio.launchdarkly is not None
    assert s.dremio.launchdarkly.sdk_key == "sdk-file-key-abcdef"
    # enabled property should be True when sdk_key is set
    assert s.dremio.launchdarkly.enabled is True


def test_launchdarkly_defaults():
    """Test that LaunchDarkly has correct default values."""
    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat",
            "launchdarkly": {}
        }
    })

    assert s.dremio.launchdarkly is not None
    assert s.dremio.launchdarkly.sdk_key is None
    # enabled property should be False when sdk_key is None
    assert s.dremio.launchdarkly.enabled is False


def test_dremio_get_flag_without_launchdarkly():
    """Test that get_flag returns default when LaunchDarkly is not configured."""
    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat"
        }
    })

    # Should return default value when LaunchDarkly is not configured
    result = s.dremio.get_flag("test.flag", "default_value")
    assert result == "default_value"


def test_dremio_get_flag_with_launchdarkly_disabled():
    """Test that get_flag returns default when LaunchDarkly is disabled."""
    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat",
            "launchdarkly": {}  # No SDK key, so disabled
        }
    })

    # Should return default value when LaunchDarkly is disabled
    result = s.dremio.get_flag("test.flag", "default_value")
    assert result == "default_value"


def test_dremio_enable_search_fallback():
    """Test that enable_search property falls back to config value when LD is disabled."""
    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat",
            "enable_search": True
        }
    })

    # Should return config value when LaunchDarkly is not configured
    assert s.dremio.enable_search is True


def test_dremio_allow_dml_fallback():
    """Test that allow_dml property falls back to config value when LD is disabled."""
    s = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat",
            "allow_dml": True
        }
    })

    # Should return config value when LaunchDarkly is not configured
    assert s.dremio.allow_dml is True
