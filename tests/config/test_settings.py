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
    "name,project_id,error",
    [
        ["valid project id", str(uuid.uuid4()), False],
        ["no project id", None, False],
        ["invalid project id", "asdfsa safsa", True],
        ["invalid project id", str(uuid.uuid4())[:-1] + "a", True],
        ["dynamic project id", "DREMIO_DYNAMIC", False],
    ],
)
def test_projects(name: str, project_id: str | None, error: bool):
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
