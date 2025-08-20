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

"""
Global pytest fixtures for dremio-mcp tests.
"""
import os
import random
from typing import AsyncGenerator, NamedTuple

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from collections import OrderedDict

from dremioai.config import settings
from dremioai.config.tools import ToolType
from dremioai.servers.mcp import Transports, init

from mocks.http_mock import (
    create_pytest_logging_server_fixture,
    start_server,
    ServerFixture,
    LoggingServerFixture,
)
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
import contextlib
from dremioai.log import set_level


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for config files"""
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_config_dir(temp_config_dir):
    """Mock the home directory to use our temporary directory"""
    with patch.object(Path, "home", return_value=temp_config_dir):
        # Also patch XDG_CONFIG_HOME environment variable
        old_env = os.environ.get("XDG_CONFIG_HOME")
        os.environ["XDG_CONFIG_HOME"] = str(temp_config_dir)
        yield temp_config_dir
        # Restore original environment
        if old_env:
            os.environ["XDG_CONFIG_HOME"] = old_env
        else:
            os.environ.pop("XDG_CONFIG_HOME", None)


@pytest.fixture
def mock_settings_instance():
    """Create a mock settings instance with default values"""
    old_settings = settings.instance()
    try:
        settings._settings.set(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": "https://test-dremio-uri.com",
                        "pat": "test-pat",
                        "project_id": "test-project-id",
                    },
                    "tools": {"server_mode": ToolType.FOR_SELF.name},
                }
            )
        )
        yield settings.instance()
    finally:
        settings._settings.set(old_settings)


@pytest.fixture
def temp_config_dir():
    """Create a temporary directory for config files"""
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_config_dir(temp_config_dir):
    """Mock the home directory to use our temporary directory"""
    with patch.object(Path, "home", return_value=temp_config_dir):
        # Also patch XDG_CONFIG_HOME environment variable
        old_env = os.environ.get("XDG_CONFIG_HOME")
        os.environ["XDG_CONFIG_HOME"] = str(temp_config_dir)
        yield temp_config_dir
        # Restore original environment
        if old_env:
            os.environ["XDG_CONFIG_HOME"] = old_env
        else:
            os.environ.pop("XDG_CONFIG_HOME", None)


def _create_logging_server(log_level="warning"):
    # Mock data for HTTP endpoints that tools will call
    mock_data = OrderedDict(
        [
            (r"/sql", "sql/job_submission.json"),  # SQL query submission
            (r"/job/test-job-12345$", "sql/job_status.json"),  # Job status check
            (r"/job/test-job-12345/results$", "sql/job_results.json"),  # Job results
            (r"/search", "search/search_results.json"),  # Search endpoints
            (r"/catalog/.*/wiki", "catalog/wiki.json"),  # Wiki endpoints
            (r"/catalog/.*/tags", "catalog/tags.json"),  # Tags endpoints
            (r"/catalog/.*/graph", "catalog/lineage.json"),  # Lineage endpoints
            (r"/catalog(/by-path)?", "catalog/table_schema.json"),  # Schema endpoints
        ]
    )

    return create_pytest_logging_server_fixture(
        mock_data=mock_data, port=8000, log_level=log_level
    )


@pytest.fixture(scope="module")
def logging_level(request):
    return "info"
    if request.config.get_verbosity() > 2:
        return "debug"
    if request.config.get_verbosity() > 1:
        return "info"
    return "warning"


@pytest.fixture(scope="module")
def logging_server(logging_level):
    server = _create_logging_server(logging_level)
    try:
        yield server
    finally:
        try:
            server.close()
        except:
            from rich import traceback

            traceback.print_exc()


class StreamableMcpServerFixture(NamedTuple):
    mcp_server: ServerFixture
    logging_server: LoggingServerFixture


@pytest.fixture
def http_streamable_mcp_server(logging_server, mock_config_dir, logging_level):
    old = settings.instance()
    sf = None
    try:
        settings.configure(force=True)
        settings._settings.set(
            settings.Settings.model_validate(
                {
                    "dremio": {
                        "uri": logging_server.url,
                        "project_id": "test-project-id",
                        "pat": "test-pat",
                        "enable_search": True,
                    },
                    "tools": {"server_mode": ToolType.FOR_DATA_PATTERNS.name},
                }
            )
        )
        settings.write_settings()
        port = random.randrange(9000, 12000)
        set_level(logging_level.upper())
        mcp_server = init(
            transport=Transports.streamable_http,
            port=port,
            mode=settings.instance().tools.server_mode,
        )

        def should_exit(v: bool):
            mcp_server.should_exit = v

        server, stop_event = start_server(
            mcp_server.run_streamable_http_async(), should_exit
        )
        sf = ServerFixture(f"http://127.0.0.1:{port}/mcp/", stop_event, server)
        yield StreamableMcpServerFixture(sf, logging_server)
    finally:
        if sf is not None:
            try:
                sf.close()
            except:
                from rich import traceback

                traceback.print_exc()
        print(f"{sf} closed")
        settings._settings.set(old)


@contextlib.asynccontextmanager
async def http_streamable_client_server(
    sf: ServerFixture, token=None
) -> AsyncGenerator[ClientSession]:
    headers = {"Authorization": f"Bearer {token}"} if token is not None else None
    async with streamablehttp_client(url=sf.url, headers=headers) as (
        read_stream,
        write_stream,
        gid,
    ):
        print(f"Client connected to {sf.url}")
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            yield session
