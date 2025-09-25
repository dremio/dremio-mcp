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
import uuid
from typing import AsyncGenerator, NamedTuple

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from collections import OrderedDict

from dremioai.config import settings
from dremioai.config.tools import ToolType
from dremioai.servers.mcp import (
    Transports,
    init,
    create_metrics_server,
)

from mocks.http_mock import (
    create_pytest_logging_server_fixture,
    start_server_with_app,
    ServerFixture,
    LoggingServerFixture,
)
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
import contextlib
from dremioai.log import set_level
from dremioai.metrics import registry
from prometheus_client import CollectorRegistry


@pytest.fixture(autouse=True)
def reset_sse_starlette_app_status():
    """
    Reset the global AppStatus.should_exit_event from sse_starlette between tests.

    This fixes the asyncio event loop binding issue where the global event gets
    bound to the first event loop and causes "bound to a different event loop"
    errors in subsequent tests.
    """
    try:
        # Import and reset the global state
        from sse_starlette.sse import AppStatus

        AppStatus.should_exit_event = None
    except ImportError:
        # sse_starlette might not be available in all test environments
        pass

    yield

    # Clean up after test
    try:
        from sse_starlette.sse import AppStatus

        AppStatus.should_exit_event = None
    except ImportError:
        pass


@pytest.fixture(autouse=True)
def reset_metrics_registry():
    """
    Reset the global metrics registry between tests.

    This ensures that metrics from previous tests don't persist and affect subsequent
    test assertions.
    """
    registry._registry = CollectorRegistry()

    yield


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
                        "project_id": uuid.uuid4(),
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


@pytest.fixture
def logging_level(request: pytest.FixtureRequest):
    if request.config.get_verbosity() > 2:
        return "debug"
    if request.config.get_verbosity() > 1:
        return "info"
    return "warning"


@pytest.fixture
def logging_server(logging_level):
    server = _create_logging_server(logging_level)
    try:
        yield server
    finally:
        server.close()


class StreamableMcpServerFixture(NamedTuple):
    mcp_server: ServerFixture
    logging_server: LoggingServerFixture
    metrics_port: int


@contextlib.asynccontextmanager
async def http_streamable_mcp_server(
    logging_server: LoggingServerFixture,
    logging_level: str,
    project_id: str = None,
    wlm_engine: str = None,
) -> AsyncGenerator[StreamableMcpServerFixture]:
    old = settings.instance()
    sf = None
    try:
        settings.configure(force=True)
        host = "127.0.0.1"
        port = random.randrange(9000, 12000)
        metrics_port = random.randrange(9000, 12000)

        # Ensure metrics port is different from main port
        while metrics_port == port:
            metrics_port = random.randrange(9000, 12000)

        config = {
            "dremio": {
                "uri": logging_server.url,
                "project_id": uuid.uuid4(),
                "pat": "test-pat",
                "enable_search": True,
                "metrics_enabled": True,
                "metrics": {
                    "enabled": True,
                    "port": metrics_port,
                },
            },
            "tools": {"server_mode": ToolType.FOR_DATA_PATTERNS.name},
        }
        if wlm_engine:
            config["dremio"]["wlm"] = {"engine_name": wlm_engine}
        settings._settings.set(settings.Settings.model_validate(config))
        settings.write_settings()

        set_level(logging_level.upper())

        # Start metrics server using asyncio
        metrics_server = create_metrics_server(
            host=host, port=metrics_port, log_level=logging_level
        )

        mcp_server = init(
            transport=Transports.streamable_http,
            port=port,
            mode=settings.instance().tools.server_mode,
            support_project_id_endpoints=project_id is not None,
        )

        app = mcp_server.streamable_http_app()
        server, stop_event = start_server_with_app(
            app,
            host=host,
            port=port,
            log_level=logging_level,
            additional_runners=[metrics_server.serve()],
        )
        sf = ServerFixture(
            f"http://{host}:{port}/mcp/{(str(project_id) + '/') if project_id else ''}",
            stop_event,
            server,
        )

        yield StreamableMcpServerFixture(sf, logging_server, metrics_port)
    finally:
        if sf is not None:
            sf.close()
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
