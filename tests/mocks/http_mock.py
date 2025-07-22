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

import json
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, Union
from unittest.mock import AsyncMock, MagicMock
from aiohttp import ClientResponse, ClientSession


class MockResponse:
    """Mock ClientResponse that returns data from files"""

    def __init__(self, data: str, status: int = 200, headers: Optional[Dict] = None):
        self.data = data
        self.status = status
        self.headers = headers or {}
        self.request_info = MagicMock()
        self.request_info.method = "GET"
        self.request_info.url = "http://mock.url"

    async def text(self) -> str:
        """Return the mock data as text"""
        return self.data

    async def json(self) -> Dict[str, Any]:
        """Return the mock data as JSON"""
        return json.loads(self.data)

    def raise_for_status(self):
        """Mock raise_for_status - only raises if status >= 400"""
        if self.status >= 400:
            raise Exception(f"HTTP {self.status}")

    @property
    def content(self):
        """Mock content property for streaming reads"""
        mock_content = MagicMock()

        async def read(chunk_size=1024):
            # Return data in chunks for download simulation
            if hasattr(self, "_read_position"):
                if self._read_position >= len(self.data):
                    return b""
                chunk = self.data[
                    self._read_position : self._read_position + chunk_size
                ].encode()
                self._read_position += chunk_size
                return chunk
            else:
                self._read_position = 0
                chunk = self.data[0:chunk_size].encode()
                self._read_position = chunk_size
                return chunk

        mock_content.read = read
        return mock_content

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        pass


class HttpMockFramework:
    """Simple HTTP mock framework for testing transport.py"""

    def __init__(self, resources_dir: str = "tests/resources"):
        self.resources_dir = Path(resources_dir)
        self.mock_responses: Dict[str, str] = {}
        self.original_session = None

    def load_mock_data(self, endpoint: str, filename: str) -> "HttpMockFramework":
        """
        Load mock data from a file for a specific endpoint

        Args:
            endpoint: The API endpoint to mock (e.g., "/api/v3/catalog")
            filename: The filename in tests/resources (e.g., "catalog/spaces.json")
        """
        file_path = self.resources_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Mock data file not found: {file_path}")

        with open(file_path, "r") as f:
            self.mock_responses[endpoint] = f.read()

        return self

    def add_mock_response(
        self, endpoint: str, response_data: Union[str, Dict]
    ) -> "HttpMockFramework":
        """
        Add a mock response directly without loading from file

        Args:
            endpoint: The API endpoint to mock
            response_data: The response data (string or dict that will be JSON serialized)
        """
        if isinstance(response_data, dict):
            response_data = json.dumps(response_data)
        self.mock_responses[endpoint] = response_data
        return self

    def _get_mock_response(self, url: str, method: str = "GET") -> MockResponse:
        """Get mock response for a URL"""
        # Extract endpoint from full URL
        for endpoint, data in self.mock_responses.items():
            if endpoint in url:
                return MockResponse(data)

        # Default response if no mock found
        return MockResponse('{"error": "No mock data found"}', status=404)

    def _mock_get(self, url: str, **kwargs) -> MockResponse:
        """Mock ClientSession.get method"""
        return self._get_mock_response(url, "GET")

    def _mock_post(self, url: str, **kwargs) -> MockResponse:
        """Mock ClientSession.post method"""
        return self._get_mock_response(url, "POST")

    def __enter__(self):
        """Context manager entry - start mocking"""
        # Store original methods
        self.original_get = ClientSession.get
        self.original_post = ClientSession.post

        # Replace with mocks
        ClientSession.get = self._mock_get
        ClientSession.post = self._mock_post

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - restore original methods"""
        # Restore original methods
        ClientSession.get = self.original_get
        ClientSession.post = self.original_post


# Convenience function for quick setup
def mock_http_client(mock_data: Dict[str, str]) -> HttpMockFramework:
    """
    Create and configure an HTTP mock framework

    Args:
        mock_data: Dictionary mapping endpoints to filenames in tests/resources

    Example:
        with mock_http_client({
            "/api/v3/catalog": "catalog/spaces.json",
            "/api/v3/sql": "sql/job_status.json"
        }) as mock:
            # Your test code here
            pass
    """
    framework = HttpMockFramework()
    for endpoint, filename in mock_data.items():
        framework.load_mock_data(endpoint, filename)
    return framework
