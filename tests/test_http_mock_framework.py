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

import pytest
import asyncio
import json
from aiohttp import ClientSession
from tests.mocks.http_mock import HttpMockFramework, mock_http_client


class TestHttpMockFramework:
    """Test the HTTP mock framework functionality"""

    @pytest.mark.asyncio
    async def test_mock_framework_with_file_data(self):
        """Test mocking with data loaded from files"""
        framework = HttpMockFramework()
        framework.load_mock_data("/api/v3/catalog", "catalog/spaces.json")

        with framework:
            async with ClientSession() as session:
                async with session.get("http://test.com/api/v3/catalog") as response:
                    data = await response.json()
                    assert "data" in data
                    assert len(data["data"]) == 2
                    assert data["data"][0]["name"] == "Sample Space"

    @pytest.mark.asyncio
    async def test_mock_framework_with_direct_data(self):
        """Test mocking with directly added response data"""
        framework = HttpMockFramework()
        test_data = {"message": "Hello from mock", "status": "success"}
        framework.add_mock_response("/api/test", test_data)

        with framework:
            async with ClientSession() as session:
                async with session.post("http://test.com/api/test") as response:
                    data = await response.json()
                    assert data["message"] == "Hello from mock"
                    assert data["status"] == "success"

    @pytest.mark.asyncio
    async def test_convenience_function(self):
        """Test the convenience function for quick setup"""
        mock_data = {
            "/api/v3/catalog": "catalog/spaces.json",
            "/api/v3/sql": "sql/job_status.json",
        }

        with mock_http_client(mock_data):
            async with ClientSession() as session:
                # Test catalog endpoint
                async with session.get("http://test.com/api/v3/catalog") as response:
                    catalog_data = await response.json()
                    assert "data" in catalog_data

                # Test SQL endpoint
                async with session.post("http://test.com/api/v3/sql") as response:
                    sql_data = await response.json()
                    assert sql_data["jobState"] == "COMPLETED"
                    assert sql_data["rowCount"] == 42

    @pytest.mark.asyncio
    async def test_mock_framework_not_found(self):
        """Test behavior when no mock data is found for an endpoint"""
        framework = HttpMockFramework()

        with framework:
            async with ClientSession() as session:
                async with session.get("http://test.com/api/unknown") as response:
                    assert response.status == 404
                    data = await response.json()
                    assert "error" in data

    @pytest.mark.asyncio
    async def test_mock_response_text_and_json(self):
        """Test that mock responses work with both text() and json() methods"""
        framework = HttpMockFramework()
        framework.load_mock_data("/api/test", "catalog/spaces.json")

        with framework:
            async with ClientSession() as session:
                async with session.get("http://test.com/api/test") as response:
                    # Test text() method
                    text_data = await response.text()
                    assert "Sample Space" in text_data

                    # Test json() method (note: this creates a new response)
                async with session.get("http://test.com/api/test") as response:
                    json_data = await response.json()
                    assert json_data["data"][0]["name"] == "Sample Space"
