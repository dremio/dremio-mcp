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
Unit tests for retry middleware and RetryConfig in transport.py
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from http import HTTPStatus
from aiohttp import ClientResponse

from dremioai.api.transport import RetryConfig, retry_middleware
from dremioai.config import settings


class TestRetryConfig:
    """Test the RetryConfig class"""

    def test_retry_config_with_default_settings(self):
        """Test RetryConfig initialization with default settings"""
        with patch.object(settings, "instance", return_value=None):
            retry_config = RetryConfig()

            # Verify default values from HttpRetry model
            assert retry_config.config.max_retries == 3
            assert retry_config.config.initial_delay == 1.0
            assert retry_config.config.max_delay == 60.0
            assert retry_config.config.backoff_multiplier == 2.0

    def test_retry_config_with_custom_settings(self, mock_settings_instance):
        """Test RetryConfig initialization with custom settings"""
        retry_config = RetryConfig()

        # Verify it uses settings from mock
        assert retry_config.config.max_retries == 5
        assert retry_config.config.initial_delay == 2.0
        assert retry_config.config.max_delay == 120.0
        assert retry_config.config.backoff_multiplier == 3.0

    def test_get_config_delay_exponential_backoff(self, mock_settings_instance):
        """Test exponential backoff calculation"""
        retry_config = RetryConfig()

        # Test exponential backoff: initial_delay * (backoff_multiplier ** attempt_number)
        # With initial_delay=2.0 and backoff_multiplier=3.0:
        assert retry_config.get_config_delay(0) == 2.0  # 2.0 * 3^0 = 2.0
        assert retry_config.get_config_delay(1) == 6.0  # 2.0 * 3^1 = 6.0
        assert retry_config.get_config_delay(2) == 18.0  # 2.0 * 3^2 = 18.0
        assert retry_config.get_config_delay(3) == 54.0  # 2.0 * 3^3 = 54.0

    def test_get_delay_without_retry_after_header(self, mock_settings_instance):
        """Test get_delay when response has no Retry-After header"""
        retry_config = RetryConfig()

        # Mock response without Retry-After header
        mock_response = MagicMock(spec=ClientResponse)
        mock_response.headers.get.return_value = None

        # Should return config delay capped at max_delay
        delay = retry_config.get_delay(mock_response, attempt_number=1)
        assert delay == 6.0  # 2.0 * 3^1 = 6.0

    def test_get_delay_with_retry_after_header(self, mock_settings_instance):
        """Test get_delay when response has Retry-After header"""
        retry_config = RetryConfig()

        # Mock response with Retry-After header
        mock_response = MagicMock(spec=ClientResponse)
        mock_response.headers.get.return_value = "3"

        # Should return minimum of config delay and Retry-After value
        delay = retry_config.get_delay(mock_response, attempt_number=1)
        assert delay == 3.0  # min(6.0, 3) = 3.0

    def test_get_delay_with_larger_retry_after_header(self, mock_settings_instance):
        """Test get_delay when Retry-After is larger than config delay"""
        retry_config = RetryConfig()

        # Mock response with large Retry-After header
        mock_response = MagicMock(spec=ClientResponse)
        mock_response.headers.get.return_value = "100"

        # Should return config delay (smaller value)
        delay = retry_config.get_delay(mock_response, attempt_number=1)
        assert delay == 6.0  # min(6.0, 100) = 6.0

    def test_get_delay_respects_max_delay(self, mock_settings_instance):
        """Test that get_delay respects max_delay setting"""
        retry_config = RetryConfig()

        # Mock response without Retry-After header
        mock_response = MagicMock(spec=ClientResponse)
        mock_response.headers.get.return_value = None

        # With attempt_number=4, config delay would be 2.0 * 3^4 = 162.0
        # But max_delay is 120.0, so it should be capped
        delay = retry_config.get_delay(mock_response, attempt_number=4)
        assert delay == 120.0  # Capped at max_delay

    def test_get_delay_with_invalid_retry_after_header(self, mock_settings_instance):
        """Test get_delay with invalid Retry-After header value"""
        retry_config = RetryConfig()

        # Mock response with invalid Retry-After header
        mock_response = MagicMock(spec=ClientResponse)
        mock_response.headers.get.return_value = "invalid"

        # Should fall back to config delay
        delay = retry_config.get_delay(mock_response, attempt_number=1)
        assert delay == 6.0  # Falls back to config delay


class TestRetryMiddleware:
    """Test the retry_middleware function"""

    @pytest.mark.asyncio
    async def test_no_retry_on_success(self, mock_settings_instance):
        """Test that middleware doesn't retry on successful response"""
        # Mock successful response
        mock_response = MagicMock(spec=ClientResponse)
        mock_response.status = HTTPStatus.OK

        # Mock handler
        mock_handler = AsyncMock(return_value=mock_response)

        # Mock request
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.url.path = "/test"

        # Call middleware
        result = await retry_middleware(mock_request, mock_handler)

        # Verify handler was called only once
        assert mock_handler.call_count == 1
        assert result == mock_response

    @pytest.mark.asyncio
    async def test_retry_on_429_then_success(self, mock_settings_instance):
        """Test that middleware retries on 429 and succeeds on retry"""
        # Mock responses: first 429, then success
        mock_response_429 = MagicMock(spec=ClientResponse)
        mock_response_429.status = HTTPStatus.TOO_MANY_REQUESTS
        mock_response_429.headers.get.return_value = None

        mock_response_ok = MagicMock(spec=ClientResponse)
        mock_response_ok.status = HTTPStatus.OK

        # Mock handler to return 429 first, then OK
        mock_handler = AsyncMock(side_effect=[mock_response_429, mock_response_ok])

        # Mock request
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.url.path = "/test"

        # Mock asyncio.sleep to avoid actual delays
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await retry_middleware(mock_request, mock_handler)

        # Verify handler was called twice
        assert mock_handler.call_count == 2
        # Verify sleep was called once with expected delay
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args[0][0] == 2.0  # initial_delay
        assert result == mock_response_ok

    @pytest.mark.asyncio
    async def test_retry_exhaustion(self, mock_settings_instance):
        """Test that middleware stops retrying after max_retries"""
        # Mock response that always returns 429
        mock_response_429 = MagicMock(spec=ClientResponse)
        mock_response_429.status = HTTPStatus.TOO_MANY_REQUESTS
        mock_response_429.headers.get.return_value = None

        # Mock handler to always return 429
        mock_handler = AsyncMock(return_value=mock_response_429)

        # Mock request
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.url.path = "/test"

        # Mock asyncio.sleep to avoid actual delays
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await retry_middleware(mock_request, mock_handler)

        # Verify handler was called max_retries + 1 times (initial + retries)
        # max_retries = 5, so total calls = 6 (attempts 0-5)
        assert mock_handler.call_count == 6
        # Verify sleep was called max_retries + 1 times (once per failed attempt)
        # Note: The current implementation sleeps even on the last attempt
        assert mock_sleep.call_count == 6
        # Final result should still be 429
        assert result.status == HTTPStatus.TOO_MANY_REQUESTS

    @pytest.mark.asyncio
    async def test_retry_with_exponential_backoff(self, mock_settings_instance):
        """Test that retry delays follow exponential backoff"""
        # Mock response that always returns 429
        mock_response_429 = MagicMock(spec=ClientResponse)
        mock_response_429.status = HTTPStatus.TOO_MANY_REQUESTS
        mock_response_429.headers.get.return_value = None

        # Mock handler to always return 429
        mock_handler = AsyncMock(return_value=mock_response_429)

        # Mock request
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.url.path = "/test"

        # Mock asyncio.sleep to capture delays
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await retry_middleware(mock_request, mock_handler)

        # Verify exponential backoff delays
        # With initial_delay=2.0, backoff_multiplier=3.0, max_delay=120.0
        # Attempts 0-5: 2.0, 6.0, 18.0, 54.0, 162.0 (capped to 120.0), 486.0 (capped to 120.0)
        expected_delays = [2.0, 6.0, 18.0, 54.0, 120.0, 120.0]
        actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
        assert actual_delays == expected_delays

    @pytest.mark.asyncio
    async def test_retry_with_retry_after_header(self, mock_settings_instance):
        """Test that middleware respects Retry-After header"""
        # Mock response with Retry-After header
        mock_response_429 = MagicMock(spec=ClientResponse)
        mock_response_429.status = HTTPStatus.TOO_MANY_REQUESTS
        mock_response_429.headers.get.return_value = "5"

        mock_response_ok = MagicMock(spec=ClientResponse)
        mock_response_ok.status = HTTPStatus.OK

        # Mock handler to return 429 first, then OK
        mock_handler = AsyncMock(side_effect=[mock_response_429, mock_response_ok])

        # Mock request
        mock_request = MagicMock()
        mock_request.method = "POST"
        mock_request.url.path = "/api/test"

        # Mock asyncio.sleep
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await retry_middleware(mock_request, mock_handler)

        # Verify sleep was called with min(config_delay, retry_after)
        # config_delay for attempt 0 = 2.0, retry_after = 5, so min = 2.0
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args[0][0] == 2.0
        assert result == mock_response_ok

    @pytest.mark.asyncio
    async def test_no_retry_on_other_errors(self, mock_settings_instance):
        """Test that middleware doesn't retry on non-429 errors"""
        # Mock response with different error status
        mock_response_500 = MagicMock(spec=ClientResponse)
        mock_response_500.status = HTTPStatus.INTERNAL_SERVER_ERROR

        # Mock handler
        mock_handler = AsyncMock(return_value=mock_response_500)

        # Mock request
        mock_request = MagicMock()
        mock_request.method = "GET"
        mock_request.url.path = "/test"

        # Call middleware
        result = await retry_middleware(mock_request, mock_handler)

        # Verify handler was called only once (no retries)
        assert mock_handler.call_count == 1
        assert result.status == HTTPStatus.INTERNAL_SERVER_ERROR


# Fixtures


@pytest.fixture
def mock_settings_instance():
    """Create a mock settings instance with custom retry configuration"""
    # Create actual HttpRetry instance with custom values
    http_retry_config = settings.HttpRetry(
        max_retries=5, initial_delay=2.0, max_delay=120.0, backoff_multiplier=3.0
    )

    mock_dremio = MagicMock()
    mock_dremio.api = MagicMock()
    mock_dremio.api.http_retry = http_retry_config

    # Create mock settings object
    mock_settings = MagicMock()
    mock_settings.dremio = mock_dremio

    # Patch settings.instance to return our mock
    with patch.object(settings, "instance", return_value=mock_settings):
        yield mock_settings
