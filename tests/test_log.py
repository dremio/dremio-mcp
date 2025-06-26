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
import sys
import json
import pytest
import logging
import structlog
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock

from dremioai import log


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files"""
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_home_dir(temp_log_dir):
    """Mock the home directory to use our temporary directory"""
    with patch.object(Path, "home", return_value=temp_log_dir):
        yield temp_log_dir


@pytest.fixture(autouse=True)
def reset_structlog():
    """Reset structlog configuration before each test"""
    # Clear any existing configuration
    structlog.reset_defaults()
    # Reset the global level
    log._level = None
    # Clear all handlers from root logger
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    yield
    # Clean up after test
    structlog.reset_defaults()
    # Clear handlers again
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)


class TestGetLogDirectory:
    """Test the get_log_directory function for different platforms"""

    def test_linux_log_directory(self, mock_home_dir):
        """Test log directory on Linux"""
        with patch("sys.platform", "linux"):
            log_dir = log.get_log_directory()
            expected = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            assert log_dir == expected

    def test_linux_with_xdg_data_home(self, mock_home_dir):
        """Test log directory on Linux with XDG_DATA_HOME set"""
        xdg_dir = mock_home_dir / "custom_xdg"
        with (
            patch("sys.platform", "linux"),
            patch.dict(os.environ, {"XDG_DATA_HOME": str(xdg_dir)}),
        ):
            log_dir = log.get_log_directory()
            expected = xdg_dir / "dremioai" / "logs"
            assert log_dir == expected

    def test_macos_log_directory(self, mock_home_dir):
        """Test log directory on macOS"""
        with patch("sys.platform", "darwin"):
            log_dir = log.get_log_directory()
            expected = mock_home_dir / "Library" / "Logs" / "dremioai"
            assert log_dir == expected

    def test_windows_log_directory(self, mock_home_dir):
        """Test log directory on Windows"""
        localappdata = mock_home_dir / "AppData" / "Local"
        with (
            patch("sys.platform", "win32"),
            patch.dict(os.environ, {"LOCALAPPDATA": str(localappdata)}),
        ):
            log_dir = log.get_log_directory()
            expected = localappdata / "dremioai" / "logs"
            assert log_dir == expected

    def test_windows_without_localappdata(self, mock_home_dir):
        """Test log directory on Windows without LOCALAPPDATA env var"""
        with patch("sys.platform", "win32"), patch.dict(os.environ, {}, clear=True):
            log_dir = log.get_log_directory()
            expected = mock_home_dir / "AppData" / "Local" / "dremioai" / "logs"
            assert log_dir == expected

    def test_custom_app_name(self, mock_home_dir):
        """Test log directory with custom app name"""
        with patch("sys.platform", "linux"):
            log_dir = log.get_log_directory("custom_app")
            expected = mock_home_dir / ".local" / "share" / "custom_app" / "logs"
            assert log_dir == expected


class TestLoggerConfiguration:
    """Test logger configuration and functionality"""

    def test_logger_creation(self):
        """Test basic logger creation"""
        logger = log.logger("test_logger")
        assert logger is not None
        # After configuration, the logger should be properly bound
        # The actual type might be a proxy until first use

    def test_logger_without_name(self):
        """Test logger creation without name"""
        logger = log.logger()
        assert logger is not None

    def test_level_functions(self):
        """Test level getting and setting"""
        # Test default level
        default_level = log.level()
        assert default_level == logging.INFO

        # Test setting level
        log.set_level(logging.DEBUG)
        assert log.level() == logging.DEBUG

        # Test environment variable override
        with patch.dict(os.environ, {"LOG_LEVEL": "WARNING"}):
            log._level = None  # Reset cached level
            assert log.level() == logging.WARNING

    def test_configure_console_only(self):
        """Test basic console configuration"""
        log.configure()
        logger = log.logger("test")

        # Should not raise any exceptions
        logger.info("Test message")
        assert structlog.is_configured()

    def test_configure_with_json_logging(self):
        """Test configuration with JSON logging enabled"""
        log.configure(enable_json_logging=True)
        logger = log.logger("test")

        # Should not raise any exceptions
        logger.info("Test JSON message", key="value")
        assert structlog.is_configured()

    def test_configure_with_env_json_logging(self):
        """Test JSON logging enabled via environment variable"""
        with patch.dict(os.environ, {"JSON_LOGGING": "1"}):
            log.configure()
            assert structlog.is_configured()


class TestFileLogging:
    """Test file logging functionality"""

    def test_configure_file_logging_default_path(self, mock_home_dir):
        """Test file logging with default path"""
        with patch("sys.platform", "linux"):
            log.configure(to_file=True)

            # Check that log directory was created
            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            assert expected_dir.exists()

            # Check that log file exists after logging
            logger = log.logger("test")
            logger.info("Test file logging")

            log_file = expected_dir / "dremioai.log"
            assert log_file.exists()

            # Verify log content
            content = log_file.read_text()
            assert "Test file logging" in content

    def test_configure_file_logging_custom_path(self, mock_home_dir):
        """Test file logging with default path (current implementation doesn't support custom paths)"""
        with patch("sys.platform", "linux"):
            log.configure(to_file=True)

            logger = log.logger("test")
            logger.info("Default path test")

            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            log_file = expected_dir / "dremioai.log"
            assert log_file.exists()
            content = log_file.read_text()
            assert "Default path test" in content

    def test_configure_file_logging_convenience_function(self, mock_home_dir):
        """Test the convenience function for file logging"""
        with patch("sys.platform", "linux"):
            log.configure_file_logging()

            logger = log.logger("test")
            logger.info("Convenience function test")

            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            log_file = expected_dir / "dremioai.log"

            assert log_file.exists()
            content = log_file.read_text()
            assert "Convenience function test" in content

    def test_configure_file_logging_with_json(self, mock_home_dir):
        """Test file logging with JSON format"""
        with patch("sys.platform", "linux"):
            log.configure_file_logging(enable_json=True)

            logger = log.logger("test")
            logger.info("JSON test", user_id=123, action="test")

            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            log_file = expected_dir / "dremioai.log"

            assert log_file.exists()
            content = log_file.read_text()

            # Should contain structured data
            assert "user_id" in content
            assert "123" in content
            assert "action" in content

    def test_rotating_file_handler_configuration(self, mock_home_dir):
        """Test that rotating file handler is properly configured"""
        with (
            patch("sys.platform", "linux"),
            patch("dremioai.log.RotatingFileHandler") as mock_handler,
        ):
            mock_instance = MagicMock()
            mock_instance.level = logging.INFO  # Set the level attribute for comparison
            mock_handler.return_value = mock_instance

            log.configure(to_file=True)

            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            expected_log_file = expected_dir / "dremioai.log"

            # Verify RotatingFileHandler was called with correct parameters
            mock_handler.assert_called_once_with(
                expected_log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
            )
            mock_instance.setLevel.assert_called_once()

    def test_log_directory_creation(self, mock_home_dir):
        """Test that log directory is created if it doesn't exist"""
        with patch("sys.platform", "linux"):
            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"

            # Ensure directory doesn't exist initially
            assert not expected_dir.exists()

            log.configure(to_file=True)

            # Directory should be created
            assert expected_dir.exists()
            assert expected_dir.is_dir()


class TestIntegration:
    """Integration tests for logging functionality"""

    def test_multiple_loggers_same_file(self, mock_home_dir):
        """Test multiple loggers writing to the same file"""
        with patch("sys.platform", "linux"):
            # Reset structlog to avoid interference from previous tests
            structlog.reset_defaults()
            log.configure(to_file=True)

            logger1 = log.logger("module1")
            logger2 = log.logger("module2")

            logger1.info("Message from module1")
            logger2.warning("Warning from module2")

            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            log_file = expected_dir / "dremioai.log"

            content = log_file.read_text()
            assert "Message from module1" in content
            assert "Warning from module2" in content
            assert "module1" in content
            assert "module2" in content

    def test_log_level_filtering(self, mock_home_dir):
        """Test that log level filtering works correctly"""
        with patch("sys.platform", "linux"):
            # Reset structlog to avoid interference from previous tests
            structlog.reset_defaults()
            log.set_level(logging.WARNING)
            log.configure(to_file=True)

            logger = log.logger("test")
            logger.debug("Debug message")  # Should not appear
            logger.info("Info message")  # Should not appear
            logger.warning("Warning message")  # Should appear
            logger.error("Error message")  # Should appear

            expected_dir = mock_home_dir / ".local" / "share" / "dremioai" / "logs"
            log_file = expected_dir / "dremioai.log"

            content = log_file.read_text()
            assert "Debug message" not in content
            assert "Info message" not in content
            assert "Warning message" in content
            assert "Error message" in content
