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

import structlog
import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from logging import basicConfig


def get_log_directory(app_name: str = "dremioai") -> Path:
    """Get the appropriate log directory for the current platform."""
    base_dir = None
    match sys.platform:
        case "win32":
            base_dir = Path(
                os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")
            )
            base_dir = base_dir / app_name / "logs"
        case "darwin":
            base_dir = Path.home() / "Library" / "Logs" / app_name
        case _:
            base_dir = (
                Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
                / app_name
                / "logs"
            )

    if not base_dir.exists():
        base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def get_log_file() -> Path:
    return get_log_directory() / "dremioai.log"


def logger(name=None):
    if not structlog.is_configured():
        configure()
    return structlog.get_logger(name)


_level = None


def configure_file_logging(enable_json=False):
    """Convenience function to configure structlog with file logging enabled."""
    configure(enable_json_logging=enable_json, to_file=True)


def level():
    global _level
    if _level is not None:
        return _level
    return getattr(logging, os.environ.get("LOG_LEVEL", "INFO"), logging.INFO)


def set_level(l):
    global _level
    _level = l
    # propagate to all loggers
    logging.getLogger().setLevel(l)
    for name in logging.getLogger().manager.loggerDict:
        logging.getLogger(name).setLevel(l)


def configure(enable_json_logging=None, to_file=False):
    if enable_json_logging is None:
        enable_json_logging = "JSON_LOGGING" in os.environ

    # Set up file logging if requested
    if to_file:
        log_file_path = get_log_file()

        # Configure rotating file handler
        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
        )
        file_handler.setLevel(level())
        logging.getLogger().addHandler(file_handler)

    renderer = (
        structlog.processors.JSONRenderer()
        if enable_json_logging
        else structlog.dev.ConsoleRenderer()
    )
    processors = [
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(),
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
        structlog.processors.EventRenamer("message"),
        renderer,
    ]
    structlog.configure(
        processors=processors,
        context_class=dict,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=(structlog.stdlib.LoggerFactory()),
    )

    set_level(level())
