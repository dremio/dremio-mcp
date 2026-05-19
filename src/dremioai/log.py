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
_scoped_level = None
_scoped_logger_names = set()


def _rename_exception_field(_logger, _name, event_dict):
    """Use stacktrace for JSON logs to keep exception payloads machine-friendly."""
    if (exception := event_dict.pop("exception", None)) is not None:
        event_dict["stacktrace"] = exception
    return event_dict


def configure_file_logging(enable_json=False):
    """Convenience function to configure structlog with file logging enabled."""
    configure(enable_json_logging=enable_json, to_file=True)


def level():
    global _level
    if _level is not None:
        return _level
    return getattr(logging, os.environ.get("LOG_LEVEL", "INFO"), logging.INFO)


def scoped_level():
    return _scoped_level


def scoped_loggers():
    return sorted(_scoped_logger_names)


def _normalize_level(l):
    if isinstance(l, str):
        return getattr(logging, l.upper(), logging.INFO)
    return l


def _set_handler_level(l):
    for handler in logging.getLogger().handlers:
        handler.setLevel(l)


def set_level(l, logger_names=None):
    global _level, _scoped_level, _scoped_logger_names
    l = _normalize_level(l)

    if logger_names:
        global_level = level()
        logger_names = set(logger_names)

        for name in _scoped_logger_names - logger_names:
            logging.getLogger(name).setLevel(global_level)

        for name in logger_names:
            logging.getLogger(name).setLevel(l)

        _scoped_logger_names = logger_names
        _scoped_level = l
        logging.getLogger().setLevel(global_level)
        _set_handler_level(min(global_level, l))
        return

    _level = l
    _scoped_level = None
    _scoped_logger_names.clear()
    logging.getLogger().setLevel(l)
    for name in logging.getLogger().manager.loggerDict:
        logging.getLogger(name).setLevel(l)
    _set_handler_level(l)


def configure(enable_json_logging=None, to_file=False):
    if enable_json_logging is None:
        enable_json_logging = "JSON_LOGGING" in os.environ

    # Set up file logging if requested
    if to_file:
        log_file_path = get_log_file()

        # Configure rotating file handler
        handler = RotatingFileHandler(
            log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB
        )
    else:
        handler = logging.StreamHandler(sys.stderr)

    logging.getLogger().handlers.clear()
    logging.getLogger().addHandler(handler)

    renderer = (
        structlog.processors.JSONRenderer()
        if enable_json_logging
        else structlog.dev.ConsoleRenderer()
    )
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.ExtraAdder(),
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
    ]
    formatter_processors = [
        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
    ]
    if enable_json_logging:
        formatter_processors.append(_rename_exception_field)
    formatter_processors.extend(
        [
            structlog.processors.EventRenamer("message"),
            renderer,
        ]
    )
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=formatter_processors,
            foreign_pre_chain=shared_processors,
        )
    )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=(structlog.stdlib.LoggerFactory()),
    )

    set_level(level())
