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
import logging
from contextvars import ContextVar
from enum import StrEnum
from typing import Optional, Any, Self, ClassVar
from dremioai import log
import ldclient
from ldclient.config import Config


class LDContextKind(StrEnum):
    """LaunchDarkly context kinds for multi-context targeting."""
    APPLICATION = "application"
    PROJECT = "projectId"
    ORGANIZATION = "orgId"


class FeatureFlagManager:
    """Manages LaunchDarkly feature flags for MCP server."""

    _log = log.logger("feature_flags")
    _instance: ClassVar[Self] = None

    # Per-request context for LD targeting.  Set by middleware / token
    # verifier; read by _build_context().  Keeps feature_flags decoupled
    # from settings (no import needed).
    _project_id: ClassVar[ContextVar[str | None]] = ContextVar("ld_project_id", default=None)
    _org_id: ClassVar[ContextVar[str | None]] = ContextVar("ld_org_id", default=None)

    @classmethod
    def set_project_id(cls, value: str | None) -> None:
        cls._project_id.set(value)

    @classmethod
    def set_org_id(cls, value: str | None) -> None:
        cls._org_id.set(value)

    def __init__(self, sdk_key: str):
        if sdk_key is not None:
            self._log.info(
                f"Initializing LaunchDarkly client with SDK key: {len(sdk_key)} bytes"
            )
            ldclient.set_config(Config(sdk_key))
            self._client = ldclient.get()

            if self._client.is_initialized():
                self._log.info("LaunchDarkly client initialized successfully")
            else:
                self._log.warning("LaunchDarkly client initialization pending")
        else:
            self._client = None

    @classmethod
    def initialize(cls, sdk_key: str = None):
        """Initialize the singleton with the given SDK key.

        Called by Settings.model_post_init to avoid circular imports
        (feature_flags never imports settings).
        """
        cls.reset()
        cls._instance = cls(sdk_key)

    @classmethod
    def instance(cls) -> Self:
        if cls._instance is None:
            cls._instance = cls(None)
        return cls._instance

    @classmethod
    def reset(cls):
        if cls._instance and cls._instance._client:
            cls._instance._client.close()
        cls._instance = None

    def is_enabled(self) -> bool:
        return self._client is not None and self._client.is_initialized()

    def _build_context(self) -> ldclient.Context:
        """Build an LD evaluation context from the current request scope.

        Uses a multi-context when project_id or org_id are available
        (set per-request via ContextVar by middleware / token verifier).
        Falls back to the same single "mcp-server" context used before
        this change.
        """
        project_id = self._project_id.get()
        org_id = self._org_id.get()

        if not project_id and not org_id:
            return ldclient.Context.create("mcp-server")

        builder = ldclient.Context.multi_builder()
        builder.add(
            ldclient.Context.builder("mcp-server").kind(LDContextKind.APPLICATION).build()
        )
        if project_id:
            builder.add(
                ldclient.Context.builder(project_id).kind(LDContextKind.PROJECT).build()
            )
        if org_id:
            builder.add(
                ldclient.Context.builder(org_id).kind(LDContextKind.ORGANIZATION).build()
            )
        return builder.build()

    def get_flag(self, flag_key: str, default: Any) -> Any:
        if not self.is_enabled():
            state, level = "enabled", logging.DEBUG
            if self._client is not None:
                state, level = "initialized", logging.WARNING
            self._log.log(
                level,
                f"Flag '{flag_key}' not evaluated, LaunchDarkly not {state}",
            )
            return default
        context = self._build_context()
        value = self._client.variation(flag_key, context, default)
        self._log.debug(f"Flag '{flag_key}' evaluated to: {value} (default: {default})")
        return value
