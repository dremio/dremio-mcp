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
from typing import Optional, Any, Self, ClassVar
from dremioai import log
import ldclient
from ldclient.config import Config


class FeatureFlagManager:
    """Manages LaunchDarkly feature flags for MCP server."""

    _log = log.logger("feature_flags")
    _instance: ClassVar[Self] = None

    def __init__(self, sdk_key: str):
        try:
            ldclient.set_config(Config(sdk_key))
            self._client = ldclient.get()

            if self._client.is_initialized():
                self._log.info("LaunchDarkly client initialized successfully")
            else:
                self._log.warning("LaunchDarkly client initialization pending")
        except:
            self._log.exception(f"Failed to initialize LaunchDarkly client")
            raise

    @classmethod
    def instance(cls) -> Self:
        """Lazily initializes from settings.instance().dremio.launchdarkly.sdk_key."""
        if cls._instance is None:
            from dremioai.config import settings

            sdk_key = settings.instance().dremio.launchdarkly.sdk_key
            cls._instance = cls(sdk_key)
        return cls._instance

    @classmethod
    def reset(cls):
        if cls._instance and cls._instance._client:
            cls._instance._client.close()
        cls._instance = None

    def is_enabled(self) -> bool:
        return self._client is not None and self._client.is_initialized()

    def get_flag(self, flag_key: str, default: Any) -> Any:
        if not self.is_enabled():
            self._log.debug(
                f"Flag '{flag_key}' not evaluated, LaunchDarkly not enabled/initialized (default: {default})"
            )
            return default
        value = self._client.variation(
            flag_key, ldclient.Context.create("mcp-server"), default
        )
        self._log.debug(f"Flag '{flag_key}' evaluated to: {value} (default: {default})")
        return value
