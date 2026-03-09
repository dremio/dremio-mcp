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
from typing import Optional, Any, Self
from dremioai import log
import ldclient
from ldclient.config import Config


class FeatureFlagManager:
    """Manages LaunchDarkly feature flags for MCP server."""

    _instance: Optional[Self] = None
    _client: Optional[ldclient.LDClient] = None

    def __init__(self, sdk_key: str):
        if not sdk_key:
            log.logger("feature_flags").warning(
                "LaunchDarkly SDK key is empty, feature flags will be disabled"
            )
            return

        try:
            ldclient.set_config(Config(sdk_key))
            self._client = ldclient.get()

            if self._client.is_initialized():
                log.logger("feature_flags").info(
                    "LaunchDarkly client initialized successfully"
                )
            else:
                log.logger("feature_flags").warning(
                    "LaunchDarkly client initialization pending"
                )
        except Exception as e:
            log.logger("feature_flags").error(
                f"Failed to initialize LaunchDarkly client: {e}"
            )

    @classmethod
    def instance(cls) -> Self:
        """Lazily initializes from settings.instance().dremio.launchdarkly.sdk_key."""
        if cls._instance is None:
            from dremioai.config import settings as _settings

            sdk_key = None
            s = _settings.instance()
            if s and s.dremio and s.dremio.launchdarkly:
                sdk_key = s.dremio.launchdarkly.sdk_key
            cls._instance = cls(sdk_key or "")
        return cls._instance

    @classmethod
    def reset(cls):
        if cls._instance and cls._instance._client:
            cls._instance._client.close()
        cls._instance = None
        cls._client = None

    def is_enabled(self) -> bool:
        return self._client is not None and self._client.is_initialized()

    def get_flag(self, flag_key: str, default: Any) -> Any:
        try:
            if self.is_enabled():
                value = self._client.variation(flag_key, ldclient.Context.create("mcp-server"), default)
                log.logger("feature_flags").debug(
                    f"Flag '{flag_key}' evaluated to: {value} (default: {default})"
                )
                return value
        except Exception:
            log.logger("feature_flags").exception(
                f"Error evaluating flag '{flag_key}' using default: {default}"
            )
        return default
