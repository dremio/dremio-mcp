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
from typing import Optional, Any, Dict, Self
from dremioai import log
import ldclient
from ldclient import Context
from ldclient.config import Config


class FeatureFlagManager:
    """
    Manages LaunchDarkly feature flags for MCP server.

    Simple wrapper around LaunchDarkly client that follows the pattern from
    launchdarkly_manual_test.py. Takes sdk_key as input during construction.
    """

    _instance: Optional[Self] = None
    _client: Optional[ldclient.LDClient] = None

    def __init__(self, sdk_key: str):
        if not sdk_key:
            log.logger("feature_flags").warning(
                "LaunchDarkly SDK key is empty, feature flags will be disabled"
            )
            return

        try:
            # Use ldclient.set_config pattern (mirrors launchdarkly_manual_test.py)
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
    def instance(cls, sdk_key: Optional[str] = None) -> Self:
        """
        Get the singleton instance of FeatureFlagManager.

        Args:
            sdk_key: LaunchDarkly SDK key (required on first call)

        Returns:
            FeatureFlagManager: The singleton instance
        """
        if cls._instance is None:
            if sdk_key is None:
                raise ValueError(
                    "sdk_key is required when creating FeatureFlagManager instance"
                )
            cls._instance = cls(sdk_key)
        return cls._instance

    @classmethod
    def reset(cls):
        """Reset the singleton instance. Useful for testing."""
        if cls._instance and cls._instance._client:
            cls._instance._client.close()
        cls._instance = None

    def is_enabled(self) -> bool:
        """
        Check if LaunchDarkly is enabled and initialized.

        Returns:
            bool: True if LaunchDarkly client is ready
        """
        return self._client is not None and self._client.is_initialized()

    def get_flag(
        self,
        flag_key: str,
        default: Any,
        project_id: Optional[str] = None,
        org_id: Optional[str] = None,
    ) -> Any:
        """
        Get feature flag value with context.

        This method evaluates a feature flag using LaunchDarkly's multi-context
        evaluation. The context should include project_id (required) and org_id (optional)
        for proper targeting.
        """
        try:
            if self.is_enabled():
                value = self._client.variation(
                    flag_key,
                    self.build_context(project_id=project_id, org_id=org_id),
                    default,
                )
                log.logger("feature_flags").debug(
                    f"Flag '{flag_key}' evaluated to: {value} (default: {default})"
                )
                return value
        except:
            log.logger("feature_flags").exception(
                f"Error evaluating flag '{flag_key}' using default: {default}"
            )
        return default

    def get_bool_flag(
        self,
        flag_key: str,
        default: bool = False,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        return bool(self.get_flag(flag_key, default, context))

    def get_string_flag(
        self,
        flag_key: str,
        default: str = "",
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        return str(self.get_flag(flag_key, default, context))

    def build_context(self, project_id=None, org_id=None) -> Context:
        # Build multi-context with projectId and orgId kinds (mirrors tests/ld.py pattern)
        multi_builder = Context.multi_builder()

        # Add contexts for each kind that has a value
        for kind, value in [("projectId", project_id), ("orgId", org_id)]:
            if value is not None:
                multi_builder.add(Context.builder(value).kind(kind).build())

        return multi_builder.build()
