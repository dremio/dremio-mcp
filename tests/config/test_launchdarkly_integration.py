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
"""Tests for LaunchDarkly integration with settings."""

import pytest
from unittest.mock import Mock, patch
from dremioai.config import settings


class TestLaunchDarklyIntegration:
    """Test LaunchDarkly automatic override functionality."""

    def test_feature_flag_override_metadata_extraction(self):
        """Test that FeatureFlagOverride metadata is correctly extracted."""
        from dremioai.config.settings import _extract_flag_metadata, Dremio
        
        metadata = _extract_flag_metadata(Dremio)
        
        # Should have metadata for enable_search and allow_dml
        assert "enable_search" in metadata
        assert "allow_dml" in metadata
        
        # Check the flag keys
        assert metadata["enable_search"].flag_key == "dremio.enable_semantic_search"
        assert metadata["allow_dml"].flag_key == "dremio.allow_dml_operations"

    def test_launchdarkly_disabled_returns_config_value(self):
        """Test that when LaunchDarkly is disabled, config values are returned."""
        cfg = settings.Settings.model_validate({
            "dremio": {
                "uri": "https://test.dremio.cloud",
                "pat": "test-pat",
                "enable_search": False,
                "allow_dml": False,
            }
        })
        
        # LaunchDarkly is disabled by default
        assert cfg.dremio.enable_search is False
        assert cfg.dremio.allow_dml is False

    @patch('dremioai.config.feature_flags.FeatureFlagManager')
    def test_launchdarkly_enabled_returns_flag_value(self, mock_flag_manager_class):
        """Test that when LaunchDarkly is enabled, flag values override config."""
        # Setup mock
        mock_manager = Mock()
        mock_manager.is_enabled.return_value = True
        mock_manager.get_flag.side_effect = lambda key, default, ctx: {
            "dremio.enable_semantic_search": True,  # Override to True
            "dremio.allow_dml_operations": True,    # Override to True
        }.get(key, default)
        mock_flag_manager_class.instance.return_value = mock_manager
        
        # Create settings with LaunchDarkly enabled
        cfg = settings.Settings.model_validate({
            "dremio": {
                "uri": "https://test.dremio.cloud",
                "pat": "test-pat",
                "enable_search": False,  # Config says False
                "allow_dml": False,      # Config says False
                "launchdarkly": {
                    "enabled": True,
                    "sdk_key": "test-key",
                }
            }
        })
        
        # Set LaunchDarkly context
        cfg.set_ld_context({"project_id": "test-project"})
        
        # Access should trigger LaunchDarkly check and return True
        assert cfg.dremio.enable_search is True  # Overridden by LD
        assert cfg.dremio.allow_dml is True      # Overridden by LD

    def test_set_ld_context_with_project_id(self):
        """Test that set_ld_context extracts project_id from settings."""
        cfg = settings.Settings.model_validate({
            "dremio": {
                "uri": "https://test.dremio.cloud",
                "pat": "test-pat",
                "project_id": "abc-123",
            }
        })
        
        # Set context without explicit project_id
        cfg.set_ld_context()
        
        # The context should have been set with project_id from settings
        assert cfg.dremio._ld_context is not None
        assert cfg.dremio._ld_context.get("project_id") == "abc-123"

    def test_set_ld_context_with_custom_context(self):
        """Test that set_ld_context accepts custom context."""
        cfg = settings.Settings.model_validate({
            "dremio": {
                "uri": "https://test.dremio.cloud",
                "pat": "test-pat",
            }
        })
        
        # Set custom context
        custom_context = {
            "project_id": "custom-project",
            "environment": "production",
            "tier": "enterprise",
        }
        cfg.set_ld_context(custom_context)
        
        # The context should have been set
        assert cfg.dremio._ld_context == custom_context

    def test_non_overridable_fields_not_affected(self):
        """Test that fields without FeatureFlagOverride are not affected."""
        cfg = settings.Settings.model_validate({
            "dremio": {
                "uri": "https://test.dremio.cloud",
                "pat": "test-pat",
            }
        })
        
        # These fields don't have FeatureFlagOverride metadata
        assert cfg.dremio.uri == "https://test.dremio.cloud"
        assert cfg.dremio.pat == "test-pat"

    @patch('dremioai.config.feature_flags.FeatureFlagManager')
    def test_launchdarkly_error_returns_default(self, mock_flag_manager_class):
        """Test that errors in LaunchDarkly evaluation return the default value."""
        # Setup mock to raise exception
        mock_manager = Mock()
        mock_manager.is_enabled.return_value = True
        mock_manager.get_flag.side_effect = Exception("LaunchDarkly error")
        mock_flag_manager_class.instance.return_value = mock_manager
        
        cfg = settings.Settings.model_validate({
            "dremio": {
                "uri": "https://test.dremio.cloud",
                "pat": "test-pat",
                "enable_search": False,
                "launchdarkly": {
                    "enabled": True,
                    "sdk_key": "test-key",
                }
            }
        })
        
        cfg.set_ld_context()
        
        # Should return config value when LaunchDarkly fails
        assert cfg.dremio.enable_search is False

