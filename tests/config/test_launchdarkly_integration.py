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
"""Tests for LaunchDarkly integration with settings.

These tests mock the LD client to verify that feature flags properly
override settings values for all flag-controlled fields in the Settings object.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from dremioai.config import settings
from dremioai.config.feature_flags import FeatureFlagManager


@pytest.fixture(autouse=True)
def reset_feature_flag_manager():
    """Reset the FeatureFlagManager singleton before each test."""
    FeatureFlagManager.reset()
    yield
    FeatureFlagManager.reset()


def _make_dremio_settings(**dremio_overrides):
    """Helper to create Settings with Dremio config."""
    base = {
        "uri": "https://test.dremio.cloud",
        "pat": "test-pat",
    }
    base.update(dremio_overrides)
    return settings.Settings.model_validate({"dremio": base})


def _make_mock_ld_client(flag_values: dict):
    """
    Create a mock LDClient that returns specified flag values.

    Args:
        flag_values: dict mapping flag_key -> value
    """
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True

    def variation_side_effect(flag_key, context, default):
        if flag_key in flag_values:
            return flag_values[flag_key]
        return default

    mock_client.variation.side_effect = variation_side_effect
    return mock_client


class TestFeatureFlagManagerMocked:
    """Test FeatureFlagManager with mocked LD client."""

    @patch("dremioai.config.feature_flags.ldclient")
    def test_initialization_with_sdk_key(self, mock_ldclient):
        mock_client = MagicMock()
        mock_client.is_initialized.return_value = True
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")

        mock_ldclient.set_config.assert_called_once()
        assert mgr.is_enabled() is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_initialization_without_sdk_key(self, mock_ldclient):
        mgr = FeatureFlagManager("")
        assert mgr.is_enabled() is False
        mock_ldclient.set_config.assert_not_called()

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_flag_returns_ld_value(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"my_flag": True})
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        result = mgr.get_flag("my_flag", default=False)

        assert result is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_flag_returns_default_when_not_found(self, mock_ldclient):
        mock_client = _make_mock_ld_client({})
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        result = mgr.get_flag("unknown_flag", default="fallback")

        assert result == "fallback"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_flag_returns_default_on_exception(self, mock_ldclient):
        mock_client = MagicMock()
        mock_client.is_initialized.return_value = True
        mock_client.variation.side_effect = Exception("LD error")
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        result = mgr.get_flag("error_flag", default="safe_default")

        assert result == "safe_default"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_bool_flag(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"bool_flag": True})
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        result = mgr.get_bool_flag("bool_flag", default=False)

        assert result is True
        assert isinstance(result, bool)

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_string_flag(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"str_flag": "variant_a"})
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        result = mgr.get_string_flag("str_flag", default="control")

        assert result == "variant_a"
        assert isinstance(result, str)

    @patch("dremioai.config.feature_flags.ldclient")
    def test_singleton_pattern(self, mock_ldclient):
        mock_client = MagicMock()
        mock_client.is_initialized.return_value = True
        mock_ldclient.get.return_value = mock_client

        mgr1 = FeatureFlagManager.instance(sdk_key="test-key")
        mgr2 = FeatureFlagManager.instance()

        assert mgr1 is mgr2

    def test_singleton_requires_sdk_key_on_first_call(self):
        with pytest.raises(ValueError, match="sdk_key is required"):
            FeatureFlagManager.instance()

    @patch("dremioai.config.feature_flags.ldclient")
    def test_build_context_with_project_and_org(self, mock_ldclient):
        mock_client = MagicMock()
        mock_client.is_initialized.return_value = True
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        ctx = mgr.build_context(project_id="proj-123", org_id="org-456")

        assert ctx is not None

    @patch("dremioai.config.feature_flags.ldclient")
    def test_build_context_with_no_values(self, mock_ldclient):
        mock_client = MagicMock()
        mock_client.is_initialized.return_value = True
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        ctx = mgr.build_context()

        assert ctx is not None


class TestDremioGetFlag:
    """Test Dremio.get_flag() integration with FeatureFlagManager."""

    def test_get_flag_returns_default_without_ld(self):
        cfg = _make_dremio_settings()
        assert cfg.dremio.get_flag("test_flag", "default_val") == "default_val"

    def test_get_flag_returns_default_with_ld_disabled(self):
        cfg = _make_dremio_settings(launchdarkly={})
        assert cfg.dremio.get_flag("test_flag", "default_val") == "default_val"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_flag_returns_ld_value_when_enabled(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"test_flag": "ld_value"})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(launchdarkly={"sdk_key": "test-key"})
        result = cfg.dremio.get_flag("test_flag", "default_val")

        assert result == "ld_value"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_flag_passes_project_id_context(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"test_flag": "value"})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            project_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            launchdarkly={"sdk_key": "test-key"},
        )
        cfg.dremio.get_flag("test_flag", "default")

        # Verify variation was called (project_id used in context)
        mock_client.variation.assert_called_once()


class TestAllowDmlFlagOverride:
    """Test that allow_dml property is properly overridden by LD flags."""

    def test_allow_dml_defaults_to_false(self):
        cfg = _make_dremio_settings()
        assert cfg.dremio.allow_dml is False

    def test_allow_dml_returns_config_value_without_ld(self):
        cfg = _make_dremio_settings(allow_dml=True)
        assert cfg.dremio.allow_dml is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_allow_dml_overridden_by_ld_true(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"allow_dml": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            allow_dml=False,  # Config says False
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.allow_dml is True  # LD overrides to True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_allow_dml_overridden_by_ld_false(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"allow_dml": False})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            allow_dml=True,  # Config says True
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.allow_dml is False  # LD overrides to False

    @patch("dremioai.config.feature_flags.ldclient")
    def test_allow_dml_falls_back_when_ld_returns_none(self, mock_ldclient):
        # LD returns None (flag not found), should fall back to config value
        mock_client = _make_mock_ld_client({})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            allow_dml=True,
            launchdarkly={"sdk_key": "test-key"},
        )
        # get_flag returns None (the default passed), so falls back to config
        assert cfg.dremio.allow_dml is True


class TestEnableSearchFlagOverride:
    """Test that enable_search property is properly overridden by LD flags."""

    def test_enable_search_defaults_to_false(self):
        cfg = _make_dremio_settings()
        assert cfg.dremio.enable_search is False

    def test_enable_search_returns_config_value_without_ld(self):
        cfg = _make_dremio_settings(enable_search=True)
        assert cfg.dremio.enable_search is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_enable_search_overridden_by_ld_true(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"enable_search": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            enable_search=False,  # Config says False
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.enable_search is True  # LD overrides to True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_enable_search_overridden_by_ld_false(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"enable_search": False})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            enable_search=True,  # Config says True
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.enable_search is False  # LD overrides to False

    @patch("dremioai.config.feature_flags.ldclient")
    def test_enable_search_falls_back_when_ld_returns_none(self, mock_ldclient):
        mock_client = _make_mock_ld_client({})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            enable_search=True,
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.enable_search is True

    def test_enable_search_via_enable_experimental_alias(self):
        cfg = _make_dremio_settings(enable_experimental=True)
        assert cfg.dremio.enable_search is True


class TestMultipleFlagOverrides:
    """Test that multiple flags can be overridden simultaneously."""

    @patch("dremioai.config.feature_flags.ldclient")
    def test_both_flags_overridden(self, mock_ldclient):
        mock_client = _make_mock_ld_client({
            "allow_dml": True,
            "enable_search": True,
        })
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            allow_dml=False,
            enable_search=False,
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.allow_dml is True
        assert cfg.dremio.enable_search is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_partial_override_one_flag_only(self, mock_ldclient):
        # Only allow_dml is in LD, enable_search falls back to config
        mock_client = _make_mock_ld_client({"allow_dml": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(
            allow_dml=False,
            enable_search=True,
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.allow_dml is True  # Overridden by LD
        assert cfg.dremio.enable_search is True  # Config value (LD returns None default)


class TestLaunchDarklyConfig:
    """Test LaunchDarkly configuration model."""

    def test_ld_disabled_by_default(self):
        cfg = _make_dremio_settings()
        assert cfg.dremio.launchdarkly is None

    def test_ld_disabled_without_sdk_key(self):
        cfg = _make_dremio_settings(launchdarkly={})
        assert cfg.dremio.launchdarkly.enabled is False

    def test_ld_enabled_with_sdk_key(self):
        cfg = _make_dremio_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.launchdarkly.enabled is True

    def test_ld_sdk_key_from_file(self, tmp_path):
        key_file = tmp_path / "sdk_key.txt"
        key_file.write_text("file-sdk-key-abc")

        cfg = _make_dremio_settings(launchdarkly={"sdk_key": f"@{key_file}"})
        assert cfg.dremio.launchdarkly.sdk_key == "file-sdk-key-abc"

    def test_ld_sdk_key_from_env(self, monkeypatch):
        monkeypatch.setenv("DREMIOAI_DREMIO__LAUNCHDARKLY__SDK_KEY", "env-sdk-key")

        cfg = settings.Settings.model_validate({
            "dremio": {
                "uri": "https://test.dremio.cloud",
                "pat": "test-pat",
                "launchdarkly": {},
            }
        })
        assert cfg.dremio.launchdarkly.sdk_key == "env-sdk-key"


class TestNonOverridableFieldsUnaffected:
    """Test that fields without LD integration are not affected."""

    @patch("dremioai.config.feature_flags.ldclient")
    def test_uri_not_affected_by_ld(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"uri": "https://evil.com"})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.uri == "https://test.dremio.cloud"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_pat_not_affected_by_ld(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"pat": "evil-pat"})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_dremio_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.pat == "test-pat"
