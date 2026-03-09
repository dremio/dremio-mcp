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
import pytest
from unittest.mock import patch, MagicMock
from dremioai.config import settings
from dremioai.config.feature_flags import FeatureFlagManager


@pytest.fixture(autouse=True)
def reset_feature_flag_manager():
    FeatureFlagManager.reset()
    old = settings._settings.get()
    yield
    FeatureFlagManager.reset()
    settings._settings.set(old)


def _make_settings(**dremio_overrides):
    base = {"uri": "https://test.dremio.cloud", "pat": "test-pat"}
    base.update(dremio_overrides)
    s = settings.Settings.model_validate({"dremio": base})
    settings._settings.set(s)
    return s


def _make_mock_ld_client(flag_values: dict):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True
    mock_client.variation.side_effect = lambda key, ctx, default: flag_values.get(key, default)
    return mock_client


class TestFeatureFlagManagerMocked:

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
        assert mgr.get_flag("my_flag", default=False) is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_flag_returns_default_when_not_found(self, mock_ldclient):
        mock_client = _make_mock_ld_client({})
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        assert mgr.get_flag("unknown_flag", default="fallback") == "fallback"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_flag_returns_default_on_exception(self, mock_ldclient):
        mock_client = MagicMock()
        mock_client.is_initialized.return_value = True
        mock_client.variation.side_effect = Exception("LD error")
        mock_ldclient.get.return_value = mock_client

        mgr = FeatureFlagManager("test-sdk-key")
        assert mgr.get_flag("error_flag", default="safe_default") == "safe_default"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_singleton_pattern(self, mock_ldclient):
        mock_client = MagicMock()
        mock_client.is_initialized.return_value = True
        mock_ldclient.get.return_value = mock_client

        _make_settings(launchdarkly={"sdk_key": "test-key"})
        mgr1 = FeatureFlagManager.instance()
        mgr2 = FeatureFlagManager.instance()

        assert mgr1 is mgr2

    def test_singleton_disabled_when_ld_not_configured(self):
        _make_settings()
        mgr = FeatureFlagManager.instance()
        assert mgr.is_enabled() is False

    def test_singleton_disabled_when_sdk_key_not_set(self):
        _make_settings(launchdarkly={})
        mgr = FeatureFlagManager.instance()
        assert mgr.is_enabled() is False


class TestGetterModel:

    def test_get_returns_field_value(self):
        cfg = _make_settings(allow_dml=True)
        assert cfg.dremio.get("allow_dml") is True

    def test_get_returns_default_field_value(self):
        cfg = _make_settings()
        assert cfg.dremio.get("allow_dml") is False

    def test_get_returns_property_value(self):
        cfg = _make_settings()
        assert cfg.dremio.get("pat") == "test-pat"

    def test_get_returns_property_with_file_resolution(self, tmp_path):
        pat_file = tmp_path / "pat.txt"
        pat_file.write_text("resolved-pat-value")
        cfg = _make_settings(pat=f"@{pat_file}")
        assert cfg.dremio.get("pat") == "resolved-pat-value"

    def test_get_returns_project_id_property(self):
        pid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        cfg = _make_settings(project_id=pid)
        assert cfg.dremio.get("project_id") == pid


class TestFlagPrefixPropagation:

    def test_dremio_prefix(self):
        cfg = _make_settings()
        assert cfg.dremio._flag_prefix == "dremio"

    def test_nested_prefix(self):
        cfg = _make_settings()
        assert cfg.dremio.api._flag_prefix == "dremio.api"

    def test_deeply_nested_prefix(self):
        cfg = _make_settings()
        assert cfg.dremio.api.http_retry._flag_prefix == "dremio.api.http_retry"

    def test_tools_prefix(self):
        cfg = settings.Settings.model_validate({})
        assert cfg.tools._flag_prefix == "tools"

    def test_wlm_prefix(self):
        cfg = _make_settings(wlm={"engine_name": "test"})
        assert cfg.dremio.wlm._flag_prefix == "dremio.wlm"

    def test_metrics_prefix(self):
        cfg = _make_settings(metrics={"enabled": True, "port": 9091})
        assert cfg.dremio.metrics._flag_prefix == "dremio.metrics"


class TestFlagAwareGetOnDremio:

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_allow_dml_overridden_by_ld(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.allow_dml": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(allow_dml=False, launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.get("allow_dml") is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_enable_search_overridden_by_ld(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.enable_search": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(enable_search=False, launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.get("enable_search") is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_falls_back_when_flag_not_in_ld(self, mock_ldclient):
        mock_client = _make_mock_ld_client({})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(allow_dml=True, launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.get("allow_dml") is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_ld_override_false(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.allow_dml": False})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(allow_dml=True, launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.get("allow_dml") is False

    def test_get_without_ld_returns_config_value(self):
        cfg = _make_settings(allow_dml=True)
        assert cfg.dremio.get("allow_dml") is True

    def test_get_without_ld_returns_default(self):
        cfg = _make_settings()
        assert cfg.dremio.get("allow_dml") is False
        assert cfg.dremio.get("enable_search") is False


class TestFlagAwareGetOnSubModels:

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_api_polling_interval_overridden(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.api.polling_interval": 5.0})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.api.get("polling_interval") == 5.0

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_http_retry_overridden(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.api.http_retry.max_retries": 50})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.api.http_retry.get("max_retries") == 50

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_metrics_enabled_overridden(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.metrics.enabled": False})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(
            metrics={"enabled": True, "port": 9091},
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.metrics.get("enabled") is False

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_wlm_engine_name_overridden(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.wlm.engine_name": "override-engine"})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(
            wlm={"engine_name": "default-engine"},
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.wlm.get("engine_name") == "override-engine"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_submodel_falls_back_when_flag_not_in_ld(self, mock_ldclient):
        mock_client = _make_mock_ld_client({})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.api.get("polling_interval") == 1
        assert cfg.dremio.api.http_retry.get("max_retries") == 20


class TestMultipleFlagOverrides:

    @patch("dremioai.config.feature_flags.ldclient")
    def test_both_dremio_flags_overridden(self, mock_ldclient):
        mock_client = _make_mock_ld_client({
            "dremio.allow_dml": True,
            "dremio.enable_search": True,
        })
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(
            allow_dml=False,
            enable_search=False,
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.get("allow_dml") is True
        assert cfg.dremio.get("enable_search") is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_mixed_dremio_and_submodel_overrides(self, mock_ldclient):
        mock_client = _make_mock_ld_client({
            "dremio.allow_dml": True,
            "dremio.api.polling_interval": 10.0,
        })
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(
            allow_dml=False,
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.get("allow_dml") is True
        assert cfg.dremio.api.get("polling_interval") == 10.0

    @patch("dremioai.config.feature_flags.ldclient")
    def test_partial_override_one_flag_only(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.allow_dml": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(
            allow_dml=False,
            enable_search=True,
            launchdarkly={"sdk_key": "test-key"},
        )
        assert cfg.dremio.get("allow_dml") is True
        assert cfg.dremio.get("enable_search") is True


class TestLaunchDarklyConfig:

    def test_ld_disabled_by_default(self):
        cfg = _make_settings()
        assert cfg.dremio.launchdarkly is None

    def test_ld_disabled_without_sdk_key(self):
        cfg = _make_settings(launchdarkly={})
        assert cfg.dremio.launchdarkly.enabled is False

    def test_ld_enabled_with_sdk_key(self):
        cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.launchdarkly.enabled is True

    def test_ld_sdk_key_from_file(self, tmp_path):
        key_file = tmp_path / "sdk_key.txt"
        key_file.write_text("file-sdk-key-abc")

        cfg = _make_settings(launchdarkly={"sdk_key": f"@{key_file}"})
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

    @patch("dremioai.config.feature_flags.ldclient")
    def test_direct_access_returns_config_value(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.allow_dml": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(allow_dml=False, launchdarkly={"sdk_key": "test-key"})
        # Direct access returns config value, not LD override
        assert cfg.dremio.allow_dml is False
        # .get() returns LD override
        assert cfg.dremio.get("allow_dml") is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_uri_not_affected(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.uri": "https://evil.com"})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
        assert cfg.dremio.uri == "https://test.dremio.cloud"

    @patch("dremioai.config.feature_flags.ldclient")
    def test_model_dump_returns_config_values(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.allow_dml": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(allow_dml=False, launchdarkly={"sdk_key": "test-key"})
        dumped = cfg.dremio.model_dump()
        assert dumped["allow_dml"] is False


class TestRawPatBehavior:

    def test_pat_direct_value(self):
        cfg = _make_settings(pat="direct-pat")
        assert cfg.dremio.pat == "direct-pat"
        assert cfg.dremio.raw_pat == "direct-pat"

    def test_pat_file_resolution(self, tmp_path):
        pat_file = tmp_path / "pat.txt"
        pat_file.write_text("file-pat-value")
        cfg = _make_settings(pat=f"@{pat_file}")
        assert cfg.dremio.pat == "file-pat-value"
        assert cfg.dremio.raw_pat == f"@{pat_file}"

    def test_pat_setter(self):
        cfg = _make_settings(pat="initial")
        cfg.dremio.pat = "updated"
        assert cfg.dremio.pat == "updated"
        assert cfg.dremio.raw_pat == "updated"

    def test_pat_serialization(self):
        cfg = _make_settings(pat="test-pat")
        dumped = cfg.dremio.model_dump(by_alias=True)
        assert "pat" in dumped

    def test_get_pat_returns_resolved_value(self, tmp_path):
        pat_file = tmp_path / "pat.txt"
        pat_file.write_text("resolved-from-file")
        cfg = _make_settings(pat=f"@{pat_file}")
        assert cfg.dremio.get("pat") == "resolved-from-file"


class TestRawEnableSearchBehavior:

    def test_enable_search_default_false(self):
        cfg = _make_settings()
        assert cfg.dremio.enable_search is False

    def test_enable_search_set_true(self):
        cfg = _make_settings(enable_search=True)
        assert cfg.dremio.enable_search is True

    def test_enable_search_via_enable_experimental_alias(self):
        d = settings.Dremio.model_validate(
            {"enable_experimental": True, "uri": "https://foo", "pat": "bar"}
        )
        assert d.enable_search is True

    def test_enable_search_serialization(self):
        cfg = _make_settings(enable_search=True)
        dumped = cfg.dremio.model_dump(by_alias=True)
        assert dumped.get("enable_search") is True

    def test_get_enable_search_without_ld(self):
        cfg = _make_settings(enable_search=True)
        assert cfg.dremio.get("enable_search") is True

    @patch("dremioai.config.feature_flags.ldclient")
    def test_get_enable_search_with_ld_override(self, mock_ldclient):
        mock_client = _make_mock_ld_client({"dremio.enable_search": True})
        mock_ldclient.get.return_value = mock_client

        cfg = _make_settings(enable_search=False, launchdarkly={"sdk_key": "test-key"})
        # Direct access: config value
        assert cfg.dremio.enable_search is False
        # .get(): LD override
        assert cfg.dremio.get("enable_search") is True
