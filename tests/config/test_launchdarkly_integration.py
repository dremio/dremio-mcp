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
from pathlib import Path

import pytest
from unittest.mock import patch, MagicMock
from yaml import safe_load
from dremioai.config import settings
from dremioai.config.feature_flags import FeatureFlagManager


@pytest.fixture(autouse=True)
def reset_feature_flag_manager():
    FeatureFlagManager.reset()
    old = settings._settings.get()
    yield
    FeatureFlagManager.reset()
    settings._settings.set(old)


def _make_settings(launchdarkly=None, **dremio_overrides):
    base = {"uri": "https://test.dremio.cloud", "pat": "test-pat"}
    base.update(dremio_overrides)
    cfg = {"dremio": base}
    if launchdarkly is not None:
        cfg["launchdarkly"] = launchdarkly
    s = settings.Settings.model_validate(cfg)
    settings._settings.set(s)
    return s


def _make_mock_ld_client(flag_values: dict):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True
    mock_client.variation.side_effect = lambda key, ctx, default: flag_values.get(key, default)
    return mock_client


# -- FeatureFlagManager -------------------------------------------------------


@patch("dremioai.config.feature_flags.ldclient")
def test_ffm_initialization_with_sdk_key(mock_ldclient):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True
    mock_ldclient.get.return_value = mock_client

    mgr = FeatureFlagManager("test-sdk-key")

    mock_ldclient.set_config.assert_called_once()
    assert mgr.is_enabled() is True


@pytest.mark.parametrize("flags,key,default,expected", [
    pytest.param({"my_flag": True}, "my_flag", False, True, id="found"),
    pytest.param({}, "unknown_flag", "fallback", "fallback", id="not_found"),
])
@patch("dremioai.config.feature_flags.ldclient")
def test_ffm_get_flag(mock_ldclient, flags, key, default, expected):
    mock_ldclient.get.return_value = _make_mock_ld_client(flags)
    mgr = FeatureFlagManager("test-sdk-key")
    assert mgr.get_flag(key, default) == expected


@patch("dremioai.config.feature_flags.ldclient")
def test_ffm_get_flag_returns_default_when_not_initialized(mock_ldclient):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = False
    mock_ldclient.get.return_value = mock_client

    mgr = FeatureFlagManager("test-sdk-key")
    assert mgr.get_flag("any_flag", "fallback") == "fallback"


@patch("dremioai.config.feature_flags.ldclient")
def test_ffm_singleton_pattern(mock_ldclient):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = True
    mock_ldclient.get.return_value = mock_client

    _make_settings(launchdarkly={"sdk_key": "test-key"})
    mgr1 = FeatureFlagManager.instance()
    mgr2 = FeatureFlagManager.instance()

    assert mgr1 is mgr2


def test_ffm_singleton_disabled_when_ld_not_configured():
    _make_settings()
    mgr = FeatureFlagManager.instance()
    assert mgr.is_enabled() is False


@patch("dremioai.config.feature_flags.ldclient")
def test_ffm_singleton_disabled_when_sdk_key_not_set(mock_ldclient):
    mock_client = MagicMock()
    mock_client.is_initialized.return_value = False
    mock_ldclient.get.return_value = mock_client

    _make_settings(launchdarkly={})
    mgr = FeatureFlagManager.instance()
    assert mgr.is_enabled() is False


# -- GetterMixin ---------------------------------------------------------------


def test_get_returns_field_value():
    cfg = _make_settings(allow_dml=True)
    assert cfg.dremio.get("allow_dml") is True


def test_get_returns_default_field_value():
    cfg = _make_settings()
    assert cfg.dremio.get("allow_dml") is False


def test_get_returns_property_value():
    cfg = _make_settings()
    assert cfg.dremio.get("pat") == "test-pat"


def test_get_returns_property_with_file_resolution(tmp_path):
    pat_file = tmp_path / "pat.txt"
    pat_file.write_text("resolved-pat-value")
    cfg = _make_settings(pat=f"@{pat_file}")
    assert cfg.dremio.get("pat") == "resolved-pat-value"


def test_get_returns_project_id_property():
    pid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    cfg = _make_settings(project_id=pid)
    assert cfg.dremio.get("project_id") == pid


@pytest.mark.parametrize("accessor,field", [
    pytest.param(lambda c: c.dremio, "nonexistent", id="dremio"),
    pytest.param(lambda c: c, "bogus", id="settings"),
    pytest.param(lambda c: c.dremio.api, "missing", id="submodel"),
])
def test_get_raises_attribute_error(accessor, field):
    cfg = _make_settings()
    with pytest.raises(AttributeError):
        accessor(cfg).get(field)


# -- Flag prefix propagation --------------------------------------------------


@pytest.mark.parametrize("accessor,expected,overrides", [
    pytest.param(lambda c: c.dremio, "dremio", {}, id="dremio"),
    pytest.param(lambda c: c.dremio.api, "dremio.api", {}, id="api"),
    pytest.param(lambda c: c.dremio.api.http_retry, "dremio.api.http_retry", {}, id="http_retry"),
    pytest.param(lambda c: c.dremio.wlm, "dremio.wlm", {"wlm": {"engine_name": "test"}}, id="wlm"),
    pytest.param(lambda c: c.dremio.metrics, "dremio.metrics", {"metrics": {"enabled": True, "port": 9091}}, id="metrics"),
])
def test_flag_prefix_propagation(accessor, expected, overrides):
    cfg = _make_settings(**overrides)
    assert accessor(cfg)._flag_prefix == expected


def test_flag_prefix_tools():
    cfg = settings.Settings.model_validate({})
    assert cfg.tools._flag_prefix == "tools"


# -- FlagAwareMixin.get() with LD overrides -----------------------------------


@pytest.mark.parametrize("flag_key,flag_val,field,config_val,expected", [
    pytest.param("dremio.allow_dml", True, "allow_dml", False, True, id="allow_dml_on"),
    pytest.param("dremio.enable_search", True, "enable_search", False, True, id="enable_search_on"),
    pytest.param("dremio.allow_dml", False, "allow_dml", True, False, id="allow_dml_off"),
])
@patch("dremioai.config.feature_flags.ldclient")
def test_ld_overrides_dremio_field(mock_ldclient, flag_key, flag_val, field, config_val, expected):
    mock_client = _make_mock_ld_client({flag_key: flag_val})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(**{field: config_val}, launchdarkly={"sdk_key": "test-key"})
    assert cfg.dremio.get(field) == expected


@patch("dremioai.config.feature_flags.ldclient")
def test_ld_falls_back_when_flag_not_in_ld(mock_ldclient):
    mock_client = _make_mock_ld_client({})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(allow_dml=True, launchdarkly={"sdk_key": "test-key"})
    assert cfg.dremio.get("allow_dml") is True


def test_get_without_ld_returns_config_value():
    cfg = _make_settings(allow_dml=True)
    assert cfg.dremio.get("allow_dml") is True


def test_get_without_ld_returns_field_default():
    cfg = _make_settings()
    assert cfg.dremio.get("allow_dml") is False
    assert cfg.dremio.get("enable_search") is False


# -- LD overrides on nested sub-models ----------------------------------------


@pytest.mark.parametrize("flag_key,flag_val,accessor,field,extra,expected", [
    pytest.param("dremio.api.polling_interval", 5.0, lambda c: c.dremio.api, "polling_interval", {}, 5.0, id="api_polling"),
    pytest.param("dremio.api.http_retry.max_retries", 50, lambda c: c.dremio.api.http_retry, "max_retries", {}, 50, id="http_retry"),
    pytest.param("dremio.metrics.enabled", False, lambda c: c.dremio.metrics, "enabled", {"metrics": {"enabled": True, "port": 9091}}, False, id="metrics"),
    pytest.param("dremio.wlm.engine_name", "override-engine", lambda c: c.dremio.wlm, "engine_name", {"wlm": {"engine_name": "default-engine"}}, "override-engine", id="wlm"),
])
@patch("dremioai.config.feature_flags.ldclient")
def test_ld_overrides_submodel(mock_ldclient, flag_key, flag_val, accessor, field, extra, expected):
    mock_client = _make_mock_ld_client({flag_key: flag_val})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(**extra, launchdarkly={"sdk_key": "test-key"})
    assert accessor(cfg).get(field) == expected


@patch("dremioai.config.feature_flags.ldclient")
def test_ld_submodel_falls_back_when_flag_not_in_ld(mock_ldclient):
    mock_client = _make_mock_ld_client({})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
    assert cfg.dremio.api.get("polling_interval") == 1
    assert cfg.dremio.api.http_retry.get("max_retries") == 20


# -- Multiple flag overrides ---------------------------------------------------


@patch("dremioai.config.feature_flags.ldclient")
def test_ld_overrides_both_dremio_flags(mock_ldclient):
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
def test_ld_mixed_dremio_and_submodel_overrides(mock_ldclient):
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
def test_ld_partial_override_one_flag_only(mock_ldclient):
    mock_client = _make_mock_ld_client({"dremio.allow_dml": True})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(
        allow_dml=False,
        enable_search=True,
        launchdarkly={"sdk_key": "test-key"},
    )
    assert cfg.dremio.get("allow_dml") is True
    assert cfg.dremio.get("enable_search") is True


# -- LaunchDarkly config model -------------------------------------------------


@pytest.mark.parametrize("ld_config,expected", [
    pytest.param(None, False, id="default"),
    pytest.param({}, False, id="no_sdk_key"),
    pytest.param({"sdk_key": "test-key"}, True, id="with_sdk_key"),
])
def test_ld_enabled_state(ld_config, expected):
    cfg = _make_settings(launchdarkly=ld_config)
    assert cfg.launchdarkly.enabled is expected


def test_ld_sdk_key_from_file(tmp_path):
    key_file = tmp_path / "sdk_key.txt"
    key_file.write_text("file-sdk-key-abc")

    cfg = _make_settings(launchdarkly={"sdk_key": f"@{key_file}"})
    assert cfg.launchdarkly.sdk_key == "file-sdk-key-abc"


def test_ld_sdk_key_from_env(monkeypatch):
    monkeypatch.setenv("DREMIOAI_LAUNCHDARKLY__SDK_KEY", "env-sdk-key")

    cfg = settings.Settings.model_validate({
        "dremio": {
            "uri": "https://test.dremio.cloud",
            "pat": "test-pat",
        }
    })
    assert cfg.launchdarkly.sdk_key == "env-sdk-key"


# -- Direct access vs .get() --------------------------------------------------


@patch("dremioai.config.feature_flags.ldclient")
def test_direct_access_returns_config_value(mock_ldclient):
    mock_client = _make_mock_ld_client({"dremio.allow_dml": True})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(allow_dml=False, launchdarkly={"sdk_key": "test-key"})
    assert cfg.dremio.allow_dml is False
    assert cfg.dremio.get("allow_dml") is True


@patch("dremioai.config.feature_flags.ldclient")
def test_uri_direct_access_not_affected(mock_ldclient):
    mock_client = _make_mock_ld_client({"dremio.uri": "https://evil.com"})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
    assert cfg.dremio.uri == "https://test.dremio.cloud"


@patch("dremioai.config.feature_flags.ldclient")
def test_model_dump_returns_config_values(mock_ldclient):
    mock_client = _make_mock_ld_client({"dremio.allow_dml": True})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(allow_dml=False, launchdarkly={"sdk_key": "test-key"})
    dumped = cfg.dremio.model_dump()
    assert dumped["allow_dml"] is False


# -- pat property behavior -----------------------------------------------------


def test_pat_direct_value():
    cfg = _make_settings(pat="direct-pat")
    assert cfg.dremio.pat == "direct-pat"
    assert cfg.dremio.raw_pat == "direct-pat"


def test_pat_file_resolution(tmp_path):
    pat_file = tmp_path / "pat.txt"
    pat_file.write_text("file-pat-value")
    cfg = _make_settings(pat=f"@{pat_file}")
    assert cfg.dremio.pat == "file-pat-value"
    assert cfg.dremio.raw_pat == f"@{pat_file}"


def test_pat_setter():
    cfg = _make_settings(pat="initial")
    cfg.dremio.pat = "updated"
    assert cfg.dremio.pat == "updated"
    assert cfg.dremio.raw_pat == "updated"


def test_pat_serialization():
    cfg = _make_settings(pat="test-pat")
    dumped = cfg.dremio.model_dump(by_alias=True)
    assert "pat" in dumped


def test_get_pat_returns_resolved_value(tmp_path):
    pat_file = tmp_path / "pat.txt"
    pat_file.write_text("resolved-from-file")
    cfg = _make_settings(pat=f"@{pat_file}")
    assert cfg.dremio.get("pat") == "resolved-from-file"


# -- enable_search behavior ----------------------------------------------------


def test_enable_search_default_false():
    cfg = _make_settings()
    assert cfg.dremio.enable_search is False


def test_enable_search_set_true():
    cfg = _make_settings(enable_search=True)
    assert cfg.dremio.enable_search is True


def test_enable_search_via_enable_experimental_alias():
    d = settings.Dremio.model_validate(
        {"enable_experimental": True, "uri": "https://foo", "pat": "bar"}
    )
    assert d.enable_search is True


def test_enable_search_serialization():
    cfg = _make_settings(enable_search=True)
    dumped = cfg.dremio.model_dump(by_alias=True)
    assert dumped.get("enable_search") is True


def test_get_enable_search_without_ld():
    cfg = _make_settings(enable_search=True)
    assert cfg.dremio.get("enable_search") is True


@patch("dremioai.config.feature_flags.ldclient")
def test_get_enable_search_with_ld_override(mock_ldclient):
    mock_client = _make_mock_ld_client({"dremio.enable_search": True})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(enable_search=False, launchdarkly={"sdk_key": "test-key"})
    assert cfg.dremio.enable_search is False
    assert cfg.dremio.get("enable_search") is True


# -- log_level -----------------------------------------------------------------


def test_log_level_default():
    cfg = settings.Settings.model_validate({})
    assert cfg.log_level == "INFO"


def test_log_level_from_config():
    cfg = settings.Settings.model_validate({"log_level": "DEBUG"})
    assert cfg.log_level == "DEBUG"


def test_log_level_get_without_ld():
    cfg = settings.Settings.model_validate({"log_level": "WARNING"})
    assert cfg.get("log_level") == "WARNING"


@patch("dremioai.config.feature_flags.ldclient")
def test_log_level_overridden_by_ld(mock_ldclient):
    mock_client = _make_mock_ld_client({"log_level": "ERROR"})
    mock_ldclient.get.return_value = mock_client

    cfg = _make_settings(launchdarkly={"sdk_key": "test-key"})
    assert cfg.get("log_level") == "ERROR"
    assert cfg.log_level == "INFO"


def test_log_level_from_env(monkeypatch):
    monkeypatch.setenv("DREMIOAI_LOG_LEVEL", "TRACE")
    cfg = settings.Settings.model_validate({})
    assert cfg.log_level == "TRACE"


# -- Golden flag keys ----------------------------------------------------------


def test_flag_keys_match_golden():
    """Prevent accidental flag key changes from field renames.

    If this test fails, a field was renamed/added/removed, which changes
    the LD flag key. Update the golden file intentionally:
        python scripts/generate_flag_keys.py --write
    """
    golden_path = Path(__file__).parent / "golden_flag_keys.yaml"
    golden = safe_load(golden_path.read_text())["flag_keys"]
    actual = settings.collect_flag_keys(settings.Settings)
    assert actual == golden, (
        "Flag keys changed! If intentional, run: python scripts/generate_flag_keys.py --write"
    )
