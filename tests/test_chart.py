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
import subprocess
import tempfile
from typing import Optional

import pytest
import shutil

from pathlib import Path

helm_dir = Path(__file__).parent.parent / "helm" / "dremio-mcp"
examples_dir = helm_dir / "examples"


class HelmChart:
    """Helper class for rendering and testing Helm charts"""

    def __init__(self, chart_path: str = helm_dir):
        self.chart_path = chart_path
        self.temp_dir = tempfile.mkdtemp(prefix="helm-test-")

    def render(
        self,
        release_name: str = "test-release",
        values_file: Optional[str | Path] = None,
        set_values: Optional[dict] = None,
    ) -> str:
        """
        Render the Helm chart with given parameters

        Args:
            release_name: Name of the release
            values_file: Path to values file
            set_values: Dictionary of values to set via --set

        Returns:
            Rendered YAML as string
        """
        cmd = ["helm", "template", release_name, self.chart_path]

        if values_file:
            cmd.extend(["-f", str(values_file)])

        if set_values:
            for key, value in set_values.items():
                cmd.extend(["--set", f"{key}={value}"])

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout

    def lint(self) -> tuple[bool, str]:
        """
        Run helm lint on the chart

        Returns:
            Tuple of (success, output)
        """
        result = subprocess.run(
            ["helm", "lint", self.chart_path], capture_output=True, text=True
        )
        return result.returncode == 0, result.stdout + result.stderr


@pytest.fixture(scope="session", autouse=True)
def check_helm_installed():
    """Skip all tests if Helm is not installed."""
    if shutil.which("helm") is None:
        pytest.skip(
            "Helm not installed — skipping all Helm-related tests.",
            allow_module_level=True,
        )


@pytest.fixture
def helm_chart():
    """Fixture providing a HelmChart instance"""
    return HelmChart()


class TestHelmLint:
    """Test helm lint validation"""

    def test_chart_passes_lint(self, helm_chart):
        """Chart should pass helm lint"""
        success, output = helm_chart.lint()
        assert success, f"Chart fails helm lint:\n{output}"


class TestOAuthMode:
    """Test OAuth mode (no PAT) configuration"""

    @pytest.fixture
    def rendered_output(self, helm_chart):
        """Render chart in OAuth mode"""
        return helm_chart.render(
            set_values={"dremio.uri": "https://dremio.example.com:9047"}
        )

    def test_configmap_contains_uri(self, rendered_output):
        """ConfigMap should contain Dremio URI"""
        assert 'uri: "https://dremio.example.com:9047"' in rendered_output

    def test_no_secret_created(self, rendered_output):
        """No Secret should be created in OAuth mode"""
        assert "kind: Secret" not in rendered_output

    def test_no_pat_reference(self, rendered_output):
        """No PAT reference should be in ConfigMap"""
        assert "pat:" not in rendered_output

    def test_default_server_mode(self, rendered_output):
        """ConfigMap should contain default server mode"""
        assert 'server_mode: "FOR_DATA_PATTERNS"' in rendered_output

    def test_config_volume_defined(self, rendered_output):
        """Config volume should be defined"""
        assert "name: config" in rendered_output

    def test_no_secrets_volume(self, rendered_output):
        """No secrets volume should be in OAuth mode"""
        assert "name: secrets" not in rendered_output

    def test_config_mount_path(self, rendered_output):
        """Config mount path should be present"""
        assert "/app/config" in rendered_output

    def test_config_argument(self, rendered_output):
        """Config argument should be present in command"""
        assert "--cfg" in rendered_output

    def test_config_file_path(self, rendered_output):
        """Config file path should be in command"""
        assert "/app/config/config.yaml" in rendered_output


class TestInlinePATMode:
    """Test inline PAT mode configuration"""

    @pytest.fixture
    def rendered_output(self, helm_chart):
        """Render chart in inline PAT mode"""
        return helm_chart.render(
            set_values={
                "dremio.uri": "https://dremio.example.com:9047",
                "dremio.pat": "test-pat-token",
            }
        )

    def test_secret_created(self, rendered_output):
        """Secret should be created with inline PAT"""
        assert "kind: Secret" in rendered_output

    def test_secret_contains_pat(self, rendered_output):
        """Secret should contain PAT value"""
        assert 'pat: "test-pat-token"' in rendered_output

    def test_configmap_references_pat_file(self, rendered_output):
        """ConfigMap should reference PAT file"""
        assert 'pat: "@/app/secrets/pat"' in rendered_output

    def test_secrets_volume_defined(self, rendered_output):
        """Secrets volume should be defined"""
        assert "name: secrets" in rendered_output

    def test_secrets_mount_path(self, rendered_output):
        """Secrets mount path should be present"""
        assert "/app/secrets" in rendered_output

    def test_auto_generated_secret_name(self, rendered_output):
        """Auto-generated secret name should be used"""
        assert "test-release-dremio-mcp-secret" in rendered_output


class TestExistingSecretMode:
    """Test existing secret mode configuration"""

    @pytest.fixture
    def rendered_output(self, helm_chart):
        """Render chart with existing secret"""
        return helm_chart.render(
            set_values={
                "dremio.uri": "https://dremio.example.com:9047",
                "dremio.existingSecret": "my-custom-secret",
            }
        )

    def test_no_secret_created(self, rendered_output):
        """No Secret should be created when using existing secret"""
        assert "kind: Secret" not in rendered_output

    def test_configmap_references_pat_file(self, rendered_output):
        """ConfigMap should reference PAT file"""
        assert 'pat: "@/app/secrets/pat"' in rendered_output

    def test_secrets_volume_defined(self, rendered_output):
        """Secrets volume should be defined"""
        assert "name: secrets" in rendered_output

    def test_uses_provided_secret_name(self, rendered_output):
        """Should use provided secret name"""
        assert "secretName: my-custom-secret" in rendered_output

    def test_does_not_use_auto_generated_secret(self, rendered_output):
        """Should not use auto-generated secret"""
        assert "test-release-dremio-mcp-secret" not in rendered_output


class TestMetricsConfiguration:
    """Test metrics configuration"""

    @pytest.fixture
    def rendered_output(self, helm_chart):
        """Render chart with metrics enabled"""
        return helm_chart.render(
            set_values={
                "dremio.uri": "https://dremio.example.com:9047",
                "metrics.enabled": "true",
                "metrics.port": "9091",
            }
        )

    def test_metrics_enabled(self, rendered_output):
        """Metrics should be enabled in ConfigMap"""
        assert "enabled: true" in rendered_output

    def test_metrics_port(self, rendered_output):
        """Metrics port should be in ConfigMap"""
        assert "port: 9091" in rendered_output


class TestDMLConfiguration:
    """Test DML configuration"""

    def test_dml_enabled(self, helm_chart):
        """DML should be enabled in ConfigMap"""
        output = helm_chart.render(
            set_values={
                "dremio.uri": "https://dremio.example.com:9047",
                "dremio.allowDml": "true",
            }
        )
        assert "allow_dml: true" in output


class TestCustomServerMode:
    """Test custom server mode configuration"""

    def test_custom_server_mode(self, helm_chart):
        """Custom server mode should be in ConfigMap"""
        output = helm_chart.render(
            set_values={
                "dremio.uri": "https://dremio.example.com:9047",
                "tools.serverMode": "FOR_SELF\\,FOR_ADMIN",
            }
        )
        assert 'server_mode: "FOR_SELF,FOR_ADMIN"' in output


class TestEnvironmentVariables:
    """Test that environment variables are removed"""

    @pytest.fixture
    def rendered_output(self, helm_chart):
        """Render chart with default config"""
        return helm_chart.render(
            set_values={"dremio.uri": "https://dremio.example.com:9047"}
        )

    def test_no_uri_env_var(self, rendered_output):
        """No URI environment variable should be present"""
        assert "DREMIOAI_DREMIO__URI" not in rendered_output

    def test_no_server_mode_env_var(self, rendered_output):
        """No server mode environment variable should be present"""
        assert "DREMIOAI_TOOLS__SERVER_MODE" not in rendered_output

    def test_no_metrics_env_vars(self, rendered_output):
        """No metrics environment variables should be present"""
        assert "DREMIOAI_DREMIO__METRICS" not in rendered_output


class TestVolumeMountSecurity:
    """Test volume mount security settings"""

    def test_volume_mounts_readonly(self, helm_chart):
        """Both config and secrets should be mounted read-only"""
        output = helm_chart.render(
            set_values={
                "dremio.uri": "https://dremio.example.com:9047",
                "dremio.pat": "test-pat",
            }
        )
        count = output.count("readOnly: true")
        assert count == 2, f"Expected 2 read-only mounts, found {count}"


class TestCommandStructure:
    """Test command structure in deployment"""

    @pytest.fixture
    def rendered_output(self, helm_chart):
        """Render chart with default config"""
        return helm_chart.render(
            set_values={"dremio.uri": "https://dremio.example.com:9047"}
        )

    def test_command_starts_with_server(self, rendered_output):
        """Command should start with dremio-mcp-server"""
        assert "- dremio-mcp-server" in rendered_output

    def test_run_subcommand(self, rendered_output):
        """Run subcommand should be present"""
        assert "- run" in rendered_output

    def test_config_flag(self, rendered_output):
        """Config flag should be present"""
        assert "- --cfg" in rendered_output

    def test_config_path(self, rendered_output):
        """Config path should be present"""
        assert "- /app/config/config.yaml" in rendered_output

    def test_streaming_http_enabled(self, rendered_output):
        """Streaming HTTP should be enabled"""
        assert "- --enable-streaming-http" in rendered_output

    def test_file_logging_disabled(self, rendered_output):
        """File logging should be disabled"""
        assert "- --no-log-to-file" in rendered_output


class TestExampleValuesFiles:
    """Test that example values files render successfully"""

    def test_oauth_production_renders(self, helm_chart):
        """values-production.yaml should render successfully"""
        try:
            output = helm_chart.render(
                values_file=examples_dir / "values-production.yaml"
            )
            assert len(output) > 0
        except subprocess.CalledProcessError as e:
            pytest.fail(f"values-oauth-production.yaml fails to render: {e}")

    def test_with_pat_renders(self, helm_chart):
        """values-with-pat.yaml should render successfully"""
        try:
            output = helm_chart.render(
                values_file=examples_dir / "values-with-pat.yaml"
            )
            assert len(output) > 0
        except subprocess.CalledProcessError as e:
            pytest.fail(f"values-with-pat.yaml fails to render: {e}")

    def test_existing_secret_renders(self, helm_chart):
        """values-with-existing-secret.yaml should render successfully"""
        try:
            output = helm_chart.render(
                values_file=examples_dir / "values-with-existing-secret.yaml"
            )
            assert len(output) > 0
        except subprocess.CalledProcessError as e:
            pytest.fail(f"values-with-existing-secret.yaml fails to render: {e}")


class TestResourceLabels:
    """Test resource labels"""

    @pytest.fixture
    def rendered_output(self, helm_chart):
        """Render chart with default config"""
        return helm_chart.render(
            set_values={"dremio.uri": "https://dremio.example.com:9047"}
        )

    def test_standard_labels(self, rendered_output):
        """Standard labels should be present"""
        assert "app.kubernetes.io/name: dremio-mcp" in rendered_output

    def test_instance_label(self, rendered_output):
        """Instance label should be present"""
        assert "app.kubernetes.io/instance: test-release" in rendered_output
