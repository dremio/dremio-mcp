"""
LaunchDarkly Feature Flag Manual Integration Test

This is a manual integration test tool for validating LaunchDarkly feature flag
integration with real LaunchDarkly services.

Usage example:
    python tests/launchdarkly_manual_test.py \
        --sdk-key "sdk-xxx" \
        --flag-name "some.flag.name" \
        --project-id "test-project-id" \
        --org-id "test-org-id"
"""

from typing import Annotated, Optional

from typer import Typer, Option
from rich import print as pp
from asyncio import run

from dremioai import log
from dremioai.config import settings

app = Typer(
    no_args_is_help=True,
    name="launchdarkly-test",
    help="Manual integration test for LaunchDarkly feature flags",
    add_completion=False,
    context_settings=dict(help_option_names=["-h", "--help"]),
)


@app.command()
def main(
    flag_name: Annotated[str, Option(help="Feature flag name to evaluate")],
    sdk_key: Annotated[
        str,
        Option(
            help="LaunchDarkly SDK key", envvar="DREMIOAI_DREMIO__LAUNCHDARKLY__SDK_KEY"
        ),
    ] = None,
    project_id: Annotated[Optional[str], Option(help="Project ID for context")] = None,
    org_id: Annotated[Optional[str], Option(help="Organization ID for context")] = None,
    default: Annotated[bool, Option(help="Default value if flag not found")] = False,
):
    if sdk_key is None and settings.instance().dremio.launchdarkly.sdk_key is None:
        raise ValueError("SDK key is required")

    async def get_flag():
        pp(f"\n[blue]Evaluating flag:[/blue] {flag_name}")
        value = settings.instance().dremio.get_flag(
            flag_name, default, project_id=project_id, org_id=org_id
        )
        pp(f"\n[green]✓ Flag Value:[/green] {value}")
        pp(f"[dim]Default:[/dim] {default}")

    run(
        settings.run_with(
            get_flag,
            {
                "dremio.launchdarkly.sdk_key": sdk_key,
                "dremio.project_id": project_id,
            },
        )
    )


if __name__ == "__main__":
    log.configure(enable_json_logging=False, to_file=False)
    app()
