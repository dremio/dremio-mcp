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
    if sdk_key is None and settings.instance().launchdarkly.sdk_key is None:
        raise ValueError("SDK key is required")

    async def get_flag():
        pp(f"\n[blue]Evaluating flag:[/blue] {flag_name}")
        value = settings.instance().dremio.get(flag_name)
        pp(f"\n[green]✓ Flag Value:[/green] {value}")
        pp(f"[dim]Default:[/dim] {default}")

    run(
        settings.run_with(
            get_flag,
            {
                "launchdarkly.sdk_key": sdk_key,
                "dremio.project_id": project_id,
            },
        )
    )


if __name__ == "__main__":
    log.configure(enable_json_logging=False, to_file=False)
    app()
