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
"""Generate the golden YAML of all possible LaunchDarkly flag keys.

Recursively walks Settings and all FlagAwareMixin sub-models to produce
a sorted list of every flag key that .get() could consult.

Usage:
    uv run python scripts/generate_flag_keys.py              # print to stdout
    uv run python scripts/generate_flag_keys.py --write      # overwrite golden file
"""
from pathlib import Path

from typer import Typer, Option
from yaml import dump

from dremioai.config.settings import Settings, collect_flag_keys

app = Typer(help="Generate the golden YAML of all possible LaunchDarkly flag keys.")

GOLDEN_PATH = Path(__file__).resolve().parent.parent / "tests" / "config" / "golden_flag_keys.yaml"


@app.command()
def main(
    write: bool = Option(False, "--write", help="Overwrite the golden file instead of printing to stdout."),
):
    """Print or write the sorted list of all LD flag keys.

    Examples:
        uv run python scripts/generate_flag_keys.py
        uv run python scripts/generate_flag_keys.py --write
    """
    keys = collect_flag_keys(Settings)
    output = dump({"flag_keys": keys}, default_flow_style=False, sort_keys=False)

    if write:
        GOLDEN_PATH.write_text(output)
        print(f"Written to {GOLDEN_PATH}")
    else:
        print(output)


if __name__ == "__main__":
    app()
