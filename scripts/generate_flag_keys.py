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
Generate the golden YAML of all possible LaunchDarkly flag keys.

Recursively walks Settings and all FlagAwareMixin sub-models to produce
a sorted list of every flag key that .get() could consult.

Usage:
    python scripts/generate_flag_keys.py              # print to stdout
    python scripts/generate_flag_keys.py --write      # overwrite golden file
"""
from pathlib import Path

from yaml import dump

from dremioai.config.settings import Settings, FlagAwareMixin

GOLDEN_PATH = Path(__file__).resolve().parent.parent / "tests" / "config" / "golden_flag_keys.yaml"


def collect_flag_keys(model_cls: type, prefix: str = "") -> list[str]:
    """Recursively collect all flag keys from a model class."""
    keys = []
    for name, field_info in model_cls.model_fields.items():
        key = f"{prefix}.{name}" if prefix else name

        # Check if the field's type is a FlagAwareMixin subclass
        annotation = field_info.annotation
        # Unwrap Optional[X] -> X
        origin = getattr(annotation, "__origin__", None)
        args = getattr(annotation, "__args__", ())
        if origin is type(None) or str(origin) == "typing.Union":
            # Filter out NoneType from Union args
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                annotation = non_none[0]

        if isinstance(annotation, type) and issubclass(annotation, FlagAwareMixin):
            keys.extend(collect_flag_keys(annotation, key))
        else:
            keys.append(key)

    return sorted(keys)


def main():
    import sys

    keys = collect_flag_keys(Settings)
    output = dump({"flag_keys": keys}, default_flow_style=False, sort_keys=False)

    if "--write" in sys.argv:
        GOLDEN_PATH.write_text(output)
        print(f"Written to {GOLDEN_PATH}")
    else:
        print(output)


if __name__ == "__main__":
    main()
