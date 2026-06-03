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

from enum import IntFlag, auto


class ToolType(IntFlag):
    FOR_SELF = auto()  # introspecting dremio cluster and its usage pattersn
    FOR_PROMETHEUS = (
        auto()
    )  # supporting any prometheus stack setup to in conjuction with dremio
    FOR_DATA_PATTERNS = (
        auto()
    )  # discovering data patterns analysis within your data using dremio's semantic layer
    EXPERIMENTAL = (
        auto()
    )  # any experimental tools that are not yet ready for production
    DYNAMIC_REMOTE_TOOLS = (
        auto()
    )  # DiscoverDynamicTools / CallDynamicTool — generic remote tool passthrough; enabled only when explicitly requested
