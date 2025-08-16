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

from dremioai import log
from dremioai.servers import tools, mcp as mcp_server
import os

# log.configure(enable_json_logging=True, to_file=True)
if mode := os.environ.get("MODE"):
    mode = [tools.ToolType[m.upper()] for m in ",".split(mode)]
app = mcp_server.init(mode=mode)


def dev():
    import mcp.cli.cli as cli

    cli.dev(__file__)


def run():
    import mcp.cli.cli as cli

    cli.run(__file__)
