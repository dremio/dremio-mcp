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

from pydantic import BaseModel, Field, BeforeValidator
from typing import List, Dict, Union, Optional, Any, Annotated

from enum import auto
from datetime import datetime
from dremioai.api.util import UStrEnum, run_in_parallel
from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
import pandas as pd
import itertools


class EngineSize(UStrEnum):
    XX_SMALL_V1 = auto()
    X_SMALL_V1 = auto()
    SMALL_V1 = auto()
    MEDIUM_V1 = auto()
    LARGE_V1 = auto()
    X_LARGE_V1 = auto()
    XX_LARGE_V1 = auto()
    XXX_LARGE_V1 = auto()


class InstanceFamily(UStrEnum):
    M5D = auto()
    M6ID = auto()
    M6GD = auto()
    DDV4 = auto()
    DDV5 = auto()


class State(UStrEnum):
    DELETING = auto()
    DISABLED = auto()
    DISABLING = auto()
    ENABLED = auto()
    ENABLING = auto()
    INVALID = auto()
    RUNNING = auto()
    STOPPED = auto()
    STOPPING = auto()
    DELETED = auto()
    UNKNOWN = auto()


def _engine_dt_validator(dt: str) -> datetime:
    return datetime.strptime(dt, "%a %b %d %H:%M:%S %Z %Y")


class Engine(BaseModel):
    id: str
    name: Optional[str]
    size: EngineSize
    active_replicas: Optional[int] = Field(..., alias="activeReplicas")
    min_replicas: Optional[int] = Field(..., alias="minReplicas")
    max_replicas: Optional[int] = Field(..., alias="maxReplicas")
    instance_family: InstanceFamily = Field(..., alias="instanceFamily")
    auto_stop_delay: Optional[int] = Field(default=None, alias="autoStopDelaySeconds")
    queue_time_limit: Optional[int] = Field(default=None, alias="queueTimeLimitSeconds")
    runtime_limit: Optional[int] = Field(default=None, alias="runtimeLimitSeconds")
    draintime_limit: Optional[int] = Field(default=None, alias="draintimeLimitSeconds")
    state: State
    queried_at: Annotated[datetime, BeforeValidator(_engine_dt_validator)] = Field(
        ..., alias="queriedAt"
    )
    status_changed_at: Annotated[datetime, BeforeValidator(_engine_dt_validator)] = (
        Field(..., alias="statusChangedAt")
    )
    description: Optional[str] = None
    tags: Optional[List[Dict[str, str]]] = Field(
        default_factory=list, alias="cloudTags"
    )
    concurrency: Optional[int] = Field(..., alias="maxConcurrency")
    additional_info: Optional[str] = Field(
        default=None, alias="additionalEngineStateInfo"
    )


class EngineList(List[Engine]):
    pass


async def get_engines(
    uri: str,
    pat: str,
    project_id: Union[List[str], str],
    engine_ids: Optional[Union[List[str], str]] = None,
    use_df: Optional[bool] = False,
    add_project_id: Optional[bool] = False,
) -> Union[pd.DataFrame, EngineList]:
    client = AsyncHttpClient()

    if isinstance(project_id, list):
        result = await run_in_parallel(
            [get_engines(uri, pat, p, engine_ids, use_df) for p in project_id]
        )
        if use_df:
            return pd.concat(result)

        return list(itertools.chain.from_iterable(result))

    if engine_ids:
        if isinstance(engine_ids, str):
            engine_ids = [engine_ids]

        async def _fetch_one(eid: str):
            client = AsyncHttpClient()
            return await client.get(
                f"/v0/projects/{project_id}/engines/{eid}", deser=Engine
            )

        el = await run_in_parallel([_fetch_one(e) for e in engine_ids])
    else:
        el = await client.get(
            f"/v0/projects/{project_id}/engines", deser=Engine, top_level_list=True
        )

    def _flatten(e: Engine) -> Dict[str, Any]:
        d = e.model_dump()
        if t := d.get("tags"):
            d["tags"] = ",".join(f"{k}={v}" for i in t for k, v in i.items())
        return d

    if use_df:
        df = pd.DataFrame(data=[_flatten(e) for e in el])
        df["project_id"] = project_id
        return df

    return EngineList(el)
