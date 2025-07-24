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

from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Union, Optional, Any, Self

from enum import auto
from datetime import datetime, timedelta
from dremioai.api.util import UStrEnum, run_in_parallel
from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
from dremioai.api.dremio.projects import get_engines_per_project
import pandas as pd
import itertools
from dremioai import log
from dremioai.config import settings


class UsageType(UStrEnum):
    PROJECT = auto()
    ENGINE = auto()


class Frequency(UStrEnum):
    DAILY = auto()
    HOURLY = auto()


class UsageData(BaseModel):
    id: str
    type: UsageType
    start: datetime = Field(..., alias="startTime")
    end: datetime = Field(..., alias="endTime")
    usage: float
    model_config = ConfigDict(use_enum_values=True)


class Usage(BaseModel):
    data: Optional[List[UsageData]] = Field(default_factory=list, alias="data")
    prev_page: Optional[str] = Field(default=None, alias="previousPageToken")
    next_page: Optional[str] = Field(default=None, alias="nextPageToken")

    def filter_nonzero(self) -> Self:
        self.data = [d for d in self.data if d.usage > 0]
        return self


class Params(BaseModel):
    max_results: Optional[int] = Field(default=500, alias="maxResults")
    frequency: Optional[Frequency] = Field(default=None)
    group_by: Optional[UsageType] = Field(default=None, alias="groupBy")
    filter: Optional[Dict[str, Any]] = Field(default_factory=dict)
    pageToken: Optional[str] = Field(default=None)

    def for_last_n_days(self, days: int = 7) -> Self:
        start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        self.filter["start_time"] = start_time
        return self

    def for_project_id(self, id: str) -> Self:
        self.filter["project_id"] = id
        log.logger().info(f"Adding project_id {id} to filter")
        return self

    def for_times(self, start: datetime, end: datetime = None) -> Self:
        self.filter["start_time"] = int(start.timestamp() * 1000)
        if end:
            self.filter["end_time"] = int(end.timestamp() * 1000)
        return self

    def for_usage(self, usage: UsageData) -> Self:
        return self.for_times(usage.start, usage.end).for_project_id(usage.id)

    def model_dump(self, *args, **kw):
        r = super().model_dump(*args, **kw)

        def _transform(k, v):
            match k:
                case "filter":
                    s = []
                    if start_time := v.get("start_time"):
                        s.append(f"start_time >= {start_time}")
                    if end_time := v.get("end_time"):
                        s.append(f"start_time <= {end_time}")
                    if id := v.get("id"):
                        s.append(f"id == '{id}'")
                    v = " && ".join(s)
                case "frequency":
                    v = v.value
                case "group_by":
                    v = v.value
                    k = "groupBy"

            return k, v

        d = dict(_transform(k, v) for k, v in r.items() if v is not None)
        return {k: v for k, v in d.items() if v}


async def get_usage(
    uri: str,
    pat: str,
    project_ids: Optional[Union[List[str], str]] = None,
    usages: Optional[List[UsageData]] = None,
    params: Optional[Params] = None,
    nonzero: Optional[bool] = True,
    add_project_id: Optional[bool] = False,
    use_df: Optional[bool] = False,
) -> Union[pd.DataFrame, List[Usage]]:

    if isinstance(project_ids, list) or isinstance(usages, list):
        params = Params() if params is None else params
        if project_ids is not None:
            tasks = [
                get_usage(
                    uri,
                    pat,
                    type,
                    params=params.model_copy().for_project_id(p),
                    use_df=use_df,
                    nonzero=nonzero,
                    add_project_id=add_project_id,
                )
                for p in project_ids
            ]
        else:
            tasks = [
                get_usage(
                    uri,
                    pat,
                    type,
                    params=params.model_copy().for_usage(u),
                    use_df=use_df,
                    nonzero=nonzero,
                    add_project_id=add_project_id,
                )
                for u in usages
            ]
        result = await run_in_parallel(tasks)
        return (
            pd.concat(result) if use_df else list(itertools.chain.from_iterable(result))
        )

    if isinstance(project_ids, str):
        params.for_project_id(project_ids)

    client = AsyncHttpClient()

    async def _get_usage(p: Params) -> Usage:
        p = p.model_dump() if p is not None else None
        u = await client.get(f"/v0/usage", params=p, deser=Usage)
        if nonzero:
            u.filter_nonzero()
        return u

    us = [await _get_usage(params)]
    while us and us[-1].next_page:
        if params is None:
            params = Params()
        params.pageToken = us[-1].next_page
        us.append(await _get_usage(params))

    df = (
        pd.DataFrame(data=(u.model_dump(mode="json") for i in us for u in i.data))
        if use_df
        else us
    )
    if add_project_id and params.filter and params.filter.get("project_id"):
        df["project_id"] = params.filter["project_id"]
    return df


async def get_consolidated_usage() -> List[pd.DataFrame]:
    uri = settings.instance().dremio.uri
    pat = settings.instance().dremio.pat
    results = await run_in_parallel(
        [get_engines_per_project(uri, pat), get_usage(uri, pat, use_df=True)]
    )
    engines_per_project, projects_usage = results
    engines_per_project.rename(columns={"name": "engine_name"}, inplace=True)

    # projects_usage = await get_usage(uri, pat, use_df=True)
    u = projects_usage.apply(
        lambda x: UsageData(
            id=x.id,
            startTime=x.start,
            endTime=x.end,
            type=UsageType[x.type],
            usage=x.usage,
        ),
        axis=1,
    ).tolist()
    engines_usage = await get_usage(
        uri,
        pat,
        params=Params(groupBy=UsageType.ENGINE),
        usages=u,
        nonzero=True,
        use_df=True,
        add_project_id=True,
    )

    engines_usage.rename(columns={"id": "engine_id"}, inplace=True)
    projects_usage.rename(columns={"id": "project_id"}, inplace=True)

    return [engines_per_project, projects_usage, engines_usage]
