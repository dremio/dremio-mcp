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

from pydantic import BaseModel, Field, AfterValidator
from typing import List, Dict, Union, Optional, Any, Annotated
from enum import auto, StrEnum
from pathlib import Path
from datetime import datetime
from dremioai.api.util import run_in_parallel
from functools import reduce
from dremioai import log
from dremioai.config import settings
from dremioai.api.transport import AsyncHttpClient

import pandas as pd


class PromQLResultStatus(StrEnum):
    SUCCESS = auto()
    ERROR = auto()


class TimeSeriesResultType(StrEnum):
    MATRIX = auto()
    VECTOR = auto()
    SCALAR = auto()
    STRING = auto()


class Histogram(BaseModel):
    count: Union[str, float, int]
    sum: Union[str, float, int]
    buckets: List[List[Union[str, float, int]]]


def _convert_values(values: List[Any]) -> List[Any]:
    for ix, v in enumerate(values):
        if type(v) == list:
            if len(v) >= 2:
                v = [datetime.fromtimestamp(int(v[0])), float(v[1])]
            else:
                v = []
        elif type(v) == int:
            v = datetime.fromtimestamp(v)
        elif type(v) == str:
            v = float(v)
        values[ix] = v
    return values


class Matrix(BaseModel):
    metric: Dict[str, Any]
    values: Annotated[Optional[List[List[Any]]], AfterValidator(_convert_values)] = (
        Field(default_factory=list)
    )
    histograms: Optional[List[Union[Histogram, List[Union[str, float]]]]] = Field(
        default_factory=list
    )

    def as_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.values, columns=["time", "value"])
        df["labels"] = ",".join(
            f"{k}={v}" for k, v in self.metric.items() if not k.startswith("__")
        )
        df["name"] = self.metric.get("__name__")
        return df


class InstantVector(BaseModel):
    metric: Dict[str, Any]
    value: Annotated[Optional[List[Any]], AfterValidator(_convert_values)] = Field(
        default_factory=list
    )
    histogram: Optional[Union[Histogram, List[Union[str, float]]]] = Field(
        default_factory=list
    )

    def as_df(self) -> pd.DataFrame:
        df = pd.DataFrame([self.value], columns=["time", "value"])
        df["labels"] = ",".join(
            f"{k}={v}" for k, v in self.metric.items() if not k.startswith("__")
        )
        df["name"] = self.metric.get("__name__")
        return df


class TimeSeriesData(BaseModel):
    type: Optional[TimeSeriesResultType] = Field(default=None, alias="resultType")
    result: Optional[List[Any]] = Field(default_factory=list)


def _convert_results(
    data: TimeSeriesData,
) -> List[Union[Matrix, InstantVector, List[Any]]]:
    if data.type == TimeSeriesResultType.MATRIX:
        return [Matrix.model_validate(d) for d in data.result]
    elif data.type == TimeSeriesResultType.VECTOR:
        return [InstantVector.model_validate(d) for d in data.result]
    return data.result


class PromQLResult(BaseModel):
    status: PromQLResultStatus
    data: Annotated[TimeSeriesData, AfterValidator(_convert_results)]
    error_type: Optional[str] = Field(default=None, alias="errorType")
    error: Optional[str] = Field(default=None)
    warnings: Optional[List[str]] = Field(default_factory=list)
    infos: Optional[List[str]] = Field(default_factory=list)


class PromQLLabelValues(BaseModel):
    status: PromQLResultStatus
    data: List[str]


async def get_promql_result(
    query: str,
    start: Optional[Union[datetime, str]] = None,
    end: Optional[Union[datetime, str]] = None,
    step: Optional[Union[int, str]] = None,
    use_df: Optional[bool] = False,
) -> Union[PromQLResult, pd.DataFrame]:
    client = AsyncHttpClient(
        settings.instance().prometheus.uri, settings.instance().prometheus.token
    )
    params = {"query": query}
    endpoint = "query_range"
    if start is not None:
        if isinstance(str, datetime):
            start = start.timestamp()
        params["start"] = start

    if step is not None:
        params["step"] = step

    if end is not None:
        if isinstance(end, datetime):
            end = end.timestamp()
        params["end"] = end

    result = await client.get(f"/api/v1/{endpoint}", params=params, deser=PromQLResult)
    if result.status == PromQLResultStatus.ERROR:
        raise Exception(result.error)

    return pd.concat([r.as_df() for r in result.data]) if use_df else result


async def get_metrics_schema(
    metric_name: str, use_df: Optional[bool] = False
) -> Union[Dict[str, str], pd.DataFrame]:
    result = await get_promql_result(metric_name, start="-30m")
    return (
        result.data[0].metric
        if not use_df
        else pd.DataFrame(result.data[0].metric.items(), columns=["label", "value"])
    )


async def get_label_values(
    label: Union[List[str], str], use_df=False
) -> Union[Dict[str, List[str]], pd.DataFrame]:
    if isinstance(label, list):
        result = await run_in_parallel([get_label_values(l, use_df) for l in label])
        if use_df:
            return pd.concat(result)
        return reduce(lambda x, y: {**x, **y}, result, {})

    client = AsyncHttpClient(
        settings.instance().prometheus.uri, settings.instance().prometheus.token
    )
    result = await client.get(
        f"/api/v1/label/{label}/values",
        params={"start": "-1d"},
        deser=PromQLLabelValues,
    )
    if result.status == PromQLResultStatus.ERROR:
        raise Exception(result.error)

    if use_df:
        df = pd.DataFrame(result.data, columns=["value"])
        df["label"] = label
        return df
    return {label: result.data}


async def get_metric_names(use_df=False) -> Union[List[str], pd.DataFrame]:
    return await get_label_values("__name__", use_df=use_df)
