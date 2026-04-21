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

from pydantic import BaseModel, Field
from typing import List, Dict, Union, Optional, Any
from dataclasses import dataclass

from enum import auto
from datetime import datetime
from dremioai.api.util import UStrEnum, run_in_parallel

import pandas as pd
import asyncio
import itertools

from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
from dremioai.config import settings


class ArcticSourceType(UStrEnum):
    BRANCH = auto()
    TAG = auto()
    COMMIT = auto()


class ArcticSource(BaseModel):
    type: ArcticSourceType = Field(..., alias="type")
    value: str


class Query(BaseModel):
    sql: str = Field(..., alias="sql")
    context: Optional[List[str]] = None
    engine_name: Optional[str] = Field(default=None, alias="engineName")
    references: Optional[Dict[str, ArcticSource]] = None


class QuerySubmission(BaseModel):
    id: str


class JobState(UStrEnum):
    NOT_SUBMITTED = auto()
    STARTING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    CANCELED = auto()
    FAILED = auto()
    CANCELLATION_REQUESTED = auto()
    PLANNING = auto()
    PENDING = auto()
    METADATA_RETRIEVAL = auto()
    QUEUED = auto()
    ENGINE_START = auto()
    EXECUTION_PLANNING = auto()
    INVALID_STATE = auto()


class QueryType(UStrEnum):
    UI_RUN = auto()
    UI_PREVIEW = auto()
    UI_INTERNAL_PREVIEW = auto()
    UI_INTERNAL_RUN = auto()
    UI_EXPORT = auto()
    ODBC = auto()
    JDBC = auto()
    REST = auto()
    ACCELERATOR_CREATE = auto()
    ACCELERATOR_DROP = auto()
    UNKNOWN = auto()
    PREPARE_INTERNAL = auto()
    ACCELERATOR_EXPLAIN = auto()
    UI_INITIAL_PREVIEW = auto()


class Relationship(UStrEnum):
    CONSIDERED = auto()
    MATCHED = auto()
    CHOSEN = auto()


class ReflectionReleationShips(BaseModel):
    dataset_id: str = Field(..., alias="datasetId")
    reflection_id: str = Field(..., alias="reflectionId")
    relationship: Relationship


class Acceleration(BaseModel):
    reflection_relationships: List[ReflectionReleationShips] = Field(
        ..., alias="reflectionRelationships"
    )


class Job(BaseModel):
    job_state: JobState = Field(..., alias="jobState")
    row_count: Optional[int] = Field(default=0, alias="rowCount")
    error_message: Optional[str] = Field(default=None, alias="errorMessage")
    started_at: Optional[datetime] = Field(default=None, alias="startedAt")
    ended_at: Optional[datetime] = Field(default=None, alias="endedAt")
    acceleration: Optional[Acceleration] = None
    query_type: QueryType = Field(..., alias="queryType")
    queue_name: Optional[str] = Field(default=None, alias="queueName")
    queue_id: Optional[str] = Field(default=None, alias="queueId")
    resource_scheduling_started_at: Optional[datetime] = Field(
        default=None, alias="resourceSchedulingStartedAt"
    )
    resource_scheduling_ended_at: Optional[datetime] = Field(
        default=None, alias="resourceSchedulingEndedAt"
    )
    cancellation_reason: Optional[str] = Field(default=None, alias="cancellationReason")

    @property
    def done(self):
        return self.job_state in {
            JobState.COMPLETED,
            JobState.CANCELED,
            JobState.FAILED,
        }

    @property
    def succeeded(self):
        return self.job_state == JobState.COMPLETED


class ResultSchemaType(BaseModel):
    name: str


class ResultSchema(BaseModel):
    name: str
    type: ResultSchemaType


class JobResults(BaseModel):
    row_count: int = Field(..., alias="rowCount")
    result_schema: Optional[List[ResultSchema]] = Field(..., alias="schema")
    rows: List[Dict[str, Any]]


class JobResultsWrapper(List[JobResults]):
    pass


class JobResultsParams(BaseModel):
    offset: Optional[int] = 0
    limit: Optional[int] = 500


async def _fetch_results(
    uri: str, pat: str, project_id: str, job_id: str, off: int, limit: int
) -> JobResults:
    client = AsyncHttpClient()
    params = JobResultsParams(offset=off, limit=limit)
    endpoint = f"/v0/projects/{project_id}" if project_id else "/api/v3"
    return await client.get(
        f"{endpoint}/job/{job_id}/results",
        params=params.model_dump(),
        deser=JobResults,
    )


async def get_results(
    project_id: str,
    qs: Union[QuerySubmission, str],
    use_df: bool = False,
    uri: Optional[str] = None,
    pat: Optional[str] = None,
    client: Optional[AsyncHttpClient] = None,
) -> JobResultsWrapper:
    if isinstance(qs, str):
        qs = QuerySubmission(id=qs)

    if client is None:
        client = AsyncHttpClient()

    delay = settings.instance().dremio.api.polling_interval

    endpoint = f"/v0/projects/{project_id}" if project_id else "/api/v3"
    job: Job = await client.get(f"{endpoint}/job/{qs.id}", deser=Job)
    while not job.done:
        await asyncio.sleep(delay)
        job = await client.get(f"{endpoint}/job/{qs.id}", deser=Job)

    if not job.succeeded:
        emsg = (
            job.error_message
            if job.error_message
            else (
                job.cancellation_reason
                if job.job_state == JobState.CANCELED
                else "Unknown error"
            )
        )
        raise RuntimeError(f"Job {qs.id} failed: {emsg}")

    if job.row_count == 0:
        return pd.DataFrame() if use_df else JobResultsWrapper([])

    limit = min(500, job.row_count)

    results = await run_in_parallel(
        [
            _fetch_results(uri, pat, project_id, qs.id, off, limit)
            for off in range(0, job.row_count, limit)
        ]
    )
    jr = JobResultsWrapper(itertools.chain(r for r in results))

    if use_df:
        df = pd.DataFrame(
            data=itertools.chain.from_iterable(jr.rows for jr in jr),
            columns=[rs.name for rs in jr[0].result_schema],
        )
        for rs in jr[0].result_schema:
            if rs.type.name == "TIMESTAMP":
                df[rs.name] = pd.to_datetime(df[rs.name])
        return df

    return jr


async def run_query(
    query: Union[Query, str], use_df: bool = False
) -> Union[JobResultsWrapper, pd.DataFrame]:
    client = AsyncHttpClient()
    if not isinstance(query, Query):
        engine_name = (
            settings.instance().dremio.wlm.engine_name
            if settings.instance().dremio.wlm is not None
            else None
        )
        query = Query(sql=query, engineName=engine_name)

    project_id = settings.instance().dremio.project_id
    endpoint = f"/v0/projects/{project_id}" if project_id else "/api/v3"
    qs: QuerySubmission = await client.post(
        f"{endpoint}/sql",
        body=query.model_dump(by_alias=True, exclude_none=True),
        deser=QuerySubmission,
    )
    return await get_results(project_id, qs, use_df=use_df, client=client)


@dataclass
class QueryResult:
    df: pd.DataFrame
    total_rows: int
    returned_rows: int

    @property
    def truncated(self) -> bool:
        return self.returned_rows < self.total_rows


async def run_query_capped(
    query: Union[Query, str], max_rows: int = 500
) -> QueryResult:
    """Submit a query and fetch at most *max_rows* rows (0 = unlimited).

    Returns a ``QueryResult`` with the DataFrame, total row count from the
    job, the number of rows actually fetched, and a ``truncated`` flag.
    """
    client = AsyncHttpClient()
    if not isinstance(query, Query):
        engine_name = (
            settings.instance().dremio.wlm.engine_name
            if settings.instance().dremio.wlm is not None
            else None
        )
        query = Query(sql=query, engineName=engine_name)

    project_id = settings.instance().dremio.project_id
    endpoint = f"/v0/projects/{project_id}" if project_id else "/api/v3"
    qs: QuerySubmission = await client.post(
        f"{endpoint}/sql",
        body=query.model_dump(by_alias=True, exclude_none=True),
        deser=QuerySubmission,
    )

    delay = settings.instance().dremio.api.polling_interval
    job: Job = await client.get(f"{endpoint}/job/{qs.id}", deser=Job)
    while not job.done:
        await asyncio.sleep(delay)
        job = await client.get(f"{endpoint}/job/{qs.id}", deser=Job)

    if not job.succeeded:
        emsg = (
            job.error_message
            if job.error_message
            else (
                job.cancellation_reason
                if job.job_state == JobState.CANCELED
                else "Unknown error"
            )
        )
        raise RuntimeError(f"Job {qs.id} failed: {emsg}")

    total_rows = job.row_count or 0
    if total_rows == 0:
        return QueryResult(df=pd.DataFrame(), total_rows=0, returned_rows=0)

    fetch_rows = total_rows if max_rows == 0 else min(total_rows, max_rows)
    page_size = min(500, fetch_rows)

    results = await run_in_parallel(
        [
            _fetch_results(None, None, project_id, qs.id, off, page_size)
            for off in range(0, fetch_rows, page_size)
        ]
    )
    jr = JobResultsWrapper(results)

    all_rows = list(itertools.chain.from_iterable(jr_page.rows for jr_page in jr))
    # The last page may return more rows than needed; trim to fetch_rows.
    all_rows = all_rows[:fetch_rows]

    if all_rows and jr[0].result_schema:
        columns = [rs.name for rs in jr[0].result_schema]
        df = pd.DataFrame(data=all_rows, columns=columns)
        for rs in jr[0].result_schema:
            if rs.type.name == "TIMESTAMP":
                df[rs.name] = pd.to_datetime(df[rs.name])
    else:
        df = pd.DataFrame(data=all_rows)

    return QueryResult(df=df, total_rows=total_rows, returned_rows=len(df))
