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

from enum import auto
from datetime import datetime
from dremioai.api.util import UStrEnum, run_in_parallel

import pandas as pd
import asyncio
import itertools

from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
from dremioai.config import settings
from dremioai.log import logger


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
    client = AsyncHttpClient(uri=uri, pat=pat)
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
        client = AsyncHttpClient(uri=uri, pat=pat)

    endpoint = f"/v0/projects/{project_id}" if project_id else "/api/v3"
    job: Job = await client.get(f"{endpoint}/job/{qs.id}", deser=Job)
    while not job.done:
        await asyncio.sleep(0.5)
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


async def run_query_flightsql(
    query: Union[Query, str], use_df: bool = False
) -> Union[JobResultsWrapper, pd.DataFrame]:
    """Run query using Flight SQL protocol."""
    try:
        from dremioai.api.transport_flightsql import FlightSQLTransport
    except ImportError:
        raise RuntimeError("Flight SQL support requires adbc-driver-flightsql. Install with: pip install adbc-driver-flightsql")

    if not isinstance(query, Query):
        query = Query(sql=query)

    config = settings.instance().dremio

    try:
        # Create Flight SQL transport
        client = FlightSQLTransport(
            uri=config.uri,
            pat=config.pat,
            project_id=config.project_id
        )

        # Execute query
        df = await client.execute_query(query.sql)

        if use_df:
            return df
        else:
            # Convert DataFrame back to JobResultsWrapper format for compatibility
            rows = df.to_dict('records')
            schema = [ResultSchema(name=col, type=ResultSchemaType(name="VARCHAR")) for col in df.columns]
            result = JobResults(rowCount=len(df), schema=schema, rows=rows)
            return JobResultsWrapper([result])

    except Exception as e:
        logger().error(f"Flight SQL query execution failed: {e}")
        raise RuntimeError(f"Flight SQL query execution failed: {e}")


async def run_query(
    query: Union[Query, str], use_df: bool = False
) -> Union[JobResultsWrapper, pd.DataFrame]:
    """Run query using the configured SQL protocol (REST or Flight SQL)."""
    config = settings.instance().dremio

    # Check if Flight SQL is configured
    sql_protocol = getattr(config, 'sql_protocol', 'REST').upper()

    if sql_protocol == 'FLIGHT_SQL':
        logger().info("Using Flight SQL protocol for query execution")
        return await run_query_flightsql(query, use_df)
    else:
        logger().info("Using REST protocol for query execution")
        # Original REST implementation
        client = AsyncHttpClient()
        if not isinstance(query, Query):
            query = Query(sql=query)

        project_id = settings.instance().dremio.project_id
        endpoint = f"/v0/projects/{project_id}" if project_id else "/api/v3"
        qs: QuerySubmission = await client.post(
            f"{endpoint}/sql", body=query.model_dump(), deser=QuerySubmission
        )
        return await get_results(project_id, qs, use_df=use_df, client=client)
