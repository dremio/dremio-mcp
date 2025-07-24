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
from dremioai.api.dremio.engines import get_engines
import pandas as pd


class ProjectState(UStrEnum):
    CREATING = auto()
    ACTIVE = auto()
    ARCHIVING = auto()
    ARCHIVED = auto()
    RESTORING = auto()
    DEACTIVATING = auto()
    INACTIVE = auto()
    ACTIVATING = auto()


class CredentialType(UStrEnum):
    ACCESS_KEY = auto()
    IAM_ROLE = auto()
    AZURE_APP_CLIENT_CREDENTIALS = auto()


class Credential(BaseModel):
    type: Optional[str] = Field(default=None)
    access_key: Optional[str] = Field(default=None, alias="accessKeyId")
    secret_access_key: Optional[str] = Field(default=None, alias="secretAccessKey")
    role_arn: Optional[str] = Field(default=None, alias="roleArn")
    instance_profile_arn: Optional[str] = Field(
        default=None, alias="instanceProfileArn"
    )
    external_id: Optional[str] = Field(default=None, alias="externalId")
    external_sign: Optional[str] = Field(default=None, alias="externalSignature")
    tenant_id: Optional[str] = Field(default=None, alias="tenantId")
    client_id: Optional[str] = Field(default=None, alias="clientId")
    client_secret: Optional[str] = Field(default=None, alias="clientSecret")
    account_name: Optional[str] = Field(default=None, alias="accountName")


class LastStateError(BaseModel):
    ts: Optional[datetime] = Field(default=None, alias="timestamp")
    error: Optional[str] = Field(default=None, alias="error")


def _project_dt_validator(dt: str) -> datetime:
    return datetime.strptime(dt, "%a %b %d %H:%M:%S %Z %Y")


class CloudType(UStrEnum):
    AWS = auto()
    AZURE = auto()
    UNKNOWN = auto()


class Project(BaseModel):
    name: str
    id: str
    type: Optional[str] = Field(default=None)
    cloud_id: str = Field(..., alias="cloudId")
    state: ProjectState
    created_by: Optional[str] = Field(default=None, alias="createdBy")
    modified_by: Optional[str] = Field(default=None, alias="modifiedBy")
    created_at: Annotated[datetime, BeforeValidator(_project_dt_validator)] = Field(
        default=None, alias="createdAt"
    )
    modified_at: Annotated[datetime, BeforeValidator(_project_dt_validator)] = Field(
        default=None, alias="modifiedAt"
    )
    project_store: Optional[str] = Field(default=None, alias="projectStore")
    num_engines: Optional[int] = Field(default=None, alias="numberOfEngines")
    credentails: Optional[Credential] = Field(default=None, alias="credentials")
    cloud_type: Optional[CloudType] = Field(default=None, alias="cloudType")
    primary_catalog: Optional[str] = Field(default=None, alias="primaryCatalogId")
    last_error: Optional[LastStateError] = Field(default=None, alias="lastStateError")


class ProjectsList(List[Project]):
    pass


async def get_projects(
    uri: str,
    pat: str,
    project_ids: Optional[Union[List[str], str]] = None,
    use_df: Optional[bool] = False,
) -> Union[pd.DataFrame, ProjectsList]:
    client = AsyncHttpClient()

    if project_ids:
        if isinstance(engine_ids, str):
            engine_ids = [engine_ids]

        async def _fetch_one(pid: str):
            client = AsyncHttpClient()
            return await client.get(f"/v0/projects/{pid}", deser=Project)

        pl = await run_in_parallel([_fetch_one(p) for p in project_ids])
    else:
        pl = await client.get(f"/v0/projects", deser=Project, top_level_list=True)

    def _flatten(e: Project) -> Dict[str, Any]:
        d = e.model_dump()
        if t := d.get("credentials"):
            d["credentials"] = d["credentials"].get("type")
        if e := d.get("last_error"):
            d["last_error"] = e.get("error")
        return d

    if use_df:
        df = pd.DataFrame(data=[_flatten(e) for e in pl])
        df.rename({"id": "project_id"}, axis=1, inplace=True)
        return df

    return ProjectsList(pl)


async def get_engines_per_project(
    uri: str, pat: str, project_ids: Optional[Union[List[str], str]] = None
) -> pd.DataFrame:
    pdf = await get_projects(uri, pat, project_ids, use_df=True)
    project_ids = pdf.project_id.tolist()
    engines_result = await get_engines(
        uri, pat, project_ids, use_df=True, add_project_id=True
    )
    pdf.rename({"name": "project_name", "state": "project_state"}, axis=1, inplace=True)
    engines_result.rename(
        {"id": "engine_id", "state": "engine_state"}, axis=1, inplace=True
    )
    return pdf.merge(engines_result, on="project_id", how="left")
