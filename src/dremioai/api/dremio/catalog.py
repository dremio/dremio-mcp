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

from pydantic import BaseModel, Field, AfterValidator, ValidationError
from typing import Annotated, List, Set, Tuple, Dict, AnyStr, Any, Union, Optional
from datetime import datetime
from enum import StrEnum, auto
from functools import partial

from dremioai.api.transport import DremioAsyncHttpClient as AsyncHttpClient
from dremioai.api.util import UStrEnum, run_in_parallel
from dremioai.config import settings
from csv import reader, excel
from io import StringIO
from functools import reduce


class CatalogItemType(UStrEnum):
    DATASET = auto()
    CONTAINER = auto()
    FILE = auto()


class ContainerSubType(UStrEnum):
    SPACE = auto()
    SOURCE = auto()
    FOLDER = auto()
    HOME = auto()
    FUNCTION = auto()


class DatasetSubType(UStrEnum):
    VIRTUAL = auto()
    PROMOTED = auto()
    DIRECT = auto()


def subset_validator(elem: UStrEnum, values: UStrEnum | List[UStrEnum]) -> UStrEnum:
    if elem in values:
        return elem
    raise ValueError(f"{elem} not in {values}")


class LineageBase(BaseModel):
    id: AnyStr
    path: List[str]
    tag: AnyStr
    created_at: Optional[datetime] = Field(..., alias="createdAt")


class LineageSource(LineageBase):
    type: Annotated[
        CatalogItemType,
        AfterValidator(partial(subset_validator, values=CatalogItemType)),
    ]
    container_type: Annotated[
        ContainerSubType,
        AfterValidator(
            partial(
                subset_validator,
                values=ContainerSubType,
            )
        ),
    ] = Field(..., alias="containerType")


class LineageParents(LineageBase):
    type: Annotated[
        CatalogItemType,
        AfterValidator(partial(subset_validator, values=CatalogItemType)),
    ]
    dataset_type: Annotated[
        DatasetSubType,
        AfterValidator(
            partial(subset_validator, values=DatasetSubType),
        ),
    ] = Field(..., alias="datasetType")


class LineageChildren(LineageBase):
    type: Annotated[
        CatalogItemType,
        AfterValidator(partial(subset_validator, values=CatalogItemType)),
    ]
    dataset_type: Annotated[
        DatasetSubType,
        AfterValidator(partial(subset_validator, values=DatasetSubType)),
    ] = Field(..., alias="datasetType")


class LineageResponse(BaseModel):
    sources: List[LineageSource]
    parents: List[LineageParents]
    children: List[LineageChildren]


async def get_lineage(dataset_id_or_path: str) -> Dict[str, Any]:
    client = AsyncHttpClient()
    if "." in dataset_id_or_path:
        response = await get_schema(dataset_id_or_path, by_id=False)
        dataset_id_or_path = response["id"]

    project_id = settings.instance().dremio.project_id
    endpoint = f"/v0/projects/{project_id}/catalog" if project_id else "/api/v3/catalog"
    result: LineageResponse = await client.get(
        f"{endpoint}/{dataset_id_or_path}/graph",
        deser=LineageResponse,
    )
    return result.model_dump()


async def get_schema(
    dataset_path_or_id: Optional[Union[List[str], str]],
    by_id: Optional[bool] = False,
    include_tags: Optional[bool] = False,
    flatten: Optional[bool] = False,
) -> Dict[str, Any]:
    client = AsyncHttpClient()
    project_id = settings.instance().dremio.project_id
    endpoint = f"/v0/projects/{project_id}/catalog" if project_id else "/api/v3/catalog"
    if by_id:
        endpoint += "/" + dataset_path_or_id
    else:
        if isinstance(dataset_path_or_id, str):
            dataset_path_or_id = list(
                reader(StringIO(dataset_path_or_id), delimiter=".", dialect=excel)
            )[0]
        endpoint += f'/by-path/{"/".join(dataset_path_or_id)}'
    schema = await client.get(endpoint)

    if include_tags:

        async def _get(id, suffix):
            client = AsyncHttpClient()
            try:
                ep = (
                    f"/v0/projects/{project_id}/catalog"
                    if project_id
                    else "/api/v3/catalog"
                )
                result = await client.get(f"{ep}/{id}/collaboration/{suffix}")
                return {suffix: result}
            except:
                return {}

        extras = {"tag": ("tags", "tags"), "wiki": ("description", "text")}
        results = await run_in_parallel([_get(schema["id"], s) for s in extras])
        for i, r in enumerate(extras):
            k, v = extras[r]
            schema[k] = results[i].get(r, {}).get(v)
        if flatten:
            flattened_schema = {
                "schema": {
                    c.get("name", ""): c.get("type", {}).get("name", "unknown")
                    for c in schema.get("fields", [])
                }
            }
            for k in extras.values():
                if v := schema.get(k[0]):
                    flattened_schema[k[0]] = v
            schema = flattened_schema

    return schema


async def get_schemas(
    dataset_path_or_ids: List[Union[List[str], str]],
    by_id: Optional[bool] = False,
    include_tags: Optional[bool] = False,
    flatten: Optional[bool] = False,
) -> List[Dict[str, Any]]:

    return await run_in_parallel(
        [get_schema(p, by_id, include_tags, flatten) for p in dataset_path_or_ids]
    )


async def get_descriptions(
    dataset_path_or_ids: List[Union[List[str], str]], by_id: Optional[bool] = False
) -> Dict[str, Any]:
    """
    For each component of the path, get the descriptions, i.e. wiki and/or tags if defined
    if by_id is True, then its parents are not considered
    """

    def get_components_of_path(path: List[str]) -> Set[Tuple[str]]:
        return set(tuple(path[: i + 1]) for i in range(len(path)))

    def to_str_path(path: List[str]) -> str:
        return ".".join(f'"{p}"' for p in path)

    def extract_description(s: Dict[str, Any]) -> Dict[str, Any]:
        d = {v: s[v] for v in ("description", "tags") if v in s}
        return d if d.get("description") or d.get("tags") else {}

    components = set()
    result = {}
    while True:
        schemas = await get_schemas(dataset_path_or_ids, by_id, include_tags=True)
        rest = set()
        for s in schemas:
            if d := extract_description(s):
                if "path" not in s:
                    components.add(s["id"])
                    result[s["name"]] = d
                else:
                    result[to_str_path(s["path"])] = d
            rest = reduce(
                lambda x, y: x | y,
                [
                    get_components_of_path(s["path"][:-1])
                    for s in schemas
                    if "path" in s
                ],
                set(),
            )

        if remaining := rest - components:
            components |= remaining
            dataset_path_or_ids = list(remaining)
            continue
        else:
            break

    return result
