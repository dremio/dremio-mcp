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
import uuid
from uuid import UUID
from urllib.parse import urlparse

from pydantic import (
    Field,
    HttpUrl,
    AfterValidator,
    BaseModel,
    ConfigDict,
    field_serializer,
    AliasChoices,
)
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource, YamlConfigSettingsSource
from typing import (
    Optional,
    Union,
    Annotated,
    Self,
    List,
    Dict,
    Any,
    Callable,
    Literal,
    Tuple,
    get_args,
    get_type_hints,
)
from dremioai.config.tools import ToolType
from enum import auto, StrEnum
from pathlib import Path
from yaml import add_representer, dump
from functools import reduce
from operator import ior
from shutil import which
from contextvars import ContextVar, copy_context
from os import environ
from importlib.util import find_spec
from datetime import datetime
from dremioai import log
from dremioai.config.feature_flags import FeatureFlagManager

ProjectId = Union[UUID, Literal["DREMIO_DYNAMIC"]]


class GetterMixin:
    """Mixin that adds .get(field_name) to any Pydantic model.

    Raises AttributeError if field_name is not a valid attribute.
    Use with BaseModel or BaseSettings via multiple inheritance.
    """

    def get(self, field_name: str):
        return getattr(self, field_name)


# Annotated marker to exclude a field from LaunchDarkly flag lookups.
# Usage: field: Annotated[type, NoFlag()] = Field(...)
class NoFlag:
    """Mark a field to skip LD flag lookups in FlagAwareMixin.get()."""
    pass


def _has_no_flag(model_cls: type, field_name: str) -> bool:
    """Check if a field has the NoFlag annotation marker."""
    info = model_cls.model_fields.get(field_name)
    return info is not None and any(isinstance(m, NoFlag) for m in info.metadata)


# FlagAwareMixin overrides .get() to check LaunchDarkly before returning the
# config value. The LD flag key is "{_flag_prefix}.{field_name}", e.g.
# "dremio.allow_dml" or "dremio.api.http_retry.max_retries".
#
# _flag_prefix is auto-set by _propagate_flag_prefixes() during
# Settings.model_post_init — it mirrors the model's nesting path.
#
# Direct attribute access (obj.field) always returns the config value;
# only .get() consults LD. This keeps model_dump() and serialization
# unaffected by remote flag state.
#
# Fields annotated with NoFlag() are excluded from LD lookups.
class FlagAwareMixin(GetterMixin):
    _flag_prefix: str = ""

    def get(self, field_name: str):
        if _has_no_flag(type(self), field_name):
            return super().get(field_name)
        key = f"{self._flag_prefix}.{field_name}" if self._flag_prefix else field_name
        return FeatureFlagManager.instance().get_flag(
            key, super().get(field_name)
        )


# Convenience base for sub-models that need both FlagAwareMixin and BaseModel.
class FlagAwareModel(FlagAwareMixin, BaseModel):
    model_config = ConfigDict(validate_assignment=True)


def _resolve_tools_settings(server_mode: Union[ToolType, int, str]) -> ToolType:
    if isinstance(server_mode, str):
        try:
            server_mode = reduce(
                ior, [ToolType[m.upper()] for m in server_mode.split(",")]
            )
        except KeyError:
            return _resolve_tools_settings(int(server_mode))

    if isinstance(server_mode, int):
        return ToolType(server_mode)

    return server_mode


class Tools(BaseModel):
    server_mode: Annotated[
        Optional[Union[ToolType, int, str]], AfterValidator(_resolve_tools_settings)
    ] = Field(default=ToolType.FOR_SELF)
    model_config = ConfigDict(validate_assignment=True, use_enum_values=True)

    @field_serializer("server_mode")
    def serialize_server_mode(self, server_mode: ToolType):
        return ",".join(m.name for m in ToolType if m & server_mode)


class DremioCloudUri(StrEnum):
    PROD = auto()
    PRODEMEA = auto()


def _resolve_dremio_uri(
    uri: Union[str, DremioCloudUri, HttpUrl],
) -> Union[HttpUrl, str]:
    if isinstance(uri, str):
        try:
            uri = DremioCloudUri[uri.upper()]
        except KeyError:
            uri = HttpUrl(uri)

    if isinstance(uri, DremioCloudUri):
        match uri:
            case DremioCloudUri.PROD:
                return f"https://api.dremio.cloud"
            case DremioCloudUri.PRODEMEA:
                return f"https://api.eu.dremio.cloud"
        return uri

    elif isinstance(uri, HttpUrl):
        uri = str(uri)

    return uri.rstrip("/")


def _resolve_token_file(pat: str) -> str:
    return (
        Path(pat[1:]).expanduser().read_text().strip() if pat.startswith("@") else pat
    )


class Model(StrEnum):
    ollama = auto()
    openai = auto()


class OAuth2(BaseModel):
    client_id: str
    refresh_token: Optional[str] = None
    dremio_user_identifier: Optional[str] = None
    expiry: Optional[datetime] = None
    model_config = ConfigDict(validate_assignment=True)

    @property
    def has_expired(self) -> bool:
        return self.expiry is not None and self.expiry < datetime.now()


class Wlm(FlagAwareModel):
    engine_name: Optional[str] = None


class Metrics(FlagAwareModel):
    enabled: Optional[bool] = True
    port: Optional[int] = 9091


class HttpRetry(FlagAwareModel):

    max_retries: Optional[int] = Field(
        default=20,
        description="Maximum number of retry attempts for rate-limited requests",
    )
    initial_delay: Optional[float] = Field(
        default=1.0, description="Initial delay in seconds before first retry"
    )
    max_delay: Optional[float] = Field(
        default=60.0, description="Maximum delay in seconds between retries"
    )
    backoff_multiplier: Optional[float] = Field(
        default=2.0, description="Multiplier for exponential backoff"
    )


class ApiSettings(FlagAwareModel):
    # HTTP retry configuration
    http_retry: Optional[HttpRetry] = Field(default_factory=HttpRetry)
    polling_interval: Optional[float] = Field(
        default=1, description="Polling interval for REST api in seconds"
    )


class LaunchDarkly(BaseModel):

    sdk_key: Optional[Annotated[str, AfterValidator(_resolve_token_file)]] = Field(
        default=None,
        description="LaunchDarkly SDK key (can be file path with @ prefix or direct value)",
    )
    model_config = ConfigDict(validate_assignment=True)

    @property
    def enabled(self) -> bool:
        return self.sdk_key is not None


class Dremio(FlagAwareModel):
    uri: Annotated[
        Union[str, HttpUrl, DremioCloudUri], AfterValidator(_resolve_dremio_uri), NoFlag()
    ]
    raw_pat: Annotated[Optional[str], NoFlag()] = Field(default=None, alias="pat")
    raw_project_id: Annotated[Optional[ProjectId], NoFlag()] = Field(default=None, alias="project_id")
    enable_search: Optional[bool] = Field(
        default=False,
        alias=AliasChoices("enable_search", "enable_experimental"),
        description="enable experimental tools",
    )
    oauth2: Optional[OAuth2] = None
    allow_dml: Optional[bool] = Field(default=False)
    extract_org_id_from_jwt: Optional[bool] = Field(
        default=False,
        description="Extract org ID from JWT aud claim for LD context targeting",
    )
    auth_issuer_uri_override: Optional[str] = None
    jwks_uri: Optional[str] = Field(
        default=None,
        description="JWKS endpoint URL for JWT signature verification and expiry checking. "
        "When set, the MCP server validates token expiry before tool execution "
        "so expired tokens trigger HTTP 401 and the client's OAuth refresh flow. "
        "Example: https://your-auth0-tenant.auth0.com/.well-known/jwks.json",
    )
    jwks_cache_lifespan: Optional[int] = Field(
        default=3600,
        description="How long (seconds) to cache JWKS keys before refetching. Default: 3600 (1 hour).",
    )
    wlm: Optional[Wlm] = None
    max_result_rows: Optional[int] = Field(
        default=500,
        description="Maximum number of rows returned by RunSqlQuery. Use 0 for unlimited.",
    )
    max_result_bytes: Optional[int] = Field(
        default=204_800,
        description="Maximum UTF-8 byte size of RunSqlQuery results. Enforced after row cap. Use 0 for unlimited.",
    )
    api: Optional[ApiSettings] = Field(default_factory=ApiSettings)
    metrics: Optional[Metrics] = None

    @field_serializer("raw_pat")
    def serialize_pat(self, pat: str):
        return self.raw_pat if pat != self.raw_pat else pat

    @property
    def oauth_configured(self) -> bool:
        return self.oauth2 is not None

    @property
    def oauth_supported(self) -> bool:
        return self.project_id is not None

    @property
    def project_id(self) -> Optional[str]:
        return str(self.raw_project_id) if self.raw_project_id else None

    @project_id.setter
    def project_id(self, v: str):
        self.raw_project_id = uuid.UUID(v)

    @property
    def pat(self) -> str:
        if v := getattr(self, "_pat_resolved", None):
            return v
        if self.raw_pat is not None and self.raw_pat.startswith("@"):
            self._pat_resolved = _resolve_token_file(self.raw_pat)
            return self._pat_resolved
        return self.raw_pat

    @pat.setter
    def pat(self, v: str):
        self.raw_pat = v
        self._pat_resolved = None

    @property
    def is_cloud(self) -> bool:
        return self.project_id is not None

    @property
    def auth_issuer_uri(self) -> Optional[str]:
        if self.auth_issuer_uri_override is not None:
            return self.auth_issuer_uri_override
        if self.is_cloud:
            uri = urlparse(self.uri)
            if uri.netloc.startswith("api."):
                uri = uri._replace(netloc=f"login.{uri.netloc[4:]}")
            return uri.geturl()
        log.logger("settings").error("Oauth not supported for non-cloud instances")
        return None

    @property
    def auth_endpoints(self) -> Optional[Tuple[str, str, str]]:
        if issuer_uri := self.auth_issuer_uri:
            return (
                f"{issuer_uri}/oauth/authorize",
                f"{issuer_uri}/oauth/token",
                f"{issuer_uri}/oauth/register",
            )
        return None

    @property
    def prometheus_metrics_enabled(self) -> bool:
        return self.metrics is not None and self.metrics.enabled

    @property
    def prometheus_metrics_port(self) -> int | None:
        return self.metrics.port if self.metrics is not None else None


class OpenAi(BaseModel):
    api_key: Annotated[str, AfterValidator(_resolve_token_file)] = None
    model: Optional[str] = Field(default="gpt-4o")
    org: Optional[str] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class Ollama(BaseModel):
    model: Optional[str] = Field(default="llama3.1")
    model_config = ConfigDict(validate_assignment=True)


class LangChain(BaseModel):
    llm: Optional[Model] = None
    openai: Optional[OpenAi] = Field(default_factory=OpenAi)
    ollama: Optional[Ollama] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class Prometheus(BaseModel):
    uri: Union[HttpUrl, str]
    token: str
    model_config = ConfigDict(validate_assignment=True)


def _resolve_executable(executable: str) -> str:
    executable = Path(executable).expanduser()
    if not executable.is_absolute():
        if (c := which(executable)) is not None:
            executable = Path(c)
    executable = executable.resolve()
    if not executable.is_file():
        raise FileNotFoundError(f"Command {executable} not found.")
    return str(executable)


class MCPServer(BaseModel):
    command: Annotated[str, AfterValidator(_resolve_executable)]
    args: Optional[List[str]] = Field(default_factory=list)
    env: Optional[Dict[str, str]] = Field(default_factory=dict)
    model_config = ConfigDict(validate_assignment=True)


class Anthropic(BaseModel):
    api_key: Annotated[str, AfterValidator(_resolve_token_file)] = None
    chat_model: Optional[str] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class BeeAI(BaseModel):
    mcp_server: Optional[MCPServer] = Field(default=None)
    sliding_memory_size: Optional[int] = Field(default=10)
    anthropic: Optional[Anthropic] = Field(default=None)
    openai: Optional[OpenAi] = Field(default=None)
    ollama: Optional[Ollama] = Field(default=None)
    model_config = ConfigDict(validate_assignment=True)


class Settings(FlagAwareMixin, BaseSettings):
    log_level: Optional[str] = Field(default="INFO")
    dremio: Optional[Dremio] = Field(default=None)
    tools: Optional[Tools] = Field(default_factory=Tools)
    launchdarkly: Optional[LaunchDarkly] = Field(default_factory=LaunchDarkly)
    prometheus: Optional[Prometheus] = Field(default=None)
    langchain: Optional[LangChain] = Field(default=None)
    beeai: Optional[BeeAI] = Field(default=None)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        env_prefix="DREMIOAI_",
        env_extra="allow",
        use_enum_values=True,
        validate_assignment=True,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls, yaml_file=_yaml_file),
            file_secret_settings,
        )

    def model_post_init(self, __context):
        _propagate_flag_prefixes(self, "")
        if self.launchdarkly and self.launchdarkly.sdk_key:
            FeatureFlagManager.initialize(self.launchdarkly.sdk_key)

    def with_overrides(self, overrides: Dict[str, Any]) -> Self:
        def set_values(aparts: List[str], value: Any, obj: Any):
            if len(aparts) == 1 and hasattr(obj, aparts[0]):
                setattr(obj, aparts[0], value)
            elif hasattr(obj, aparts[0]):
                set_values(aparts[1:], value, getattr(obj, aparts[0]))

        for aparts, value in [
            (attr.split("."), value)
            for attr, value in overrides.items()
            if value is not None
        ]:
            set_values(aparts, value, self)

        return self


def _propagate_flag_prefixes(obj: BaseModel, prefix: str):
    for name in type(obj).model_fields:
        child = getattr(obj, name, None)
        if isinstance(child, FlagAwareMixin):
            child_prefix = f"{prefix}.{name}" if prefix else name
            child._flag_prefix = child_prefix
            _propagate_flag_prefixes(child, child_prefix)


def collect_flag_keys(model_cls: type, prefix: str = "") -> list[str]:
    """Recursively collect all LD flag keys from a FlagAwareMixin model class."""
    keys = []
    hints = get_type_hints(model_cls, include_extras=True)
    for name in model_cls.model_fields:
        if _has_no_flag(model_cls, name):
            continue
        key = f"{prefix}.{name}" if prefix else name
        annotation = hints[name]
        # Unwrap Optional[X] (Union[X, None]) to get the inner type X.
        # Without this, annotation is a generic alias (not a type), so
        # isinstance(annotation, type) would be False and we'd never
        # recurse into sub-models. We only unwrap when there's exactly
        # one non-None arg (i.e. Optional[X]); complex Unions are left as-is.
        inner = [a for a in get_args(annotation) if a is not type(None)]
        if len(inner) == 1:
            annotation = inner[0]
        if isinstance(annotation, type) and issubclass(annotation, FlagAwareMixin):
            keys.extend(collect_flag_keys(annotation, key))
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
            # Non-flag-aware sub-models (e.g. LangChain, BeeAI) are opaque
            # objects, not individual LD flags — skip them entirely.
            continue
        else:
            keys.append(key)
    return sorted(keys)


# Module-level holder so configure() can pass the YAML path to the Settings constructor
_yaml_file: Path | None = None


_settings: ContextVar[Settings] = ContextVar("settings", default=None)


# the default config is ~/.config/dremioai/config.yaml, use it if it exists
def default_config() -> Path:
    _top = "dremioai"
    if (_top := find_spec(__name__)) and _top.name:
        _top = _top.name.split(".")[0]
    return (
        Path(environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        / _top
        / "config.yaml"
    )


# configures the settings using the given config file and overwrites the global
# settings instance if force is True
def configure(cfg: Union[str, Path] = None, force=False) -> ContextVar[Settings]:
    global _settings
    if force and isinstance(_settings.get(), Settings):
        old = _settings.get()
        try:
            _settings.set(None)
            configure(cfg, force=False)
        except:
            # don't replace the old if there is an issue setting the new value
            _settings.set(old)
            raise

    if isinstance(cfg, str):
        cfg = Path(cfg)

    if cfg is None:
        cfg = default_config()

    if not cfg.exists():
        cfg.parent.mkdir(parents=True, exist_ok=True)
        cfg.touch()

    global _yaml_file
    _yaml_file = cfg
    _settings.set(Settings())

    return _settings


# Get the current settings instance if one has been configured. If not try
# to configure it using the default config file. If that fails, create a new
# empty settings instance.
def instance() -> Settings | None:
    global _settings
    if not isinstance(_settings.get(), Settings):
        try:
            configure()  # use default config, if exists
        except FileNotFoundError:
            # no default config, create a new default one
            _settings.set(Settings())
    return _settings.get()


async def run_with(
    func: Callable,
    overrides: Optional[Dict[str, Any]] = {},
    args: Optional[List[Any]] = [],
    kw: Optional[Dict[str, Any]] = {},
) -> Any:
    global _settings

    async def _call():
        tok = _settings.set(instance().model_copy(deep=True).with_overrides(overrides))
        try:
            return await func(*args, **kw)
        finally:
            _settings.reset(tok)

    ctx = copy_context()
    return await _call()


def write_settings(
    cfg: Path = None, inst: Settings = None, dry_run: bool = False
) -> str | None:
    if cfg is None:
        cfg = default_config()

    if not isinstance(inst, Settings):
        inst = instance()

    d = inst.model_dump(
        exclude_none=True, mode="json", exclude_unset=True, by_alias=True
    )
    add_representer(
        str,
        lambda dumper, data: dumper.represent_scalar(
            "tag:yaml.org,2002:str", data, style=('"' if "@" in data else None)
        ),
    )
    if dry_run:
        return dump(d)

    if not cfg.exists() or not cfg.parent.exists():
        cfg.parent.mkdir(parents=True, exist_ok=True)

    with cfg.open("w") as f:
        dump(d, f)
