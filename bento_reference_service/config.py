import json

from fastapi import Depends
from functools import lru_cache
from pathlib import Path
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
)
from typing import Annotated, Any

from .constants import SERVICE_GROUP, SERVICE_ARTIFACT

__all__ = [
    "Config",
    "get_config",
    "ConfigDependency",
]


class CorsOriginsParsingSource(EnvSettingsSource):
    def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
        if field_name == "cors_origins":
            return tuple(x.strip() for x in value.split(";"))
        return json.loads(value) if value_is_complex else value


class Config(BaseSettings):
    service_id: str = f"{SERVICE_GROUP}:{SERVICE_ARTIFACT}"
    service_name: str = "Bento Reference Service"
    service_url_base_path: str = "http://127.0.0.1:5000"  # Base path to construct URIs from

    database_uri: str = "postgres://localhost:5432"
    data_path: Path = Path(__file__).parent / "data"

    file_response_chunk_size: int = 1024 * 16  # 16 KiB at a time
    response_substring_limit: int = 10000  # TODO: Refine default

    # /service-info customization
    service_contact_url: str = "mailto:info@c3g.ca"

    cors_origins: tuple[str, ...] = ("*",)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (CorsOriginsParsingSource(settings_cls),)


@lru_cache()
def get_config():
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
