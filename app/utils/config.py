import os
import pathlib
from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    count_workers: int
    archive_folder: str | pathlib.Path
    database_folder: str | pathlib.Path
    model_config = SettingsConfigDict(env_file="env_config", env_file_encoding="utf-8")
    host: str
    port: int


settings = Settings()
