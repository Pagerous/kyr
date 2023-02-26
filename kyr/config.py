import os
import tomllib
from pathlib import Path
from typing import Any, Sequence

from kyr.management import PROJECT_DIRECTORY


class Config:
    CONFIG_LOCATION: Path = PROJECT_DIRECTORY.joinpath("config.toml")

    _config: dict = {}
    _file_loaded: bool = False

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        key_components = key.split(".")
        top_key = key_components[0]

        if not cls._file_loaded:
            config_from_file = cls._load_config_file()
            cls._config.update(config_from_file)
            cls._file_loaded = True

        value = cls._config.get(top_key)
        if value is None:
            value = os.getenv(top_key.upper())
            cls._config[top_key] = value

        if isinstance(value, dict) and len(key_components) > 1:
            return cls._get_inner_config(value, key_components[1:], default)

        return value or default

    @staticmethod
    def _get_inner_config(config: dict, key_components: Sequence[str], default: Any) -> Any:
        if len(key_components) == 0:
            return config
        for key_component in key_components:
            config = config.get(key_component)
            if config is None:
                return default
        return config

    @classmethod
    def _load_config_file(cls) -> dict:
        if not cls.CONFIG_LOCATION.exists():
            return {}
        with open(cls.CONFIG_LOCATION, "rb") as f:
            return tomllib.load(f)
