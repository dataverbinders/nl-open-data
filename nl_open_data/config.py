import os
from pathlib import Path
from importlib import import_module

from box import Box


CONFIG_TOML = "user_config.toml"
config_path = Path(__file__).parent / CONFIG_TOML

if config_path.exists():
    os.environ["PREFECT__USER_CONFIG_PATH"] = config_path.as_posix()

config = getattr(import_module("prefect"), "config")
