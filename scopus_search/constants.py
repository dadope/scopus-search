import json
from pathlib import Path

from .util.db_manager import DbManager

_user_dir = Path.home()
project_data_dir = _user_dir / ".scopus_search"
_resources_dir = project_data_dir / "resources"
_config_file = project_data_dir / "config.json"
_default_db_path = _resources_dir / "database.db"

project_data_dir.mkdir(exist_ok=True)
_resources_dir.mkdir(exist_ok=True)

DEFAULT_OUTPUT_FILETYPE = "json"
DEFAULT_NAME_INPUT_FORMAT = "{surname}, {given_name}"
DEFAULT_NAME_OUTPUT_FORMAT = "{surname}, {given_name}"

if _config_file.exists():
    with open(str(_config_file)) as file:
        CONFIG = json.load(file)
        file.close()
else:
    _default_settings = {
        "api_key": "<api key>"
    }

    with open(str(_config_file), "w") as file:
        json.dump(_default_settings, file)
        file.close()

    CONFIG = {}

API_KEY = CONFIG["api_key"] if "api_key" in CONFIG else None
DB_PATH = CONFIG["db_path"] if "db_path" in CONFIG else str(_default_db_path)

db_manager = DbManager(DB_PATH)
