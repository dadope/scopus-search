import json

from util.db_manager import DbManager

_config_file = open("config.json")
CONFIG = json.load(_config_file)
_config_file.close()

API_KEY = CONFIG["apikey"]

db_manager = DbManager()
