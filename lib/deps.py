"""
Module encapsulates shared dependencies
"""
import yaml

from lib.gateways import tg

with open(file="config/main_config.yaml", encoding="utf-8") as _f:
    MAIN_CFG = yaml.safe_load(stream=_f)
with open(file="secrets/secrets.yaml", encoding="utf-8") as _f:
    SECRETS = yaml.safe_load(stream=_f)
with open(file=MAIN_CFG["db"]["create_sql"], encoding="utf-8") as _f:
    _create_sql_string = _f.read()
    CREATE_SQL_STATEMENTS = _create_sql_string.split(";")

TG_GW = tg.TelegramGateway(
    bot_secret=SECRETS["telegram"]["bot_secret"],
    base_url=SECRETS["telegram"]["base_url"],
    chat_id=SECRETS["telegram"]["chat_id"],
    log_chat_id=SECRETS["telegram"]["log_chat_id"],
    send_msg_endpoint=SECRETS["telegram"]["send_msg_endpoint"],
    rps=MAIN_CFG["telegram"]["rps"]
)