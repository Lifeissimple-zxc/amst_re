"""
Module encapsulates shared dependencies
"""
from lib.deps import MAIN_CFG, SECRETS
from lib.gateways import tg

TG_GW = tg.TelegramGateway(
    bot_secret=SECRETS["telegram"]["bot_secret"],
    base_url=SECRETS["telegram"]["base_url"],
    chat_id=SECRETS["telegram"]["chat_id"],
    send_msg_endpoint=SECRETS["telegram"]["send_msg_endpoint"],
    rps=MAIN_CFG["telegram"]["rps"]
)