import yaml

from lib.gateways import tg

import requests
# run sql
# fetch currently existing urls to a set
# read search url data, assemble a set
# find net new urls
# append to a db

with open("secrets/secrets.yaml") as _f:
    SECRETS = yaml.safe_load(stream=_f)


telegram = tg.TelegramGateway(
    bot_secret=SECRETS["telegram"]["bot_secret"],
    base_url=SECRETS["telegram"]["base_url"],
    chat_id=SECRETS["telegram"]["chat_id"],
    send_msg_endpoint=SECRETS["telegram"]["send_msg_endpoint"]
)

CHAT_ID = -908558775

msg = "Ciao from my custom class, @zxcsamaaaa"

r = telegram.send_message("Ciao from my custom class, @zxcsamaaaa")