import atexit
import yaml
import sqlite3
import uuid
import time
import logging
import random
from logging import config

import pandas as pd

from lib.gateways import tg, amst_re


with open("config/logging_config.yaml") as _f:
    LOG_CFG = yaml.safe_load(_f)
    print(LOG_CFG)
config.dictConfig(LOG_CFG)
main_logger = logging.getLogger("main_logger")

with open("secrets/secrets.yaml") as _f:
    SECRETS = yaml.safe_load(stream=_f)

with open(file="src/mvp_db.sql", encoding="utf") as _f:
    CREATE_SQL = _f.read()

DB_PATH = "storage/mvp_db.sqlite"

SEARCHES = [
    "https://www.pararius.com/apartments/amstelveen/1000-2000/50m2",
    "https://www.pararius.com/apartments/amsterdam/1000-2000/50m2"
]

RUN_UUID = str(uuid.uuid4())

main_logger.debug("Read all the constants")

db_conn = sqlite3.connect(DB_PATH)
cursor = db_conn.cursor()
cursor.execute(CREATE_SQL)
db_conn.commit()
atexit.register(db_conn.close)

main_logger.debug("Prepared database")

pararius = amst_re.ParariusGateway(rental_listing_pattern="for-rent",
                                   base_url="https://pararius.com")
telegram = tg.TelegramGateway(
    bot_secret=SECRETS["telegram"]["bot_secret"],
    base_url=SECRETS["telegram"]["base_url"],
    chat_id=SECRETS["telegram"]["chat_id"],
    send_msg_endpoint=SECRETS["telegram"]["send_msg_endpoint"]
)

main_logger.debug("Instantiated gateways")

curr_data = pd.read_sql(sql="SELECT ad_url FROM seen_ads",
                        con=db_conn)
if len(curr_data) == 0:
    current_urls = set()
else:
    current_urls = set(curr_data["ad_url"])
main_logger.debug("Read current data from the db, len is %s",
                  len(current_urls))

new_data = pd.DataFrame()
df_schema = {
    "ad_url": "object",
    "search_url": "object",
    "run_uuid": "object",
    "seen_on": "int64"
}

main_logger.debug("Starting searches")
for search in SEARCHES:
    pararius.perform_search(search)
    # Here we filter out net new ads using sets
    net_new_listings = pararius.session_listings.difference(
        current_urls
    )
    main_logger.debug("Done with %s", search)
    if len(net_new_listings) == 0:
        main_logger.debug("No new listings from %s", search)
        continue
    
    main_logger.debug("Sending new listings to telegram")
    for listing in net_new_listings:
        msg = f"New listing for {search}:\n{listing}"
        time.sleep(random.randint(0, 3))
        telegram.send_message(msg)
    
    # Prepare an interim df
    temp_df = pd.DataFrame(columns=df_schema.keys())
    temp_df["ad_url"] = list(net_new_listings)
    temp_df["search_url"] = search
    temp_df["run_uuid"] = RUN_UUID
    temp_df["seen_on"] = int(time.time()*1000)
    main_logger.debug("Prepared interim df")

    new_data = pd.concat(objs=[new_data, temp_df], ignore_index=True, sort=False)

if len(new_data) > 0:
    new_data = new_data.astype(df_schema)
    main_logger.debug("Net new data: %s\n", new_data)
    main_logger.debug("Appending results to the db")
    new_data.to_sql(name="seen_ads", con=db_conn,
                    if_exists="append", index=False)
    
main_logger.debug("Done, tutto bene")
main_logger.debug("#################################")

