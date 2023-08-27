import yaml
import uuid
import time
import logging
import random
from logging import config

import pandas as pd

from lib.deps import MAIN_CFG, SECRETS, CREATE_SQL
from lib.gateways import tg, amst_re
from lib import simple_db_wrapper

# Logging boilerplate
with open("config/logging_config.yaml") as _f:
    LOG_CFG = yaml.safe_load(_f)
config.dictConfig(LOG_CFG)
main_logger = logging.getLogger("main_logger")

RUN_UUID = str(uuid.uuid4())

main_logger.debug("Read all the constants")

db = simple_db_wrapper.SimpleDb(db_path=MAIN_CFG["db"]["path"])
db.run_create_sql(CREATE_SQL)
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

def main():
    """
    Encompasses the main logic of the script
    """
    main_logger.debug("#################################")
    main_logger.debug("Starting new run")
    curr_data = pd.read_sql(sql="SELECT ad_url FROM seen_ads",
                            con=db.conn)
    if len(curr_data) == 0:
        current_urls = set()
    else:
        current_urls = set(curr_data["ad_url"])
    main_logger.debug("Read current data from the db, len is %s",
                    len(current_urls))

    new_data = pd.DataFrame()

    main_logger.debug("Starting searches")

    # Move this to be picked up from a gsheet
    for search in SECRETS["pararius"]["searches"]:
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
            r = telegram.send_message(msg)
            if r.status_code != 200:
                main_logger.warning("Message failed for %s", listing)
        
        # Prepare an interim df
        temp_df = pd.DataFrame(columns=MAIN_CFG["df_schema"].keys())
        temp_df["ad_url"] = list(net_new_listings)
        temp_df["search_url"] = search
        temp_df["run_uuid"] = RUN_UUID
        temp_df["seen_on"] = int(time.time()*1000)
        main_logger.debug("Prepared interim df")

        new_data = pd.concat(objs=[new_data, temp_df], ignore_index=True, sort=False)

    if len(new_data) > 0:
        new_data = new_data.astype(MAIN_CFG["df_schema"])
        main_logger.debug("Net new data: %s\n", new_data)
        main_logger.debug("Appending results to the db")
        new_data.to_sql(name="seen_ads", con=db.conn,
                        if_exists="append", index=False)
    main_logger.debug("Done, tutto bene")
   

try:
    main()
except Exception as e:
    main_logger.exception("Exception in main: %s", e)
finally:
    main_logger.debug("#################################")