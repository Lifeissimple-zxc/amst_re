import yaml
import uuid
import time
import logging
import sys
from logging import config

import pandas as pd

from lib.deps import MAIN_CFG, SECRETS, CREATE_SQL
from lib.gateways import tg, amst_re
from lib import simple_db_wrapper
from lib.gateways.base import proxy_fetcher


# Logging boilerplate
with open("config/logging_config.yaml") as _f:
    LOG_CFG = yaml.safe_load(_f)
config.dictConfig(LOG_CFG)
main_logger = logging.getLogger("main_logger")

RUN_UUID = str(uuid.uuid4())
DEBUG = bool(int(sys.argv[2]))
PROXIES = sys.argv[3]

main_logger.debug("Read all the constants")

ssl_proxies_fetcher = proxy_fetcher.SSLProxiesFetcher(
    url=MAIN_CFG["proxies"]["sslproxies"]["url"]
)

free_prx_fetcher = proxy_fetcher.FreeProxiesFetcher(
    url=MAIN_CFG["proxies"]["freeproxy"]["url"],
    headers=MAIN_CFG["proxies"]["freeproxy"]["headers"]
)


db = simple_db_wrapper.SimpleDb(db_path=MAIN_CFG["db"]["path"])
db.run_create_sql(CREATE_SQL)
main_logger.debug("Prepared database")

# Pararius needs proxies, ugly implemtation, will fix later maybe
if PROXIES == "ssl":
    proxies = ssl_proxies_fetcher.get_proxies()
    main_logger.debug("Fetched SSL proxies")
    pararius = amst_re.ParariusGateway(proxy_list=proxies,
                                       **MAIN_CFG["pararius"])
elif PROXIES == "free":
    free_proxies = free_prx_fetcher.get_proxies()
    main_logger.debug("Fetched free proxies")
    pararius = amst_re.ParariusGateway(proxy_list=free_proxies,
                                       **MAIN_CFG["pararius"])
else:
    pararius = amst_re.ParariusGateway(**MAIN_CFG["pararius"])
funda = amst_re.FundaGateway(headers=MAIN_CFG["http_headers"], **MAIN_CFG["funda"])

telegram = tg.TelegramGateway(
    bot_secret=SECRETS["telegram"]["bot_secret"],
    base_url=SECRETS["telegram"]["base_url"],
    chat_id=SECRETS["telegram"]["chat_id"],
    send_msg_endpoint=SECRETS["telegram"]["send_msg_endpoint"],
    rps=MAIN_CFG["telegram"]["rps"]
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
    for search in SECRETS["searches"]:
        worker_class = choose_gateway(
            url=search, pararius=pararius, funda=funda
        )
        if isinstance(worker_class, ValueError):
            main_logger.warning("Got error %s for %s", worker_class, search)
            continue

        worker_class.perform_search(search_url=search, debug_mode=DEBUG)
        # Here we filter out net new ads using sets
        net_new_listings = worker_class.session_listings.difference(
            current_urls
        )
        
        main_logger.debug("Done with %s", search)
        main_logger.debug("Got %s net new listings", len(net_new_listings))
        if len(net_new_listings) == 0:
            main_logger.debug("No new listings from %s", search)
            continue
        
        if sys.argv[1] != "shadow":
            main_logger.debug("Sending new listings to telegram")
            for listing in net_new_listings:
                main_logger.debug(
                    "Sending an alert on %s, search class in %s",
                    listing, worker_class
                )
                r = telegram.send_message(f"New listing: \n{listing}")
                if r.status_code != 200:
                    main_logger.warning("Message failed for %s", listing)
        else:
            main_logger.debug(
                "SHADOW MODE, not sending tg messages"
            )
        
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
        new_data = new_data.drop_duplicates(subset=["ad_url"], keep="first")
        main_logger.debug("Net new data: %s\n", new_data)
        main_logger.debug("Appending results to the db")
        new_data.to_sql(name="seen_ads", con=db.conn,
                        if_exists="append", index=False)
    main_logger.debug("Done, tutto bene")


def choose_gateway(url: str, pararius: amst_re.ParariusGateway,
                   funda: amst_re.FundaGateway):
    worker_class = None
    
    if "pararius" in url:
        worker_class = pararius
    elif "funda" in url:
        worker_class = funda
    else:
        worker_class = ValueError(f"Can't decide on worker for url: {url}")
    
    return worker_class
   

try:
    main()
except Exception as e:
    main_logger.exception("Exception in main: %s", e)
finally:
    main_logger.debug("#################################")