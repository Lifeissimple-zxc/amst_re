"""
Main script for monitoring purchases
"""
import logging
import sys
import time
import uuid
from logging import config

import pandas as pd
import yaml

from lib import simple_db_wrapper, utils
from lib.deps import CREATE_SQL_STATEMENTS, MAIN_CFG, TG_GW
from lib.gateways import amst_re, google_sheets
from lib.gateways.base import proxy_fetcher

# Logging boilerplate
with open("config/logging_config.yaml") as _f:
    LOG_CFG = yaml.safe_load(_f)
config.dictConfig(LOG_CFG)
main_logger = logging.getLogger("main_logger")

RUN_UUID = str(uuid.uuid4())
DEBUG = bool(int(sys.argv[2]))
if not DEBUG:
    main_logger.setLevel(logging.INFO)

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
for stmt in CREATE_SQL_STATEMENTS:
    db.run_create_sql(stmt)
main_logger.debug("Prepared database")

proxies = None
if PROXIES == "ssl":
    proxies = ssl_proxies_fetcher.get_proxies()
    main_logger.debug("Fetched SSL proxies")
if PROXIES == "free":
    proxies = free_prx_fetcher.get_proxies()
    main_logger.debug("Fetched free proxies")
pararius = amst_re.ParariusGateway(proxy_list=proxies, **MAIN_CFG["pararius"])
funda = amst_re.FundaGateway(
    headers=MAIN_CFG["http_headers"],
    **MAIN_CFG["funda"]
)

sheets = google_sheets.GoogleSheetsGateway(**MAIN_CFG["google"]["init"])


def main():
    "Main logic of the parser"
    main_logger.debug("#################################")
    main_logger.debug("Starting new run")
    curr_data = pd.read_sql(sql="SELECT ad_url FROM seen_ads", con=db.conn)
    current_urls = set()
    if len(curr_data) > 0:
        current_urls = set(curr_data["ad_url"])
    main_logger.debug(
        "Read current data from the db, len is %s", len(current_urls)
    )
    new_data = pd.DataFrame()
    main_logger.debug("fetching search urls from google sheets")
    # fetch search URLS from google
    search_df, e = sheets.read_sheet(
        sheet_id=MAIN_CFG["google"]["sheet_id"],
        tab_name=MAIN_CFG["google"]["search_urls_tab"],
        as_df=True
    )
    if e is not None:
        raise e
    # Perform search
    for search in search_df["url"].to_list():
        parser = utils.choose_gateway(
            url=search, pararius=pararius, funda=funda
        )
        if isinstance(parser, ValueError):
            main_logger.warning(
                "Got error %s for %s", parser, search,
                extra={"skip_tg": True}
            )
            continue
        
        try:
            parser.perform_search(search_url=search, debug_mode=DEBUG)
        except amst_re.ZeroListingsFoundException as e:
            main_logger.warning("no listings found for %s after retries", search)
            continue
        net_new_listings = parser.session_listings.difference(current_urls)
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
                    listing, parser
                )
                r = TG_GW.send_message(f"New listing: \n{listing}")
                if r.status_code != 200:
                    main_logger.warning("Message failed for %s", listing)

        # Prepare an interim df
        temp_df = pd.DataFrame(columns=MAIN_CFG["df_schema"].keys())
        temp_df["ad_url"] = list(net_new_listings)
        temp_df["search_url"] = search
        temp_df["run_uuid"] = RUN_UUID
        temp_df["seen_on"] = int(time.time()*1000)
        main_logger.debug("Prepared interim df")
        new_data = pd.concat(objs=[new_data, temp_df],
                             ignore_index=True, sort=False)
    
    if len(new_data) > 0:
        new_data = new_data.astype(MAIN_CFG["df_schema"])
        new_data = new_data.drop_duplicates(subset=["ad_url"], keep="first")
        main_logger.debug("Net new data: %s\n", new_data)
        main_logger.debug("Appending results to the db")
        new_data.to_sql(name="seen_ads", con=db.conn,
                        if_exists="append", index=False)
    main_logger.debug("Done, tutto bene")

try:
    main()
except Exception as e:
    main_logger.error("Exception in main: %s", e, exc_info=1)
finally:
    main_logger.debug("#################################")