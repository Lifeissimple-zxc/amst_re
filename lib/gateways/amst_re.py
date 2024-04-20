"""
Implements gateways to fetch data from NL real estate data sources
"""
import atexit
import json
import logging
import re
from typing import Optional

import bs4
import requests
import retry

from lib.gateways.base import rps_limiter

main_logger = logging.getLogger("main_logger")

# Parsing mode constants
PARSING_MODE_RENT = 0
PARSING_MODE_BUY = 1
PARSING_MODES = {
    "rent": PARSING_MODE_RENT,
    "buy": PARSING_MODE_BUY
}

class ZeroListingsFoundException(ValueError):
    """
    Custom exception for triggering a retry decorator
    """
    def __init__(self, msg: str):
        super().__init__(msg)

class BaseGateway:
    """
    Implements methods common across data sources
    """
    def __init__(self, rps: float, headers: dict = None,
                 concurrent_requests: int = None,
                 proxy_list: Optional[list] = None):
        """Constructor of the class"""
        self.get_timeout = 10
        # Verify is false as long as proxies are not in use
        self.verify = True
        self.sesh = requests.session()
        if proxy_list is not None:
            self.proxy_list = iter(proxy_list)
        # Set headers if we have any
        if headers is not None:
            self.sesh.headers.update(headers)
        atexit.register(self.sesh.close)
        self.limiter = rps_limiter.ThreadingLimiter(
            rps=rps, concurrent_requests=concurrent_requests
        )

    def _set_sesh_proxy(self):
        """
        Update's proxies used by session
        """
        try:
            curr_prox = next(self.proxy_list)
            main_logger.debug("Updating sesh proxy to %s", curr_prox)
            self.sesh.proxies.update(
                {
                    "http": curr_prox,
                    "https": curr_prox 
                }
            )
        except StopIteration as e:
            main_logger.error("Ran out of proxies, stopping execution")
            raise e

    @staticmethod
    def _process_response(url: str, r: requests.Response):
        """
        Converts respone to a tuple of data, err form
        """
        main_logger.debug("Got status %s for %s", r.status_code, url)
        if r.status_code != 200:
            main_logger.warning(
                "Url %s got bad status %s", url, r.status_code
            )
        if 400 <= r.status_code < 500:
            msg = f"Bad request for {url}"
            e = ValueError(msg)
            main_logger.error(e)
            return r.text, e
        elif r.status_code >= 500:
            msg = f"Server error for {url}"
            e = ValueError(msg)
            main_logger.error(e)
            return r.text, e
        else:
            return r.text, None

    def _get_html_page(self, url: str) -> tuple:
        """Performs a GET request to the url accounting for proxies"""
        data, e = None, None
        try:
            with self.limiter:
                r = self.sesh.get(url, verify=self.verify, timeout=self.get_timeout)
                main_logger.debug("status code: %s", r.status_code)
            data, e = self._process_response(url=url, r=r)
            if e is None:
                return data, e
            if self.proxy_list is None:
                main_logger.warning(
                    "No proxies suppliled, can't perfrom recursive calls"
                )
                return data, e
            self._set_sesh_proxy()
            return self._get_html_page(url=url)
        except (requests.exceptions.Timeout,
                requests.exceptions.ProxyError,
                OSError) as e:
            self._set_sesh_proxy()
            return self._get_html_page(url=url)
        
    def fetch_page(self, url: str, features: str) -> tuple:
        """
        Fetches the page and parrses it to a bs4 object
        :return: tuple(bs4 object, error if any)
        """
        page, e = self._get_html_page(url)
        if e is not None:
            main_logger.warning(
                "Err is not none when fetching %s: %s", url, e
            )
        try:
            page_data = bs4.BeautifulSoup(markup=page, features=features)
            return page_data, None
        except Exception as e:
            main_logger.exception(
                "Can't parse page data from %s to bs4, this is bad.", url
            )
            return None, e
        

class ParariusGateway(BaseGateway):
    """
    Class fetches data from pararius.com
    """
    def __init__(self, rental_listing_pattern: str, base_urls: dict,
                 buy_listing_pattern: str, auth_url: str,
                 rps: float, headers: dict = None,
                 concurrent_requests: int = None,
                 proxy_list: Optional[list] = None):
        """
        Constuctor where we inhering from BaseGateway parent
        """
        super().__init__(rps=rps, headers=headers,
                         concurrent_requests=concurrent_requests,
                         proxy_list=proxy_list)
        self.rental_listing_pattern = rental_listing_pattern
        self.buy_listing_pattern = buy_listing_pattern
        self.base_urls = base_urls
        self.session_listings = set()
        self.auth_url = auth_url
        self._get_token()
        if hasattr(self, "proxy_list") and self.proxy_list is not None:
            self._set_sesh_proxy()
            self.verify = False

    def _get_token(self):
        "Fetches pararius auth token and stores within self.sesh"
        main_logger.debug(
            "cookie len before auth: %s",
            len(self.sesh.cookies.items())
        )
        try:
            r = self.sesh.get(url=self.auth_url, timeout=self.get_timeout)
        except Exception as e:
            main_logger.error("error pararius auth, listings will not be fetched: %s", e)  # noqa: E501
            return
        main_logger.debug("fetched auth cookie %s", r.cookies)
        self.sesh.cookies = r.cookies
        main_logger.debug("cookie set to %s", self.sesh.cookies)

    
    def get_all_listings(self, page_soup: bs4.BeautifulSoup,
                         mode: int, base_url: str):
        """
        Fetches all listings from a pararius search resuts page
        """
        call_results = 0
        pattern_to_check = (
            self.rental_listing_pattern if mode == PARSING_MODE_RENT
            else self.buy_listing_pattern
        )
        links = page_soup.find_all("a")
        for link in links:
            listing = f"{base_url}{link.get('href')}"
            if listing in self.session_listings:
                continue
            if pattern_to_check not in listing:
                main_logger.debug("%s does not meet %s pattern",
                                  listing, pattern_to_check)
                continue
            main_logger.debug(
                "%s is net new a rental listing", listing
            )
            self.session_listings.add(listing)
            call_results += 1
        return call_results

    @staticmethod
    def get_next_page_link(page_soup: bs4.BeautifulSoup):
        """Gets next page of the search if there is any"""
        # Kinda naive to locate by class, but should work
        next_page_el = page_soup.select_one(
            "li.pagination__item.pagination__item--next"
        )
        if next_page_el is None:
            main_logger.debug("Reached the last page of search")
            return
        return next_page_el.find("a").get("href")

    @retry.retry(exceptions=ZeroListingsFoundException, tries=50, delay=2)
    def perform_search(self, search_url: str, mode: int,
                       debug_mode: Optional[bool] = None):
        """
        Performs a search on one search url
        """
        if mode not in {PARSING_MODE_BUY, PARSING_MODE_RENT}:
            raise NotImplementedError("Unexpcted search mode")
        main_logger.debug("Attempting a search with mode %s", mode)
        if debug_mode is None:
            debug_mode = True
        main_logger.debug("Searching for %s with debug mode %s",
                          search_url, debug_mode)
        
        page_soup, e = self.fetch_page(url=search_url, features="html.parser")
        if debug_mode:
            main_logger.debug("Page soup for url %s: %s", search_url, page_soup) 
        if e is not None:
            main_logger.error("Failed to fetch %s", search_url)
            raise e
        
        base_url = (
            self.base_urls["rent"] if mode == PARSING_MODE_RENT
            else self.base_urls["buy"]
        )
        results = self.get_all_listings(page_soup=page_soup, mode=mode,
                                        base_url=base_url)
        if results == 0:
            main_logger.warning(
                "Did not locate listings for %s.", search_url
            )
            if hasattr(self, "proxy_list"):
                main_logger.warning("updating proxies")
                self._set_sesh_proxy()
            raise ZeroListingsFoundException(
                msg=f"Did not locate listings for {search_url}"
            )
        main_logger.debug("Session listings at %s after searching %s",
                          len(self.session_listings), search_url)
        # check if last
        next_p = self.get_next_page_link(page_soup=page_soup)
        if next_p is None:
            return
        # recursive call, we don't get here if the page was last
        self.perform_search(search_url=f"{base_url}{next_p}",
                            mode=mode, debug_mode=debug_mode)


class FundaGateway(BaseGateway):
    """
    Class fetches dat from funda.nl
    """
    def __init__(self, rps: float,
                 results_per_page: int,
                 next_page_pattern: str,
                 headers: dict = None,
                 concurrent_requests: int = None):
        super().__init__(rps=rps, headers=headers,
                         concurrent_requests=concurrent_requests)
        self.results_per_page = results_per_page
        self.next_page_pattern = next_page_pattern
        self.session_listings = set()
        if hasattr(self, "proxy_list") and self.proxy_list is not None:
            self._set_sesh_proxy()
            self.verify = False

    def get_all_rentals(self, page_soup: bs4.BeautifulSoup):
        """
        Fetches all rentals from a funda search results page
        """
        script_tag = page_soup.find_all("script", {"type": "application/ld+json"})
        main_logger.debug("Located script tag")
        try:
            json_data = json.loads(script_tag[0].contents[0])
            main_logger.debug("Loaded listings data to json")
            urls = set(
                [item["url"] for item in json_data["itemListElement"]]
                )
            
            res_count = len(urls)
            main_logger.debug("Parsed listings to a set")
            # Get net new urls and add to self
            self.session_listings = self.session_listings.union(urls)
            main_logger.debug("Saved listings within self")
            return res_count
        except IndexError:
            main_logger.debug("No listings on page")
            return 0
    
    def get_next_page_link(self, search_url: str):
        """Gets url of next page of the search"""
        match = re.findall(pattern=self.next_page_pattern,
                           string=search_url)
        # Check for an unexpected case
        if len(match) > 1:
            raise ValueError(
                f"More than one result for '{self.next_page_pattern}' in {search_url}"
            )
        if len(match) > 0:
            page = match[0]
            main_logger.debug("RE found a match: %s", match)
            clean_search_url = re.sub(pattern=self.next_page_pattern,
                                      string=search_url, repl="")
            next_url = f"{clean_search_url}&search_result={int(page)+1}"
        else:
            next_url = f"{search_url}&search_result=2"
        return next_url
    
    def _perform_search(self, search_url: str,
                        debug_mode: Optional[bool] = None):
        """
        Performs a search on one search url (recursively reads different result pages)
        """
        if debug_mode is None:
            debug_mode = False
        main_logger.debug("Searching for %s", search_url)
        page_soup, e = self.fetch_page(url=search_url, features="html.parser")
        if debug_mode:
            main_logger.debug("Page soup for url %s: %s", search_url, page_soup) 
        if e is not None:
            main_logger.error("Failed to fetch %s", search_url)
            raise e
        res_cnt = self.get_all_rentals(page_soup=page_soup)
        main_logger.debug("Session listings at %s after searching %s",
                          len(self.session_listings), search_url)
        # Base case
        if res_cnt < self.results_per_page:
            return
        # Defining a url for recursive call
        # First time we call a url, it does not have search_result
        next_url = self.get_next_page_link(search_url=search_url)
        
        self._perform_search(search_url=next_url, debug_mode=debug_mode)

    def perform_search(self, search_url: str, mode: int,
                       debug_mode: Optional[bool] = None):
        """
        Performs a search on one search url (recursively reads different result pages)
        """
        main_logger.debug("Search with %s mode", mode)
        self._perform_search(search_url=search_url, debug_mode=debug_mode)
       