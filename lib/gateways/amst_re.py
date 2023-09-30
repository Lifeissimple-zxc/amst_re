"""
Implements gateways to fetch data from NL real estate data sources
"""
import atexit
import logging
import json
import re
from typing import Optional

import bs4
import requests

from lib.gateways.base import rps_limiter

main_logger = logging.getLogger("main_logger")

class BaseGateway:
    """
    Implements methods common across data sources
    """
    def __init__(self, rps: float, headers: dict = None,
                 concurrent_requests: int = None):
        """Constructor of the class"""
        self.sesh = requests.session()
        # Set headers if we have any
        if headers is not None:
            self.sesh.headers.update(headers)
        atexit.register(self.sesh.close)
        self.limiter = rps_limiter.ThreadingLimiter(
            rps=rps, concurrent_requests=concurrent_requests
        )

    def _get_html_page(self, url: str) -> tuple:
        """Performs a GET request to the url"""
        with self.limiter:
            r = self.sesh.get(url)
        main_logger.debug("Got status %s for %s", r.status_code, url)
        if r.status_code != 200:
            main_logger.warning(
                "Url %s got bad status %s", url, r.status_code
            )
        if 400 <= r.status_code < 500:
            msg = f"Bad request for {url}"
            e = ValueError(msg)
            main_logger.error(e)
            return None, e
        elif r.status_code >= 500:
            msg = f"Server error for {url}"
            e = ValueError(msg)
            main_logger.error(e)
            return None, e
        else:
            return r.text, None
        
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
    def __init__(self, rental_listing_pattern: str, base_url: str,
                 rps: float, headers: dict = None,
                 concurrent_requests: int = None):
        """
        Constuctor where we inhering from BaseGateway parent
        """
        super().__init__(rps=rps, headers=headers,
                         concurrent_requests=concurrent_requests)
        self.rental_listing_pattern = rental_listing_pattern
        self.base_url = base_url
        self.session_listings = set()

    def get_all_rentals(self, page_soup: bs4.BeautifulSoup):
        """
        Fetches all rentals from a pararius search resuts page
        """
        links = page_soup.find_all("a")
        for link in links:
            listing = f"{self.base_url}{link.get('href')}"
            if listing in self.session_listings:
                continue
            if self.rental_listing_pattern not in listing:
                continue
            main_logger.debug(
                "%s is net new a rental listing", listing
            )
            self.session_listings.add(listing)

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
    
    def perform_search(self, search_url: str,
                       debug_mode: Optional[bool] = None):
        """
        Performs a search on one search url
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
        # get all rentals
        self.get_all_rentals(page_soup=page_soup)
        main_logger.debug("Session listings at %s after searching %s",
                          len(self.session_listings), search_url)
        # check if last
        next_p = self.get_next_page_link(page_soup=page_soup)
        if next_p is None:
            return

        self.perform_search(f"{self.base_url}{next_p}")


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

    def get_all_rentals(self, page_soup: bs4.BeautifulSoup):
        """
        Fetches all rentals from a funda search results page
        """
        script_tag = page_soup.find_all("script", {"type": "application/ld+json"})
        main_logger.debug("Located script tag")
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
    
    def perform_search(self, search_url: str):
        """
        Performs a search on one search url (recursively reads different result pages)
        """
        main_logger.debug("Searching for %s", search_url)
        page_soup, e = self.fetch_page(url=search_url, features="html.parser")
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
        
        self.perform_search(next_url)




