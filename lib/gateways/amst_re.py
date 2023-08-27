"""
Implements gateways to fetch data from NL real estate data sources
"""
import atexit
import time
import random
import logging

import bs4
import requests

main_logger = logging.getLogger("main_logger")

class BaseGateway:
    """
    Implements methods common across data sources
    """
    def __init__(self):
        """Constructor of the class"""
        self.sesh = requests.session()
        atexit.register(self.sesh.close)

    def _get_html_page(self, url: str) -> tuple:
        """Performs a GET request to the url"""
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
        
    def fetch_page(self, url: str) -> tuple:
        """
        Fetches the page and parrses it to a bs4 object
        :return: tuple(bs4 object, error if any)
        """
        page, e = self._get_html_page(url)
        time.sleep(5)
        if e is not None:
            main_logger.warning(
                "Err is not none when fetching %s: %s", url, e
            )
        try:
            page_data = bs4.BeautifulSoup(markup=page, features="html.parser")
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
    def __init__(self, rental_listing_pattern: str, base_url: str):
        """
        Constuctor where we inhering from BaseGateway parent
        """
        super().__init__()
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
    
    def perform_search(self, search_url: str) -> bool:
        """
        Performs a search on one search url
        """
        main_logger.debug("Searching for %s", search_url)
        page_soup, e = self.fetch_page(url=search_url)
        if e is not None:
            main_logger.error("Failed to fetch %s", search_url)
            raise e
        # get all rentals
        self.get_all_rentals(page_soup=page_soup)
        # check if last
        next_p = self.get_next_page_link(page_soup=page_soup)
        if next_p is None:
            return
        main_logger.debug("Session listings at %s after searching %s",
                          len(self.session_listings), search_url)
        
        time.sleep(random.randint(1, 5))
        self.perform_search(f"{self.base_url}{next_p}")
        

