import base64
import logging
import re
from typing import Optional

import bs4
import requests

main_logger = logging.getLogger("main_logger")

class BaseProxyFetcher:
    """
    Stores repetitive code used by children fetchers
    """
    def __init__(self, url: str, headers: Optional[dict] = None):
        """
        Instantiates the class storing url within self
        """
        self.url = url
        self.headers = headers
    
    def fetch_proxy_page(self):
        """
        Makes a GET request to url
        """
        resp = requests.get(url=self.url, headers=self.headers)
        if (status := resp.status_code) != 200:
            e =  ValueError(f"Bad resp status: {status}")
            main_logger.error("Can't get proxies: %s", e)
            return None, e
        main_logger.debug("Got ok resp from %s url", self.url)
        return resp, None
    
    @staticmethod
    def response_to_soup(resp: requests.Response):
        """
        Converts response to a bs4 object
        """
        return bs4.BeautifulSoup(markup=resp.text, features="html.parser")

class SSLProxiesFetcher(BaseProxyFetcher):
    """
    Class fetches proxies from sslproxies.org
    """
    def __init__(self, url: str, headers: Optional[dict] = None):
        """
        Instantiates the class storing url within self
        """
        super().__init__(url=url, headers=headers)

    @staticmethod
    def parse_proxies_data(soup: bs4.BeautifulSoup):
        """
        Parses page contents from sslproxies.org
        """
        proxy_rows = soup.find("table").tbody.find_all("tr")
        proxies = []
        for row in proxy_rows:
            prox = str(
                row.find_all("td")[0].string #ip is first col
                + ":" +
                row.find_all("td")[1].string #port is second col
            )
            proxies.append(prox)
        main_logger.debug("Parsed proxy data")
        return proxies

    def get_proxies(self):
        """
        Encompasses all the logic for fetching proxies from sslproxies
        """
        resp, e = self.fetch_proxy_page()
        if e is not None:
            raise e
        soup = self.response_to_soup(resp=resp)
        main_logger.debug("Converted response to soup")
        proxies = self.parse_proxies_data(soup=soup)
        return proxies


class FreeProxiesFetcher(BaseProxyFetcher):
    """
    Class fetches proxies from free-proxy.cz
    """
    def __init__(self, url: str, headers: Optional[dict] = None):
        """
        Instantiates the class storing url & headers within self
        """
        super().__init__(url=url, headers=headers)

    @staticmethod
    def parse_proxies_data(soup: bs4.BeautifulSoup):
        """
        Parses page contents from sslproxies.org
        """
        proxies = []
        proxy_rows = soup.find(id="proxy_list").tbody.find_all("tr")
        for row in proxy_rows:
            contents = row.find_all("td")
            ip_js = str(contents[0].script.decode_contents())
            try:
                ip_64 = re.findall('decode\("(.+)"\)', ip_js)[0]
                ip = base64.b64decode(ip_64).decode("utf-8")
                port = contents[1].span.string
                prox = f"{ip}:{port}"
                proxies.append(prox)
            except IndexError:
                continue
        main_logger.debug("Parsed proxy data")
        return proxies
    
    def get_proxies(self):
        """
        Encompasses all the logic for fetching proxies from sslproxies
        """
        resp, e = self.fetch_proxy_page()
        if e is not None:
            raise e
        soup = self.response_to_soup(resp=resp)
        main_logger.debug("Converted response to soup")
        proxies = self.parse_proxies_data(soup=soup)
        return proxies
