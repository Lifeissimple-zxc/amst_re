import base64
import logging
import re

import bs4
import requests

main_logger = logging.getLogger("main_logger")

class ProxyFetcher:
    def get_ssl_proxies(proxy_site):
        data = bs4.BeautifulSoup(requests.get(proxy_site).text, 'html.parser')
        proxy_table = data.find('table')
        proxy_rows = proxy_table.tbody.find_all('tr')
        proxies = []
        for row in proxy_rows:
            prox = str(
                row.find_all('td')[0].string #ip is first col
                + ':' +
                row.find_all('td')[1].string #port is second col
            )
            proxies.append(prox)
        return proxies

    def get_free_proxies(proxy_site, hdrs=None):
        proxies = []
        data = bs4.BeautifulSoup(markup=requests.get(url=proxy_site,
                                                     headers = hdrs).text,
                                features='html.parser')
        proxy_table = data.find(id = 'proxy_list')
        proxy_rows = proxy_table.tbody.find_all('tr')
        for row in proxy_rows:
            contents = row.find_all('td')
            ip_js = str(contents[0].script.decode_contents())
            try:
                ip_64 = re.findall('decode\("(.+)"\)', ip_js)[0]
                ip = base64.b64decode(ip_64).decode('utf-8')
                port = contents[1].span.string
                prox = f"{ip}:{port}"
                proxies.append(prox)
            except IndexError:
                continue
        return proxies