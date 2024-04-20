"""
Module implements MISC funcs used in main scripts
"""
from lib.gateways import amst_re


def choose_gateway(url: str, pararius: amst_re.ParariusGateway,
                   funda: amst_re.FundaGateway):
    "Decides on the gw to use for a given url"
    worker_class = None
    if "pararius" in url:
        worker_class = pararius
        if not hasattr(worker_class, "proxy_list"):
            return ValueError("no value in running pararius withou proxies")
    elif "funda" in url:
        worker_class = funda
    else:
        worker_class = ValueError(f"Can't decide on worker for url: {url}")
    
    return worker_class