"""
Implements a simple gateway module to send messages to a tg chat
"""
import atexit
import logging

import requests
from typing import Optional

from lib.gateways.base import my_retry_sync, rps_limiter, ex

main_logger = logging.getLogger("main_logger")

MSG_CHAR_LIMIT = 4096

class TelegramGateway:
    """
    Sends messages to telegram using a bot
    """
    def __init__(self, bot_secret: str, base_url: str,
                 chat_id: int, log_chat_id: int,
                 send_msg_endpoint: str,
                 rps: float, concurrent_requests: int = None):
        """
        Constructor of the class
        """
        self.send_msg_url = self._prepare_updates_chat_url(
            bot_secret=bot_secret,
            base_url=base_url,
            send_msg_endpoint=send_msg_endpoint
        )
        self.limiter = rps_limiter.ThreadingLimiter(
            rps=rps, concurrent_requests=concurrent_requests
        )
        self.base_message_data = {"chat_id": chat_id}
        self.log_message_data = {"chat_id": log_chat_id}
        self.sesh = requests.session()
        # self.sesh.headers.update({"Content-Type": "application/json"})
        atexit.register(self.sesh.close)
        

    @staticmethod
    def _prepare_updates_chat_url(bot_secret: str, base_url: str,
                                  send_msg_endpoint: str) -> str:
        """
        Prepares a url for the chat where updates are sent
        """
        return f"{base_url}/bot{bot_secret}/{send_msg_endpoint}"
    
    @staticmethod
    def _truncate_to_char_limit(msg: str):
        return msg[:MSG_CHAR_LIMIT]
    
    @my_retry_sync.simple_async_retry(
        exceptions=(ex.CustomRetriableException,), logger=main_logger,
        retries=5, delay=2
    )
    def send_message(self, msg_str: str,
                     is_log: Optional[bool] = None) -> requests.Response:
        """
        Sends a message to the chat_id provided in the constructor
        :return: tuple(response object, err if any)
        """
        if is_log is None:
            is_log = False
        msg_text = self._truncate_to_char_limit(msg=msg_str)
        msg_data = {**self.base_message_data, **{"text": msg_text}}
        if is_log:
            msg_data = {**self.log_message_data, **{"text": msg_text}}
        
        main_logger.debug("sending a TG message: %s", msg_data)
        with self.limiter:
            r = self.sesh.post(url=self.send_msg_url, data=msg_data)
        main_logger.debug("TG response status: %s. Resp: %s",
                          r.status_code, r.text)
        if 500 < r.status_code <= 600:
            raise ex.CustomRetriableException(
                msg="Server error, worth retrying"
            )
        return r
        
