"""
Implements a simple gateway module to send messages to a tg chat
"""
import atexit
import logging

import requests

from lib.gateways.base import rps_limiter, my_retry_sync

main_logger = logging.getLogger("main_logger")

MSG_CHAR_LIMIT = 4096

class TelegramGateway:
    """
    Sends messages to telegram using a bot
    """
    def __init__(self, bot_secret: str, base_url: str,
                 chat_id: int, send_msg_endpoint: str,
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
        self.sesh = requests.session()
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
        exceptions=(Exception,), logger=main_logger,
        retries=5, delay=2
    )
    def send_message(self, msg_str: str) -> requests.Response:
        """
        Sends a message to the chat_id provided in the constructor
        :return: tuple(response object, err if any)
        """
        msg_data = {
            **self.base_message_data,
            **{"text": self._truncate_to_char_limit(msg=msg_str)}
        }
        with self.limiter:
            r = self.sesh.post(url=self.send_msg_url, data=msg_data)
        return r
        
