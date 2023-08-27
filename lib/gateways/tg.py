"""
Implements a simple gateway module to send messages to a tg chat
"""
import atexit
import logging

import requests

main_logger = logging.getLogger("main_logger")

class TelegramGateway:
    """
    Sends messages to telegram using a bot
    """
    def __init__(self, bot_secret: str, base_url: str,
                 chat_id: int, send_msg_endpoint: str):
        """
        Constructor of the class
        """
        self.send_msg_url = self._prepare_updates_chat_url(
            bot_secret=bot_secret,
            base_url=base_url,
            send_msg_endpoint=send_msg_endpoint
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
    
    def send_message(self, msg_str: str) -> requests.Response:
        """
        Sends a message to the chat_id provided in the constructor
        :return: tuple(response object, err if any)
        """
        msg_data = {
            **self.base_message_data, **{"text": msg_str}
        }
        return self.sesh.post(url=self.send_msg_url, data=msg_data)
        
