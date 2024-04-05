"""
Module implements a custom exception to trigger retry logic
"""

class CustomRetriableException(Exception):
    """
    Custom exception class to differentiate cases worth triggering a retry
    """
    def __init__(self, msg: str, og_exception: Exception):
        "Instantiates the exception"
        super().__init__(msg)
        self.og_exception = og_exception