"""
Module implements a ratelimiter to be used by gateways
"""
import time
import threading


class ThreadingLimiter:
    """
    Usage: >> limiter = ThreadingLimiter(RPS, # of concurrent requests)
           >> with limiter:
           >>     # make request here.
    """
    def __init__(self, rps: float, concurrent_requests: int = None):
        """Constructor"""
        self.rps_lock = threading.Lock()
        self.interval_ms = 1000/rps
        self.last_request_time = time.time() * 1000

        self.concurrency = False
        self.sem = None
        if concurrent_requests is not None:
            self.concurrency = True
            self.sem = threading.Semaphore(concurrent_requests)

    def __enter__(self):
        if self.concurrency:
            self.sem.acquire()
        with self.rps_lock:
            time.sleep(
                max(
                    0.,
                    self.interval_ms - ((time.time() * 1000) - self.last_request_time)
                )/1000
            )
            self.last_request_time = time.time() * 1000

    def __exit__(self, exc_type, exc, tb):
        if self.concurrency:
            self.sem.release()