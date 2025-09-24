# rate_limiter.py
import time
from collections import defaultdict

class RateLimiter:
    def __init__(self, max_requests: int, timeout: float):
        self.max_requests = max_requests
        self.timeout = timeout
        self.requests = defaultdict(list)  # {client_id: [timestamps]}
    
    def allow(self, client_id: str) -> bool:
        now = time.time()
        timestamps = self.requests[client_id]

        # Remove expired timestamps
        self.requests[client_id] = [
            ts for ts in timestamps if now - ts < self.timeout
        ]
        
        if len(self.requests[client_id]) >= self.max_requests:
            return False
        
        self.requests[client_id].append(now)
        return True
