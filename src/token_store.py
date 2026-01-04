import os
import json
import redis
from typing import Optional

class RedisTokenStore:
    def __init__(self):
        self.r = redis.Redis(
            host=os.getenv("REDIS_HOST", "127.0.0.1"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            username=os.getenv("REDIS_USER"),
            password=os.getenv("REDIS_PW"),
            decode_responses=True,
        )
        self.key = os.getenv("KIWOOM_TOKEN_KEY", "forecast:oauth:access_token")

    def set_token(self, token: str, ttl_seconds: int, token_type: str = "Bearer"):
        payload = json.dumps({"token": token, "token_type": token_type})
        self.r.set(self.key, payload, ex=max(60, ttl_seconds))

    def get_token(self) -> Optional[dict]:
        raw = self.r.get(self.key)
        if not raw:
            return None
        return json.loads(raw)
