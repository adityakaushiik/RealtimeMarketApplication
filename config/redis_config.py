from typing import Dict, Set

import redis.asyncio as aioredis
from fastapi import WebSocket

active_connections: Dict[WebSocket, Set[str]] = {}
redis = aioredis.from_url("redis://localhost", decode_responses=True)
