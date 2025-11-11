from typing import Dict, Set

from fastapi import WebSocket


class WebSocketManager:
    def __init__(self):
        self.active_connections: Dict[WebSocket, Set[str]] = {}

    def connect(self, websocket: WebSocket):
        self.active_connections[websocket] = set()

    def disconnect(self, websocket: WebSocket):
        self.active_connections.pop(websocket)


websocket_manager = WebSocketManager()


def get_websocket_manager() -> WebSocketManager:
    return websocket_manager
