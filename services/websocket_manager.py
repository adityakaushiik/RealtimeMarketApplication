from typing import Dict, Set

from fastapi import WebSocket


class WebSocketManager:
    def __init__(self):
        self.active_connections: Dict[WebSocket, Set[str]] = {}
        self.active_channels: Set[str] = set()

    def connect(self, websocket: WebSocket):
        self.active_connections[websocket] = set()

    def subscribe(self, websocket: WebSocket, channel: str):
        if websocket in self.active_connections:
            self.active_connections[websocket].add(channel)
            self.active_channels.add(channel)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.pop(websocket)

    def unsubscribe(self, websocket: WebSocket, channel: str):
        if websocket in self.active_connections:
            self.active_connections[websocket].discard(channel)
            if not any(
                channel in channels for channels in self.active_connections.values()
            ):
                self.active_channels.discard(channel)

    def get_active_channels(self) -> Set[str]:
        return self.active_channels


websocket_manager = WebSocketManager()


def get_websocket_manager() -> WebSocketManager:
    return websocket_manager
