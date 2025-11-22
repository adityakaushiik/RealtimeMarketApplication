from typing import Dict, Set

from fastapi import WebSocket


class WebSocketManager:
    def __init__(self):
        self.active_connections: Dict[WebSocket, Set[str]] = {}
        self.active_channels: Dict[str, int] = {}

    def connect(self, websocket: WebSocket):
        self.active_connections[websocket] = set()

    def subscribe(self, websocket: WebSocket, channel: str):
        if websocket in self.active_connections:
            self.active_connections[websocket].add(channel)
            if channel not in self.active_channels:
                self.active_channels[channel] = 0
            self.active_channels[channel] += 1

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            channels = self.active_connections[websocket].copy()
            for ch in channels:
                self.unsubscribe(websocket, ch)
            del self.active_connections[websocket]

    def unsubscribe(self, websocket: WebSocket, channel: str):
        if websocket in self.active_connections and channel in self.active_connections[websocket]:
            self.active_connections[websocket].discard(channel)
            self.active_channels[channel] -= 1
            if self.active_channels[channel] == 0:
                del self.active_channels[channel]

    def get_active_channels(self) -> Set[str]:
        return set(self.active_channels.keys())


websocket_manager = WebSocketManager()


def get_websocket_manager() -> WebSocketManager:
    return websocket_manager
