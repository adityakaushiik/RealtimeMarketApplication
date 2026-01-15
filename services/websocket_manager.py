from typing import Dict, Set

from fastapi import WebSocket


class WebSocketManager:
    def __init__(self):
        # Maps WebSocket -> Set of subscribed channels
        self.active_connections: Dict[WebSocket, Set[str]] = {}
        # Maps Channel -> Set of subscribed WebSockets (Reverse index for fast broadcast)
        self.channel_subscribers: Dict[str, Set[WebSocket]] = {}
        # Callbacks for subscription changes
        self.callbacks: list[callable] = []

    def register_callback(self, callback: callable):
        self.callbacks.append(callback)

    def _notify_callbacks(self):
        for callback in self.callbacks:
            try:
                callback()
            except Exception as e:
                print(f"Error in websocket callback: {e}")

    def connect(self, websocket: WebSocket):
        self.active_connections[websocket] = set()

    def subscribe(self, websocket: WebSocket, channel: str):
        if websocket in self.active_connections:
            self.active_connections[websocket].add(channel)

            if channel not in self.channel_subscribers:
                self.channel_subscribers[channel] = set()
            self.channel_subscribers[channel].add(websocket)

            self._notify_callbacks()

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            # Copy to avoid runtime error during iteration
            channels = self.active_connections[websocket].copy()
            for ch in channels:
                self.unsubscribe(websocket, ch, notify=False)
            del self.active_connections[websocket]
            self._notify_callbacks()

    def unsubscribe(self, websocket: WebSocket, channel: str, notify: bool = True):
        if (
            websocket in self.active_connections
            and channel in self.active_connections[websocket]
        ):
            self.active_connections[websocket].discard(channel)

            if channel in self.channel_subscribers:
                self.channel_subscribers[channel].discard(websocket)
                if not self.channel_subscribers[channel]:
                    del self.channel_subscribers[channel]

            if notify:
                self._notify_callbacks()

    def get_active_channels(self) -> Set[str]:
        return set(self.channel_subscribers.keys())

    def get_subscribers(self, channel: str) -> Set[WebSocket]:
        """Get all WebSockets subscribed to a specific channel (O(1) lookup)."""
        return self.channel_subscribers.get(channel, set())


websocket_manager = WebSocketManager()


def get_websocket_manager() -> WebSocketManager:
    return websocket_manager
