"""
Base provider interface for all market data providers.
All providers (Yahoo Finance, Dhan, etc.) must implement this interface.
"""
from abc import ABC, abstractmethod
from typing import Callable, Optional, Set


class BaseMarketDataProvider(ABC):
    """Abstract base class for all market data providers"""

    def __init__(self, provider_code: str, callback: Optional[Callable] = None):
        """
        Initialize the provider.

        Args:
            provider_code: Unique code for this provider (e.g., 'YF', 'DHAN')
            callback: Function to call when new market data arrives
        """
        self.provider_code = provider_code
        self.callback = callback
        self.websocket_connection = None
        self.subscribed_symbols: Set[str] = set()
        self.is_connected = False

    @abstractmethod
    def connect_websocket(self, symbols: list[str]):
        """
        Connect to provider's WebSocket and subscribe to initial symbols.

        Args:
            symbols: List of provider-specific symbol codes to subscribe to
        """
        pass

    @abstractmethod
    def disconnect_websocket(self):
        """Disconnect from provider's WebSocket and cleanup resources."""
        pass

    @abstractmethod
    def subscribe_symbols(self, symbols: list[str]):
        """
        Add symbols to existing subscription.

        Args:
            symbols: List of provider-specific symbol codes to subscribe to
        """
        pass

    @abstractmethod
    def unsubscribe_symbols(self, symbols: list[str]):
        """
        Remove symbols from subscription.

        Args:
            symbols: List of provider-specific symbol codes to unsubscribe from
        """
        pass

    @abstractmethod
    def message_handler(self, message: dict):
        """
        Handle incoming WebSocket messages.
        Must normalize provider-specific format to DataIngestionFormat.

        Args:
            message: Raw message from provider's WebSocket
        """
        pass

    def get_status(self) -> dict:
        """Get current status of this provider"""
        return {
            "provider_code": self.provider_code,
            "connected": self.is_connected,
            "subscribed_count": len(self.subscribed_symbols),
            "symbols": list(self.subscribed_symbols),
        }

