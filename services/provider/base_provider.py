"""
Base provider interface for all market data providers.
All providers (Yahoo Finance, Dhan, etc.) must implement this interface.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Set, List

from models import Instrument, PriceHistoryIntraday, PriceHistoryDaily


from services.data.data_queue import get_data_ingestion_queue_instance


class BaseMarketDataProvider(ABC):
    """Abstract base class for all market data providers"""

    def __init__(self, provider_code: str):
        """
        Initialize the provider.

        Args:
            provider_code: Unique code for this provider (e.g., 'YF', 'DHAN')
        """
        self.provider_code = provider_code
        self.data_queue = get_data_ingestion_queue_instance()
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
    async def subscribe_symbols(self, symbols: list[str]):
        """
        Add symbols to existing subscription.

        Args:
            symbols: List of provider-specific symbol codes to subscribe to
        """
        pass

    @abstractmethod
    async def unsubscribe_symbols(self, symbols: list[str]):
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

    @abstractmethod
    async def get_intraday_prices(
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "5m",
    ) -> dict[str, list[PriceHistoryIntraday]]:
        """
        Fetch intraday price history for given instruments.

        Args:
            instruments: List of Instrument objects to fetch data for
            start_date: Start datetime (inclusive)
            end_date: End datetime (inclusive)
            timeframe: Timeframe string (e.g., '5m')
        """
        pass

    @abstractmethod
    async def get_daily_prices(
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "1d",
    ) -> dict[str, list[PriceHistoryDaily]]:
        """
        Fetch daily price history for given instruments.

        Args:
            instruments: List of Instrument objects to fetch data for
            start_date: Start datetime (inclusive)
            end_date: End datetime (inclusive)
            timeframe: Timeframe string (e.g., '1d')
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
