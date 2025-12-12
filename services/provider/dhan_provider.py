"""
Dhan market data provider implementation.
Connects to Dhan's WebSocket API for real-time market data.
"""

import time
from typing import List

from dhanhq import marketfeed

from config.logger import logger
from config.settings import get_settings
from models import Instrument, PriceHistoryDaily, PriceHistoryIntraday
from services.provider.base_provider import BaseMarketDataProvider
from utils.common_constants import DataIngestionFormat


class DhanProvider(BaseMarketDataProvider):
    """Dhan market data provider using dhanhq SDK"""

    def __init__(self, callback=None, client_id: str = None, access_token: str = None):
        super().__init__(provider_code="DHAN", callback=callback)

        # Get credentials from settings or parameters
        settings = get_settings()
        self.client_id = client_id or getattr(
            settings, "DHAN_CLIENT_ID", "PLACEHOLDER_CLIENT_ID"
        )
        self.access_token = access_token or getattr(
            settings, "DHAN_ACCESS_TOKEN", "PLACEHOLDER_ACCESS_TOKEN"
        )

        if not self.callback:
            raise ValueError(
                "Callback function must be provided for handling messages."
            )

    def connect_websocket(self, symbols: list[str]):
        """Connect to Dhan WebSocket for live data."""
        try:
            # Initialize Dhan market feed
            self.websocket_connection = marketfeed.DhanFeed(
                client_id=self.client_id,
                access_token=self.access_token,
                instruments=self._prepare_instruments(symbols),
            )

            # Set up message handlers
            self.websocket_connection.on_connect = self._on_connect
            self.websocket_connection.on_message = self.message_handler
            self.websocket_connection.on_close = self._on_close
            self.websocket_connection.on_error = self._on_error

            # Connect to WebSocket
            self.websocket_connection.connect()

            self.subscribed_symbols.update(symbols)
            self.is_connected = True
            logger.info(f"Dhan provider connected with {len(symbols)} symbols")

        except Exception as e:
            self.is_connected = False
            logger.error(f"Error connecting Dhan WebSocket: {e}")
            raise

    def _prepare_instruments(self, symbols: list[str]) -> list[tuple]:
        """
        Convert symbol list to Dhan's instrument format.
        Dhan expects: [(exchange_segment, security_id), ...]

        For now, we'll parse symbols in format: "EXCHANGE:SYMBOL" or just "SYMBOL"
        You can adjust this based on your database format.
        """
        instruments = []
        for symbol in symbols:
            try:
                # Example format parsing - adjust based on your needs
                # If symbol is like "NSE:RELIANCE" or "RELIANCE-EQ"
                if ":" in symbol:
                    exchange, sec_id = symbol.split(":")
                    instruments.append((exchange, sec_id))
                elif "-" in symbol:
                    # Assume NSE format like "RELIANCE-EQ"
                    sec_id = symbol.split("-")[0]
                    instruments.append(("NSE_EQ", sec_id))
                else:
                    # Default to NSE equity
                    instruments.append(("NSE_EQ", symbol))
            except Exception as e:
                logger.warning(f"Could not parse Dhan symbol {symbol}: {e}")

        return instruments

    def message_handler(self, message: dict):
        """
        Handle incoming messages from Dhan WebSocket.
        Normalize Dhan's format to DataIngestionFormat.

        Dhan message format (example):
        {
            "type": "Ticker Data",
            "exchange_segment": "NSE_EQ",
            "security_id": "RELIANCE",
            "LTP": 2456.75,
            "LTT": "09:15:23",
            "volume": 125000,
            ...
        }
        """
        try:
            # Extract relevant fields from Dhan message
            # Adjust field names based on actual Dhan API response
            symbol = message.get("security_id", "")
            exchange = message.get("exchange_segment", "")

            # Reconstruct full symbol identifier
            full_symbol = f"{symbol}-EQ" if exchange == "NSE_EQ" else symbol

            # Get price (LTP = Last Traded Price)
            price = float(message.get("LTP", 0) or message.get("last_price", 0))

            # Get volume
            volume = float(message.get("volume", 0) or message.get("traded_volume", 0))

            # Get timestamp - convert to Unix timestamp if needed
            timestamp = message.get("timestamp")
            if not timestamp:
                # If no timestamp provided, use current time
                timestamp = int(time.time())
            elif isinstance(timestamp, str):
                # Parse time string if needed
                timestamp = int(time.time())  # Fallback to current time
            else:
                timestamp = int(timestamp)

            # Validate data
            if not symbol or price <= 0:
                logger.debug(f"Skipping invalid Dhan message: {message}")
                return

            # Send normalized data to callback
            self.callback(
                DataIngestionFormat(
                    symbol=full_symbol,
                    price=price,
                    volume=volume,
                    timestamp=timestamp,
                    provider_code="DHAN",
                )
            )

        except Exception as e:
            logger.error(f"Error handling Dhan message: {e}, message: {message}")

    def disconnect_websocket(self):
        """Disconnect from Dhan WebSocket."""
        if self.websocket_connection:
            try:
                self.websocket_connection.disconnect()
                self.websocket_connection = None
                self.subscribed_symbols.clear()
                self.is_connected = False
                logger.info("Dhan WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Dhan WebSocket: {e}")

    def subscribe_symbols(self, symbols: list[str]):
        """Add new symbols to existing Dhan subscription."""
        if self.websocket_connection and symbols:
            try:
                instruments = self._prepare_instruments(symbols)
                # Dhan SDK method to add instruments (adjust based on actual API)
                if hasattr(self.websocket_connection, "subscribe"):
                    self.websocket_connection.subscribe(instruments)
                elif hasattr(self.websocket_connection, "add_instruments"):
                    self.websocket_connection.add_instruments(instruments)

                self.subscribed_symbols.update(symbols)
                logger.info(f"Dhan subscribed to {len(symbols)} new symbols")
            except Exception as e:
                logger.error(f"Error subscribing to Dhan symbols: {e}")

    def unsubscribe_symbols(self, symbols: list[str]):
        """Remove symbols from Dhan subscription."""
        if self.websocket_connection and symbols:
            try:
                instruments = self._prepare_instruments(symbols)
                # Dhan SDK method to remove instruments (adjust based on actual API)
                if hasattr(self.websocket_connection, "unsubscribe"):
                    self.websocket_connection.unsubscribe(instruments)
                elif hasattr(self.websocket_connection, "remove_instruments"):
                    self.websocket_connection.remove_instruments(instruments)

                self.subscribed_symbols.difference_update(symbols)
                logger.info(f"Dhan unsubscribed from {len(symbols)} symbols")
            except Exception as e:
                logger.error(f"Error unsubscribing from Dhan symbols: {e}")

    def _on_connect(self):
        """Callback when connection is established"""
        self.is_connected = True
        logger.info("Dhan WebSocket connection established")

    def _on_close(self):
        """Callback when connection is closed"""
        self.is_connected = False
        logger.warning("Dhan WebSocket connection closed")

    def _on_error(self, error):
        """Callback when error occurs"""
        logger.error(f"Dhan WebSocket error: {error}")

    def get_intraday_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryIntraday]]:
        pass

    def get_daily_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryDaily]]:
        pass
