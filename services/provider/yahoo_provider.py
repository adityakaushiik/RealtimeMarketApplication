from typing import List

import yfinance as yf

from config.logger import logger
from models import Instrument, PriceHistoryDaily, PriceHistoryIntraday
from services.provider.base_provider import BaseMarketDataProvider
from utils.common_constants import DataIngestionFormat


class YahooFinanceProvider(BaseMarketDataProvider):
    def __init__(self, callback=None):
        super().__init__(provider_code="YF", callback=callback)

    def connect_websocket(self, symbols: list[str]):
        """Connect to Yahoo Finance WebSocket for live data."""
        try:
            self.websocket_connection = yf.WebSocket()
            self.websocket_connection.subscribe(symbols)
            self.subscribed_symbols.update(symbols)

            if not self.callback:
                raise ValueError(
                    "Callback function must be provided for handling messages."
                )

            self.is_connected = True
            logger.info(f"Yahoo Finance connected with {len(symbols)} symbols")

            # Run the listener in a separate thread to avoid blocking the main initialization flow
            import threading

            self._listen_thread = threading.Thread(
                target=self.websocket_connection.listen, args=(self.message_handler,)
            )
            self._listen_thread.daemon = True
            self._listen_thread.start()

        except Exception as e:
            self.is_connected = False
            logger.error(f"Error connecting Yahoo Finance WebSocket: {e}")
            raise

    def message_handler(self, message: dict):
        """Handle incoming messages from Yahoo Finance WebSocket."""
        try:
            # print(f"Yahoo Finance message received: {message}")
            self.callback(
                DataIngestionFormat(
                    symbol=message["id"],
                    price=message["price"],
                    volume=message.get("day_volume", 0),
                    timestamp=int(message["time"]),
                    provider_code="YF",
                )
            )
        except Exception as e:
            logger.error(f"Error handling Yahoo Finance message: {e}")

    def disconnect_websocket(self):
        """Disconnect from Yahoo Finance WebSocket."""
        if self.websocket_connection:
            try:
                self.websocket_connection.unsubscribe_all()
                self.websocket_connection = None
                self.subscribed_symbols.clear()
                self.is_connected = False
                logger.info("Yahoo Finance WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Yahoo Finance: {e}")

    def subscribe_symbols(self, symbols: list[str]):
        """Add new symbols to existing Yahoo Finance subscription."""
        if self.websocket_connection and symbols:
            try:
                self.websocket_connection.subscribe(symbols)
                self.subscribed_symbols.update(symbols)
                logger.info(f"Yahoo Finance subscribed to {len(symbols)} new symbols")
            except Exception as e:
                logger.error(f"Error subscribing to Yahoo Finance symbols: {e}")

    def unsubscribe_symbols(self, symbols: list[str]):
        """Remove symbols from Yahoo Finance subscription."""
        if self.websocket_connection and symbols:
            try:
                self.websocket_connection.unsubscribe(symbols)
                self.subscribed_symbols.difference_update(symbols)
                logger.info(f"Yahoo Finance unsubscribed from {len(symbols)} symbols")
            except Exception as e:
                logger.error(f"Error unsubscribing from Yahoo Finance symbols: {e}")

    def get_intraday_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryIntraday]]:
        pass

    def get_daily_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryDaily]]:
        pass
