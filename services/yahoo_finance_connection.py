import yfinance as yf

from utils.common_constants import DataIngestionFormat


class YahooFinanceProvider:
    def __init__(self, callback=None):
        self.websocket_connection = None
        self.callback = callback

    def connect_websocket(self, symbols: list[str]):
        """Connect to Yahoo Finance WebSocket for live data."""
        self.websocket_connection = yf.WebSocket()
        self.websocket_connection.subscribe(symbols)

        if not self.callback:
            raise ValueError(
                "Callback function must be provided for handling messages."
            )
        self.websocket_connection.listen(self.message_handler)

    def message_handler(self, message: dict):
        """Handle incoming messages from Yahoo Finance WebSocket."""
        self.callback(
            DataIngestionFormat(
                symbol=message["symbol"],
                price=message["price"],
                volume=message["day_volume"],
                timestamp=int(message["time"]),
                provider_code="YF",
            )
        )

    def disconnect_websocket(self):
        """Disconnect from Yahoo Finance WebSocket."""
        if self.websocket_connection:
            self.websocket_connection.unsubscribe_all()
            self.websocket_connection = None

    # Intraday Data Fetching will be implemented here in future
    # Daily Data Fetching will be implemented here in future
