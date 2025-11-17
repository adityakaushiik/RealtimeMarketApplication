import yfinance as yf

from services.redis_helper import get_redis_helper


class YahooFinanceProvider:
    def __init__(self):
        self.websocket_connection = None
        self.redis_helper = get_redis_helper()

    def connect_websocket(self, symbols: list[str]):
        """Connect to Yahoo Finance WebSocket for live data."""
        self.websocket_connection = yf.WebSocket()
        self.websocket_connection.subscribe(symbols)
        self.websocket_connection.listen(self.message_handler)

    def message_handler(self, message: dict):
        symbol = message.get("id", "")
        channel = "stocks:" + symbol
        prices_dict = self.redis_helper.get_value("prices_dict")
        prices_dict[channel] = message
        print(
            f"Received message for {symbol}: {message}"
        )

    def disconnect_websocket(self):
        """Disconnect from Yahoo Finance WebSocket."""
        if self.websocket_connection:
            self.websocket_connection.unsubscribe_all()
            self.websocket_connection = None

    # Intraday Data Fetching will be implemented here in future
    # Daily Data Fetching will be implemented here in future
