import asyncio
from datetime import timezone
from typing import List

import pandas as pd
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

    async def get_intraday_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryIntraday]]:
        """
        Fetch intraday prices (5m interval) for the last 5 days.
        """
        if not instruments:
            return {}

        symbols = [i.symbol for i in instruments]
        logger.info(f"Fetching intraday prices from YF for {len(symbols)} symbols")

        try:
            # Run blocking yfinance download in a thread
            # period="5d" is the max for 5m interval in yfinance free tier usually
            df = await asyncio.to_thread(
                yf.download,
                tickers=symbols,
                period="5d",
                interval="5m",
                group_by="ticker",
                threads=True,
                progress=False,
                auto_adjust=False,
            )

            if df.empty:
                logger.warning("YF returned empty dataframe for intraday prices")
                return {}

            result = {}

            # Handle single symbol vs multiple symbols structure
            if len(symbols) == 1:
                symbol = symbols[0]
                # If single symbol, columns are just Open, High, etc.
                # We wrap it to treat uniformly
                result[symbol] = self._parse_intraday_dataframe(df, symbol)
            else:
                # Multi-index columns: (Ticker, OHLCV)
                for symbol in symbols:
                    try:
                        # Check if symbol is in columns (top level)
                        if symbol in df.columns:
                            symbol_df = df[symbol].dropna()
                            result[symbol] = self._parse_intraday_dataframe(symbol_df, symbol)
                    except Exception as e:
                        logger.error(f"Error parsing intraday data for {symbol}: {e}")

            return result

        except Exception as e:
            logger.error(f"Error fetching intraday prices from YF: {e}")
            return {}

    def _parse_intraday_dataframe(self, df: pd.DataFrame, symbol: str) -> List[PriceHistoryIntraday]:
        records = []
        for index, row in df.iterrows():
            try:
                # index is Timestamp
                dt = index.to_pydatetime()
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)

                records.append(
                    PriceHistoryIntraday(
                        instrument_id=0, # Placeholder, will be set by caller
                        datetime=dt,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        volume=int(row["Volume"]),
                        interval="5m",
                        resolve_required=False
                    )
                )
            except Exception as e:
                continue
        return records

    async def get_daily_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryDaily]]:
        """
        Fetch daily prices for the last 1 year.
        """
        if not instruments:
            return {}

        symbols = [i.symbol for i in instruments]
        logger.info(f"Fetching daily prices from YF for {len(symbols)} symbols")

        try:
            # Run blocking yfinance download in a thread
            df = await asyncio.to_thread(
                yf.download,
                tickers=symbols,
                period="1y",
                interval="1d",
                group_by="ticker",
                threads=True,
                progress=False,
                actions=True, # To get Dividends and Splits if needed
                auto_adjust=False,
            )

            if df.empty:
                logger.warning("YF returned empty dataframe for daily prices")
                return {}

            result = {}

            if len(symbols) == 1:
                symbol = symbols[0]
                result[symbol] = self._parse_daily_dataframe(df, symbol)
            else:
                for symbol in symbols:
                    try:
                        if symbol in df.columns:
                            symbol_df = df[symbol].dropna()
                            result[symbol] = self._parse_daily_dataframe(symbol_df, symbol)
                    except Exception as e:
                        logger.error(f"Error parsing daily data for {symbol}: {e}")

            return result

        except Exception as e:
            logger.error(f"Error fetching daily prices from YF: {e}")
            return {}

    def _parse_daily_dataframe(self, df: pd.DataFrame, symbol: str) -> List[PriceHistoryDaily]:
        records = []
        for index, row in df.iterrows():
            try:
                dt = index.to_pydatetime()
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)

                # Handle potential missing columns
                adj_close = float(row["Adj Close"]) if "Adj Close" in row else None

                records.append(
                    PriceHistoryDaily(
                        instrument_id=0, # Placeholder
                        datetime=dt,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        adj_close=adj_close,
                        volume=int(row["Volume"]),
                        resolve_required=False
                    )
                )
            except Exception as e:
                continue
        return records

