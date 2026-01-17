import asyncio
import time
from datetime import timezone, datetime, timedelta
from typing import List, Optional, Dict

import pandas as pd
import yfinance as yf

from config.logger import logger
from models import Instrument, PriceHistoryDaily, PriceHistoryIntraday
from services.provider.base_provider import BaseMarketDataProvider
from utils.common_constants import DataIngestionFormat
from services.data.redis_mapping import get_redis_mapping_helper


class YahooFinanceProvider(BaseMarketDataProvider):
    def __init__(self):
        super().__init__(provider_code="YF")
        self.redis_mapper = get_redis_mapping_helper()
        self.symbol_map: Dict[str, str] = {}

    async def connect_websocket(self, symbols: list[str]):
        """Connect to Yahoo Finance WebSocket for live data using AsyncWebSocket."""
        try:
            # Load mappings
            try:
                self.symbol_map = await self.redis_mapper.get_all_p2i_mappings("YF")
                logger.info(f"Loaded {len(self.symbol_map)} symbol mappings for Yahoo")
            except Exception as e:
                logger.error(f"Failed to load symbol mappings: {e}")

            # Initialize AsyncWebSocket
            self.websocket_connection = yf.AsyncWebSocket()

            # Subscribe to symbols
            await self.websocket_connection.subscribe(symbols)
            self.subscribed_symbols.update(symbols)
            self.is_connected = True
            logger.info(f"Yahoo Finance connected with {len(symbols)} symbols")

            # Start the listener task
            self._listen_task = asyncio.create_task(self._run_listener())

        except Exception as e:
            self.is_connected = False
            logger.error(f"Error connecting Yahoo Finance WebSocket: {e}")
            raise

    async def _run_listener(self):
        """Run the WebSocket listener."""
        try:
            await self.websocket_connection.listen(self.message_handler)
        except Exception as e:
            logger.error(f"Yahoo Finance WebSocket listener stopped: {e}")
            self.is_connected = False

    async def message_handler(self, message: dict):
        """Handle incoming messages from Yahoo Finance WebSocket."""
        try:
            ts = int(time.time() * 1000)

            # print(f"Yahoo Finance message received: {message}")
            p_symbol = message["id"]
            final_symbol = self.symbol_map.get(p_symbol, p_symbol)

            data = DataIngestionFormat(
                symbol=final_symbol,
                price=message["price"],
                volume=message.get("day_volume", 0),
                timestamp=ts,
                provider_code="YF",
            )

            # Directly await since we are in the same loop
            await self.data_queue.add_data(data)

        except Exception as e:
            logger.error(f"Error handling Yahoo Finance message: {e}")

    async def disconnect_websocket(self):
        """Disconnect from Yahoo Finance WebSocket."""
        if self.websocket_connection:
            try:
                if self.subscribed_symbols:
                    # Depending on yfinance version, unsubscribe might be needed or just closing
                    # The library might strictly require a list, converting set to list
                    await self.websocket_connection.unsubscribe(
                        list(self.subscribed_symbols)
                    )

                # Cancel the listener task if running
                if hasattr(self, "_listen_task") and self._listen_task:
                    self._listen_task.cancel()
                    try:
                        await self._listen_task
                    except asyncio.CancelledError:
                        pass

                self.websocket_connection = None
                self.subscribed_symbols.clear()
                self.is_connected = False
                logger.info("Yahoo Finance WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting Yahoo Finance: {e}")

    async def subscribe_symbols(self, symbols: list[str]):
        """Add new symbols to existing Yahoo Finance subscription."""
        if not self.websocket_connection:
            logger.info(
                f"Yahoo Finance not connected. Connecting with {len(symbols)} symbols."
            )
            await self.connect_websocket(symbols)
            return

        if self.websocket_connection and symbols:
            try:
                await self.websocket_connection.subscribe(symbols)
                self.subscribed_symbols.update(symbols)
                logger.info(f"Yahoo Finance subscribed to {len(symbols)} new symbols")
            except Exception as e:
                logger.error(f"Error subscribing to Yahoo Finance symbols: {e}")

    async def unsubscribe_symbols(self, symbols: list[str]):
        """Remove symbols from Yahoo Finance subscription."""
        if self.websocket_connection and symbols:
            try:
                await self.websocket_connection.unsubscribe(symbols)
                self.subscribed_symbols.difference_update(symbols)
                logger.info(f"Yahoo Finance unsubscribed from {len(symbols)} symbols")
            except Exception as e:
                logger.error(f"Error unsubscribing from Yahoo Finance symbols: {e}")

    async def get_intraday_prices(
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "5m",
    ) -> dict[str, list[PriceHistoryIntraday]]:
        """
        Fetch intraday prices (5m interval) for the specified date range.
        """
        if not instruments:
            return {}

        symbols = [i.symbol for i in instruments]
        logger.info(f"Fetching intraday prices from YF for {len(symbols)} symbols")

        # Default to last 5 days if no dates provided
        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=5)

        try:
            # Run blocking yfinance download in a thread
            # period="5d" is the max for 5m interval in yfinance free tier usually
            # But we try to use start/end if provided.
            # Note: yfinance 5m data is limited to last 60 days.

            # YF download might fail with "No timezone found" if symbol is delisted or incorrect.
            # ignore_tz=False is default but sometimes issues arise.
            # Let's try to fetch with error handling.

            df = await asyncio.to_thread(
                yf.download,
                tickers=symbols,
                start=start_date,
                end=end_date,
                interval="5m",
                group_by="ticker",
                threads=True,
                progress=False,
                auto_adjust=False,
                ignore_tz=True,  # Attempt to fix "no timezone found" by ignoring tz alignment during download
            )

            if df.empty:
                logger.warning("YF returned empty dataframe for intraday prices")
                return {}

            result = {}

            # Handle single symbol vs multiple symbols structure
            if len(symbols) == 1:
                symbol = symbols[0]
                # Check if MultiIndex (happens if group_by='ticker' is respected even for 1 symbol)
                if isinstance(df.columns, pd.MultiIndex):
                    try:
                        symbol_df = df[symbol]
                        result[symbol] = self._parse_intraday_dataframe(
                            symbol_df, symbol
                        )
                    except KeyError:
                        # Fallback if symbol is not top level (maybe it's not MultiIndex but looks like it?)
                        # Or maybe columns are just Open, High...
                        result[symbol] = self._parse_intraday_dataframe(df, symbol)
                else:
                    result[symbol] = self._parse_intraday_dataframe(df, symbol)
            else:
                # Multi-index columns: (Ticker, OHLCV)
                for symbol in symbols:
                    try:
                        # Check if symbol is in columns (top level)
                        if symbol in df.columns:
                            symbol_df = df[symbol].dropna()
                            result[symbol] = self._parse_intraday_dataframe(
                                symbol_df, symbol
                            )
                    except Exception as e:
                        logger.error(f"Error parsing intraday data for {symbol}: {e}")

            return result

        except Exception as e:
            logger.error(f"Error fetching intraday prices from YF: {e}")
            return {}

    def _parse_intraday_dataframe(
        self, df: pd.DataFrame, symbol: str
    ) -> List[PriceHistoryIntraday]:
        records = []
        for index, row in df.iterrows():
            try:
                # index is Timestamp
                if isinstance(index, pd.Timestamp):
                    dt = index.to_pydatetime()
                else:
                    dt = pd.to_datetime(index).to_pydatetime()

                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)

                records.append(
                    PriceHistoryIntraday(
                        instrument_id=0,  # Placeholder, will be set by caller
                        datetime=dt,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        volume=int(row["Volume"]),
                        interval="5m",
                        resolve_required=False,
                    )
                )
            except Exception as e:
                continue
        return records

    async def get_daily_prices(
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "1d",
        duration_in_days: int = 365,
    ) -> dict[str, list[PriceHistoryDaily]]:
        """
        Fetch daily prices for the specified date range.
        """
        if not instruments:
            return {}

        symbols = [i.symbol for i in instruments]
        logger.info(f"Fetching daily prices from YF for {len(symbols)} symbols")

        # Default to last 1 year if no dates provided
        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=duration_in_days)

        try:
            # Run blocking yfinance download in a thread
            df = await asyncio.to_thread(
                yf.download,
                tickers=symbols,
                start=start_date,
                end=end_date,
                interval="1d",
                group_by="ticker",
                threads=True,
                progress=False,
                actions=True,  # To get Dividends and Splits if needed
                auto_adjust=False,
                ignore_tz=True,  # Fix for "no timezone found" errors
            )

            if df.empty:
                logger.warning("YF returned empty dataframe for daily prices")
                return {}

            result = {}

            if len(symbols) == 1:
                symbol = symbols[0]
                if isinstance(df.columns, pd.MultiIndex):
                    try:
                        symbol_df = df[symbol]
                        result[symbol] = self._parse_daily_dataframe(symbol_df, symbol)
                    except KeyError:
                        result[symbol] = self._parse_daily_dataframe(df, symbol)
                else:
                    result[symbol] = self._parse_daily_dataframe(df, symbol)
            else:
                for symbol in symbols:
                    try:
                        if symbol in df.columns:
                            symbol_df = df[symbol].dropna()
                            result[symbol] = self._parse_daily_dataframe(
                                symbol_df, symbol
                            )
                    except Exception as e:
                        logger.error(f"Error parsing daily data for {symbol}: {e}")

            return result

        except Exception as e:
            logger.error(f"Error fetching daily prices from YF: {e}")
            return {}

    def _parse_daily_dataframe(
        self, df: pd.DataFrame, symbol: str
    ) -> List[PriceHistoryDaily]:
        records = []
        for index, row in df.iterrows():
            try:
                if isinstance(index, pd.Timestamp):
                    dt = index.to_pydatetime()
                else:
                    dt = pd.to_datetime(index).to_pydatetime()

                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc)
                else:
                    dt = dt.replace(tzinfo=timezone.utc)

                # Handle potential missing columns
                adj_close = float(row["Adj Close"]) if "Adj Close" in row else None

                records.append(
                    PriceHistoryDaily(
                        instrument_id=0,  # Placeholder
                        datetime=dt,
                        open=float(row["Open"]),
                        high=float(row["High"]),
                        low=float(row["Low"]),
                        close=float(row["Close"]),
                        adj_close=adj_close,
                        volume=int(row["Volume"]),
                        resolve_required=False,
                    )
                )
            except Exception as e:
                continue
        return records
