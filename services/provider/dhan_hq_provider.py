"""
Dhan market data provider implementation using official dhanhq library.

This is an alternative implementation to dhan_provider.py that uses the
official dhanhq library (v2.0.2+) instead of manual WebSocket handling.

NOTE: The dhanhq library's DhanFeed class is designed to run in the main thread
and blocks when calling run_forever(). This implementation uses a subprocess
approach to run it in isolation, or can be configured to use the library's
async methods directly.

Usage:
    Replace DhanProvider with DhanHQProvider in provider_manager.py
"""

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any, Callable, Dict
import pytz
import struct

import websockets

from dhanhq import dhanhq as DhanHQ
from dhanhq import marketfeed

from config.logger import logger
from config.settings import get_settings
from models import Instrument, PriceHistoryDaily, PriceHistoryIntraday
from services.provider.base_provider import BaseMarketDataProvider
from utils.common_constants import DataIngestionFormat


class DhanHQProvider(BaseMarketDataProvider):
    """
    Dhan market data provider using official dhanhq library.

    This implementation uses:
    - Custom async WebSocket connection based on dhanhq patterns
    - dhanhq.dhanhq for REST API calls (historical data)
    """

    # Exchange segment mapping (dhanhq constants)
    EXCHANGE_SEGMENTS = {
        "NSE_EQ": marketfeed.NSE,
        "BSE_EQ": marketfeed.BSE,
        "NSE_FNO": marketfeed.NSE_FNO,
        "BSE_FNO": marketfeed.BSE_FNO,
        "MCX_COMM": marketfeed.MCX,
        "NSE_CURRENCY": marketfeed.NSE_CURR,
        "BSE_CURRENCY": marketfeed.BSE_CURR,
        "IDX_I": marketfeed.IDX,
        # Aliases
        "NSE": marketfeed.NSE,
        "BSE": marketfeed.BSE,
        "NFO": marketfeed.NSE_FNO,
        "MCX": marketfeed.MCX,
    }

    WS_URL = "wss://api-feed.dhan.co"

    def __init__(self, callback: Callable = None, provider_manager: Any = None):
        super().__init__(provider_code="DHAN_HQ", callback=callback)
        self.provider_manager = provider_manager

        # Get credentials from settings
        settings = get_settings()
        self.client_id = settings.DHAN_CLIENT_ID
        self.access_token = settings.DHAN_ACCESS_TOKEN

        # DhanHQ REST API client
        self.dhan_client = DhanHQ(self.client_id, self.access_token)

        # WebSocket state
        self.ws = None
        self._running = False
        self._ws_task: Optional[asyncio.Task] = None
        self._instruments: List[tuple] = []

        # Track last processed data to detect duplicates
        self._last_tick_times: Dict[str, int] = {}

        # Map to store latest tick data for each symbol
        self.symbol_tick_map: Dict[str, Dict[str, Any]] = {}

        # Task for periodic printing
        self._print_task: Optional[asyncio.Task] = None

        # Stats
        self.stats_tick_count = 0
        self.stats_duplicate_count = 0

        if not self.callback:
            raise ValueError(
                "Callback function must be provided for handling messages."
            )

        logger.info(f"DhanHQProvider initialized with client_id: {self.client_id[:4]}***")

    def _prepare_instruments_for_feed(self, symbols: List[str]) -> List[tuple]:
        """Convert symbol list to format expected by DhanFeed."""
        instruments = []

        for symbol in symbols:
            try:
                exchange = "NSE_EQ"
                sec_id = symbol

                if ":" in symbol:
                    parts = symbol.split(":")
                    if len(parts) == 2:
                        exchange, sec_id = parts
                elif "-" in symbol:
                    sec_id = symbol.split("-")[0]

                exchange = exchange.upper()
                exchange_segment = self.EXCHANGE_SEGMENTS.get(exchange, marketfeed.NSE)

                if not str(sec_id).isdigit():
                    logger.warning(f"Security ID {sec_id} is not numeric.")
                    continue

                subscription_mode = marketfeed.Quote
                instruments.append((exchange_segment, str(sec_id), subscription_mode))

            except Exception as e:
                logger.error(f"Error preparing instrument for {symbol}: {e}")
                continue

        return instruments

    def connect_websocket(self, symbols: List[str]):
        """Connect to Dhan WebSocket using async approach."""
        if self._running:
            logger.warning("DhanHQ WebSocket is already running")
            return

        self._running = True
        self.subscribed_symbols.update(symbols)
        self._instruments = self._prepare_instruments_for_feed(symbols)

        if not self._instruments:
            logger.error("No valid instruments to subscribe")
            self._running = False
            return

        logger.info(f"DhanHQProvider connecting with {len(self._instruments)} instruments")

        # Start the WebSocket connection in the current event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule the coroutine
                self._ws_task = asyncio.create_task(self._run_websocket())
            else:
                # Run directly
                loop.run_until_complete(self._run_websocket())
        except RuntimeError:
            # No event loop, create one in a thread
            self._ws_thread = threading.Thread(
                target=self._run_ws_in_thread,
                daemon=True
            )
            self._ws_thread.start()

        self.is_connected = True
        logger.info("DhanHQProvider WebSocket connection initiated")

        # Start the periodic printing task
        self._print_task = asyncio.create_task(self._print_symbol_map_periodically())

    def _run_ws_in_thread(self):
        """Run WebSocket in a separate thread with its own event loop."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run_websocket())
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
        finally:
            loop.close()

    async def _run_websocket(self):
        """Main WebSocket connection and message handling loop."""
        # Use v2 URL format with authentication in query parameters (like original DhanProvider)
        url = f"{self.WS_URL}?version=2&token={self.access_token}&clientId={self.client_id}&authType=2"
        reconnect_delay = 5

        logger.info("Connecting to Dhan WebSocket (v2)...")

        while self._running:
            try:
                async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
                    self.ws = ws
                    logger.info("WebSocket connected (v2 auth via URL)")

                    # Wait for connection to stabilize
                    await asyncio.sleep(1)

                    # Subscribe to instruments using JSON format (v2 style)
                    await self._subscribe_v2(ws)

                    logger.info("Subscribed, waiting for data...")
                    reconnect_delay = 5  # Reset on success

                    # Receive messages
                    while self._running:
                        try:
                            message = await ws.recv()
                            self._process_message(message)
                        except websockets.ConnectionClosed as e:
                            logger.warning(f"WebSocket connection closed: {e}")
                            break

            except Exception as e:
                logger.error(f"WebSocket error: {e}")

            if self._running:
                logger.info(f"Reconnecting in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

        logger.info("WebSocket loop ended")

    async def _subscribe_v2(self, ws):
        """Subscribe to instruments using v2 JSON format."""
        import json

        # Group instruments by subscription code
        instruments_list = []
        for exchange_segment, security_id, subscription_code in self._instruments:
            # Map exchange segment number to string
            exchange_map = {
                0: "IDX_I",
                1: "NSE_EQ",
                2: "NSE_FNO",
                3: "NSE_CURRENCY",
                4: "BSE_EQ",
                5: "MCX_COMM",
                7: "BSE_CURRENCY",
                8: "BSE_FNO",
            }
            ex_seg_str = exchange_map.get(exchange_segment, "NSE_EQ")
            instruments_list.append({
                "ExchangeSegment": ex_seg_str,
                "SecurityId": security_id
            })

        # Use Quote subscription (17)
        payload = {
            "RequestCode": 17,  # SUBSCRIBE_QUOTE
            "InstrumentCount": len(instruments_list),
            "InstrumentList": instruments_list
        }

        logger.info(f"Sending v2 subscription: {json.dumps(payload)}")
        await ws.send(json.dumps(payload))
        logger.info(f"Subscribed to {len(instruments_list)} instruments")

    def _process_message(self, message: bytes):
        """Process incoming WebSocket message."""
        if len(message) < 8:
            return

        try:
            response_code = struct.unpack('<B', message[0:1])[0]

            if response_code == 2:  # Ticker
                self._process_ticker(message)
            elif response_code == 4:  # Quote
                self._process_quote(message)
            elif response_code == 8:  # Full
                self._process_quote(message)  # Similar processing
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _process_ticker(self, message: bytes):
        """Process ticker message."""
        try:
            exchange_segment = struct.unpack('<B', message[3:4])[0]
            security_id = struct.unpack('<I', message[4:8])[0]
            ltp = struct.unpack('<f', message[8:12])[0]
            ltt = struct.unpack('<I', message[12:16])[0]

            self._emit_tick(str(security_id), ltp, 0, ltt)
        except Exception as e:
            logger.error(f"Error processing ticker: {e}")

    def _process_quote(self, message: bytes):
        """Process quote message."""
        try:
            exchange_segment = struct.unpack('<B', message[3:4])[0]
            security_id = struct.unpack('<I', message[4:8])[0]
            ltp = struct.unpack('<f', message[8:12])[0]
            ltq = struct.unpack('<H', message[12:14])[0]
            ltt = struct.unpack('<I', message[14:18])[0]

            self._emit_tick(str(security_id), ltp, ltq, ltt)
        except Exception as e:
            logger.error(f"Error processing quote: {e}")

    def _emit_tick(self, symbol: str, price: float, volume: float, timestamp: int):
        """Emit tick to callback."""
        # Check for duplicate
        last_time = self._last_tick_times.get(symbol)
        if last_time == timestamp:
            self.stats_duplicate_count += 1
            return

        self._last_tick_times[symbol] = timestamp

        # Convert timestamp (IST to UTC)
        ts = timestamp - 19800
        if ts < 10000000000:
            ts = ts * 1000

        # Map security_id to internal symbol
        internal_symbol = self.provider_manager.get_internal_symbol_sync("DHAN_HQ", symbol) if self.provider_manager else symbol

        # Update the symbol tick map
        self.symbol_tick_map[internal_symbol] = {
            "price": float(price),
            "volume": float(volume),
            "timestamp": ts,
            "last_update": datetime.now(timezone.utc)
        }

        ingestion_data = DataIngestionFormat(
            symbol=internal_symbol,
            price=float(price),
            volume=float(volume),
            timestamp=ts,
            provider_code="DHAN_HQ",
        )

        self.callback(ingestion_data)
        self.stats_tick_count += 1

        if self.stats_tick_count % 500 == 0:
            logger.info(f"DhanHQProvider: {self.stats_tick_count} ticks received")

    async def _print_symbol_map_periodically(self):
        """Print the symbol tick map every 5 minutes."""
        while self._running:
            await asyncio.sleep(300)  # 5 minutes
            if not self._running:
                break
            logger.info("Printing symbol tick map:")
            for symbol, data in self.symbol_tick_map.items():
                logger.info(f"  {symbol}: price={data['price']}, volume={data['volume']}, timestamp={data['timestamp']}, last_update={data['last_update']}")

    def disconnect_websocket(self):
        """Disconnect from Dhan WebSocket."""
        logger.info("Disconnecting DhanHQProvider...")
        self._running = False

        if self._ws_task:
            self._ws_task.cancel()
            self._ws_task = None

        if self._print_task:
            self._print_task.cancel()
            self._print_task = None

        # Note: WebSocket will be closed by the context manager in _run_websocket
        self.ws = None

        self.is_connected = False
        logger.info("DhanHQProvider disconnected")

    def subscribe_symbols(self, symbols: List[str]):
        """Add new symbols to subscription."""
        new_symbols = set(symbols) - self.subscribed_symbols
        if not new_symbols:
            return

        self.subscribed_symbols.update(new_symbols)
        new_instruments = self._prepare_instruments_for_feed(list(new_symbols))
        self._instruments.extend(new_instruments)

        if self.ws and new_instruments:
            import json
            # Map exchange segment numbers to strings
            exchange_map = {
                0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO", 3: "NSE_CURRENCY",
                4: "BSE_EQ", 5: "MCX_COMM", 7: "BSE_CURRENCY", 8: "BSE_FNO",
            }
            instruments_list = []
            for exchange_segment, security_id, _ in new_instruments:
                ex_seg_str = exchange_map.get(exchange_segment, "NSE_EQ")
                instruments_list.append({
                    "ExchangeSegment": ex_seg_str,
                    "SecurityId": security_id
                })

            payload = {
                "RequestCode": 17,
                "InstrumentCount": len(instruments_list),
                "InstrumentList": instruments_list
            }

            asyncio.create_task(self.ws.send(json.dumps(payload)))
            logger.info(f"Subscribed to {len(new_instruments)} new symbols")

    def unsubscribe_symbols(self, symbols: List[str]):
        """Remove symbols from subscription."""
        self.subscribed_symbols.difference_update(symbols)

    def message_handler(self, message: dict):
        """Handle incoming WebSocket messages."""
        pass

    async def get_intraday_prices(
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = '5m',
    ) -> dict[str, list[PriceHistoryIntraday]]:
        """Fetch intraday price history using dhanhq REST API."""
        results = {}

        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=3)

        ist_tz = pytz.timezone('Asia/Kolkata')

        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        start_date_ist = start_date.astimezone(ist_tz)
        end_date_ist = end_date.astimezone(ist_tz)
        start_date_ist = start_date_ist - timedelta(minutes=5)
        end_date_ist = end_date_ist + timedelta(minutes=5)

        from_date_str = start_date_ist.strftime("%Y-%m-%d")
        to_date_str = end_date_ist.strftime("%Y-%m-%d")

        for instrument in instruments:
            try:
                security_id = instrument.symbol
                if self.provider_manager:
                    sid = await self.provider_manager.get_search_code("DHAN", instrument.symbol)
                    if sid:
                        security_id = sid

                data = await asyncio.to_thread(
                    self.dhan_client.intraday_minute_data,
                    security_id,
                    "NSE_EQ",
                    "EQUITY"
                )

                if not data or data.get('status') != 'success':
                    continue

                history = []
                chart_data = data.get('data', {})
                timestamps = chart_data.get('timestamp', [])
                opens = chart_data.get('open', [])
                highs = chart_data.get('high', [])
                lows = chart_data.get('low', [])
                closes = chart_data.get('close', [])
                volumes = chart_data.get('volume', [])

                for i in range(len(timestamps)):
                    ts = timestamps[i]
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    history.append(
                        PriceHistoryIntraday(
                            instrument_id=instrument.id,
                            datetime=dt,
                            open=opens[i] if i < len(opens) else None,
                            high=highs[i] if i < len(highs) else None,
                            low=lows[i] if i < len(lows) else None,
                            close=closes[i] if i < len(closes) else None,
                            volume=volumes[i] if i < len(volumes) else 0,
                            interval="5m",
                            resolve_required=False
                        )
                    )
                results[instrument.symbol] = history
            except Exception as e:
                logger.error(f"Error fetching intraday for {instrument.symbol}: {e}")

        return results

    async def get_daily_prices(
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = '1d',
    ) -> dict[str, list[PriceHistoryDaily]]:
        """Fetch daily price history using dhanhq REST API."""
        results = {}

        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=30)

        ist_tz = pytz.timezone('Asia/Kolkata')

        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        start_date_ist = start_date.astimezone(ist_tz)
        end_date_ist = end_date.astimezone(ist_tz)

        from_date_str = start_date_ist.strftime("%Y-%m-%d")
        to_date_str = end_date_ist.strftime("%Y-%m-%d")

        for instrument in instruments:
            try:
                security_id = instrument.symbol
                if self.provider_manager:
                    sid = await self.provider_manager.get_search_code("DHAN", instrument.symbol)
                    if sid:
                        security_id = sid

                data = await asyncio.to_thread(
                    self.dhan_client.historical_daily_data,
                    security_id,
                    "NSE_EQ",
                    "EQUITY",
                    0,
                    from_date_str,
                    to_date_str
                )

                if not data or data.get('status') != 'success':
                    continue

                history = []
                chart_data = data.get('data', {})
                timestamps = chart_data.get('timestamp', [])
                opens = chart_data.get('open', [])
                highs = chart_data.get('high', [])
                lows = chart_data.get('low', [])
                closes = chart_data.get('close', [])
                volumes = chart_data.get('volume', [])

                for i in range(len(timestamps)):
                    ts = timestamps[i]
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    history.append(
                        PriceHistoryDaily(
                            instrument_id=instrument.id,
                            datetime=dt,
                            open=opens[i] if i < len(opens) else None,
                            high=highs[i] if i < len(highs) else None,
                            low=lows[i] if i < len(lows) else None,
                            close=closes[i] if i < len(closes) else None,
                            volume=volumes[i] if i < len(volumes) else 0,
                            resolve_required=False
                        )
                    )
                results[instrument.symbol] = history
            except Exception as e:
                logger.error(f"Error fetching daily for {instrument.symbol}: {e}")

        return results

    def get_status(self) -> dict:
        """Get current status of this provider."""
        base_status = super().get_status()
        base_status.update({
            "implementation": "dhanhq library (custom websocket)",
            "tick_count": self.stats_tick_count,
            "duplicate_count": self.stats_duplicate_count,
            "feed_active": self._running,
        })
        return base_status

