"""
Dhan market data provider implementation using websockets library directly.
Connects to Dhan's WebSocket API for real-time market data.

Findings about Dhan API:
1. Timestamp Format: Dhan sends timestamps in IST (Indian Standard Time) but as a Unix timestamp relative to UTC epoch.
   This means the timestamps are 5.5 hours (19800 seconds) ahead of UTC.
   Example: If it's 10:00 AM UTC, Dhan sends a timestamp that corresponds to 10:00 AM + 5.5 hours = 3:30 PM UTC.
   Fix: We subtract 19800 seconds from the received timestamp to get the correct UTC timestamp.

2. Symbol Mapping: Dhan uses numeric Security IDs (e.g., "1333" for HDFCBANK-EQ).
   The application needs to map these IDs back to internal symbols (e.g., "HDFCBANK") for storage and processing.
   This mapping is handled in ProviderManager, which now maintains a bidirectional map.

3. Data Format:
   - Ticker (Response Code 2): Contains LTP (Last Traded Price) and LTT (Last Traded Time). No volume.
   - Quote (Response Code 4): Contains LTP, LTT, Volume, etc.
   - Timestamps are 32-bit integers (seconds).
"""

import asyncio
import json
import struct
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import websockets
import requests

from config.logger import logger
from config.settings import get_settings
from models import Instrument, PriceHistoryDaily, PriceHistoryIntraday
from services.provider.base_provider import BaseMarketDataProvider
from utils.common_constants import DataIngestionFormat
from enum import IntEnum


class FeedRequestCode(IntEnum):
    CONNECT = 11
    DISCONNECT = 12
    SUBSCRIBE_TICKER = 15
    UNSUBSCRIBE_TICKER = 16
    SUBSCRIBE_QUOTE = 17
    UNSUBSCRIBE_QUOTE = 18
    SUBSCRIBE_FULL = 21
    UNSUBSCRIBE_FULL = 22
    SUBSCRIBE_DEPTH = 23
    UNSUBSCRIBE_DEPTH = 24


class FeedResponseCode(IntEnum):
    INDEX = 1
    TICKER = 2
    QUOTE = 4
    OI = 5
    PREV_CLOSE = 6
    MARKET_STATUS = 7
    FULL = 8
    DISCONNECT = 50


class DhanProvider(BaseMarketDataProvider):
    """Dhan market data provider using websockets library directly"""

    # Dhan API Constants
    WS_URL = "wss://api-feed.dhan.co"
    
    # Exchange Segment Map
    EXCHANGE_MAP = {
        0: "IDX_I",
        1: "NSE_EQ",
        2: "NSE_FNO",
        3: "NSE_CURRENCY",
        4: "BSE_EQ",
        5: "MCX_COMM",
        7: "NSE_EQ",
        8: "BSE_FNO",
        11: "NSE_EQ", # Added mapping for NSE
        12: "BSE_EQ", # Added mapping for BSE
    }
    
    # Reverse map for subscription
    EXCHANGE_MAP_REV = {v: k for k, v in EXCHANGE_MAP.items()}

    # Common mappings for convenience
    EXCHANGE_ALIAS = {
        "NSE": "NSE_EQ",
        "BSE": "BSE_EQ",
        "NFO": "NSE_FNO",
        "CDS": "NSE_CURRENCY",
        "MCX": "MCX_COMM",
    }
    
    REST_URL = "https://api.dhan.co/v2"

    def __init__(self, callback=None, provider_manager=None):
        super().__init__(provider_code="DHAN", callback=callback)
        self.provider_manager = provider_manager

        # Get credentials from settings
        settings = get_settings()
        self.client_id = settings.DHAN_CLIENT_ID
        self.access_token = settings.DHAN_ACCESS_TOKEN
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connection_task = None
        self._running = False
        
        # Rate Limiting
        self._last_request_time = 0
        self._request_interval = 0.2  # 5 requests per second max

        if not self.callback:
            raise ValueError(
                "Callback function must be provided for handling messages."
            )

    async def _make_request(self, endpoint: str, payload: dict) -> dict:
        """Helper to make async HTTP requests to Dhan API"""
        # Rate Limiting
        elapsed = time.time() - self._last_request_time
        if elapsed < self._request_interval:
            await asyncio.sleep(self._request_interval - elapsed)
        self._last_request_time = time.time()

        url = f"{self.REST_URL}{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "Accept": "application/json",
        }

        try:
            # Using asyncio.to_thread to run blocking requests in a separate thread
            response = await asyncio.to_thread(
                requests.post, url, json=payload, headers=headers
            )
            
            if response.status_code == 429:
                logger.warning("Dhan API Rate Limit Hit (429). Retrying after 1s...")
                await asyncio.sleep(1)
                return await self._make_request(endpoint, payload)

            if not response.ok:
                logger.error(f"Dhan API error: {response.status_code} - {response.text} - Payload: {payload}")

            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Dhan API request failed: {e}")
            return {}

    async def connect_websocket(self, symbols: list[str]):
        """Connect to Dhan WebSocket for live data."""
        if self._running:
            logger.warning("Dhan WebSocket is already running")
            return

        self._running = True
        self.subscribed_symbols.update(symbols)
        
        # Start the connection loop in background
        self._connection_task = asyncio.create_task(self._run_websocket_loop())
        
        # Add callback to log any exceptions
        def handle_connection_result(task):
            try:
                task.result()
            except asyncio.CancelledError:
                logger.warning("Dhan connection task cancelled")
            except Exception as e:
                logger.error(f"Dhan connection task failed with error: {e}", exc_info=True)
                self.is_connected = False
                self._running = False

        self._connection_task.add_done_callback(handle_connection_result)
        logger.info(f"Dhan provider connecting with {len(symbols)} symbols")

    async def _run_websocket_loop(self):
        """Main WebSocket loop handling connection, subscription and data processing"""
        url = f"{self.WS_URL}?version=2&token={self.access_token}&clientId={self.client_id}&authType=2"
        reconnect_delay = 5
        
        while self._running:
            try:
                logger.info("Connecting to Dhan WebSocket...")
                async with websockets.connect(url, ping_interval=30, ping_timeout=10) as websocket:
                    self.ws = websocket
                    self.is_connected = True
                    reconnect_delay = 5  # Reset delay on successful connection
                    logger.info("Dhan WebSocket connection established.")
                    
                    # Wait a moment to ensure connection is stable before subscribing
                    await asyncio.sleep(1)
                    
                    # Subscribe to initial symbols
                    if self.subscribed_symbols:
                        logger.info(f"Subscribing to {len(self.subscribed_symbols)} symbols...")
                        await self._subscribe_batch(list(self.subscribed_symbols))
                    
                    # Message loop
                    while self._running:
                        try:
                            message = await websocket.recv()
                            await self._process_message(message)
                        except websockets.ConnectionClosed as e:
                            logger.warning(f"Dhan WebSocket connection closed: {e.code} - {e.reason}")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
            
            except Exception as e:
                logger.error(f"Dhan WebSocket connection error: {e}")
            
            finally:
                self.is_connected = False
                self.ws = None
                
            if self._running:
                logger.info(f"Reconnecting to Dhan WebSocket in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)  # Exponential backoff up to 60s

    async def _subscribe_batch(self, symbols: list[str]):
        """Subscribe to a batch of symbols"""
        if not self.ws or not self.is_connected:
            logger.warning("Cannot subscribe: WebSocket is not connected")
            return

        instruments = self._prepare_instruments(symbols)
        if not instruments:
            logger.warning("No valid instruments to subscribe")
            return
        
        # Group by request code (15 for Ticker, 17 for Quote, 21 for Full)
        # Using 15 (Ticker) as requested
        request_code = FeedRequestCode.SUBSCRIBE_TICKER
        
        # Split into chunks of 100 (API limit)
        chunk_size = 100
        for i in range(0, len(instruments), chunk_size):
            batch = instruments[i:i + chunk_size]
            
            payload = {
                "RequestCode": request_code,
                "InstrumentCount": len(batch),
                "InstrumentList": [
                    {
                        "ExchangeSegment": ex_seg,
                        "SecurityId": sec_id
                    } for ex_seg, sec_id in batch
                ]
            }
            
            logger.info(f"Sending subscription payload: {json.dumps(payload)}")
            await self.ws.send(json.dumps(payload))
            logger.info(f"Sent subscription request for {len(batch)} symbols")

    def message_handler(self, message: dict):
        """
        Handle incoming WebSocket messages.
        Must normalize provider-specific format to DataIngestionFormat.
        
        Note: In this implementation, _process_message handles the binary parsing
        and calls the callback directly. This method is kept to satisfy the abstract base class.
        """
        pass

    async def _process_message(self, message: bytes):
        """Process binary message from Dhan"""
        try:
            # Parse header (8 bytes)
            # Byte 0: Feed Response Code
            # Byte 1-2: Message Length
            # Byte 3: Exchange Segment
            # Byte 4-7: Security ID
            
            if len(message) < 8:
                return

            # Correct unpacking based on documentation
            # Header: 1 byte code, 2 bytes length, 1 byte exchange, 4 bytes security_id
            response_code = struct.unpack('<B', message[0:1])[0]
            msg_len = struct.unpack('<H', message[1:3])[0]
            exchange_segment = struct.unpack('<B', message[3:4])[0]
            security_id = struct.unpack('<I', message[4:8])[0]
            
            # Map exchange segment code to string
            exchange_str = self.EXCHANGE_MAP.get(exchange_segment, str(exchange_segment))
            
            # Parse payload based on response code
            data = {}
            
            if response_code == FeedResponseCode.TICKER:
                # 9-12: LTP (Last Traded Price) (float32)
                # 13-16: LTT (Last Traded Time) (int32)
                ltp = struct.unpack('<f', message[8:12])[0]
                ltt = struct.unpack('<I', message[12:16])[0]
                
                data = {
                    "symbol": str(security_id),
                    "exchange": exchange_str,
                    "price": ltp,
                    "timestamp": ltt,
                    "volume": 0  # Ticker doesn't have volume
                }
                
            elif response_code == FeedResponseCode.QUOTE:
                # 9-12: LTP (Last Traded Price) (float32)
                # 13-14: Last Traded Qty (int16)
                # 15-18: LTT (Last Traded Time) (int32)
                # 19-22: ATP (Average Traded Price) (float32)
                # 23-26: Volume (int32)
                
                ltp = struct.unpack('<f', message[8:12])[0]
                ltt = struct.unpack('<I', message[14:18])[0]
                volume = struct.unpack('<I', message[22:26])[0]
                
                data = {
                    "symbol": str(security_id),
                    "exchange": exchange_str,
                    "price": ltp,
                    "timestamp": ltt,
                    "volume": float(volume)
                }
                
            elif response_code == FeedResponseCode.DISCONNECT:
                logger.warning(f"Received disconnect packet: {message}")
                return

            if data:
                # Send to callback
                # Convert timestamp to milliseconds if it's in seconds
                ts = int(data['timestamp'])

                # Dhan sends timestamps in IST (Indian Standard Time) but as a Unix timestamp relative to UTC epoch.
                # e.g. 10:00 AM IST is sent as timestamp(10:00 AM UTC).
                # We need to subtract 5.5 hours (19800 seconds) to get the true UTC timestamp.
                # 19800 seconds = 5 hours * 3600 + 30 minutes * 60
                ts = ts - 19800

                if ts < 10000000000: # Less than 10 billion, likely seconds
                    ts = ts * 1000

                self.callback(
                    DataIngestionFormat(
                        symbol=data['symbol'],
                        price=float(data['price']),
                        volume=float(data['volume']),
                        timestamp=ts,
                        provider_code="DHAN",
                    )
                )
                
        except Exception as e:
            logger.error(f"Error parsing binary message: {e}")

    def _prepare_instruments(self, symbols: list[str]) -> list[tuple]:
        """Convert symbols to Dhan subscription format"""
        instruments = []
        
        for symbol in symbols:
            try:
                exchange = "NSE_EQ"  # Default
                sec_id = symbol
                
                if ":" in symbol:
                    parts = symbol.split(":")
                    if len(parts) == 2:
                        exchange, sec_id = parts
                elif "-" in symbol:
                    sec_id = symbol.split("-")[0]
                
                # Normalize exchange
                exchange = exchange.upper()
                exchange = self.EXCHANGE_ALIAS.get(exchange, exchange)

                # Validate exchange is a valid string (e.g. NSE_EQ)
                if exchange not in self.EXCHANGE_MAP_REV:
                     logger.warning(f"Unknown exchange segment: {exchange} for symbol {symbol}")
                
                if not str(sec_id).isdigit():
                    logger.warning(f"Security ID {sec_id} is not numeric. Dhan API expects numeric Security IDs.")

                instruments.append((exchange, sec_id))
            except Exception as e:
                logger.error(f"Error preparing instrument for {symbol}: {e}")
                continue
        
        if not instruments:
            logger.warning("No valid instruments found to subscribe")
            
        return instruments

    def disconnect_websocket(self):
        """Disconnect from Dhan WebSocket."""
        self._running = False
        if self._connection_task:
            self._connection_task.cancel()
        self.is_connected = False
        logger.info("Dhan WebSocket disconnected")

    def subscribe_symbols(self, symbols: list[str]):
        """Add new symbols to subscription"""
        self.subscribed_symbols.update(symbols)
        if self.is_connected:
            asyncio.create_task(self._subscribe_batch(symbols))

    async def _unsubscribe_batch(self, symbols: list[str]):
        """Unsubscribe from a batch of symbols"""
        if not self.ws or not self.is_connected:
            return

        instruments = self._prepare_instruments(symbols)
        if not instruments:
            return
        
        # Group by request code (16 for Unsubscribe Ticker)
        request_code = FeedRequestCode.UNSUBSCRIBE_TICKER
        
        # Split into chunks of 100 (API limit)
        chunk_size = 100
        for i in range(0, len(instruments), chunk_size):
            batch = instruments[i:i + chunk_size]
            
            payload = {
                "RequestCode": request_code,
                "InstrumentCount": len(batch),
                "InstrumentList": [
                    {
                        "ExchangeSegment": ex_seg,
                        "SecurityId": sec_id
                    } for ex_seg, sec_id in batch
                ]
            }
            
            logger.info(f"Sending unsubscribe payload: {json.dumps(payload)}")
            await self.ws.send(json.dumps(payload))
            logger.info(f"Sent unsubscribe request for {len(batch)} symbols")

    def unsubscribe_symbols(self, symbols: list[str]):
        """Remove symbols from subscription"""
        self.subscribed_symbols.difference_update(symbols)
        if self.is_connected:
            asyncio.create_task(self._unsubscribe_batch(symbols))

    async def get_intraday_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryIntraday]]:
        """
        Fetch intraday price history for given instruments (last 3 days).
        """
        results = {}
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=3)
        
        # Format dates as required by Dhan API (YYYY-MM-DD HH:MM:SS)
        from_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        to_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

        print(from_date_str, to_date_str)

        for instrument in instruments:
            security_id = instrument.symbol
            if self.provider_manager:
                sid = await self.provider_manager.get_search_code(self.provider_code, instrument.symbol)
                if sid:
                    security_id = sid
                else:
                    logger.warning(f"Could not find securityId for {instrument.symbol} in Dhan provider")
                    continue

            # Determine exchange segment from instrument.exchange_id
            exchange_segment = self.EXCHANGE_MAP.get(instrument.exchange_id, "NSE_EQ")
            
            payload = {
                "securityId": str(security_id),
                "exchangeSegment": exchange_segment,
                "instrument": "EQUITY", # Default
                "interval": "5", # 5 minutes
                "oi": False,
                "fromDate": from_date_str,
                "toDate": to_date_str
            }
            
            # Rate limit before request
            await asyncio.sleep(0.2)
            
            data = await self._make_request("/charts/intraday", payload)
            
            if not data or "timestamp" not in data:
                continue
                
            history = []
            timestamps = data.get("timestamp", [])
            opens = data.get("open", [])
            highs = data.get("high", [])
            lows = data.get("low", [])
            closes = data.get("close", [])
            volumes = data.get("volume", [])
            
            for i in range(len(timestamps)):
                try:
                    # Dhan returns epoch timestamp (seconds or ms? API doc says "Epoch timestamp")
                    # Example: 1328845020 -> 2012-02-10... seems to be seconds.
                    ts = timestamps[i]
                    # Dhan sends timestamps in IST but as UTC epoch. Subtract 5.5 hours.
                    ts = ts - 19800
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    
                    history.append(
                        PriceHistoryIntraday(
                            instrument_id=instrument.id,
                            datetime=dt,
                            open=opens[i],
                            high=highs[i],
                            low=lows[i],
                            close=closes[i],
                            volume=volumes[i],
                            interval="5m",
                            resolve_required=False
                        )
                    )
                except Exception as e:
                    logger.error(f"Error parsing intraday data for {instrument.symbol}: {e}")
                    continue
            
            results[instrument.symbol] = history

        return results

    async def get_daily_prices(
        self, instruments: List[Instrument]
    ) -> dict[str, list[PriceHistoryDaily]]:
        """
        Fetch daily price history for given instruments (last 30 days).
        """
        results = {}
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        
        # Format dates as required by Dhan API (YYYY-MM-DD)
        from_date_str = start_date.strftime("%Y-%m-%d")
        to_date_str = end_date.strftime("%Y-%m-%d")

        for instrument in instruments:
            security_id = instrument.symbol
            if self.provider_manager:
                sid = await self.provider_manager.get_search_code(self.provider_code, instrument.symbol)
                if sid:
                    security_id = sid
                else:
                    logger.warning(f"Could not find securityId for {instrument.symbol} in Dhan provider")
                    continue

            exchange_segment = self.EXCHANGE_MAP.get(instrument.exchange_id, "NSE_EQ")
            
            payload = {
                "securityId": security_id,
                "exchangeSegment": exchange_segment,
                "instrument": "EQUITY",
                "expiryCode": 0,
                "oi": False,
                "fromDate": from_date_str,
                "toDate": to_date_str
            }
            
            data = await self._make_request("/charts/historical", payload)
            
            if not data or "timestamp" not in data:
                continue
                
            history = []
            timestamps = data.get("timestamp", [])
            opens = data.get("open", [])
            highs = data.get("high", [])
            lows = data.get("low", [])
            closes = data.get("close", [])
            volumes = data.get("volume", [])
            
            for i in range(len(timestamps)):
                try:
                    ts = timestamps[i]
                    # Dhan sends timestamps in IST but as UTC epoch. Subtract 5.5 hours.
                    ts = ts - 19800
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    
                    history.append(
                        PriceHistoryDaily(
                            instrument_id=instrument.id,
                            datetime=dt,
                            open=opens[i],
                            high=highs[i],
                            low=lows[i],
                            close=closes[i],
                            volume=volumes[i],
                            resolve_required=False
                        )
                    )
                except Exception as e:
                    logger.error(f"Error parsing daily data for {instrument.symbol}: {e}")
                    continue
            
            results[instrument.symbol] = history

        return results
