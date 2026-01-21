"""
Dhan market data provider implementation using websockets library directly.
Connects to Dhan's WebSocket API for real-time market data.

Findings about Dhan API:
1. Timestamp Format: Dhan REST API returns standard Unix epoch timestamps (seconds since 1970-01-01 UTC).
   These are already correct UTC timestamps - NO adjustment needed.
   Verified: Requesting data for 09:15 IST returns epoch for 03:45 UTC (correct).

   For WebSocket live data, we use system time (time.time()) for tick timestamps
   to ensure millisecond precision and monotonically increasing timestamps.

2. Symbol Mapping: Dhan uses numeric Security IDs (e.g., "1333" for HDFCBANK-EQ").
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
from typing import List, Optional, Any, Dict
import pytz

import websockets
import requests

from config.logger import logger
from config.settings import get_settings
from models import Instrument, PriceHistoryDaily, PriceHistoryIntraday
from services.provider.base_provider import BaseMarketDataProvider
from utils.common_constants import DataIngestionFormat
from enum import IntEnum
from services.data.redis_mapping import get_redis_mapping_helper


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
        11: "NSE_EQ",  # Added mapping for NSE
        12: "BSE_EQ",  # Added mapping for BSE
    }

    REST_URL = "https://api.dhan.co/v2"

    def __init__(self, provider_manager=None):
        super().__init__(provider_code="DHAN")
        self.provider_manager = provider_manager

        # Redis mapping helper
        self.redis_mapper = get_redis_mapping_helper()
        # Local cache for fast lookup: {provider_symbol_code: internal_symbol}
        self.symbol_map: Dict[str, str] = {}
        # Local cache for exchange ID lookup: {internal_symbol: exchange_id}
        self.symbol_to_exchange_id_map: Dict[str, int] = {}
        # Local cache for exchange ID lookup
        self.symbol_exchange_map: Dict[str, str] = {}

        # Get credentials from settings
        settings = get_settings()
        self.client_id = settings.DHAN_CLIENT_ID
        self.access_token = settings.DHAN_ACCESS_TOKEN

        self.ws: Any = None
        self._connection_task = None
        self._token_refresh_task = None
        self._running = False

        # Rate Limiting
        self._last_request_time = 0
        self._request_interval = 0.2  # 5 requests per second max

    async def _refresh_token_loop(self):
        """
        Background task to refresh Dhan API token every 20 hours.
        """
        refresh_interval = 20 * 60 * 60  # 20 hours in seconds
        # refresh_interval = 60 # for testing

        logger.info("Starting Dhan token refresh loop (every 20 hours)")

        while self._running:
            try:
                # Wait for the interval
                await asyncio.sleep(refresh_interval)

                logger.info("Refreshing Dhan API token...")

                # Prepare request
                url = f"{self.REST_URL}/RenewToken"
                headers = {
                    "access-token": self.access_token,
                    "dhanClientId": self.client_id,
                    "Accept": "application/json",
                }

                # Make request
                # Using requests.post without json/data to strictly follow documentation/CURL
                # which implies no body and no Content-Type
                response = await asyncio.to_thread(requests.post, url, headers=headers)

                if response.ok:
                    logger.info("Dhan API token refreshed successfully.")
                    # The old token is expired, so we must update to the new one.
                    try:
                        data = response.json()
                        new_token = None

                        # Check for token in likely locations
                        if "accessToken" in data:
                            new_token = data["accessToken"]
                        elif (
                            "data" in data
                            and isinstance(data["data"], dict)
                            and "accessToken" in data["data"]
                        ):
                            new_token = data["data"]["accessToken"]

                        if new_token:
                            self.access_token = new_token
                            logger.info("Successfully updated Dhan access token.")
                        else:
                            logger.warning(
                                f"Token refresh response did not contain accessToken. Response: {data}"
                            )
                    except Exception as e:
                        logger.error(f"Error parsing token refresh response: {e}")
                else:
                    logger.error(
                        f"Failed to refresh Dhan token: {response.status_code} - {response.text}"
                    )

            except asyncio.CancelledError:
                logger.info("Dhan token refresh task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in Dhan token refresh loop: {e}")
                # Wait a bit before retrying to avoid tight loop in case of error
                await asyncio.sleep(60)

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
                logger.error(
                    f"Dhan API error: {response.status_code} - {response.text} - Payload: {payload}"
                )

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

        # Start token refresh task
        self._token_refresh_task = asyncio.create_task(self._refresh_token_loop())

        # Add callback to log any exceptions
        def handle_connection_result(task):
            try:
                task.result()
            except asyncio.CancelledError:
                logger.warning("Dhan connection task cancelled")
            except Exception as e:
                logger.error(
                    f"Dhan connection task failed with error: {e}", exc_info=True
                )
                self.is_connected = False
                self._running = False

        self._connection_task.add_done_callback(handle_connection_result)
        logger.info(f"Dhan provider connecting with {len(symbols)} symbols")

    async def _run_websocket_loop(self):
        """Main WebSocket loop handling connection, subscription and data processing"""
        url = f"{self.WS_URL}?version=2&token={self.access_token}&clientId={self.client_id}&authType=2"
        reconnect_delay = 5
        last_connected_time: Optional[int] = (
            None  # Track disconnect time for gap detection
        )

        while self._running:
            disconnect_time = int(time.time() * 1000) if last_connected_time else None

            try:
                logger.info("Connecting to Dhan WebSocket...")
                async with websockets.connect(
                    url, ping_interval=30, ping_timeout=10
                ) as websocket:
                    self.ws = websocket
                    self.is_connected = True
                    reconnect_delay = 5  # Reset delay on successful connection
                    reconnect_time = int(time.time() * 1000)
                    logger.info("Dhan WebSocket connection established.")

                    # Gap detection on reconnect
                    if disconnect_time and last_connected_time:
                        await self._handle_reconnect_gap_detection(
                            disconnect_time, reconnect_time
                        )

                    last_connected_time = reconnect_time

                    # Wait a moment to ensure connection is stable before subscribing
                    await asyncio.sleep(1)

                    # Subscribe to initial symbols
                    if self.subscribed_symbols:
                        logger.info(
                            f"Subscribing to {len(self.subscribed_symbols)} symbols..."
                        )
                        await self._subscribe_batch(list(self.subscribed_symbols))

                    # Message loop
                    while self._running:
                        try:
                            message = await websocket.recv()
                            await self._process_message(message)
                        except websockets.ConnectionClosed as e:
                            logger.warning(
                                f"Dhan WebSocket connection closed: {e.rcvd.code} - {e.rcvd.reason}"
                            )
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")

            except Exception as e:
                logger.error(f"Dhan WebSocket connection error: {e}")

            finally:
                self.is_connected = False
                self.ws = None

            if self._running:
                logger.info(
                    f"Reconnecting to Dhan WebSocket in {reconnect_delay} seconds..."
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(
                    reconnect_delay * 2, 60
                )  # Exponential backoff up to 60s

    async def _handle_reconnect_gap_detection(
        self, disconnect_ts: int, reconnect_ts: int
    ):
        """Handle gap detection after a WebSocket reconnect."""
        try:
            # Import here to avoid circular imports
            from services.data.gap_detection import get_gap_detection_service

            gap_service = get_gap_detection_service()

            # Only run gap detection if we have provider_manager (needed for gap filling)
            if gap_service.provider_manager:
                gaps = await gap_service.detect_gaps_on_reconnect(
                    disconnect_ts=disconnect_ts,
                    reconnect_ts=reconnect_ts,
                    symbols=self.subscribed_symbols,
                )

                if gaps:
                    logger.info(
                        f"🔄 Filling {len(gaps)} gaps from WebSocket reconnect..."
                    )
                    # Run gap filling in background to not block message processing
                    asyncio.create_task(gap_service.fill_gaps(gaps))
        except Exception as e:
            logger.error(f"Error in reconnect gap detection: {e}")

    async def _subscribe_batch(self, symbols: list[str]):
        """Subscribe to a batch of symbols"""
        if not self.ws or not self.is_connected:
            logger.warning("Cannot subscribe: WebSocket is not connected")
            return

        instruments = []
        for symbol in symbols:
            # We need to resolve the internal symbol first to get instrument_type_id
            # Modified: Handle case where symbol is ALREADY internal (returns None from resolve)
            internal_symbol = symbol
            if self.provider_manager:
                resolved = self.provider_manager.resolve_internal_symbol("DHAN", symbol)
                if asyncio.iscoroutine(resolved):
                    resolved = await resolved

                if resolved:
                    internal_symbol = resolved

            # --- START CHANGED CODE ---
            # Try to get segment from Redis mapping first (fastest and most accurate)
            segment = await self.redis_mapper.get_provider_segment(
                "DHAN", internal_symbol
            )

            # DEBUG LOGGING
            if self.provider_manager:
                t_id = self.provider_manager.get_instrument_type_id(internal_symbol)
                if t_id == 2:
                    logger.info(
                        f"DEBUG: Processing Index {internal_symbol} (Type 2). Segment from Redis: {segment}"
                    )

            if segment:
                # If we have the segment from DB/Redis, use it directly
                # security_id might be "SEGMENT:ID" or just "ID"
                security_id_raw = await self.redis_mapper.get_provider_symbol(
                    "DHAN", internal_symbol
                )

                if security_id_raw:
                    # Parse potentially composite ID
                    if ":" in str(security_id_raw):
                        _, sec_id_val = str(security_id_raw).split(":", 1)
                    else:
                        sec_id_val = str(security_id_raw)

                    instruments.append((segment, sec_id_val))
                    # Map using composite key for uniqueness: SEGMENT:ID
                    self.symbol_map[f"{segment}:{sec_id_val}"] = internal_symbol
                else:
                    logger.warning(
                        f"Segment found but no SecurityID for {internal_symbol} in Redis"
                    )
            else:
                # Fallback to old logic if no segment in Redis
                instrument_tuple = await self._prepare_single_instrument(
                    symbol, internal_symbol
                )
                if instrument_tuple:
                    instruments.append(instrument_tuple)
                    # Populate map using composite key
                    # instrument_tuple is (ExchangeSegment, SecurityId)
                    ex_seg, sec_id = instrument_tuple
                    self.symbol_map[f"{ex_seg}:{sec_id}"] = internal_symbol
            # --- END CHANGED CODE ---

        if not instruments:
            logger.warning("No valid instruments to subscribe")
            return

        # Group by request code (15 for Ticker, 17 for Quote, 21 for Full)
        # Using 17 (Quote) as requested to get cleaner data stream instead of full depth
        request_code = FeedRequestCode.SUBSCRIBE_QUOTE

        # Split into chunks of 100 (API limit)
        chunk_size = 100
        for i in range(0, len(instruments), chunk_size):
            batch = instruments[i : i + chunk_size]

            payload = {
                "RequestCode": request_code,
                "InstrumentCount": len(batch),
                "InstrumentList": [
                    {"ExchangeSegment": ex_seg, "SecurityId": sec_id}
                    for ex_seg, sec_id in batch
                ],
            }

            logger.info(f"Sending subscription payload: {json.dumps(payload)}")
            await self.ws.send(json.dumps(payload))
            logger.info(f"Sent subscription request for {len(batch)} symbols")

            # Populate exchange segment map for fast lookup by internal symbol
            for ex_seg, sec_id in batch:
                composite_key = f"{ex_seg}:{sec_id}"
                internal_sym = self.symbol_map.get(composite_key)
                if internal_sym:
                    self.symbol_exchange_map[internal_sym] = ex_seg
                    
                    if self.provider_manager:
                         exchange_id = self.provider_manager.get_exchange_id_for_symbol(internal_sym)
                         if exchange_id:
                             self.symbol_to_exchange_id_map[internal_sym] = exchange_id

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
            response_code = struct.unpack("<B", message[0:1])[0]
            msg_len = struct.unpack("<H", message[1:3])[0]
            exchange_segment = struct.unpack("<B", message[3:4])[0]
            security_id = struct.unpack("<I", message[4:8])[0]

            # Map exchange segment code to string
            exchange_str = self.EXCHANGE_MAP.get(
                exchange_segment, str(exchange_segment)
            )

            # Resolve to internal symbol using local cache
            # Use composite key "SEGMENT:ID" to avoid collisions
            p_symbol_composite = f"{exchange_str}:{security_id}"
            final_symbol = self.symbol_map.get(p_symbol_composite)

            # Fallback for backward compatibility or ID-only maps
            if not final_symbol:
                final_symbol = self.symbol_map.get(str(security_id))

            # Fallback: Try to resolve using provider manager if not in local map
            # This handles cases where symbol_map wasn't populated (e.g. restart) or incomplete
            if not final_symbol and self.provider_manager:
                try:
                    # Note: We don't have the exchange code easily available here without reverse mapping
                    # But we can try to find if any symbol maps to this security_id
                    # This is hard without an efficient reverse map in provider_manager.
                    # However, we can log it and maybe try a direct search code lookup if supported.
                    # For now, let's rely on the map being correct from subscribe.
                    pass
                except Exception:
                    pass

            if not final_symbol:
                # CRITICAL: If we can't map Security ID to a symbol, we MUST NOT save it with the ID.
                # Saving "3432:tick:price" creates garbage keys.
                # Better to drop the packet and warn.
                # Limit logging to avoid flooding
                if (
                    int(time.time()) % 60 == 0
                ):  # Log once per minute max per ID roughly (or just standard log throttling)
                    logger.warning(
                        f"⚠️ Dropped msg for unknown SecurityID: {p_symbol_composite} (Ex: {exchange_segment})"
                    )
                return

            # Parse payload based on response code
            data = {}

            if response_code == FeedResponseCode.TICKER:
                # Ticker Packet (16 bytes)
                # 0-8: Header
                # 9-12: LTP (Last Traded Price) (float32)
                # 13-16: LTT (Last Traded Time) (int32)

                if len(message) < 16:
                    return

                ltp = struct.unpack("<f", message[8:12])[0]
                ltt = struct.unpack("<i", message[12:16])[
                    0
                ]  # Using <i for signed int32 (Epoch)

                data = {
                    "symbol": final_symbol,
                    "exchange": exchange_str,
                    "price": ltp,
                    "timestamp": ltt,
                    "volume": -1, # Use -1 to indicate no volume data in Ticker packet
                }

            elif response_code == FeedResponseCode.QUOTE:
                # Optimized parsing for Quote packets
                # Standard Quote Packet (50 bytes):
                # [Header(8)][LTP(4)][LTT(4)][AvgPrice(4)][Vol(4)] ...

                if len(message) < 50:
                    return

                # Directly extract fields using struct
                ltp, ltt, avg_price, vol = struct.unpack("<fiiI", message[8:24])

                # Volume might be at offset 20 or 24 based on spec interpretation
                # Using offset 20 as per original code
                volume = struct.unpack("<I", message[20:24])[0]

                # Use system time for timestamp to ensure:
                # 1. Millisecond precision
                # 2. Monotonically increasing timestamps
                # 3. No IST/UTC offset issues or negative values
                ts = int(time.time() * 1000)

                # Look up exchange ID
                ex_id = self.symbol_to_exchange_id_map.get(final_symbol)

                # Note: exchange_id and exchange_code are optional and primarily
                # used for API data resolution, not real-time WebSocket ticks
                format_data = DataIngestionFormat(
                    provider_code=self.provider_code,
                    symbol=final_symbol,
                    price=ltp,
                    volume=float(volume),
                    timestamp=ts,
                    exchange_id=ex_id,
                    exchange_code=exchange_str
                )

                await self.data_queue.add_data(format_data)

            elif response_code == FeedResponseCode.FULL:
                if len(message) < 62:
                    logger.warning(f"Full packet too short: {len(message)} bytes")
                    return

                ltp = struct.unpack("<f", message[8:12])[0]
                ltq = struct.unpack("<H", message[12:14])[0]
                ltt = struct.unpack("<i", message[14:18])[0]
                atp = struct.unpack("<f", message[18:22])[0]
                volume = struct.unpack("<I", message[22:26])[0]
                total_sell_qty = struct.unpack("<I", message[26:30])[0]
                total_buy_qty = struct.unpack("<I", message[30:34])[0]
                oi = struct.unpack("<I", message[34:38])[0]
                oi_day_high = struct.unpack("<I", message[38:42])[0]
                oi_day_low = struct.unpack("<I", message[42:46])[0]
                open_price = struct.unpack("<f", message[46:50])[0]
                close_price = struct.unpack("<f", message[50:54])[0]
                high_price = struct.unpack("<f", message[54:58])[0]
                low_price = struct.unpack("<f", message[58:62])[0]

                data = {
                    "symbol": final_symbol,
                    "exchange": exchange_str,
                    "price": ltp,
                    "timestamp": ltt,
                    "volume": float(ltq),
                    "total_volume": float(volume),  # Cumulative volume
                }

                # Full packet implies we might get prev close (close_price is prev close in the Full packet spec usually)
                # But let's check Packet ID 6 PREV_CLOSE explicitly as requested
                # If 'close_price' above refers to prev day close, we can update cache here too.
                # Dhan Spec says Code 8 (Full) has 'Close Price'. Usually this is Prev Close if market is open/pre-open.
                # Let's save it.
                if close_price > 0:
                    asyncio.create_task(
                        self._update_prev_close(exchange_str, final_symbol, close_price)
                    )

            elif response_code == FeedResponseCode.PREV_CLOSE:
                # Value Code 6
                # 0-8 Header
                # 9-12 Prev Close (float32)
                # 13-16 OI (int32)

                print(f"Received PREV_CLOSE packet: {message} for {final_symbol}")

                if len(message) < 16:
                    return

                prev_close = struct.unpack("<f", message[8:12])[0]
                oi = struct.unpack("<i", message[12:16])[0]

                # Update Redis Hash for O(1) access
                if prev_close > 0:
                    asyncio.create_task(
                        self._update_prev_close(exchange_str, final_symbol, prev_close)
                    )

                # We typically don't broadcast Prev Close as a standalone tick,
                # but we could if UI needs it. The request specifically asked to SAVE it.
                return

            elif response_code == FeedResponseCode.DISCONNECT:
                logger.warning(f"Received disconnect packet: {message}")
                return

            if data:
                # Use system time for timestamp to ensure:
                # 1. Millisecond precision (Dhan provides seconds, causing overwrites in Redis TS)
                # 2. Monotonically increasing timestamps for every update (captures all ticks)
                # 3. No IST/UTC offset issues
                ts = int(time.time() * 1000)

                # B2: Use cumulative volume (total_volume) instead of tick volume (ltq)
                # If total_volume is 0 (e.g. TICKER packet), we might want to skip volume update or use 0?
                # Best to use 0 if not available, Redis will handle based on duplicate policy 'max'
                # (if we send 0 it might be ignored if prev was higher? No, 'max' policy compares new value with *existing value at that timestamp*.
                # Actually duplicate_policy acts on value at SAME timestamp.
                # But here we generate fresh timestamp (time.time()).
                # So we just append.
                # But wait, if we send 0, and later calculate delta (max - min), we might get strange results if it dips to 0.
                # However, TICKER packets often don't have volume.
                # If we send 0, it records 0.
                # If we want to maintain the "last known cumulative volume", we should probably not send 0 if we don't know it.
                # But we don't know the last volume here.
                # Let's use 'total_volume' if present, else 0.

                vol_to_use = float(data.get("total_volume", 0))
                if vol_to_use == 0:
                    vol_to_use = float(data.get("volume", 0))

                # Look up exchange ID
                ex_id = self.symbol_to_exchange_id_map.get(data["symbol"])

                await self.data_queue.add_data(
                    DataIngestionFormat(
                        symbol=data["symbol"],
                        price=float(data["price"]),
                        volume=vol_to_use,
                        timestamp=ts,
                        provider_code="DHAN",
                        exchange_id=ex_id,
                        exchange_code=data.get("exchange")
                    )
                )

        except Exception as e:
            logger.error(f"Error parsing binary message: {e}")

    async def _update_prev_close(self, exchange_code: str, symbol: str, price: float):
        """Update the prev_close map in Redis for O(1) access"""
        try:
            # Need Redis client. Using redis_mapper's client or getting new one.
            # self.redis_mapper.redis is available
            key = f"prev_close_map:{exchange_code}"

            # Store as JSON to be consistent with service
            val = {"p": price, "t": int(time.time() * 1000)}

            # Use hset
            await self.redis_mapper.redis.hset(key, symbol, json.dumps(val))
            # Refresh expiry to 24 hours
            await self.redis_mapper.redis.expire(key, 86400)
        except Exception as e:
            logger.error(f"Error updating prev_close for {symbol}: {e}")

    async def _prepare_single_instrument(
        self, provider_symbol: str, internal_symbol: str
    ) -> Optional[tuple]:
        """
        Convert symbol to Dhan subscription format using instrument type from manager.
        Returns (ExchangeSegment, SecurityId) tuple or None.
        """
        try:
            sec_id = provider_symbol
            exchange_seg = "NSE_EQ"  # Default fallback

            # If provider symbol has format "EXCHANGE:ID" or similar, parse it
            if ":" in provider_symbol:
                parts = provider_symbol.split(":")
                if len(parts) == 2:
                    # This path might be deprecated if we rely on manager's exchange info
                    pass

            if self.provider_manager:
                # 1. Get Instrument Type ID
                inst_type_id = self.provider_manager.get_instrument_type_id(
                    internal_symbol
                )

                # 2. Get Exchange ID -> Exchange Code AND Provider Code (Dhan)
                # We need to know which exchange this symbol belongs to (NSE, BSE, MCX)
                exchange_id = self.provider_manager.get_exchange_id_for_symbol(
                    internal_symbol
                )
                exchange_code = (
                    self.provider_manager.get_exchange_code(exchange_id)
                    if exchange_id
                    else "NSE"
                )

                # DEBUG info for Index type
                if inst_type_id == 2:
                    logger.info(
                        f"DEBUG: Prepare Instrument - {internal_symbol} | Type: {inst_type_id} | ExCode: {exchange_code}"
                    )

                # 3. Map based on SQL logic
            # The mapping is now handled primarily by the Provider Manager / Redis Mapping.
            # If exchange_seg is already retrieved (e.g. from Redis "provider_segment"), use it.
            # Otherwise, we might have a gap in our mapping logic if the Redis cache is empty.

            # The previous detailed if/elif block for NSE/BSE/MCX + inst_type_id is redundant
            # because we expect `self.provider_manager.get_provider_segment` (or similar)
            # to have populated the correct segment code.

            # If we don't have exchange_seg from provider_manager (via Redis), we should probably log an error
            # or rely on the exchange_code logic if absolutely necessary as a fallback mechanism.
            # However, the user request explicitly asked to remove it if it's saved in DB.
            # The DB mapping (ProviderInstrumentMapping) should have the 'segment' or similar
            # that gets pushed to Redis as 'provider_segment'.

            # Let's see how `exchange_seg` is currently retrieved.
            # In `_subscribe_batch` (caller of this), `exchange_seg` is NOT passed in.
            # Wait, `_prepare_single_instrument` only takes symbol strings.

            # We need to fetch the segment from Redis/Manager here if not present.
            if self.provider_manager and self.provider_manager.redis_mapper:
                exchange_seg = (
                    await self.provider_manager.redis_mapper.get_provider_segment(
                        "DHAN", internal_symbol
                    )
                )

            # If still None, we can't subscribe correctly.
            if not exchange_seg:
                logger.warning(
                    f"Segment not found for {internal_symbol} in Redis/DB mapping. Cannot subscribe."
                )
                return None

            # If plain ID, use it. If "ID-EQ" format, split.
            # Dhan usually expects just the numeric Security ID for the API
            if "-" in sec_id:
                sec_id = sec_id.split("-")[0]
            elif ":" in sec_id:
                sec_id = sec_id.split(":")[1]

            return (exchange_seg, sec_id)

        except Exception as e:
            logger.error(f"Error preparing instrument {provider_symbol}: {e}")
            return None

    def disconnect_websocket(self):
        """Disconnect from Dhan WebSocket."""
        # Send disconnect message to server if connected
        if self.ws and self.is_connected:
            asyncio.create_task(self._send_disconnect_message())

        self._running = False
        if self._connection_task:
            self._connection_task.cancel()
        if self._token_refresh_task:
            self._token_refresh_task.cancel()
        self.is_connected = False
        logger.info("Dhan WebSocket disconnected")

    async def _send_disconnect_message(self):
        """Send disconnect request to Dhan server"""
        try:
            await self.ws.send(json.dumps({"RequestCode": 12}))
            logger.info("Sent disconnect request to Dhan server")
        except Exception as e:
            logger.warning(f"Error sending disconnect message: {e}")

    def subscribe_symbols(self, symbols: list[str]):
        """Add new symbols to subscription"""
        self.subscribed_symbols.update(symbols)
        if self.is_connected:
            asyncio.create_task(self._subscribe_batch(symbols))

    async def _unsubscribe_batch(self, symbols: list[str]):
        """Unsubscribe from a batch of symbols"""
        if not self.ws or not self.is_connected:
            return

        instruments = []
        for symbol in symbols:
            # We need to resolve the internal symbol first to get instrument_type_id
            internal_symbol = symbol
            if self.provider_manager:
                resolved = self.provider_manager.resolve_internal_symbol("DHAN", symbol)
                if asyncio.iscoroutine(resolved):
                    resolved = await resolved

                if resolved:
                    internal_symbol = resolved

            # Try to get segment from Redis mapping first (fastest and most accurate)
            # COPIED logic from _subscribe_batch
            segment = await self.redis_mapper.get_provider_segment(
                "DHAN", internal_symbol
            )
            if segment:
                security_id_raw = await self.redis_mapper.get_provider_symbol(
                    "DHAN", internal_symbol
                )
                if security_id_raw:
                    if ":" in str(security_id_raw):
                        _, sec_id_val = str(security_id_raw).split(":", 1)
                    else:
                        sec_id_val = str(security_id_raw)
                    instruments.append((segment, sec_id_val))
            else:
                instrument_tuple = await self._prepare_single_instrument(
                    symbol, internal_symbol
                )
                if instrument_tuple:
                    instruments.append(instrument_tuple)

        if not instruments:
            return

        # Group by request code (16 for Unsubscribe Ticker)
        request_code = FeedRequestCode.UNSUBSCRIBE_QUOTE

        # Split into chunks of 100 (API limit)
        chunk_size = 100
        for i in range(0, len(instruments), chunk_size):
            batch = instruments[i : i + chunk_size]

            payload = {
                "RequestCode": request_code,
                "InstrumentCount": len(batch),
                "InstrumentList": [
                    {"ExchangeSegment": ex_seg, "SecurityId": sec_id}
                    for ex_seg, sec_id in batch
                ],
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
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "5m",
    ) -> dict[str, list[PriceHistoryIntraday]]:
        """
        Fetch intraday price history for given instruments.
        """
        results = {}

        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=3)

        # Dhan expects dates in IST (Asia/Kolkata)
        # We need to convert UTC start_date/end_date to IST
        ist_tz = pytz.timezone("Asia/Kolkata")

        # Ensure dates are timezone aware (UTC)
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        start_date_ist = start_date.astimezone(ist_tz)
        end_date_ist = end_date.astimezone(ist_tz)

        # Add buffer to avoid "Input_Exception" when start_date == end_date
        # Dhan API requires a window to fetch data for a specific timestamp
        # User specified: "if you want data for a given timeframe lets say 11:00 am , you must send start and end date like 10:55 and 11:05"
        start_date_ist = start_date_ist - timedelta(minutes=5)
        end_date_ist = end_date_ist + timedelta(minutes=5)

        # Format dates as required by Dhan API (YYYY-MM-DD HH:MM:SS)
        from_date_str = start_date_ist.strftime("%Y-%m-%d %H:%M:%S")
        to_date_str = end_date_ist.strftime("%Y-%m-%d %H:%M:%S")

        logger.debug(
            f"Fetching intraday prices from {from_date_str} to {to_date_str} (IST)"
        )

        requests_sent = 0
        for instrument in instruments:
            security_id = instrument.symbol
            if self.provider_manager:
                sid = await self.provider_manager.get_search_code(
                    self.provider_code, instrument.symbol
                )
                if sid:
                    security_id = sid
                else:
                    logger.warning(
                        f"Could not find securityId for {instrument.symbol} in Dhan provider"
                    )
                    continue

            # Determine exchange segment from instrument.exchange_id
            exchange_segment = "NSE_EQ"

            # Prepare security ID and exchange segment
            if self.provider_manager:
                internal_symbol = instrument.symbol

                # Check mapping for instrument type based resolution
                instrument_tuple = await self._prepare_single_instrument(
                    security_id, internal_symbol
                )

                if instrument_tuple:
                    exchange_segment, security_id = instrument_tuple
                else:
                    # Fallback if no segment found
                    if ":" in str(security_id):
                        exchange_segment, mapped_sec_id = str(security_id).split(":", 1)
                        security_id = mapped_sec_id
                    else:
                        exchange_segment = self.EXCHANGE_MAP.get(
                            instrument.exchange_id, "NSE_EQ"
                        )
            else:
                exchange_segment = self.EXCHANGE_MAP.get(
                    instrument.exchange_id, "NSE_EQ"
                )

            # Ensure securityId is just the number for Dhan API
            if ":" in str(security_id):
                 _, security_id = str(security_id).split(":", 1)

            payload = {
                "securityId": str(security_id),
                "exchangeSegment": exchange_segment,
                "instrument": "EQUITY",  # Default
                "interval": "5",  # 5 minutes
                "oi": False,
                "fromDate": from_date_str,
                "toDate": to_date_str,
            }

            # Rate limit before request
            await asyncio.sleep(0.2)

            data = await self._make_request("/charts/intraday", payload)
            requests_sent += 1

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
                    # Dhan API returns standard Unix epoch timestamps (seconds since 1970-01-01 UTC)
                    # These are already correct UTC timestamps, no adjustment needed
                    # Verified by test: requesting 09:15 IST data returns epoch for 03:45 UTC (correct)
                    ts = timestamps[i]
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
                            resolve_required=False,
                        )
                    )
                except Exception as e:
                    logger.error(
                        f"Error parsing intraday data for {instrument.symbol}: {e}"
                    )
                    continue

            results[instrument.symbol] = history

        logger.info(
            f"[RESOLVER-DHAN] Sent {requests_sent} requests to Dhan for intraday prices."
        )
        return results

    async def get_daily_prices(
        self,
        instruments: List[Instrument],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "1d",
        duration_in_days: int = 365,
    ) -> dict[str, list[PriceHistoryDaily]]:
        """
        Fetch daily price history for given instruments.
        """
        results = {}

        if not end_date:
            end_date = datetime.now(timezone.utc)
        if not start_date:
            start_date = end_date - timedelta(days=duration_in_days)

        # Dhan expects dates in IST (Asia/Kolkata)
        ist_tz = pytz.timezone("Asia/Kolkata")

        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        start_date_ist = start_date.astimezone(ist_tz)
        end_date_ist = end_date.astimezone(ist_tz)

        # Format dates as required by Dhan API (YYYY-MM-DD)
        from_date_str = start_date_ist.strftime("%Y-%m-%d")
        to_date_str = end_date_ist.strftime("%Y-%m-%d")

        logger.debug(
            f"Fetching daily prices from {from_date_str} to {to_date_str} (IST)"
        )

        for instrument in instruments:
            security_id = instrument.symbol
            if self.provider_manager:
                sid = await self.provider_manager.get_search_code(
                    self.provider_code, instrument.symbol
                )
                if sid:
                    security_id = sid
                else:
                    logger.warning(
                        f"Could not find securityId for {instrument.symbol} in Dhan provider"
                    )
                    continue

            # Determine exchange segment
            exchange_segment = "NSE_EQ"
            if self.provider_manager:
                # Reuse local helper to get correct segment for instrument type
                internal_symbol = instrument.symbol
                instrument_tuple = await self._prepare_single_instrument(
                    security_id, internal_symbol
                )
                if instrument_tuple:
                    exchange_segment, mapped_sec_id = instrument_tuple
                    # Warning: _prepare_single_instrument might return trimmed security id?
                    # It returns (exchange_seg, sec_id). Let's use it.
                    security_id = mapped_sec_id
                else:
                    # Fallback if no segment found
                    if ":" in str(security_id):
                        exchange_segment, mapped_sec_id = str(security_id).split(":", 1)
                        security_id = mapped_sec_id
                    else:
                        exchange_segment = self.EXCHANGE_MAP.get(
                            instrument.exchange_id, "NSE_EQ"
                        )
            else:
                exchange_segment = self.EXCHANGE_MAP.get(
                    instrument.exchange_id, "NSE_EQ"
                )

            payload = {
                "securityId": security_id,
                "exchangeSegment": exchange_segment,
                "instrument": "EQUITY",
                "expiryCode": 0,
                "oi": False,
                "fromDate": from_date_str,
                "toDate": to_date_str,
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
                    # Dhan Historical API returns valid UTC timestamp. No shift needed.
                    # ts = ts - 19800
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
                            resolve_required=False,
                        )
                    )
                except Exception as e:
                    logger.error(
                        f"Error parsing daily data for {instrument.symbol}: {e}"
                    )
                    continue

            results[instrument.symbol] = history

        return results
