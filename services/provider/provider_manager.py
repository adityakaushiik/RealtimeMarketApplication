"""
Provider Manager - Orchestrates multiple market data providers.
Routes symbols to appropriate providers based on exchange mappings from database.
"""

import asyncio
from typing import Dict, Optional

from config.logger import logger
from models import (
    Instrument,
    PriceHistoryIntraday,
    PriceHistoryDaily,
)
from services.provider.base_provider import BaseMarketDataProvider
from services.provider.yahoo_provider import YahooFinanceProvider
from services.provider.dhan_provider import DhanProvider
from services.data.redis_mapping import get_redis_mapping_helper

try:
    from config.redis_config import get_redis
except ImportError:
    get_redis = None


class ProviderManager:
    """
    Manages multiple market data providers and routes symbols
    to the correct provider based on exchange mappings.

    Added:
    - provider_symbol_map: Cached mapping of provider_code -> provider_search_code -> internal instrument symbol
      Useful to translate inbound provider-specific symbols back to internal canonical symbols.
    """

    REDIS_KEY_SYMBOL_MAP = "provider_manager:symbol_map"
    REDIS_KEY_TYPE_MAP = "provider_manager:instrument_type_map"

    def __init__(self):
        # self.callback = None
        self.providers: Dict[str, BaseMarketDataProvider] = {}

        self.exchange_to_provider: Dict[int, str] = {}  # exchange_id -> provider_code
        self.exchange_id_to_code: Dict[
            int, str
        ]  # exchange_id -> exchange_code ("NSE", "BSE")
        self.symbol_to_provider: Dict[
            str, str
        ]  # internal symbol (provider search symbol currently) -> provider_code
        self.symbol_to_exchange: Dict[str, int] = {}  # internal symbol -> exchange_id

        self.symbol_to_instrument_type: Dict[
            str, int
        ]  # internal symbol -> instrument_type_id
        self._initialized = False

        # Redis Mapping Helper
        self.redis_mapper = get_redis_mapping_helper()

    async def initialize(self):
        """
        Load exchange-provider mappings from database and
        initialize provider instances.
        """
        if self._initialized:
            logger.info("ProviderManager already initialized.")
            return

        logger.info("Initializing ProviderManager...")

        # Load routing info from Redis into memory for fast sync access (optional but good for performance)
        # OR we can purely rely on RedisMapper. For now, let's keep local caches used in sync task
        # populated from Redis data we just synced.

        # 1. Load Exchange -> Provider map
        self.exchange_to_provider = await self.redis_mapper.get_exchange_provider_map()
        self.exchange_id_to_code = await self.redis_mapper.get_exchange_code_map()

        # 2. Load Symbol Routing maps
        self.symbol_to_provider = await self.redis_mapper.get_symbol_provider_map()
        self.symbol_to_exchange = await self.redis_mapper.get_symbol_exchange_map()
        self.symbol_to_instrument_type = await self.redis_mapper.get_symbol_type_map()

        # Initialize provider instances based on populated map
        unique_providers = set(self.exchange_to_provider.values())
        for provider_code in unique_providers:
            # Check if provider is actually active/known before recreating
            # (In redis_mapper code we only synced active providers, so this is safe)
            self.providers[provider_code] = self._create_provider_instance(
                provider_code
            )
            logger.info(f"Initialized provider: {provider_code}")

        self._initialized = True
        logger.info(f"ProviderManager initialized with {len(self.providers)} providers")

        # Automatically start providers with default recordable symbols
        logger.info("Auto-starting providers with recordable symbols...")
        symbols_by_provider = await self.get_symbols_by_provider(
            check_should_record=True
        )
        await self.start_all_providers(symbols_by_provider)

        # Start the dynamic subscription sync task
        await self.start_sync_task()

    def _create_provider_instance(
        self, provider_code: str
    ) -> Optional[BaseMarketDataProvider]:
        """Factory method to create provider instances"""

        if provider_code == "YF":
            return YahooFinanceProvider()
        elif provider_code == "DHAN":
            return DhanProvider(provider_manager=self)
        else:
            logger.error(
                f"Unknown provider code: {provider_code} - skipping initialization"
            )
            raise ValueError(
                f"Unknown provider code: {provider_code}. Please add it to the factory method."
            )

    async def get_symbols_by_provider(
        self, check_should_record: bool = False
    ) -> Dict[str, list[str]]:
        """
        Query database to get all instruments grouped by provider.
        Returns: {"YF": ["AAPL", "TSLA"], "DHAN": ["RELIANCE-EQ", "TCS-EQ"]}

        Side effects:
        - Populates symbol_to_provider & symbol_to_exchange for quick lookups.
        """
        # Logic shifted to RedisMappingHelper
        return await self.redis_mapper.get_symbols_by_provider(
            check_should_record=check_should_record
        )

    async def start_all_providers(self, symbols_by_provider: Dict[str, list[str]]):
        """Connect all providers with their respective symbols"""
        logger.info("Starting all provider connections...")

        from fastapi.concurrency import run_in_threadpool

        # from fastapi.concurrency import run_in_threadpool # removing unused import

        if not symbols_by_provider:
            # We now support empty launch effectively, but let's log
            pass

        tasks = []
        for provider_code, symbols in symbols_by_provider.items():
            if provider_code in self.providers and self.providers[provider_code]:
                logger.info(f"Connecting {provider_code} with {len(symbols)} symbols")
                provider = self.providers[provider_code]

                if asyncio.iscoroutinefunction(provider.connect_websocket):
                    # For async providers (like Dhan)
                    task = provider.connect_websocket(symbols)
                else:
                    # For sync providers (like Yahoo) - run in threadpool
                    task = run_in_threadpool(provider.connect_websocket, symbols)
                tasks.append(task)

        # Connect all providers in parallel
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("All providers connection attempts completed")
        else:
            logger.warning("No providers to connect")

    def stop_all_providers(self):
        """Disconnect all provider connections"""
        logger.info("Stopping all provider connections...")
        for provider_code, provider in self.providers.items():
            if provider:
                try:
                    provider.disconnect_websocket()
                    logger.info(f"Disconnected {provider_code}")
                except Exception as e:
                    logger.error(f"Error disconnecting {provider_code}: {e}")

    async def subscribe_to_symbols(self, symbols: list[str]):
        """
        Dynamically subscribe to symbols across appropriate providers.
        Routes each symbol to its correct provider.
        """
        if not symbols:
            return

        # Group symbols by provider
        symbols_by_provider: Dict[str, list[str]] = {}

        for symbol in symbols:
            provider_code = self.symbol_to_provider.get(symbol)
            if provider_code:
                if provider_code not in symbols_by_provider:
                    symbols_by_provider[provider_code] = []
                symbols_by_provider[provider_code].append(symbol)
            else:
                logger.debug(f"No provider mapping found for symbol: {symbol}")

        # Subscribe to each provider
        for provider_code, provider_symbols in symbols_by_provider.items():
            if provider_code in self.providers and self.providers[provider_code]:
                try:
                    logger.debug(f"Subscribing to {provider_code} {provider_symbols}")
                    await self.providers[provider_code].subscribe_symbols(
                        provider_symbols
                    )

                    logger.info(
                        f"Subscribed to {len(provider_symbols)} symbols on {provider_code}"
                    )
                except Exception as e:
                    logger.error(f"Error subscribing to {provider_code}: {e}")

    async def unsubscribe_from_symbols(self, symbols: list[str]):
        """
        Dynamically unsubscribe from symbols across appropriate providers.
        """
        if not symbols:
            return

        # Group symbols by provider
        symbols_by_provider: Dict[str, list[str]] = {}

        for symbol in symbols:
            provider_code = self.symbol_to_provider.get(symbol)
            if provider_code:
                if provider_code not in symbols_by_provider:
                    symbols_by_provider[provider_code] = []
                symbols_by_provider[provider_code].append(symbol)

        # Unsubscribe from each provider
        for provider_code, provider_symbols in symbols_by_provider.items():
            if provider_code in self.providers and self.providers[provider_code]:
                try:
                    await self.providers[provider_code].unsubscribe_symbols(
                        provider_symbols
                    )

                    logger.info(
                        f"Unsubscribed from {len(provider_symbols)} symbols on {provider_code}"
                    )
                except Exception as e:
                    logger.error(f"Error unsubscribing from {provider_code}: {e}")

    # NEW: Sync Logic moved from DataIngestion
    def _on_subscription_change(self):
        """Callback for when websocket subscriptions change."""
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(lambda: self._subscription_change_event.set())
        except RuntimeError:
            pass  # No running loop

    async def start_sync_task(self):
        """Start dynamic subscription synchronization task."""
        if hasattr(self, "_sync_task") and self._sync_task:
            return

        from services.websocket_manager import get_websocket_manager

        self._subscription_change_event = asyncio.Event()
        self._sync_task = asyncio.create_task(self._sync_subscriptions_loop())

        # Register callback
        get_websocket_manager().register_callback(self._on_subscription_change)
        logger.info("ProviderManager: sync task started.")

    def stop_sync_task(self):
        """Stop dynamic subscription synchronization task."""
        if hasattr(self, "_sync_task") and self._sync_task:
            self._sync_task.cancel()
            self._sync_task = None
            logger.info("ProviderManager: sync task stopped.")

    async def _sync_subscriptions_loop(self):
        """
        Periodically sync provider subscriptions with active client channels.
        This ensures we only fetch data for symbols that clients are watching.
        """
        logger.info("ProviderManager: Starting dynamic subscription sync loop...")
        from services.websocket_manager import get_websocket_manager

        while True:
            try:
                # Wait for event or timeout (heartbeat)
                try:
                    await asyncio.wait_for(
                        self._subscription_change_event.wait(), timeout=10.0
                    )
                except asyncio.TimeoutError:
                    pass

                self._subscription_change_event.clear()

                # Get symbols clients are actively watching
                websocket_manager = get_websocket_manager()
                active_channels = websocket_manager.get_active_channels()

                # Calculate target subscriptions based to active client channels AND recordable symbols
                # We must ensuring we don't unsubscribe from recordable symbols
                recordable_map = await self.redis_mapper.get_symbol_record_map()

                # Start with active channels (internal symbols)
                target_internal_symbols = set(active_channels)

                # Add all recordable symbols to target list
                for sym, should_record in recordable_map.items():
                    if should_record:
                        target_internal_symbols.add(sym)

                # Cache mappings to avoid repeated Redis calls
                provider_i2p_maps = {}
                provider_p2i_maps = {}  # NEW: Reverse map cache
                target_subscriptions = set()

                for internal_symbol in target_internal_symbols:
                    # Find provider for this symbol
                    provider_code = self.symbol_to_provider.get(internal_symbol)

                    if provider_code:
                        if provider_code not in provider_i2p_maps:
                            provider_i2p_maps[
                                provider_code
                            ] = await self.redis_mapper.get_all_i2p_mappings(
                                provider_code
                            )
                            # Prefetch reverse map too
                            provider_p2i_maps[
                                provider_code
                            ] = await self.redis_mapper.get_all_p2i_mappings(
                                provider_code
                            )

                        search_code = provider_i2p_maps[provider_code].get(
                            internal_symbol
                        )
                        # We track internal symbols that successfully resolved to a search code
                        if search_code:
                            # We don't add search_code to target_subscriptions anymore
                            # We keep target_internal_symbols as the source of truth
                            pass

                # Calculate Current Subscriptions (Internal)
                current_internal_subscriptions = set()

                for provider_code, provider in self.providers.items():
                    if provider:
                        # If we haven't fetched p2i map yet (because no target needed it?), fetch it now
                        if provider_code not in provider_p2i_maps:
                            provider_p2i_maps[
                                provider_code
                            ] = await self.redis_mapper.get_all_p2i_mappings(
                                provider_code
                            )

                        p2i = provider_p2i_maps.get(provider_code, {})

                        for sub_code in provider.subscribed_symbols:
                            internal = p2i.get(sub_code)
                            if internal:
                                current_internal_subscriptions.add(internal)
                            elif sub_code in self.symbol_to_provider:
                                # It's already an internal symbol!
                                current_internal_subscriptions.add(sub_code)
                            else:
                                # This happens if provider has a subscription that we don't have a mapping for locally
                                # e.g. manual subscription? Or stale?
                                # We can't manage it if we don't know who it is. Ignore.
                                pass

                # Calculate diff using INTERNAL SYMBOLS
                to_subscribe = target_internal_symbols - current_internal_subscriptions
                to_unsubscribe = (
                    current_internal_subscriptions - target_internal_symbols
                )

                # Filter to_subscribe: ensure they map to valid providers/search_codes
                # We can't subscribe to something with no provider mapping
                valid_to_subscribe = []
                for sym in to_subscribe:
                    p_code = self.symbol_to_provider.get(sym)
                    if p_code:
                        if p_code not in provider_i2p_maps:
                            provider_i2p_maps[
                                p_code
                            ] = await self.redis_mapper.get_all_i2p_mappings(p_code)
                        if provider_i2p_maps[p_code].get(sym):
                            valid_to_subscribe.append(sym)

                # Apply changes
                if valid_to_subscribe:
                    await self.subscribe_to_symbols(valid_to_subscribe)
                    logger.info(
                        f"📈 Subscribed to {len(valid_to_subscribe)} new symbols"
                    )

                if to_unsubscribe:
                    await self.unsubscribe_from_symbols(list(to_unsubscribe))
                    logger.info(f"📉 Unsubscribed from {len(to_unsubscribe)} symbols")

            except asyncio.CancelledError:
                logger.info("Subscription sync task cancelled")
                break
            except Exception as e:
                logger.error(f"Error syncing subscriptions: {e}")

    async def get_intraday_prices(
        self,
        instrument: Instrument,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        timeframe: str = "1d",
    ) -> list["PriceHistoryIntraday"]:
        """Fetch intraday prices for a single instrument from its provider."""
        provider_code = self.exchange_to_provider.get(instrument.exchange_id)
        if not provider_code:
            return []

        provider = self.providers.get(provider_code)
        if not provider:
            return []

        result = await provider.get_intraday_prices([instrument])
        return result.get(instrument.symbol, [])

    async def get_daily_prices(
        self,
        instrument: Instrument,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        timeframe: str = "1d",
    ) -> list["PriceHistoryDaily"]:
        """Fetch daily prices for a single instrument from its provider."""
        provider_code = self.exchange_to_provider.get(instrument.exchange_id)
        if not provider_code:
            return []

        provider = self.providers.get(provider_code)
        if not provider:
            return []

        result = await provider.get_daily_prices([instrument])
        return result.get(instrument.symbol, [])

    ### HELPER METHODS ###

    def get_instrument_type_id(self, symbol: str) -> Optional[int]:
        """Get instrument type ID for a symbol"""
        return self.symbol_to_instrument_type.get(symbol)

    def get_exchange_id_for_symbol(self, symbol: str) -> Optional[int]:
        """Get exchange ID for a symbol"""
        return self.symbol_to_exchange.get(symbol)

    def get_exchange_code(self, exchange_id: int) -> Optional[str]:
        """Get exchange code (NSE, BSE, etc.) by ID"""
        return self.exchange_id_to_code.get(exchange_id)

    async def resolve_internal_symbol(
        self, provider_code: str, provider_search_code: str
    ) -> Optional[str]:
        """
        Resolve an internal instrument symbol from a provider code & provider search code.
        Uses RedisMappingHelper.
        """
        return await self.redis_mapper.get_internal_symbol(
            provider_code, provider_search_code
        )

    async def get_search_code(
        self, provider_code: str, internal_symbol: str
    ) -> Optional[str]:
        """
        Get provider specific search code for an internal symbol.
        Uses RedisMappingHelper.
        """
        return await self.redis_mapper.get_provider_symbol(
            provider_code, internal_symbol
        )


# Global reference for dependency injection
_provider_manager_instance = None


def get_provider_manager() -> ProviderManager:
    global _provider_manager_instance
    if _provider_manager_instance is None:
        _provider_manager_instance = ProviderManager()
    return _provider_manager_instance
