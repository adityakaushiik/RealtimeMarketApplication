"""
Provider Manager - Orchestrates multiple market data providers.
Routes symbols to appropriate providers based on exchange mappings from database.
"""

import asyncio
from typing import Dict, Optional

from sqlalchemy import select

from config.database_config import get_db_session
from config.logger import logger
from models import (
    Exchange,
    Provider,
    ExchangeProviderMapping,
    Instrument,
    ProviderInstrumentMapping,
    PriceHistoryIntraday,
    PriceHistoryDaily,
)
from services.provider.base_provider import BaseMarketDataProvider
from services.provider.yahoo_provider import YahooFinanceProvider
from services.provider.dhan_provider import DhanProvider


class ProviderManager:
    """
    Manages multiple market data providers and routes symbols
    to the correct provider based on exchange mappings.

    Added:
    - provider_symbol_map: Cached mapping of provider_code -> provider_search_code -> internal instrument symbol
      Useful to translate inbound provider-specific symbols back to internal canonical symbols.
    """

    def __init__(self):
        self.callback = None
        self.providers: Dict[str, BaseMarketDataProvider] = {}


        self.exchange_to_provider: Dict[int, str] = {}  # exchange_id -> provider_code
        self.symbol_to_provider: Dict[
            str, str
        ] = {}  # internal symbol (provider search symbol currently) -> provider_code
        self.symbol_to_exchange: Dict[str, int] = {}  # internal symbol -> exchange_id


        # New: provider_symbol_map caches provider specific search code -> internal instrument symbol
        # Structure: {"YF": {"AAPL": "AAPL"}, "DHAN": {"123213": "RELIANCE"}}
        self.provider_symbol_map: Dict[str, Dict[str, str]] = {}


        # New: internal_to_search_map caches internal instrument symbol -> provider specific search code
        # Structure: {"YF": {"AAPL": "AAPL"}, "DHAN": {"RELIANCE": "RELIANCE-EQ"}}
        self.internal_to_search_map: Dict[str, Dict[str, str]] = {}
        self._initialized = False

    async def initialize(self):
        """
        Load exchange-provider mappings from database and
        initialize provider instances.
        """
        if self._initialized:
            logger.info("ProviderManager already initialized.")
            return

        logger.info("Initializing ProviderManager...")

        async for session in get_db_session():
            # Query all active exchange-provider mappings
            result = await session.execute(
                select(
                    Exchange.id,
                    Exchange.code,
                    Provider.code.label("provider_code"),
                    ExchangeProviderMapping.is_primary,
                )
                .join(
                    ExchangeProviderMapping,
                    Exchange.id == ExchangeProviderMapping.exchange_id,
                )
                .join(Provider, Provider.id == ExchangeProviderMapping.provider_id)
                .where(
                    ExchangeProviderMapping.is_active == True,
                    ExchangeProviderMapping.is_primary == True,
                    Provider.is_active == True,
                )
            )

            mappings = result.all()

            # Build exchange -> provider mapping
            for mapping in mappings:
                self.exchange_to_provider[mapping.id] = mapping.provider_code
                logger.info(f"Mapped {mapping.code} → {mapping.provider_code}")

            # Initialize provider instances
            unique_providers = set(self.exchange_to_provider.values())
            for provider_code in unique_providers:
                self.providers[provider_code] = self._create_provider_instance(
                    provider_code
                )
                logger.info(f"Initialized provider: {provider_code}")

        self._initialized = True
        logger.info(f"ProviderManager initialized with {len(self.providers)} providers")

    def _create_provider_instance(
        self, provider_code: str
    ) -> Optional[BaseMarketDataProvider]:
        """Factory method to create provider instances"""

        if not self.callback:
            logger.error(
                "ProviderManager callback not set before provider initialization"
            )
            raise ValueError(
                "ProviderManager callback must be set before initializing providers"
            )

        if provider_code == "YF":
            return YahooFinanceProvider(callback=self.callback)
        elif provider_code == "DHAN":
            return DhanProvider(callback=self.callback, provider_manager=self)
        else:
            logger.error(
                f"Unknown provider code: {provider_code} - skipping initialization"
            )
            raise ValueError(
                f"Unknown provider code: {provider_code}. Please add it to the factory method."
            )

    async def get_symbols_by_provider(self) -> Dict[str, list[str]]:
        """
        Query database to get all instruments grouped by provider.
        Returns: {"YF": ["AAPL", "TSLA"], "DHAN": ["RELIANCE-EQ", "TCS-EQ"]}

        Side effects:
        - Populates symbol_to_provider & symbol_to_exchange for quick lookups.
        - Populates provider_symbol_map with provider-specific search codes mapped back to internal instrument symbols.
        """
        symbols_by_provider: Dict[str, list[str]] = {}

        async for session in get_db_session():
            # Query instruments with their exchanges and provider mappings
            result = await session.execute(
                select(
                    Instrument.id,
                    Instrument.symbol,  # internal canonical symbol
                    Instrument.exchange_id,
                    ProviderInstrumentMapping.provider_instrument_search_code,  # provider specific search code
                    Provider.code.label("provider_code"),
                )
                .join(
                    ProviderInstrumentMapping,
                    Instrument.id == ProviderInstrumentMapping.instrument_id,
                )
                .join(Provider, Provider.id == ProviderInstrumentMapping.provider_id)
                .join(
                    ExchangeProviderMapping,
                    (ExchangeProviderMapping.provider_id == Provider.id)
                    & (ExchangeProviderMapping.exchange_id == Instrument.exchange_id),
                )
                .where(
                    Instrument.is_active == True,
                    Instrument.blacklisted == False,
                    Instrument.delisted == False,
                    ExchangeProviderMapping.is_active == True,
                    ExchangeProviderMapping.is_primary == True,
                    Provider.is_active == True,
                )
            )

            rows = result.all()

            for row in rows:
                provider_code = row.provider_code
                provider_search_code = row.provider_instrument_search_code
                internal_symbol = row.symbol

                if provider_code not in symbols_by_provider:
                    symbols_by_provider[provider_code] = []
                if provider_code not in self.provider_symbol_map:
                    self.provider_symbol_map[provider_code] = {}
                if provider_code not in self.internal_to_search_map:
                    self.internal_to_search_map[provider_code] = {}

                # Append provider specific search code to subscription list
                symbols_by_provider[provider_code].append(provider_search_code)

                # Cache mappings
                # Note: symbol_to_provider and symbol_to_exchange are keyed by provider_search_code
                # This might be problematic if different providers use the same search code for different instruments
                # But for now, we assume uniqueness or that we look up by provider context

                # IMPORTANT: We should also map the internal symbol to provider/exchange for reverse lookups
                self.symbol_to_provider[internal_symbol] = provider_code
                self.symbol_to_exchange[internal_symbol] = row.exchange_id

                self.symbol_to_provider[provider_search_code] = provider_code
                self.symbol_to_exchange[provider_search_code] = row.exchange_id

                # provider_symbol_map holds provider search code -> internal instrument symbol
                self.provider_symbol_map[provider_code][provider_search_code] = (
                    internal_symbol
                )
                # internal_to_search_map holds internal instrument symbol -> provider search code
                self.internal_to_search_map[provider_code][internal_symbol] = provider_search_code

            # Log statistics
            logger.info(
                f"Loaded {sum(len(v) for v in symbols_by_provider.values())} total symbols"
            )
            for provider_code, symbols in symbols_by_provider.items():
                logger.info(f"  {provider_code}: {len(symbols)} symbols")

        return symbols_by_provider

    async def get_provider_symbol_mapping(self) -> Dict[str, Dict[str, str]]:
        """
        Returns provider_symbol_map: provider_code -> provider_search_code -> internal instrument symbol.
        Ensures cache is populated (loads symbols if empty).
        Example: {"YF": {"AAPL": "AAPL", "TSLA": "TSLA"}, "DHAN": {"RELIANCE-EQ": "RELIANCE"}}
        """
        if not self.provider_symbol_map:
            await self.get_symbols_by_provider()
        return self.provider_symbol_map

    async def resolve_internal_symbol(
        self, provider_code: str, provider_search_code: str
    ) -> Optional[str]:
        """
        Resolve an internal instrument symbol from a provider code & provider search code.
        Lazy-loads mapping if needed.
        Returns None if not found.
        """
        if not self.provider_symbol_map:
            await self.get_symbols_by_provider()
        return self.provider_symbol_map.get(provider_code, {}).get(provider_search_code)

    def get_internal_symbol_sync(
        self, provider_code: str, provider_search_code: str
    ) -> Optional[str]:
        """
        Synchronous version of resolve_internal_symbol for hot paths (like data ingestion).
        Assumes provider_symbol_map is already populated.
        """
        if not self.provider_symbol_map:
            return None
        return self.provider_symbol_map.get(provider_code, {}).get(provider_search_code)

    async def get_search_code(self, provider_code: str, internal_symbol: str) -> Optional[str]:
        """
        Get provider specific search code for an internal symbol.
        Lazy-loads mapping if needed.
        """
        if not self.internal_to_search_map:
            await self.get_symbols_by_provider()
        
        return self.internal_to_search_map.get(provider_code, {}).get(internal_symbol)

    async def start_all_providers(self, symbols_by_provider: Dict[str, list[str]]):
        """Connect all providers with their respective symbols"""
        logger.info("Starting all provider connections...")

        from fastapi.concurrency import run_in_threadpool

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
                    task = run_in_threadpool(
                        provider.connect_websocket, symbols
                    )
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
                    self.providers[provider_code].subscribe_symbols(provider_symbols)
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
                    self.providers[provider_code].unsubscribe_symbols(provider_symbols)
                    logger.info(
                        f"Unsubscribed from {len(provider_symbols)} symbols on {provider_code}"
                    )
                except Exception as e:
                    logger.error(f"Error unsubscribing from {provider_code}: {e}")

    def get_provider_status(self) -> Dict[str, dict]:
        """Get status of all providers for monitoring"""
        status = {}
        for provider_code, provider in self.providers.items():
            if provider:
                status[provider_code] = provider.get_status()
            else:
                status[provider_code] = {
                    "provider_code": provider_code,
                    "connected": False,
                    "subscribed_count": 0,
                    "symbols": [],
                }
        return status

    def get_provider_for_symbol(self, symbol: str) -> Optional[str]:
        """Get the provider code for a given symbol"""
        return self.symbol_to_provider.get(symbol)

    def is_initialized(self) -> bool:
        """Check if provider manager is initialized"""
        return self._initialized

    async def get_intraday_prices(
        self, instrument: Instrument,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            timeframe: str = '1d',
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
        self, instrument: Instrument,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            timeframe: str = '1d',
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

# Global reference for dependency injection
_provider_manager_instance = None
def get_provider_manager() -> ProviderManager:
    global _provider_manager_instance
    if _provider_manager_instance is None:
        _provider_manager_instance = ProviderManager()
    return _provider_manager_instance
