"""
Redis Mapping Helper Service.
Responsible for syncing database mappings to Redis hashes and providing efficient lookup methods.
"""
from typing import Dict, Optional, Any, List

from sqlalchemy import select

from config.logger import logger
from config.redis_config import get_redis
from config.database_config import get_db_session
from models import Provider, Instrument, ProviderInstrumentMapping, Exchange, ExchangeProviderMapping

class RedisMappingHelper:
    """
    Manages caching of symbol mappings in Redis.
    Structure:
    - map:p2i:{provider_code} -> Hash {provider_symbol: internal_symbol}
    - map:i2p:{provider_code} -> Hash {internal_symbol: provider_symbol}
    - map:global:s2p -> Hash {internal_symbol: provider_code} (Routing)
    - map:global:s2e -> Hash {internal_symbol: exchange_id}
    - map:global:s2t -> Hash {internal_symbol: instrument_type_id}
    - map:global:e2p -> Hash {exchange_id: provider_code}
    - map:global:e2c -> Hash {exchange_id: exchange_code}
    - map:global:s2rec -> Hash {internal_symbol: should_record_data boolean}
    """

    KEY_PREFIX_P2I = "map:p2i" # Provider to Internal
    KEY_PREFIX_I2P = "map:i2p" # Internal to Provider
    KEY_PREFIX_I2S = "map:i2s" # Internal to Provider Segment (Dhan specific)

    KEY_PREFIX_S2P = "map:global:s2p" # Symbol to Provider (Routing)
    KEY_PREFIX_S2E = "map:global:s2e" # Symbol to Exchange
    KEY_PREFIX_S2T = "map:global:s2t" # Symbol to Instrument Type
    KEY_PREFIX_E2P = "map:global:e2p" # Exchange to Provider
    KEY_PREFIX_E2C = "map:global:e2c" # Exchange to Exchange Code
    KEY_PREFIX_S2REC = "map:global:s2rec" # Symbol to should_record_data boolean

    def __init__(self):
        self.redis = get_redis()

    @classmethod
    def get_p2i_key(cls, provider_code: str) -> str:
        return f"{cls.KEY_PREFIX_P2I}:{provider_code}"

    @classmethod
    def get_i2p_key(cls, provider_code: str) -> str:
        return f"{cls.KEY_PREFIX_I2P}:{provider_code}"

    @classmethod
    def get_i2s_key(cls, provider_code: str) -> str:
        """Get Redis key for Internal Symbol -> Provider Segment mapping"""
        return f"{cls.KEY_PREFIX_I2S}:{provider_code}"

    async def sync_mappings_from_db(self):
        """
        Loads all mappings from SQL database and populates Redis hashes.
        This should be called on application startup.
        """
        logger.info("Starting Redis mapping sync...")

        async for session in get_db_session():
            try:
                # 1. Fetch all active providers
                providers_result = await session.execute(
                    select(Provider).where(Provider.is_active == True)
                )
                providers = providers_result.scalars().all()

                for provider in providers:
                    await self._sync_provider_mappings(session, provider)

                # 2. Sync routing (primary) mappings
                await self._sync_routing_mappings(session)

                logger.info("Redis mapping sync completed successfully.")

            except Exception as e:
                logger.error(f"Error syncing Redis mappings: {e}")
                raise

    async def _sync_routing_mappings(self, session):
        """
        Syncs global routing configurations.
        Replicates logic from ProviderManager:
        - Exchange -> Primary Provider
        - Instrument -> Primary Provider (based on Exchange)
        - Instrument -> Exchange
        - Instrument -> Instrument Type
        """
        logger.info("Syncing proper routing mappings...")

        # A. Sync Exchange Mappings
        ex_result = await session.execute(
            select(
                Exchange.id,
                Exchange.code,
                Provider.code.label("provider_code")
            )
            .join(ExchangeProviderMapping, Exchange.id == ExchangeProviderMapping.exchange_id)
            .join(Provider, Provider.id == ExchangeProviderMapping.provider_id)
            .where(
                ExchangeProviderMapping.is_active == True,
                ExchangeProviderMapping.is_primary == True,
                Provider.is_active == True
            )
        )

        ex_rows = ex_result.all()
        e2p_map = {str(row.id): row.provider_code for row in ex_rows}
        e2c_map = {str(row.id): row.code for row in ex_rows}

        # B. Sync Instrument Routing Mappings
        # Logic: Instrument -> Exchange -> Primary Provider -> Provider Instrument Code
        # We need to map Internal Symbol -> Primary Provider Code

        inst_result = await session.execute(
            select(
                Instrument.symbol,
                Instrument.exchange_id,
                Instrument.instrument_type_id,
                Provider.code.label("provider_code"),
                Instrument.should_record_data
            )
            .join(ExchangeProviderMapping, Instrument.exchange_id == ExchangeProviderMapping.exchange_id)
            .join(Provider, Provider.id == ExchangeProviderMapping.provider_id)
            .where(
                Instrument.is_active == True,
                Instrument.blacklisted == False,
                Instrument.delisted == False,
                ExchangeProviderMapping.is_active == True,
                ExchangeProviderMapping.is_primary == True,
                Provider.is_active == True
            )
        )

        inst_rows = inst_result.all()

        s2p_map = {}
        s2e_map = {}
        s2t_map = {}
        s2rec_map = {}

        for row in inst_rows:
            s2p_map[row.symbol] = row.provider_code
            s2e_map[row.symbol] = str(row.exchange_id)
            s2t_map[row.symbol] = str(row.instrument_type_id)
            # Store value string as '1' or '0'
            s2rec_map[row.symbol] = '1' if row.should_record_data else '0'

        # Write to Redis
        async with self.redis.pipeline(transaction=True) as pipe:
            # Exchange mappings
            await pipe.delete(self.KEY_PREFIX_E2P, self.KEY_PREFIX_E2C)
            if e2p_map: await pipe.hset(self.KEY_PREFIX_E2P, mapping=e2p_map)
            if e2c_map: await pipe.hset(self.KEY_PREFIX_E2C, mapping=e2c_map)

            # Instrument mappings
            await pipe.delete(self.KEY_PREFIX_S2P, self.KEY_PREFIX_S2E, self.KEY_PREFIX_S2T, self.KEY_PREFIX_S2REC)
            if s2p_map: await pipe.hset(self.KEY_PREFIX_S2P, mapping=s2p_map)
            if s2e_map: await pipe.hset(self.KEY_PREFIX_S2E, mapping=s2e_map)
            if s2t_map: await pipe.hset(self.KEY_PREFIX_S2T, mapping=s2t_map)
            if s2rec_map: await pipe.hset(self.KEY_PREFIX_S2REC, mapping=s2rec_map)

            await pipe.execute()

        logger.info(f"Synced routing: {len(e2p_map)} exchanges, {len(s2p_map)} instruments")

    async def _sync_provider_mappings(self, session, provider: Provider):
        """Sync mappings for a single provider"""
        logger.info(f"Syncing mappings for provider: {provider.code}")

        # Query mappings joining with Instrument to get internal symbol
        stmt = (
            select(
                ProviderInstrumentMapping.provider_instrument_search_code,
                ProviderInstrumentMapping.provider_instrument_segment,  # Fetch segment
                Instrument.symbol.label("internal_symbol")
            )
            .join(Instrument, Instrument.id == ProviderInstrumentMapping.instrument_id)
            .where(
                ProviderInstrumentMapping.provider_id == provider.id,
                ProviderInstrumentMapping.is_active == True
            )
        )

        result = await session.execute(stmt)
        mappings = result.all()

        if not mappings:
            logger.warning(f"No active mappings found for {provider.code}")
            return

        # Prepare bulk dictionaries
        p2i_map = {row.provider_instrument_search_code: row.internal_symbol for row in mappings}
        i2p_map = {row.internal_symbol: row.provider_instrument_search_code for row in mappings}

        # Only populate segment map if segments are present (e.g. for Dhan)
        i2s_map = {
            row.internal_symbol: row.provider_instrument_segment
            for row in mappings
            if row.provider_instrument_segment
        }

        # Write to Redis
        p2i_key = self.get_p2i_key(provider.code)
        i2p_key = self.get_i2p_key(provider.code)
        i2s_key = self.get_i2s_key(provider.code)

        # Use pipeline for atomicity and speed
        async with self.redis.pipeline(transaction=True) as pipe:
            # Clear existing keys first to avoid stale data
            await pipe.delete(p2i_key)
            await pipe.delete(i2p_key)
            await pipe.delete(i2s_key)

            if p2i_map:
                await pipe.hset(p2i_key, mapping=p2i_map)

            if i2p_map:
                await pipe.hset(i2p_key, mapping=i2p_map)

            if i2s_map:
                await pipe.hset(i2s_key, mapping=i2s_map)

            await pipe.execute()

        logger.info(f"Synced {len(mappings)} mappings for {provider.code}")

    async def get_all_p2i_mappings(self, provider_code: str) -> Dict[str, str]:
        """Get all Provider->Internal mappings for a provider"""
        key = self.get_p2i_key(provider_code)
        # Redis hgetall returns bytes if decode_responses=False, assume we handle bytes
        data = await self.redis.hgetall(key)
        # data is {b'search_code': b'symbol'}
        # Decode keys and values to strings
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in data.items()}

    async def get_all_i2p_mappings(self, provider_code: str) -> Dict[str, str]:
        """Get all Internal->Provider mappings for a provider"""
        key = self.get_i2p_key(provider_code)
        data = await self.redis.hgetall(key)
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in data.items()}

    async def get_provider_symbol(self, provider_code: str, internal_symbol: str) -> Optional[str]:
        """Get provider specific symbol code from internal symbol"""
        key = self.get_i2p_key(provider_code)
        val = await self.redis.hget(key, internal_symbol)
        return val.decode("utf-8") if val else None

    async def get_provider_segment(self, provider_code: str, internal_symbol: str) -> Optional[str]:
        """Get provider specific instrument segment from internal symbol (e.g. for Dhan)"""
        key = self.get_i2s_key(provider_code)
        val = await self.redis.hget(key, internal_symbol)
        return val.decode("utf-8") if val else None

    async def get_internal_symbol(self, provider_code: str, provider_symbol: str) -> Optional[str]:
        """Get single internal symbol for a provider symbol"""
        key = self.get_p2i_key(provider_code)
        val = await self.redis.hget(key, provider_symbol)
        return val.decode("utf-8") if val else None

    async def get_provider_symbol(self, provider_code: str, internal_symbol: str) -> Optional[str]:
        """Get single provider symbol for an internal symbol"""
        key = self.get_i2p_key(provider_code)
        val = await self.redis.hget(key, internal_symbol)
        return val.decode("utf-8") if val else None

    # --- New Accessor Methods ---

    async def get_symbol_provider_map(self) -> Dict[str, str]:
        """Get All Internal Symbol -> Provider Code"""
        data = await self.redis.hgetall(self.KEY_PREFIX_S2P)
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in data.items()}

    async def get_symbol_record_map(self) -> Dict[str, bool]:
        """Get All Internal Symbol -> Should Record Data (bool)"""
        data = await self.redis.hgetall(self.KEY_PREFIX_S2REC)
        # Redis stores bool as '1' or '0' string typically if we stored int, or 'True'/'False' if we stored bool
        # In _sync_routing_mappings, python bool is auto converted to string. It usually becomes 'True'/'False' or '1'/'0'.
        # Let's verify how redis client handles python bool. It often converts to "True"/"False" string.
        # But safest is to treat as string.

        # Actually sqlalchemy execution returns python bools.
        # When we do `s2rec_map[row.symbol] = row.should_record_data` -> dict has bools.
        # Redis-py `hset(..., mapping=dict)` calls str() on values?
        # Redis stores strings. Python `str(True)` is `'True'`.

        res = {}
        for k, v in data.items():
            val_str = v.decode("utf-8")
            res[k.decode("utf-8")] = (val_str == 'True' or val_str == '1')
        return res

    async def get_symbol_exchange_map(self) -> Dict[str, int]:
        """Get All Internal Symbol -> Exchange ID"""
        data = await self.redis.hgetall(self.KEY_PREFIX_S2E)
        return {k.decode("utf-8"): int(v.decode("utf-8")) for k, v in data.items()}

    async def get_symbol_type_map(self) -> Dict[str, int]:
        """Get All Internal Symbol -> Instrument Type ID"""
        data = await self.redis.hgetall(self.KEY_PREFIX_S2T)
        return {k.decode("utf-8"): int(v.decode("utf-8")) for k, v in data.items()}

    async def get_exchange_provider_map(self) -> Dict[int, str]:
        """Get Exchange ID -> Provider Code"""
        data = await self.redis.hgetall(self.KEY_PREFIX_E2P)
        return {int(k.decode("utf-8")): v.decode("utf-8") for k, v in data.items()}

    async def get_exchange_code_map(self) -> Dict[int, str]:
        """Get Exchange ID -> Exchange Code"""
        data = await self.redis.hgetall(self.KEY_PREFIX_E2C)
        return {int(k.decode("utf-8")): v.decode("utf-8") for k, v in data.items()}

    async def get_routing_info_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get routing info for a single symbol."""
        # Using separate gets for now, could be optimized with pipeline if needed frequently
        provider = await self.redis.hget(self.KEY_PREFIX_S2P, symbol)

        if not provider:
            return None

        provider_code = provider.decode('utf-8')
        exchange_id = await self.redis.hget(self.KEY_PREFIX_S2E, symbol)
        search_code = await self.get_provider_symbol(provider_code, symbol)

        return {
            "symbol": symbol,
            "provider": provider_code,
            "exchange_id": int(exchange_id.decode('utf-8')) if exchange_id else None,
            "search_code": search_code
        }

    async def get_symbols_by_provider(self, check_should_record: bool = False) -> Dict[str, List[str]]:
        """
        Returns dictionary of {provider_code: [provider_search_codes]}
        only for symbols that are routed to that provider (Primary).
        Used for initial subscription.

        If check_should_record is True, only includes symbols that have should_record_data=True.
        """
        s2p = await self.get_symbol_provider_map()

        record_map = {}
        if check_should_record:
            record_map = await self.get_symbol_record_map()

        # Group by provider
        provider_internal_symbols: Dict[str, List[str]] = {}
        for sym, prov in s2p.items():
            if check_should_record:
                # If symbol not in record_map or False, skip
                if not record_map.get(sym, False):
                    continue

            if prov not in provider_internal_symbols:
                provider_internal_symbols[prov] = []
            provider_internal_symbols[prov].append(sym)

        result = {}

        for provider, symbols in provider_internal_symbols.items():
            # Get mapping for this provider
            i2p_map = await self.get_all_i2p_mappings(provider)

            search_codes = []
            for sym in symbols:
                if sym in i2p_map:
                    search_codes.append(i2p_map[sym])

            if search_codes:
                result[provider] = search_codes

        return result

# Singleton instance
_redis_mapping_helper = None

def get_redis_mapping_helper() -> RedisMappingHelper:
    global _redis_mapping_helper
    if _redis_mapping_helper is None:
        _redis_mapping_helper = RedisMappingHelper()
    return _redis_mapping_helper
