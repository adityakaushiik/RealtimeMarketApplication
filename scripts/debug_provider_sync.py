import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from services.provider.provider_manager import ProviderManager
from config.logger import logger

# Set logger to print to console
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

async def debug_sync_logic():
    print("--- Starting Debug ---")

    target_symbol = "WAAREEENER"
    print(f"Target Symbol: {target_symbol}")

    pm = ProviderManager()

    # emulate initialize partly or fully
    # We need redis mapper to be ready.

    print("Initializing Provider Manager (loads mappings from Redis)...")
    try:
        await pm.initialize()
    except Exception as e:
        print(f"Error initializing: {e}")
        return

    # 1. Check symbol_to_provider
    print("\n--- 1. Checking Local Memory Maps ---")
    provider_code = pm.symbol_to_provider.get(target_symbol)
    print(f"pm.symbol_to_provider.get('{target_symbol}') -> {provider_code}")

    if not provider_code:
        print("❌ FAILURE: Symbol not mapped to any provider in 'symbol_to_provider'.")
        print("Checking redis_mapper direct lookup...")
        prov_direct = await pm.redis_mapper.get_provider_for_symbol(target_symbol)
        print(f"Redis get_provider_for_symbol('{target_symbol}') -> {prov_direct}")

        # If failure here, check DB mapping sync
        print("Hint: If Provider is missing, run force_alembic_sync.py or check 'instrument' + 'exchange_provider_mapping' tables.")
        return

    # 2. Check I2P Mapping (Internal to Provider Search Code)
    print(f"\n--- 2. Checking I2P Mapping for provider '{provider_code}' ---")
    i2p_map = await pm.redis_mapper.get_all_i2p_mappings(provider_code)
    search_code = i2p_map.get(target_symbol)
    print(f"i2p_map.get('{target_symbol}') -> {search_code}")

    if not search_code:
        print(f"❌ FAILURE: Internal symbol '{target_symbol}' has no mapping to provider Search Code in I2P map.")
        print("This explains why 'to_subscribe' ignores it.")

        # Debug why
        p2i_map = await pm.redis_mapper.get_all_p2i_mappings(provider_code)
        # Scan p2i for this symbol
        found = False
        for k, v in p2i_map.items():
            if v == target_symbol:
                print(f"✅ Found in reverse map (P2I): '{k}' -> '{v}'")
                found = True
                break
        if not found:
             print("❌ Not found in P2I map either.")

        # Check specific single lookup
        single_i2p = await pm.redis_mapper.get_provider_symbol(provider_code, target_symbol)
        print(f"Single lookup get_provider_symbol -> {single_i2p}")
        return

    print(f"✅ SUCCESS: Mapped '{target_symbol}' -> '{search_code}'")

    # 3. Check Logic Simulation
    print("\n--- 3. Simulating Sync Logic ---")
    active_channels = {target_symbol}
    print(f"Active Channels (Mock): {active_channels}")

    target_internal_symbols = set(active_channels)

    provider_i2p_maps = {}
    target_subscriptions = set()

    for internal_symbol in target_internal_symbols:
        p_code = pm.symbol_to_provider.get(internal_symbol)
        if p_code:
            current_i2p = await pm.redis_mapper.get_all_i2p_mappings(p_code)
            s_code = current_i2p.get(internal_symbol)
            if s_code:
                print(f"Adding to target_subscriptions: {s_code}")
                target_subscriptions.add(s_code)
            else:
                print(f"❌ Logic Loop: Failed to find search code for {internal_symbol}")
        else:
            print(f"❌ Logic Loop: Failed to find provider for {internal_symbol}")

    print(f"Calculated target_subscriptions: {target_subscriptions}")

    # 4. Check Provider State
    print("\n--- 4. Current Provider State (Before Sub) ---")
    provider = pm.providers.get(provider_code)
    print(f"Provider Subscribed: {provider.subscribed_symbols}")

    # 5. Simulate Subscription
    to_subscribe = target_subscriptions - provider.subscribed_symbols
    print(f"To Subscribe: {to_subscribe}")
    if to_subscribe:
        await pm.subscribe_to_symbols(list(to_subscribe))
        print("Called subscribe_to_symbols.")

    print(f"Provider Subscribed (After Sub): {provider.subscribed_symbols}")

    # 6. Simulate Loop 2
    print("\n--- 6. Simulate Loop 2 ---")
    current_subscriptions = set()
    for pc, p in pm.providers.items():
        if p: current_subscriptions.update(p.subscribed_symbols)

    print(f"Current Global Subscriptions: {current_subscriptions}")

    to_subscribe_2 = target_subscriptions - current_subscriptions
    print(f"To Subscribe (Loop 2): {to_subscribe_2}")

    if not to_subscribe_2:
        print("✅ SUCCESS: Persistence verified. Loop 2 has nothing to subscribe.")
    else:
        print("❌ FAILURE: Persistence failed. Loop 2 wants to re-subscribe.")

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(debug_sync_logic())

