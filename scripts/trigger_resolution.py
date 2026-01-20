"""Manually trigger data resolution"""
import asyncio
import sys
import os

if sys.stdout:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.provider.provider_manager import ProviderManager
from services.data.data_resolver import DataResolver


async def trigger_resolution():
    print("Initializing ProviderManager...")
    pm = ProviderManager()
    await pm.initialize()

    print("Creating DataResolver...")
    resolver = DataResolver(pm)

    print("Triggering resolution...")
    await resolver.resolve_intraday_prices()

    print("Done!")

    # Cleanup
    for p in pm.providers.values():
        if hasattr(p, 'disconnect_websocket'):
            p.disconnect_websocket()


if __name__ == "__main__":
    asyncio.run(trigger_resolution())

