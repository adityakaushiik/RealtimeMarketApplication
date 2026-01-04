"""Simple test for DhanHQ provider"""
import sys
import time
import threading
import asyncio

sys.path.insert(0, r"C:\Users\tech\OneDrive\Documents\GitHub\RealtimeMarketApplication")

from dhanhq import marketfeed
from config.settings import get_settings

settings = get_settings()

print("Starting simple DhanFeed test...")
print(f"Client ID: {settings.DHAN_CLIENT_ID[:4]}***")

# Instruments: (exchange, security_id, mode)
# KOTAKBANK = 1922
instruments = [(1, "1922", 17)]

print(f"Instruments: {instruments}")

# Track received data
received_data = []

def run_feed():
    """Run DhanFeed in separate thread"""
    print("Feed thread: Creating event loop...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    print("Feed thread: Creating DhanFeed...")
    feed = marketfeed.DhanFeed(
        settings.DHAN_CLIENT_ID,
        settings.DHAN_ACCESS_TOKEN,
        instruments,
        17  # Quote mode
    )

    print("Feed thread: Calling run_forever...")
    feed.run_forever()

# Start feed thread
print("Starting feed thread...")
feed_thread = threading.Thread(target=run_feed, daemon=True)
feed_thread.start()

# Wait and check
print("Waiting 10 seconds for data...")
time.sleep(10)

print("Done!")

