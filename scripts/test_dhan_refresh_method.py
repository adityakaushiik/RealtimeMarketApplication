import asyncio
import os
import sys
import logging
import requests

# Add project root to path
sys.path.append(os.getcwd())

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    force=True,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

from config.settings import get_settings
from services.provider.dhan_provider import DhanProvider

async def test_token_refresh():
    logger.info("Starting Dhan Token Refresh Test")
    print("Starting Dhan Token Refresh Test (stdout)")

    # Initialize provider to get credentials
    try:
        provider = DhanProvider()
        logger.info("DhanProvider initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize DhanProvider: {e}")
        return

    # Use the same logic as in _refresh_token_loop from dhan_provider.py
    url = f"{provider.REST_URL}/RenewToken"
    headers = {
        "access-token": provider.access_token,
        "dhanClientId": provider.client_id
    }

    logger.info(f"Target URL: {url}")
    masked_token = f"{provider.access_token[:5]}...{provider.access_token[-5:]}" if provider.access_token else "None"
    logger.info(f"Using Headers: access-token={masked_token}, dhanClientId={provider.client_id}")

    try:
        logger.info("Sending GET request to RenewToken...")
        # Using requests.get instead of post, as curl default is GET
        response = await asyncio.to_thread(requests.get, url, headers=headers)

        if response.ok:
            logger.info("Dhan API token refreshed successfully.")
            # The old token is expired, so we must update to the new one.
            try:
                data = response.json()
                new_token = None

                # Check for token in likely locations
                if "accessToken" in data:
                    new_token = data["accessToken"]
                elif "token" in data:
                    new_token = data["token"]
                elif (
                    "data" in data
                    and isinstance(data["data"], dict)
                    and "accessToken" in data["data"]
                ):
                    new_token = data["data"]["accessToken"]
                elif (
                    "data" in data
                    and isinstance(data["data"], dict)
                    and "token" in data["data"]
                ):
                    new_token = data["data"]["token"]

                if new_token:
                    provider.access_token = new_token
                    logger.info("Successfully updated Dhan access token.")
                    logger.info("✅ SUCCESS: Token refresh completed successfully")
                else:
                    logger.warning(
                        f"Token refresh response did not contain accessToken or token. Response: {data}"
                    )
                    logger.error("❌ FAILED: No token found in response")
            except Exception as e:
                logger.error(f"Error parsing token refresh response: {e}")
                logger.error("❌ FAILED: Could not parse response")
        else:
            logger.error(
                f"Failed to refresh Dhan token: {response.status_code} - {response.text}"
            )
            logger.error("❌ FAILED: Token refresh request returned error status")

    except Exception as e:
        logger.error(f"Exception during request: {e}", exc_info=True)
        logger.error("❌ FAILED: Exception occurred during token refresh")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_token_refresh())
