import asyncio
import os
import sys
import logging

# Add project root to path
sys.path.append(os.getcwd())

from config.settings import get_settings
from services.provider.dhan_provider import DhanProvider
import requests

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_dhan_token_refresh():
    """
    Test script to verify Dhan token refresh logic.
    We'll set a very short interval for the refresh (10 seconds)
    and observe if it attempts to refresh and if it succeeds or fails.
    """
    settings = get_settings()
    if not settings.DHAN_CLIENT_ID or not settings.DHAN_ACCESS_TOKEN:
        logger.error("Dhan credentials not found in settings")
        return

    logger.info("Initializing Dhan Provider test...")
    dhan_provider = DhanProvider()

    logger.info(f"Current Access Token (first 10 chars): {dhan_provider.access_token[:10]}...")

    # 1. Test if current token is valid (Get Fund Limits which is usually available)
    logger.info("1. Testing if current token is valid by fetching Fund limits...")
    fund_url = f"{dhan_provider.REST_URL}/fundlimit"
    fund_headers = {
        "access-token": dhan_provider.access_token,
        "dhan-client-id": dhan_provider.client_id, # Try with dash
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Also try without specific client id header if that fails, as per _make_request
    base_headers = {
        "access-token": dhan_provider.access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        # Try generic request first to see if token works
        resp = requests.get(fund_url, headers=base_headers)
        logger.info(f"Fund Limit Status (Base Headers): {resp.status_code}")
        if resp.ok:
             logger.info("Current Token is VALID.")
        else:
             logger.warning(f"Current Token might be INVALID: {resp.text}")
    except Exception as e:
        logger.error(f"Fund limit check failed: {e}")

    logger.info("-" * 20)
    logger.info("2. Attempting to refresh token with variations...")
    url = f"{dhan_provider.REST_URL}/RenewToken"

    # Variation A: As per current code
    headers_a = {
        "access-token": dhan_provider.access_token,
        "dhanClientId": dhan_provider.client_id,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    logger.info("Variation A: current code (dhanClientId header)")
    try:
        response = requests.post(url, headers=headers_a, json={})
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
    except Exception as e: logger.error(e)

    # Variation B: Payload with client id
    logger.info("Variation B: Body with dhanClientId")
    try:
        response = requests.post(url, headers=base_headers, json={"dhanClientId": dhan_provider.client_id})
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
    except Exception as e: logger.error(e)

    # Variation C: Header with dashes
    logger.info("Variation C: Header 'dhan-client-id'")
    headers_c = base_headers.copy()
    headers_c["dhan-client-id"] = dhan_provider.client_id
    try:
        response = requests.post(url, headers=headers_c, json={})
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
    except Exception as e: logger.error(e)

    # Variation D: POST with empty body but correct Content-Type (maybe it needs empty object {})
    # (Already doing json={} above)

    # Variation D: Check with correct payload structure?
    # Some docs say: { "dhanClientId": "...", "validity": "..." ?? }
    # Let's try sending the client ID in the body as "dhanClientId" AND "accessToken" in header
    logger.info("Variation D: Body with 'dhanClientId' and 'accessToken' in body too")
    try:
        payload = {
            "dhanClientId": dhan_provider.client_id,
            "accessToken": dhan_provider.access_token
        }
        response = requests.post(url, headers=base_headers, json=payload)
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
    except Exception as e: logger.error(e)

    # Variation E: Exact headers from user request (POST with empty body)
    logger.info("Variation E: Exact headers from user request (access-token, dhanClientId) no Content-Type")
    headers_e = {
        "access-token": dhan_provider.access_token,
        "dhanClientId": dhan_provider.client_id
    }
    try:
        # requests.post(url) sends a POST request. If no data/json is provided, body is empty.
        response = requests.post(url, headers=headers_e)
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
        logger.info(f"Request headers sent: {response.request.headers}")
    except Exception as e: logger.error(e)

    # Variation F: Explicit empty data
    logger.info("Variation F: headers_e with data=''")
    try:
        response = requests.post(url, headers=headers_e, data="")
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
    except Exception as e: logger.error(e)

    # Variation G: Strict CURL replication
    logger.info("Variation G: Strict CURL replication (No Content-Type, No Body)")
    headers_g = {
        "access-token": dhan_provider.access_token,
        "dhanClientId": dhan_provider.client_id
    }
    try:
        # requests.post without data/json usually sets Content-Length: 0
        # and does NOT set Content-Type.
        response = requests.post(url, headers=headers_g)
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
        logger.info(f"Request headers sent: {response.request.headers}")
    except Exception as e: logger.error(e)

    # Variation H: Hardcoded User Credentials (Exact Replica)
    logger.info("Variation H: Hardcoded User Credentials from prompt")
    hardcoded_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY4MjI0MTg2LCJpYXQiOjE3NjgxMzc3ODYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5MTA4Mjk5In0._IzSyQrlNxj3eI4k0GrIUC4K0JfvVbIyIoPD9lx6-Q14bUEjcNS2Q4Vw7WUbBxMVZTc21a2dqut_0n5mEglIPQ"
    hardcoded_client_id = "1109108299"

    headers_h = {
        "access-token": hardcoded_token,
        "dhanClientId": hardcoded_client_id
    }
    # No Accept, No Content-Type

    try:
        response = requests.post(url, headers=headers_h)
        logger.info(f"Status: {response.status_code}, Body: {response.text}")
        logger.info(f"Request headers sent: {response.request.headers}")
    except Exception as e: logger.error(e)

if __name__ == "__main__":
    try:
        asyncio.run(test_dhan_token_refresh())
    except KeyboardInterrupt:
        pass
