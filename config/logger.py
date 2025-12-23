import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s"
    # format="%(asctime)s - %(name)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
