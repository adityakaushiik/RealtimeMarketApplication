import struct
import json

# def pack_ohlcv(channel: str, timestamp: float, ohlcv: list[float]) -> bytes:
#     """
#     Packs a timestamp (float) and OHLCV data (list of 5 floats: [open, high, low, close, volume])
#     into a binary format using little-endian double-precision floats (8 bytes each, total 48 bytes).
#
#     Args:
#         channel (str): The channel that the timestamp belongs to.
#         timestamp (float): Unix timestamp in seconds.
#         ohlcv (list[float]): List of 5 floats representing OHLCV values.
#
#     Returns:
#         bytes: The packed binary data.
#
#     Raises:
#         ValueError: If ohlcv does not have exactly 5 elements.
#     """
#     if len(ohlcv) != 5:
#         raise ValueError("OHLCV must be a list of exactly 5 floats.")
#
#     open_, high, low, close, volume = ohlcv
#     numbers_bytes = None
#
#     try:
#         # Pack the 6 numeric fields first (fixed 48 bytes)
#         numbers_bytes = struct.pack("<dddddd", timestamp, open_, high, low, close, volume)
#
#         # Encode channel and null-terminate (add b'\x00')
#         channel_bytes = channel.encode('utf-8') + b'\x00'
#
#         final_bytes = numbers_bytes + channel_bytes
#
#     except Exception as e:
#         print(e)
#         final_bytes = b''
#
#     return final_bytes


def pack_ohlcv_json(channel: str, timestamp: float, ohlcv: list[float]) -> str:

    res = {
        "channel": channel,
        "timestamp": timestamp,
        "ohlcv": ohlcv
    }

    return json.dumps(res)


def unpack_ohlcv(binary_data: bytes) -> tuple[float, list[float]]:
    """
    Unpacks binary data back into a timestamp (float) and OHLCV list.

    Args:
        binary_data (bytes): The packed binary data (must be exactly 48 bytes).

    Returns:
        tuple[float, list[float]]: (timestamp, [open, high, low, close, volume])

    Raises:
        struct.error: If the data length is incorrect or unpacking fails.
        ValueError: If the data length is not 48 bytes.
    """
    if len(binary_data) != 48:
        raise ValueError("Binary data must be exactly 48 bytes.")

    timestamp, open_, high, low, close, volume = struct.unpack("<dddddd", binary_data)
    return timestamp, [open_, high, low, close, volume]
