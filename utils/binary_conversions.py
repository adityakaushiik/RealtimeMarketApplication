from construct import Struct, CString, Int64ub, Float32b

# Snapshot: symbol (null-terminated UTF-8), timestamp, open/high/low/close/prev_close (floats), volume
snapshot_format = Struct(
    "symbol" / CString("utf8"),
    "timestamp" / Int64ub,
    "open" / Float32b,
    "high" / Float32b,
    "low" / Float32b,
    "close" / Float32b,
    "prev_close" / Float32b,
    "volume" / Int64ub,
)

example_snapshot_data = {
    "symbol": "AAPL",
    "timestamp": 1732600000,
    "open": 150.0,
    "high": 152.0,
    "low": 149.0,
    "close": 151.5,
    "prev_close": 149.5,
    "volume": 1234567890,
}

# Update: symbol, timestamp, price (float), volume, size (tick volume)
update_format = Struct(
    "symbol" / CString("utf8"),
    "timestamp" / Int64ub,
    "price" / Float32b,
    "volume" / Int64ub,
    "size" / Int64ub,
)

# Update example
example_update_data = {
    "symbol": "AAPL",
    "timestamp": 1732601000,
    "price": 151.8,
    "volume": 987654321,
    "size": 100,
}


def pack_snapshot(data: dict) -> bytes:
    """Pack snapshot data into binary format."""
    return snapshot_format.build(data)


def pack_update(data: dict) -> bytes:
    """Pack update data into binary format."""
    return update_format.build(data)
