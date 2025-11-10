import json

from fastapi import WebSocket, WebSocketDisconnect

from config.redis_config import active_connections, redis
from main import app


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for clients to subscribe/unsubscribe to stocks."""
    await websocket.accept()
    active_connections[websocket] = set()
    print("🔌 Client connected")

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)

            action = msg.get("action")
            channel = msg.get("channel")

            if action == "subscribe" and channel:
                active_connections[websocket].add(channel)
                await websocket.send_json({"message": f"✅ Subscribed to {channel}"})

            elif action == "unsubscribe" and channel:
                active_connections[websocket].discard(channel)
                await websocket.send_json({"message": f"❌ Unsubscribed from {channel}"})

    except WebSocketDisconnect:
        print("🔌 Client disconnected")
        del active_connections[websocket]


@app.post("/publish/{symbol}")
async def publish_stock(symbol: str, price: float):
    """Simulate publishing a stock price update."""
    channel = f"stocks:{symbol.upper()}"
    payload = json.dumps({"symbol": symbol.upper(), "price": price})
    await redis.publish(channel, payload)
    print(f"📢 Published to {channel}: {payload}")
    return {"status": "ok", "channel": channel, "data": payload}
