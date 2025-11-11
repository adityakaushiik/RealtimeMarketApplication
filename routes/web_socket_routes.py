import json

from fastapi import WebSocket, WebSocketDisconnect
from fastapi.params import Depends

from main import app
from services.websocket_manager import WebSocketManager, get_websocket_manager
from utils.common_constants import WebSocketMessageType


@app.websocket("/ws")
async def websocket_endpoint(
        websocket: WebSocket,
        websocket_manager: WebSocketManager = Depends(get_websocket_manager)
):
    """Main WebSocket endpoint for clients to subscribe/unsubscribe to stocks."""
    await websocket.accept()
    websocket_manager.connect(websocket)

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)

            message_type = msg.get("message_type")
            channel = msg.get("channel")

            if message_type == WebSocketMessageType.SUBSCRIBE.value and channel:
                websocket_manager.active_connections[websocket].add(channel)
                await websocket.send_json({
                    "message_type": WebSocketMessageType.INFO.value,
                    "message": f"✅ Subscribed to {channel}"
                })

            elif message_type == WebSocketMessageType.UNSUBSCRIBE.value and channel:
                websocket_manager.active_connections[websocket].discard(channel)
                await websocket.send_json({
                    "message_type": WebSocketMessageType.INFO.value,
                    "message": f"❌ Unsubscribed from {channel}"
                })

    except WebSocketDisconnect:
        print("🔌 Client disconnected")
        websocket_manager.disconnect(websocket)