import json

from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from fastapi.params import Depends

from services.websocket_manager import WebSocketManager, get_websocket_manager
from utils.common_constants import WebSocketMessageType

websocket_route = APIRouter(prefix="", tags=["socket"])

@websocket_route.websocket("/ws")
async def websocket_endpoint(
        websocket: WebSocket,
        websocket_manager: WebSocketManager = Depends(get_websocket_manager),
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
                websocket_manager.subscribe(websocket, channel)
                await websocket.send_json(
                    {
                        "message_type": WebSocketMessageType.INFO.value,
                        "message": f"Subscribed to {channel}",
                    }
                )

            elif message_type == WebSocketMessageType.UNSUBSCRIBE.value and channel:
                websocket_manager.unsubscribe(websocket, channel)
                await websocket.send_json(
                    {
                        "message_type": WebSocketMessageType.INFO.value,
                        "message": f"Unsubscribed from {channel}",
                    }
                )

    except WebSocketDisconnect:
        print("🔌 Client disconnected")
        websocket_manager.disconnect(websocket)
