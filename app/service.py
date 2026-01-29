# import json
import os
# from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# RABBIT_URL = os.getenv("RABBIT_URL")
# EXCHANGE_NAME = "ATV_Project_Exchange"

# clients = set()

# @asynccontextmanager
# async def lifespan(_app: FastAPI):
#     """Starts rabbit mq task"""
#     task = asyncio.create_task(rabbit_mq_listener())
#     try:
#         yield
#     finally:
#         task.cancel()

# app = FastAPI(lifespan=lifespan)

# @app.websocket("/ws")
# async def websocket_endpoint(ws: WebSocket):
#     """Open websocket and keep alive"""
#     await ws.accept()
#     clients.add(ws)
#     try:
#         while True:
#             await ws.receive_text()  # Keep connection alive
#     except WebSocketDisconnect:
#         clients.remove(ws)

# # TODO: make this function send a video 
# async def send_notification(message: str):
#     """Send payload through open socket"""
#     payload = {"type": "notification", "message": message}
#     disconnected_clients = set()

#     for ws in clients:
#         try:
#             await ws.send_json(payload)
#         except (WebSocketDisconnect, RuntimeError) as e:
#             print(f"Failed to send to client: {e}")
#             disconnected_clients.add(ws)

#     # Remove all disconnected clients safely
#     clients.difference_update(disconnected_clients)

import pika

RABBIT_URL = os.getenv("RABBIT_URL")
EXCHANGE_NAME = "ATV_Project_Exchange"

connection = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
channel = connection.channel()
channel.queue_declare(queue=EXCHANGE_NAME, durable=True)
channel.basic_qos(prefetch_count=1)

def on_message(ch, method, properties, body):
    filename = properties.headers.get("filename", "received.mp4")
    with open(filename, "wb") as f:
        f.write(body)
    print(f"Received {len(body)} bytes")
    ch.basic_ack(method.delivery_tag)

channel.basic_consume(queue=EXCHANGE_NAME, on_message_callback=on_message)
channel.start_consuming()
