import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# import pika
import aio_pika
from contextlib import asynccontextmanager
import asyncio
import json
import os

RABBIT_URL = os.getenv("RABBIT_URL")
EXCHANGE_NAME = "ATV_Project_Exchange"

@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Starts rabbit mq task"""
    task = asyncio.create_task(rabbit_mq_listener())
    try:
        yield
    finally:
        task.cancel()

app = FastAPI(lifespan=lifespan)

app.mount("/videos", StaticFiles(directory="videos"), name="videos")

async def video_ready(fileName: str):
    """Send payload through open socket"""
    payload = {"type": "video_ready", "message": fileName}
    disconnected_clients = set()

    for ws in clients:
        try:
            await ws.send_json(payload)
        except (WebSocketDisconnect, RuntimeError) as e:
            print(f"Failed to send to client: {e}")
            disconnected_clients.add(ws)

    # Remove all disconnected clients safely
    clients.difference_update(disconnected_clients)

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """Open websocket and keep alive"""
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        clients.remove(ws)

# connection = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
# channel = connection.channel()
# channel.queue_declare(queue=EXCHANGE_NAME, durable=True)
# channel.basic_qos(prefetch_count=1)

# def on_message(ch, method, properties, body):
#     filename = properties.headers.get("filename", "received.mp4")
#     with open(filename, "wb") as f:
#         f.write(body)
#     print(f"Received {len(body)} bytes")
#     video_ready(fileName)
#     ch.basic_ack(method.delivery_tag)

# channel.basic_consume(queue=EXCHANGE_NAME, on_message_callback=on_message)
# channel.start_consuming()

async def rabbit_mq_listener():
    """
    Setup queue, exchange, set binding etc, then wait for messages
    """
    conn = await aio_pika.connect_robust(RABBIT_URL)
    ch = await conn.channel()

    # Declare the topic exchange
    ex = await ch.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC)

    # Queue for events
    video_queue = await ch.declare_queue("video_events_queue")

    # Bind to routing key
    await video_queue.bind(ex, routing_key="video.*")

    print("Listening for order events (routing keys: 'video.*')...")

    async def read_queue(queue, queue_name):
        async with queue.iterator() as q:
            async for msg in q:
                async with msg.process():
                    data = msg.body
                    print(f"{queue_name} Event:", msg.routing_key, data)
                    filename = msg.headers.get("filename", "received.mp4")
                    with open(filename, "wb") as f:
                        f.write(msg.body)
                    print(f"Received {len(msg.body)} bytes")
                    await video_ready(fileName)
    await asyncio.gather(
        read_queue(video_queue, "Video"),
    )
