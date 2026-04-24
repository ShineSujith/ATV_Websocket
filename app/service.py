import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import aio_pika
from contextlib import asynccontextmanager
import asyncio
from moviepy import VideoFileClip, concatenate_videoclips

RABBIT_URL = os.getenv("RABBIT_URL")
EXCHANGE_NAME = "ATV_Project_Exchange"

VIDEOS_DIR = Path("videos")
os.makedirs(VIDEOS_DIR, exist_ok=True)

clients = set()

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
                    filename = os.path.basename(msg.headers.get("filename", "received.mp4"))
                    file_path = os.path.join(VIDEOS_DIR, "current_output.mp4")
                    incoming_path = os.path.join(VIDEOS_DIR, filename)
                    output_path = os.path.join(VIDEOS_DIR, "output_temp.mp4")
                    print(f"Received {len(data)} bytes")
                    if os.path.exists(file_path):
                        with open(incoming_path, "wb") as f:
                            f.write(data)
                        vid1 = VideoFileClip(file_path)
                        vid2 = VideoFileClip(incoming_path)
                        try:
                            final = concatenate_videoclips([vid1, vid2])
                            final.write_videofile(output_path)
                        finally:
                            final.close()
                            vid1.close()
                            vid2.close()
                        os.replace(output_path, file_path)
                    else:
                        with open(file_path, "wb") as f:
                            f.write(data)
                    await video_ready("current_output.mp4")
    await asyncio.gather(
        read_queue(video_queue, "Video"),
    )
