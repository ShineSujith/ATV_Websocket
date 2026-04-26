from app.service import clients, video_ready, rabbit_mq_listener
import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock

def test_websocket_connection(client):
    """Test websocket"""

    with client.websocket_connect("/ws") as websocket:
        websocket.send_text("ping")
        assert len(clients) == 1
        websocket.close()

    assert len(clients) == 0

class WebSocketSendError(RuntimeError):
    """Simulate an error"""

class FakeWebSocket:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    async def send_json(self, data):
        if self.fail:
            raise WebSocketSendError("WebSocket failed")
        self.sent.append(data)

@pytest.mark.asyncio
async def test_send_video_ready():
    ws1 = FakeWebSocket()
    clients.add(ws1)
    await video_ready("test")
    expected_payload = [{"type": "video_ready", "message": "test"}]
    assert ws1.sent == expected_payload
    clients.remove(ws1)

@pytest.mark.asyncio
async def test_send_video_ready_failed():
    ws1 = FakeWebSocket(fail=True)
    clients.add(ws1)
    await video_ready("test")
    assert ws1 not in clients

class FakeQueueIterator:
    """Simulates queue.iterator()"""
    def __init__(self, messages):
        self._messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    def __aiter__(self):
        async def gen():
            for msg in self._messages:
                yield msg
        return gen()

class FakeProcessCM:
    """Fake async context manager for msg.process()"""
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

class FakeMessage:
    """Fake message"""
    def __init__(self, body, routing_key):
        self.body = body
        self.routing_key = routing_key
        self.headers = {"filename": "output_tmp.mp4"}

    def process(self):
        """Fake proccess"""
        return FakeProcessCM()

class FakeQueue:
    """Fake queue"""
    def __init__(self, messages):
        self._messages = messages

    async def bind(self, *args, **kwargs):
        """Bind"""
        return None

    def iterator(self):
        """Iterator"""
        return FakeQueueIterator(self._messages)

class FakeChannel:
    """Fake channel"""
    def __init__(self, messages_by_queue):
        self._messages_by_queue = messages_by_queue

    async def declare_exchange(self, *args, **kwargs):
        """Exchange object isn't used in test"""
        return self

    async def declare_queue(self, queue_name, *args, **kwargs):
        """Declare queue - return messages specific to this queue"""
        messages = self._messages_by_queue.get(queue_name, [])
        return FakeQueue(messages)

class FakeConnection:
    """Fake connection"""
    def __init__(self, messages_by_queue):
        self._messages_by_queue = messages_by_queue
        self._channel = FakeChannel(messages_by_queue)

    async def channel(self):
        """Channel"""
        return self._channel

@pytest.mark.asyncio
async def test_rabbit_listener(tmp_path):
    fake_msg = FakeMessage(body=json.dumps("test").encode(), routing_key="video.test")

    async def fake_connect(*args, **kwargs):
        return FakeConnection({"video_events_queue": [fake_msg]})

    mock_clip = MagicMock()
    mock_final = MagicMock()

    def fake_write_videofile(path):
        open(path, "wb").write(b"concatenated_video")

    mock_final.write_videofile.side_effect = fake_write_videofile

    (tmp_path / "current_output.mp4").write_bytes(b"existing_video")

    with patch("app.service.aio_pika.connect_robust", new=fake_connect), \
         patch("app.service.VIDEOS_DIR", str(tmp_path)), \
         patch("app.service.VideoFileClip", return_value=mock_clip), \
         patch("app.service.concatenate_videoclips", return_value=mock_final) as mock_concat, \
         patch("app.service.video_ready", new_callable=AsyncMock):

        task = asyncio.create_task(rabbit_mq_listener())
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_concat.assert_called_once()
        mock_final.write_videofile.assert_called_once()

@pytest.mark.asyncio
async def test_rabbit_listener_first_video(tmp_path):
    """First message: no existing file, video is written directly."""
    video_bytes = b"fake_video_data"
    fake_msg = FakeMessage(body=video_bytes, routing_key="video.upload")
    fake_msg.headers = {"filename": "received.mp4"}

    async def fake_connect(*args, **kwargs):
        return FakeConnection({"video_events_queue": [fake_msg]})

    with patch("app.service.aio_pika.connect_robust", new=fake_connect), \
         patch("app.service.VIDEOS_DIR", str(tmp_path)), \
         patch("app.service.VideoFileClip") as mock_video_file_clip, \
         patch("app.service.video_ready", new_callable=AsyncMock) as mock_send:

        task = asyncio.create_task(rabbit_mq_listener())
        await asyncio.sleep(0.1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # File should be written directly, no concatenation
        output = tmp_path / "current_output.mp4"
        assert output.exists()
        assert output.read_bytes() == video_bytes
        mock_video_file_clip.assert_not_called()
        mock_send.assert_called_once_with("current_output.mp4")