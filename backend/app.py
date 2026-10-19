import json
import logging
import os
import queue
import threading
import time
from datetime import timezone
from pathlib import Path

from azure.eventhub import EventHubConsumerClient
from dotenv import load_dotenv
from flask import Flask, Response, send_from_directory

logging.getLogger("azure.eventhub").setLevel(logging.CRITICAL)

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

CONN = os.environ.get("IOTHUB_CONNECTION_STRING", "")
if not CONN:
    raise SystemExit("IOTHUB_CONNECTION_STRING is not set. Copy .env.example to .env and fill it in.")

DIST = ROOT / "frontend" / "dist"

app = Flask(__name__)

state_lock = threading.Lock()
subscribers = set()
state = {
    "raw": [],
    "text": "",
    "event_time": None,
    "event_time_iso": None,
    "count": 0,
    "status": "connecting",
}


def publish(updates):
    with state_lock:
        state.update(updates)
        snapshot = dict(state)
    for q in list(subscribers):
        try:
            q.put_nowait(snapshot)
        except queue.Full:
            pass


def on_event(partition_context, event):
    body = event.body
    if isinstance(body, str):
        raw = body.encode("utf-8")
    elif isinstance(body, (bytes, bytearray, memoryview)):
        raw = bytes(body)
    else:
        raw = b"".join(bytes(chunk) for chunk in body)

    with state_lock:
        count = state["count"] + 1

    event_time = None
    event_time_iso = None
    if event.enqueued_time is not None:
        event_time = event.enqueued_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        event_time_iso = event.enqueued_time.astimezone(timezone.utc).isoformat()

    publish({
        "raw": list(raw),
        "text": raw.decode("utf-8", errors="replace"),
        "event_time": event_time,
        "event_time_iso": event_time_iso,
        "count": count,
        "status": "live",
    })


def consumer_loop():
    while True:
        try:
            publish({"status": "connecting"})
            client = EventHubConsumerClient.from_connection_string(CONN, consumer_group="$Default")
            with client:
                publish({"status": "listening"})
                client.receive(on_event=on_event, starting_position="@latest", max_wait_time=30)
        except Exception as exc:
            publish({"status": "error: " + str(exc)})
            time.sleep(5)


@app.route("/latest")
def latest():
    with state_lock:
        snapshot = dict(state)
    return Response(json.dumps(snapshot), mimetype="application/json")


@app.route("/stream")
def stream():
    def gen():
        q = queue.Queue(maxsize=200)
        subscribers.add(q)
        try:
            with state_lock:
                snapshot = dict(state)
            yield "data: " + json.dumps(snapshot) + "\n\n"
            while True:
                try:
                    yield "data: " + json.dumps(q.get(timeout=15)) + "\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            subscribers.discard(q)

    return Response(gen(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def frontend(path):
    if not DIST.exists():
        return Response(
            "Frontend not built yet. Run: cd frontend && npm install && npm run build",
            status=503,
            mimetype="text/plain",
        )
    target = DIST / path
    if path and target.is_file():
        return send_from_directory(DIST, path)
    return send_from_directory(DIST, "index.html")


if __name__ == "__main__":
    threading.Thread(target=consumer_loop, daemon=True).start()
    app.run(host="127.0.0.1", port=5000, threaded=True)
