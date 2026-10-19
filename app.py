import json
import logging
import os
import queue
import threading
import time
from datetime import timezone

from azure.eventhub import EventHubConsumerClient
from dotenv import load_dotenv
from flask import Flask, Response

logging.getLogger("azure.eventhub").setLevel(logging.CRITICAL)

load_dotenv()

CONN = os.environ.get("IOTHUB_CONNECTION_STRING", "")
if not CONN:
    raise SystemExit("IOTHUB_CONNECTION_STRING is not set. Copy .env.example to .env and fill it in.")

app = Flask(__name__)

state_lock = threading.Lock()
subscribers = set()
state = {
    "raw": [],
    "text": "",
    "event_time": None,
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
    if event.enqueued_time is not None:
        event_time = event.enqueued_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    publish({
        "raw": list(raw),
        "text": raw.decode("utf-8", errors="replace"),
        "event_time": event_time,
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


PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>IIoT Live Endpoint Data</title>
<style>
  body { font-family: Consolas, monospace; background:#0f172a; color:#e2e8f0; margin:0; padding:24px; }
  .banner { background:#1d4ed8; padding:16px 20px; border-radius:10px; font-size:22px; font-weight:bold; letter-spacing:1px; }
  .grid { display:grid; gap:16px; margin-top:18px; }
  .card { background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px 20px; }
  .card h2 { margin:0 0 10px; font-size:13px; text-transform:uppercase; color:#94a3b8; letter-spacing:1.5px; }
  .value { font-size:19px; word-break:break-all; line-height:1.7; min-height:30px; }
  #raw { color:#38bdf8; }
  #text { color:#4ade80; }
  .meta { margin-top:16px; display:flex; gap:28px; flex-wrap:wrap; color:#94a3b8; font-size:14px; }
  .dot { display:inline-block; width:10px; height:10px; border-radius:50%; background:#f59e0b; margin-right:8px; }
  .dot.live { background:#22c55e; box-shadow:0 0 8px #22c55e; }
  .dot.error { background:#ef4444; }
</style>
</head>
<body>
  <div class="banner">NAME: CHANDRESH P &amp; ROLL NO: 24EIR016</div>
  <div class="grid">
    <div class="card">
      <h2>Raw ASCII Values - live from endpoint</h2>
      <div class="value" id="raw">waiting for data ...</div>
    </div>
    <div class="card">
      <h2>String Output - same live bytes decoded</h2>
      <div class="value" id="text">waiting for data ...</div>
    </div>
  </div>
  <div class="meta">
    <span><span class="dot" id="dot"></span><span id="status">connecting</span></span>
    <span>events received: <span id="count">0</span></span>
    <span>last event (UTC): <span id="time">-</span></span>
  </div>
<script>
  const rawEl = document.getElementById("raw");
  const textEl = document.getElementById("text");
  const statusEl = document.getElementById("status");
  const countEl = document.getElementById("count");
  const timeEl = document.getElementById("time");
  const dotEl = document.getElementById("dot");

  function apply(s) {
    if (s.raw && s.raw.length) rawEl.textContent = s.raw.join(" ");
    if (s.text !== undefined && s.text !== null) textEl.textContent = s.text;
    if (s.count !== undefined) countEl.textContent = s.count;
    timeEl.textContent = s.event_time || "-";
    statusEl.textContent = s.status;
    dotEl.className = "dot" +
      (s.status === "live" ? " live" : (s.status && s.status.indexOf("error") === 0 ? " error" : ""));
  }

  const es = new EventSource("/stream");
  es.onmessage = (e) => apply(JSON.parse(e.data));
  es.onerror = () => { statusEl.textContent = "reconnecting ..."; dotEl.className = "dot error"; };
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


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


if __name__ == "__main__":
    threading.Thread(target=consumer_loop, daemon=True).start()
    app.run(host="127.0.0.1", port=5000, threaded=True)
