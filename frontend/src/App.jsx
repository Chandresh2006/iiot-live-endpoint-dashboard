import { useEffect, useState } from "react";
import "./App.css";

const initialState = {
  raw: [],
  text: "",
  event_time: null,
  event_time_iso: null,
  count: 0,
  status: "connecting",
};

const STALE_AFTER_SECONDS = 30;

function formatAge(seconds) {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function App() {
  const [data, setData] = useState(initialState);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const source = new EventSource("/stream");
    source.onmessage = (event) => {
      setData(JSON.parse(event.data));
      setNow(Date.now());
    };
    source.onerror = () =>
      setData((prev) => ({ ...prev, status: "reconnecting ..." }));
    return () => source.close();
  }, []);

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const hasData = data.count > 0;
  const eventMs = data.event_time_iso ? Date.parse(data.event_time_iso) : null;
  const ageSeconds =
    hasData && eventMs ? Math.max(0, Math.floor((now - eventMs) / 1000)) : null;
  const isStale = ageSeconds !== null && ageSeconds > STALE_AFTER_SECONDS;
  const isError = data.status.startsWith("error");

  let statusText = data.status;
  let statusClass = "";
  if (isError) {
    statusClass = "error";
  } else if (isStale) {
    statusClass = "stale";
    statusText = `no data for ${formatAge(ageSeconds)}`;
  } else if (hasData) {
    statusClass = "live";
    statusText = "live";
  }

  return (
    <div className={"page" + (isStale ? " stale" : "")}>
      <header className="banner">NAME: CHANDRESH P &amp; ROLL NO: 24EIR016</header>

      <main className="grid">
        <section className="card">
          <h2>Raw ASCII Values - live from endpoint</h2>
          <div className="value raw">
            {hasData ? data.raw.join(" ") : "waiting for data ..."}
          </div>
        </section>

        <section className="card">
          <h2>String Output - same live bytes decoded</h2>
          <div className="value text">
            {hasData ? data.text : "waiting for data ..."}
          </div>
        </section>
      </main>

      <footer className="meta">
        <span>
          <span className={"dot " + statusClass} />
          {statusText}
        </span>
        <span>events received: {data.count}</span>
        <span>last event (UTC): {data.event_time ?? "-"}</span>
      </footer>
    </div>
  );
}

export default App;
