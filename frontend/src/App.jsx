import { useEffect, useState } from "react";
import "./App.css";

const initialState = {
  raw: [],
  text: "",
  event_time: null,
  count: 0,
  status: "connecting",
};

function App() {
  const [data, setData] = useState(initialState);

  useEffect(() => {
    const source = new EventSource("/stream");
    source.onmessage = (event) => setData(JSON.parse(event.data));
    source.onerror = () =>
      setData((prev) => ({ ...prev, status: "reconnecting ..." }));
    return () => source.close();
  }, []);

  const statusClass =
    data.status === "live" ? "live" : data.status.startsWith("error") ? "error" : "";

  return (
    <div className="page">
      <header className="banner">NAME: CHANDRESH P &amp; ROLL NO: 24EIR016</header>

      <main className="grid">
        <section className="card">
          <h2>Raw ASCII Values - live from endpoint</h2>
          <div className="value raw">
            {data.raw.length > 0 ? data.raw.join(" ") : "waiting for data ..."}
          </div>
        </section>

        <section className="card">
          <h2>String Output - same live bytes decoded</h2>
          <div className="value text">
            {data.count > 0 ? data.text : "waiting for data ..."}
          </div>
        </section>
      </main>

      <footer className="meta">
        <span>
          <span className={"dot " + statusClass} />
          {data.status}
        </span>
        <span>events received: {data.count}</span>
        <span>last event (UTC): {data.event_time ?? "-"}</span>
      </footer>
    </div>
  );
}

export default App;
