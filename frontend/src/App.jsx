import { useState } from "react";
import TickerTape from "./components/TickerTape.jsx";
import PriceChart from "./components/PriceChart.jsx";
import TradeFeed from "./components/TradeFeed.jsx";
import NewsFeed from "./components/NewsFeed.jsx";
import RetailFeed from "./components/RetailFeed.jsx";

const TRACKED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];

export default function App() {
  const [symbol, setSymbol] = useState(TRACKED_SYMBOLS[0]);

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-title">
          <span className="app-title-mark">◆</span>
          <span>PIPELINE</span>
          <span className="app-title-sub mono">/ market feed</span>
        </div>
        <div className="app-status">
          <span className="status-dot" />
          <span className="mono">LIVE</span>
        </div>
      </header>

      <TickerTape />

      <nav className="symbol-tabs">
        {TRACKED_SYMBOLS.map((s) => (
          <button
            key={s}
            className={`symbol-tab mono ${s === symbol ? "active" : ""}`}
            onClick={() => setSymbol(s)}
          >
            {s}
          </button>
        ))}
      </nav>

      <main className="app-grid">
        <PriceChart symbol={symbol} />
        <TradeFeed symbol={symbol} />
      </main>

      <section className="app-section">
        <div className="app-section-label mono">OTHER SOURCES / not crypto</div>
        <div className="secondary-grid">
          <NewsFeed />
          <RetailFeed />
        </div>
      </section>

      <footer className="app-footer mono">
        binance ws + hnrss poll + csv drop → kafka → postgres → fastapi → react
      </footer>
    </div>
  );
}
