import { useEffect, useRef, useState } from "react";
import { usePolling } from "../hooks/usePolling.js";
import { getTicker } from "../api.js";

function TickerItem({ item, prevPrice }) {
  const direction =
    prevPrice == null ? null : item.price > prevPrice ? "up" : item.price < prevPrice ? "down" : null;

  return (
    <span className="ticker-item">
      <span className="ticker-symbol">{item.symbol}</span>
      <span className={`ticker-price mono ${direction ? `flash-${direction}` : ""}`}>
        {item.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
      </span>
      <span className={`ticker-arrow ${direction || ""}`}>
        {direction === "up" ? "▲" : direction === "down" ? "▼" : "·"}
      </span>
    </span>
  );
}

export default function TickerTape() {
  const { data } = usePolling(getTicker, 1500);
  const prevPrices = useRef({});
  const [displayData, setDisplayData] = useState([]);

  useEffect(() => {
    if (!data) return;
    setDisplayData(data);
    const next = {};
    data.forEach((d) => (next[d.symbol] = d.price));
    // Keep previous prices around for one render cycle so we can diff direction,
    // then update for next comparison.
    const timer = setTimeout(() => {
      prevPrices.current = next;
    }, 50);
    return () => clearTimeout(timer);
  }, [data]);

  if (!displayData.length) {
    return (
      <div className="ticker-tape">
        <div className="ticker-track">
          <span className="ticker-loading mono">CONNECTING TO FEED…</span>
        </div>
      </div>
    );
  }

  // Duplicate the list so the scroll loop is seamless
  const loopItems = [...displayData, ...displayData];

  return (
    <div className="ticker-tape">
      <div className="ticker-track">
        {loopItems.map((item, i) => (
          <TickerItem key={`${item.symbol}-${i}`} item={item} prevPrice={prevPrices.current[item.symbol]} />
        ))}
      </div>
    </div>
  );
}
