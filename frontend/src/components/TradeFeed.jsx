import { usePolling } from "../hooks/usePolling.js";
import { getTrades } from "../api.js";

function timeAgo(isoString) {
  const seconds = Math.max(0, (Date.now() - new Date(isoString).getTime()) / 1000);
  if (seconds < 1) return "now";
  if (seconds < 60) return `${seconds.toFixed(0)}s ago`;
  return `${(seconds / 60).toFixed(0)}m ago`;
}

export default function TradeFeed({ symbol }) {
  const { data, error } = usePolling(() => getTrades(symbol, 30), 1500, [symbol]);

  return (
    <div className="panel trade-feed">
      <div className="panel-header">
        <span className="panel-title">TRADE FEED</span>
        <span className="panel-sub mono">{symbol}</span>
      </div>

      {error && <div className="feed-error mono">feed unavailable — retrying…</div>}

      <div className="feed-header-row mono">
        <span>SIDE</span>
        <span>PRICE</span>
        <span>QTY</span>
        <span>TIME</span>
      </div>

      <div className="feed-rows">
        {(data || []).map((trade, i) => (
          <div
            key={`${trade.trade_time}-${i}`}
            className={`feed-row ${trade.is_buyer_maker ? "sell" : "buy"}`}
          >
            <span className="feed-side mono">{trade.is_buyer_maker ? "SELL" : "BUY"}</span>
            <span className="feed-price mono">
              {trade.price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </span>
            <span className="feed-qty mono">{trade.quantity.toFixed(5)}</span>
            <span className="feed-time mono">{timeAgo(trade.trade_time)}</span>
          </div>
        ))}
        {!data && !error && <div className="feed-loading mono">loading trades…</div>}
      </div>
    </div>
  );
}
