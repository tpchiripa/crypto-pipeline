import { usePolling } from "../hooks/usePolling.js";
import { getRetailTransactions } from "../api.js";

function timeAgo(isoString) {
  const seconds = Math.max(0, (Date.now() - new Date(isoString).getTime()) / 1000);
  if (seconds < 60) return `${seconds.toFixed(0)}s ago`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(0)}m ago`;
  return `${(seconds / 3600).toFixed(0)}h ago`;
}

function formatMoney(value) {
  if (value == null) return "—";
  return value.toLocaleString(undefined, { style: "currency", currency: "USD" });
}

export default function RetailFeed() {
  const { data, error } = usePolling(() => getRetailTransactions(15), 5000);

  return (
    <div className="panel retail-feed">
      <div className="panel-header">
        <span className="panel-title">RETAIL FEED</span>
        <span className="panel-sub mono">CSV drop-folder</span>
      </div>

      {error && <div className="feed-error mono">feed unavailable — retrying…</div>}

      <div className="retail-header-row mono">
        <span>PRODUCT</span>
        <span>QTY</span>
        <span>TOTAL</span>
        <span>TIME</span>
      </div>

      <div className="retail-rows">
        {(data || []).map((tx, i) => (
          <div key={`${tx.received_at}-${i}`} className="retail-row">
            <span className="retail-product">
              {tx.product_name}
              {tx.store_id && <span className="retail-store mono"> · {tx.store_id}</span>}
            </span>
            <span className="retail-qty mono">{tx.quantity ?? "—"}</span>
            <span className="retail-total mono">{formatMoney(tx.total_amount)}</span>
            <span className="retail-time mono">{timeAgo(tx.received_at)}</span>
          </div>
        ))}
        {!data && !error && <div className="feed-loading mono">loading transactions…</div>}
        {data && data.length === 0 && (
          <div className="feed-loading mono">
            no transactions yet — drop a CSV into data/incoming/
          </div>
        )}
      </div>
    </div>
  );
}
