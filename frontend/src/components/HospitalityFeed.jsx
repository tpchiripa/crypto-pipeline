import { usePolling } from "../hooks/usePolling.js";
import { getHospitalityItems } from "../api.js";

function formatMoney(value) {
  if (value == null) return "—";
  return value.toLocaleString(undefined, { style: "currency", currency: "USD" });
}

function formatStandard(value, unit) {
  if (value == null || !unit) return "unrecognized unit";
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ${unit}`;
}

export default function HospitalityFeed() {
  const { data, error } = usePolling(() => getHospitalityItems(15), 5000);

  return (
    <div className="panel hospitality-feed">
      <div className="panel-header">
        <span className="panel-title">HOSPITALITY INVENTORY</span>
        <span className="panel-sub mono">unit-standardized</span>
      </div>

      {error && <div className="feed-error mono">feed unavailable — retrying…</div>}

      <div className="hosp-header-row mono">
        <span>INGREDIENT</span>
        <span>RAW</span>
        <span>STANDARD</span>
        <span>COST</span>
      </div>

      <div className="hosp-rows">
        {(data || []).map((item, i) => (
          <div key={`${item.received_at}-${i}`} className="hosp-row">
            <span className="hosp-ingredient">
              {item.ingredient_name}
              {item.supplier && <span className="hosp-supplier mono"> · {item.supplier}</span>}
            </span>
            <span className="hosp-raw mono">
              {item.quantity_raw ?? "—"} {item.unit_raw ?? ""}
            </span>
            <span className={`hosp-standard mono ${!item.unit_standard ? "hosp-unrecognized" : ""}`}>
              {formatStandard(item.quantity_standard, item.unit_standard)}
            </span>
            <span className="hosp-cost mono">{formatMoney(item.cost)}</span>
          </div>
        ))}
        {!data && !error && <div className="feed-loading mono">loading inventory…</div>}
        {data && data.length === 0 && (
          <div className="feed-loading mono">
            no items yet — drop a CSV into data/hospitality_incoming/
          </div>
        )}
      </div>
    </div>
  );
}
