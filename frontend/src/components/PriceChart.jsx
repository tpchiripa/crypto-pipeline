import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { usePolling } from "../hooks/usePolling.js";
import { getTrades } from "../api.js";

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const point = payload[0].payload;
  return (
    <div className="chart-tooltip mono">
      <div>{point.price.toLocaleString(undefined, { minimumFractionDigits: 2 })}</div>
      <div className="chart-tooltip-time">{new Date(point.trade_time).toLocaleTimeString()}</div>
    </div>
  );
}

export default function PriceChart({ symbol }) {
  const { data } = usePolling(() => getTrades(symbol, 60), 2000, [symbol]);

  // API returns newest-first; chart wants chronological order left-to-right
  const chartData = data ? [...data].reverse() : [];

  const prices = chartData.map((d) => d.price);
  const min = prices.length ? Math.min(...prices) : 0;
  const max = prices.length ? Math.max(...prices) : 1;
  const pad = (max - min) * 0.15 || max * 0.001;

  return (
    <div className="panel price-chart">
      <div className="panel-header">
        <span className="panel-title">PRICE</span>
        <span className="panel-sub mono">{symbol} · last {chartData.length} trades</span>
      </div>

      <div className="chart-area">
        {chartData.length > 1 ? (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="priceFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--amber)" stopOpacity={0.35} />
                  <stop offset="100%" stopColor="var(--amber)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="var(--line)" strokeDasharray="2 4" vertical={false} />
              <XAxis dataKey="trade_time" hide />
              <YAxis
                domain={[min - pad, max + pad]}
                orientation="right"
                tick={{ fill: "var(--ink-dim)", fontSize: 11, fontFamily: "var(--font-mono)" }}
                axisLine={false}
                tickLine={false}
                width={70}
                tickFormatter={(v) => v.toLocaleString(undefined, { maximumFractionDigits: 2 })}
              />
              <Tooltip content={<CustomTooltip />} />
              <Area
                type="monotone"
                dataKey="price"
                stroke="var(--amber)"
                strokeWidth={1.5}
                fill="url(#priceFill)"
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <div className="chart-loading mono">gathering price history…</div>
        )}
      </div>
    </div>
  );
}
