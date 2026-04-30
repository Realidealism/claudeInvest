import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

interface FundColumn {
  code: string;
  name: string;
}

interface FundChange {
  curr: number | null;
  prev: number | null;
  diff: number | null;
}

interface TickerChange {
  ticker_name: string;
  funds: Record<string, FundChange>;
}

interface FlowData {
  periods: string[];
  fund_columns: FundColumn[];
  changes: Record<string, TickerChange>;
}

export default function FlowPage() {
  const [data, setData] = useState<FlowData | null>(null);
  const [sortBy, setSortBy] = useState<"ticker" | "activity">("activity");

  useEffect(() => {
    fetch("/data/flow.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const tickers = Object.entries(data.changes);

  // Sort
  const sorted = [...tickers].sort((a, b) => {
    if (sortBy === "activity") {
      const aAct = Object.values(a[1].funds).reduce((s, f) => s + Math.abs(f.diff || 0), 0);
      const bAct = Object.values(b[1].funds).reduce((s, f) => s + Math.abs(f.diff || 0), 0);
      return bAct - aAct;
    }
    return a[0].localeCompare(b[0]);
  });

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">資金流向熱力圖</h2>
      <p className="text-xs text-text-secondary mb-4">
        {data.periods[0]} → {data.periods[1]} 各基金 Top 10 估算張數變化
      </p>

      <div className="flex gap-2 mb-4">
        <button
          onClick={() => setSortBy("activity")}
          className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
            sortBy === "activity" ? "bg-accent text-white" : "bg-surface-alt text-text-secondary border border-border"
          }`}
        >
          依異動排序
        </button>
        <button
          onClick={() => setSortBy("ticker")}
          className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
            sortBy === "ticker" ? "bg-accent text-white" : "bg-surface-alt text-text-secondary border border-border"
          }`}
        >
          依代號排序
        </button>
      </div>

      <div className="overflow-x-auto">
        <table className="text-xs">
          <thead>
            <tr className="border-b border-border text-text-secondary">
              <th className="py-2 pr-4 text-left font-medium sticky left-0 bg-surface z-10 w-16">代號</th>
              <th className="py-2 pr-4 text-left font-medium sticky left-[4.5rem] bg-surface z-10 w-20">名稱</th>
              {data.fund_columns.map((f) => (
                <th key={f.code} className="py-2 px-1 text-center font-medium whitespace-nowrap">
                  <div className="w-14 truncate" title={f.name}>{f.name.slice(0, 4)}</div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map(([ticker, change]) => (
              <tr key={ticker} className="border-b border-border/30">
                <td className="py-2 pr-4 font-mono sticky left-0 bg-surface z-10 w-16">
                  <Link to={`/stock/${ticker}`} className="text-accent hover:underline">{ticker}</Link>
                </td>
                <td className="py-2 pr-4 sticky left-[4.5rem] bg-surface z-10 w-20 truncate">
                  {change.ticker_name}
                </td>
                {data.fund_columns.map((col) => {
                  const fc = change.funds[col.code];
                  if (!fc) return <td key={col.code} className="py-2 px-3 text-center text-text-secondary">·</td>;
                  const diff = fc.diff || 0;
                  const lots = Math.round(diff / 1000); // convert shares to 張
                  const absLots = Math.abs(lots);
                  // Color intensity based on magnitude (100張 = full intensity)
                  const opacity = Math.min(absLots / 100, 1);
                  // Taiwan convention: red = buy/increase, green = sell/decrease
                  const bg = lots > 0
                    ? `rgba(239, 68, 68, ${opacity * 0.3})`
                    : lots < 0
                    ? `rgba(34, 197, 94, ${opacity * 0.3})`
                    : undefined;
                  return (
                    <td
                      key={col.code}
                      className="py-2 px-3 text-center font-mono"
                      style={{ backgroundColor: bg }}
                      title={`${fc.prev?.toFixed(1) || "—"}% → ${fc.curr?.toFixed(1)}% (${lots > 0 ? "+" : ""}${lots}張)`}
                    >
                      {lots !== 0 ? (
                        <span className={lots > 0 ? "text-negative" : "text-positive"}>
                          {lots > 0 ? "+" : ""}{lots}
                        </span>
                      ) : (
                        <span className="text-text-secondary">·</span>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
