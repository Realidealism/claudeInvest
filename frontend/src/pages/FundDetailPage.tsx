import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";

interface Holding {
  ticker: string;
  ticker_name: string;
  rank?: number;
  weight: number | null;
}

interface FundInfo {
  id: number;
  code: string;
  name: string;
  fund_type: string;
  company: string;
  manager_name: string | null;
}

interface FundsData {
  funds: FundInfo[];
  holdings: Record<string, { monthly: Record<string, Holding[]>; quarterly: Record<string, Holding[]> }>;
  latest_monthly: string;
  latest_quarterly: string;
}

export default function FundDetailPage() {
  const { code } = useParams<{ code: string }>();
  const [data, setData] = useState<FundsData | null>(null);
  const [selectedPeriod, setSelectedPeriod] = useState<string>("");

  useEffect(() => {
    fetch("/data/funds.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  // Set initial period once data + code are available
  useEffect(() => {
    if (!data || !code) return;
    const h = data.holdings[code];
    const periods = Object.keys(h?.monthly || {}).sort().reverse();
    if (periods.length > 0 && !selectedPeriod) {
      setSelectedPeriod(periods[0]);
    }
  }, [data, code]);

  if (!data || !code) return <div className="text-text-secondary">Loading...</div>;

  const fund = data.funds.find((f) => f.code === code);
  const fundHoldings = data.holdings[code];

  if (!fund) return <div className="text-negative">Fund not found: {code}</div>;

  const monthly = fundHoldings?.monthly || {};
  const quarterly = fundHoldings?.quarterly || {};
  const periods = Object.keys(monthly).sort().reverse();
  const qPeriods = Object.keys(quarterly).sort().reverse();

  const isEtf = fund.fund_type === "etf";
  const allHoldings = monthly[selectedPeriod] || [];
  const displayHoldings = isEtf ? allHoldings.slice(0, 50) : allHoldings;
  const totalWeight = displayHoldings.reduce((sum, h) => sum + (h.weight || 0), 0);

  // Previous period for comparison
  const periodIdx = periods.indexOf(selectedPeriod);
  const prevPeriod = periodIdx < periods.length - 1 ? periods[periodIdx + 1] : null;
  const prevHoldings = prevPeriod ? monthly[prevPeriod] || [] : [];
  const prevMap = Object.fromEntries(prevHoldings.map((h) => [h.ticker, h]));

  return (
    <div>
      <div className="flex items-center gap-2 mb-4">
        <Link to="/funds" className="text-text-secondary hover:text-text-primary text-sm">&larr; 返回</Link>
        <h2 className="text-lg font-semibold">{fund.name}</h2>
        <span className="text-xs text-text-secondary">
          {fund.company} / {fund.manager_name || "—"}
        </span>
      </div>

      {/* Period selector */}
      <div className="flex items-center gap-3 mb-4">
        <label className="text-xs text-text-secondary">{fund.fund_type === "etf" ? "持股日期:" : "月報期間:"}</label>
        <select
          value={selectedPeriod}
          onChange={(e) => setSelectedPeriod(e.target.value)}
          className="bg-surface-alt border border-border rounded px-3 py-1.5 text-sm text-text-primary"
        >
          {periods.map((p) => (
            <option key={p} value={p}>{p}</option>
          ))}
        </select>
      </div>

      {/* Holdings bar chart + table */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Visual bar chart */}
        <div className="bg-surface-alt border border-border rounded-lg p-4">
          <h3 className="text-sm font-semibold mb-3 text-text-secondary">
            {fund.fund_type === "etf" ? "持股權重" : "Top 10 持股權重"} ({selectedPeriod})
          </h3>
          <div className="space-y-1.5">
            {displayHoldings.map((h) => {
              const w = h.weight || 0;
              const prev = prevMap[h.ticker];
              const diff = prev ? w - (prev.weight || 0) : null;
              return (
                <div key={h.ticker} className="flex items-center gap-2 text-xs">
                  <span className="w-16 font-mono shrink-0">{h.ticker}</span>
                  <span className="w-16 shrink-0 truncate">{h.ticker_name}</span>
                  <div className="flex-1 h-4 bg-surface rounded overflow-hidden">
                    <div
                      className="h-full bg-accent/60 rounded"
                      style={{ width: `${Math.min(w / 20 * 100, 100)}%` }}
                    />
                  </div>
                  <span className="w-14 text-right font-mono">{w.toFixed(1)}%</span>
                  {diff != null && (
                    <span className={`w-14 text-right font-mono ${diff > 0 ? "text-positive" : diff < 0 ? "text-negative" : "text-text-secondary"}`}>
                      {diff > 0 ? "+" : ""}{diff.toFixed(1)}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
          <div className="mt-2 pt-2 border-t border-border text-xs text-text-secondary">
            {isEtf ? `Top 50 合計: ${totalWeight.toFixed(1)}%` : `Top 10 合計: ${totalWeight.toFixed(1)}%`}
            <span className="ml-2 text-text-secondary/60">
              {isEtf ? `(共 ${allHoldings.length} 檔)` : "(月報 Top 10)"}
            </span>
          </div>
        </div>

        {/* Quarterly holdings if available */}
        {qPeriods.length > 0 && (
          <div className="bg-surface-alt border border-border rounded-lg p-4">
            <h3 className="text-sm font-semibold mb-3 text-text-secondary">
              季報完整持股 ({qPeriods[0]})
            </h3>
            <div className="max-h-[600px] overflow-y-auto space-y-0.5">
              {(quarterly[qPeriods[0]] || []).map((h) => (
                <div key={h.ticker} className="flex items-center gap-2 text-xs">
                  <span className="w-16 font-mono shrink-0">{h.ticker}</span>
                  <span className="w-20 shrink-0 truncate">{h.ticker_name}</span>
                  <div className="flex-1 h-3 bg-surface rounded overflow-hidden">
                    <div
                      className="h-full bg-accent/40 rounded"
                      style={{ width: `${Math.min((h.weight || 0) / 15 * 100, 100)}%` }}
                    />
                  </div>
                  <span className="w-14 text-right font-mono">{h.weight?.toFixed(1)}%</span>
                </div>
              ))}
            </div>
            <div className="mt-2 pt-2 border-t border-border text-xs text-text-secondary">
              共 {quarterly[qPeriods[0]]?.length || 0} 檔 (季報 &ge;1%)
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
