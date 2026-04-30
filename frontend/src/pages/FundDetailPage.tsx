import { useEffect, useState } from "react";
import { useParams, Link, useNavigate } from "react-router-dom";

interface Holding {
  ticker: string;
  ticker_name: string;
  rank?: number;
  weight: number | null;
  shares?: number | null;
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
  const navigate = useNavigate();
  const [data, setData] = useState<FundsData | null>(null);
  const [selectedPeriod, setSelectedPeriod] = useState<string>("");
  const [selectedQPeriod, setSelectedQPeriod] = useState<string>("");

  useEffect(() => {
    fetch("/data/funds.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  // Reset period when switching funds
  useEffect(() => {
    if (!data || !code) return;
    const h = data.holdings[code];
    const periods = Object.keys(h?.monthly || {}).sort().reverse();
    setSelectedPeriod(periods[0] || "");
    const qp = Object.keys(data.holdings[code]?.quarterly || {}).sort().reverse();
    setSelectedQPeriod(qp[0] || "");
  }, [data, code]);

  if (!data || !code) return <div className="text-text-secondary">Loading...</div>;

  const fund = data.funds.find((f) => f.code === code);
  const fundHoldings = data.holdings[code];

  if (!fund) return <div className="text-negative">Fund not found: {code}</div>;

  // Navigation: same fund_type list for prev/next
  const sameType = data.funds.filter((f) => f.fund_type === fund.fund_type);
  const currentIdx = sameType.findIndex((f) => f.code === code);
  const prevFund = currentIdx > 0 ? sameType[currentIdx - 1] : null;
  const nextFund = currentIdx < sameType.length - 1 ? sameType[currentIdx + 1] : null;

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
      <div className="flex items-center mb-4">
        <Link to="/funds" className="text-text-secondary hover:text-text-primary text-sm shrink-0">&larr; 返回</Link>
        <div className="flex-1 flex items-center justify-center gap-6">
          <button
            onClick={() => prevFund && navigate(`/fund/${prevFund.code}`)}
            disabled={!prevFund}
            className={`w-8 h-8 rounded flex items-center justify-center shrink-0 ${prevFund ? "bg-surface-alt border border-border text-text-primary hover:bg-surface-hover" : "text-text-secondary/30 cursor-not-allowed"}`}
            title={prevFund?.name}
          >
            ◀
          </button>
          <div className="text-center min-w-48">
            <h2 className="text-lg font-semibold">{fund.name}</h2>
            <span className="text-xs text-text-secondary">
              {fund.company} / {fund.manager_name || "—"}
              <span className="ml-2 text-text-secondary/50">({currentIdx + 1}/{sameType.length})</span>
            </span>
          </div>
          <button
            onClick={() => nextFund && navigate(`/fund/${nextFund.code}`)}
            disabled={!nextFund}
            className={`w-8 h-8 rounded flex items-center justify-center shrink-0 ${nextFund ? "bg-surface-alt border border-border text-text-primary hover:bg-surface-hover" : "text-text-secondary/30 cursor-not-allowed"}`}
            title={nextFund?.name}
          >
            ▶
          </button>
        </div>
      </div>

      {/* Period selectors */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-2">
        <div className="flex items-center gap-2">
          <label className="text-xs text-text-secondary">{isEtf ? "持股日期:" : "月報期間:"}</label>
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
        {qPeriods.length > 0 && (
          <div className="flex items-center gap-2">
            <label className="text-xs text-text-secondary">季報期間:</label>
            <select
              value={selectedQPeriod}
              onChange={(e) => setSelectedQPeriod(e.target.value)}
              className="bg-surface-alt border border-border rounded px-3 py-1.5 text-sm text-text-primary"
            >
              {qPeriods.map((p) => (
                <option key={p} value={p}>{p}</option>
              ))}
            </select>
          </div>
        )}
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
              const sharesDiff = (isEtf && prev?.shares != null && h.shares != null)
                ? h.shares - prev.shares
                : null;
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
                    <span className={`w-14 text-right font-mono ${diff > 0 ? "text-negative" : diff < 0 ? "text-positive" : "text-text-secondary"}`}>
                      {diff > 0 ? "+" : ""}{diff.toFixed(1)}
                    </span>
                  )}
                  {sharesDiff != null && (
                    <span className={`w-20 text-right font-mono ${sharesDiff > 0 ? "text-negative" : sharesDiff < 0 ? "text-positive" : "text-text-secondary"}`}>
                      {sharesDiff > 0 ? "+" : ""}{(sharesDiff / 1000).toFixed(0)}張
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
              季報完整持股 ({selectedQPeriod})
            </h3>
            <div className="max-h-[600px] overflow-y-auto space-y-0.5">
              {(quarterly[selectedQPeriod] || []).map((h) => (
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
              共 {quarterly[selectedQPeriod]?.length || 0} 檔 (季報 &ge;1%)
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
