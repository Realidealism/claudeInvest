import { useEffect, useState } from "react";

interface Holding {
  ticker: string;
  ticker_name: string;
  rank?: number;
  weight: number | null;
  shares?: number;
}

interface Pair {
  fund_code: string;
  fund_name: string;
  etf_code: string;
  etf_name: string;
  manager: string;
  fund_holdings: Holding[];
  etf_holdings: Holding[];
  overlap: string[];
}

interface DualTrackData {
  pairs: Pair[];
  latest_monthly: string;
}

export default function DualTrackPage() {
  const [data, setData] = useState<DualTrackData | null>(null);
  const [activePair, setActivePair] = useState<number>(0);

  useEffect(() => {
    fetch("/data/dual_track.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const pair = data.pairs[activePair];
  if (!pair) return <div className="text-negative">No dual-track pairs found.</div>;

  // ETF top 20 for display
  const etfTop = pair.etf_holdings.slice(0, 20);
  const overlapSet = new Set(pair.overlap);

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">雙軌比對</h2>
      <p className="text-xs text-text-secondary mb-4">
        基金月報 ({data.latest_monthly}) vs ETF 最新持股
      </p>

      {/* Pair selector */}
      <div className="flex flex-wrap gap-2 mb-5">
        {data.pairs.map((p, i) => (
          <button
            key={i}
            onClick={() => setActivePair(i)}
            className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
              activePair === i
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:text-text-primary border border-border"
            }`}
          >
            {p.manager}: {p.fund_name} ↔ {p.etf_code}
          </button>
        ))}
      </div>

      {/* Side-by-side comparison */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Fund side */}
        <div className="bg-surface-alt border border-border rounded-lg p-4">
          <h3 className="text-sm font-semibold mb-3">
            {pair.fund_name}
            <span className="text-text-secondary font-normal ml-2">基金 Top 10</span>
          </h3>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-text-secondary">
                <th className="py-1 pr-2 text-left font-medium">#</th>
                <th className="py-1 pr-2 text-left font-medium">代號</th>
                <th className="py-1 pr-2 text-left font-medium">名稱</th>
                <th className="py-1 text-right font-medium">權重</th>
              </tr>
            </thead>
            <tbody>
              {pair.fund_holdings.map((h) => (
                <tr
                  key={h.ticker}
                  className={`border-b border-border/30 ${
                    overlapSet.has(h.ticker) ? "bg-accent/5" : ""
                  }`}
                >
                  <td className="py-1 pr-2 text-text-secondary">{h.rank}</td>
                  <td className="py-1 pr-2 font-mono">
                    {h.ticker}
                    {overlapSet.has(h.ticker) && (
                      <span className="ml-1 text-accent">●</span>
                    )}
                  </td>
                  <td className="py-1 pr-2">{h.ticker_name}</td>
                  <td className="py-1 text-right font-mono">{h.weight?.toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* ETF side */}
        <div className="bg-surface-alt border border-border rounded-lg p-4">
          <h3 className="text-sm font-semibold mb-3">
            {pair.etf_code} {pair.etf_name}
            <span className="text-text-secondary font-normal ml-2">ETF Top 20</span>
          </h3>
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-text-secondary">
                <th className="py-1 pr-2 text-left font-medium">#</th>
                <th className="py-1 pr-2 text-left font-medium">代號</th>
                <th className="py-1 pr-2 text-left font-medium">名稱</th>
                <th className="py-1 text-right font-medium">權重</th>
              </tr>
            </thead>
            <tbody>
              {etfTop.map((h, i) => (
                <tr
                  key={h.ticker}
                  className={`border-b border-border/30 ${
                    overlapSet.has(h.ticker) ? "bg-accent/5" : ""
                  }`}
                >
                  <td className="py-1 pr-2 text-text-secondary">{i + 1}</td>
                  <td className="py-1 pr-2 font-mono">
                    {h.ticker}
                    {overlapSet.has(h.ticker) && (
                      <span className="ml-1 text-accent">●</span>
                    )}
                  </td>
                  <td className="py-1 pr-2">{h.ticker_name}</td>
                  <td className="py-1 text-right font-mono">{h.weight?.toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Overlap summary */}
      <div className="mt-4 bg-surface-alt border border-border rounded-lg px-4 py-3">
        <span className="text-sm text-text-secondary">
          <span className="text-accent">●</span> 重疊標的: {pair.overlap.length} 檔
          {pair.overlap.length > 0 && (
            <span className="ml-2 font-mono text-xs">
              {pair.overlap.join(", ")}
            </span>
          )}
        </span>
      </div>
    </div>
  );
}
