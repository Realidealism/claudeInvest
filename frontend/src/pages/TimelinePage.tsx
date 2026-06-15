import { useEffect, useState } from "react";

interface Holding {
  ticker: string;
  ticker_name: string;
  market?: string;
  rank: number;
  weight: number | null;
}

// TradingView uses 'TWSE:' for TWSE and 'TPEX:' (all caps) for TPEx.
function tvUrl(ticker: string, market: string | undefined): string {
  const prefix = market === "TWSE" ? "TWSE" : "TPEX";
  return `https://tw.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

interface Trajectory {
  name: string;
  periods: Record<string, Holding[]>;
}

interface TimelineData {
  monthly_periods: string[];
  quarterly_periods: string[];
  trajectories: Record<string, Trajectory>;
}

export default function TimelinePage() {
  const [data, setData] = useState<TimelineData | null>(null);
  const [activeFund, setActiveFund] = useState<string>("");

  useEffect(() => {
    fetch("/data/timeline.json")
      .then((r) => r.json())
      .then((d: TimelineData) => {
        setData(d);
        const codes = Object.keys(d.trajectories);
        if (codes.length > 0) setActiveFund(codes[0]);
      });
  }, []);

  if (!data || !activeFund) return <div className="text-text-secondary">Loading...</div>;

  const traj = data.trajectories[activeFund];
  const periods = data.monthly_periods.slice().reverse();

  // Collect all tickers across all periods for this fund
  const allTickers = new Map<string, { name: string; market?: string }>();
  for (const holdings of Object.values(traj.periods)) {
    for (const h of holdings) {
      if (!allTickers.has(h.ticker)) {
        allTickers.set(h.ticker, { name: h.ticker_name, market: h.market });
      }
    }
  }

  // Build ticker × period weight matrix
  const tickerList = [...allTickers.entries()].sort((a, b) => a[0].localeCompare(b[0]));

  // For each period, build a ticker→holding map
  const periodMaps = periods.map((p) => {
    const map = new Map<string, Holding>();
    for (const h of traj.periods[p] || []) {
      map.set(h.ticker, h);
    }
    return map;
  });

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">月季交叉時間軸</h2>
      <p className="text-xs text-text-secondary mb-4">追蹤各基金持股在不同期間的進出變化</p>

      {/* Fund selector */}
      <div className="flex flex-wrap gap-1.5 mb-5">
        {Object.entries(data.trajectories).map(([code, t]) => (
          <button
            key={code}
            onClick={() => setActiveFund(code)}
            className={`px-3 py-1.5 rounded text-xs font-medium transition-colors ${
              activeFund === code
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:text-text-primary border border-border"
            }`}
          >
            {t.name}
          </button>
        ))}
      </div>

      {/* Timeline table */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-text-secondary">
              <th className="py-2 pr-3 text-left font-medium sticky left-0 bg-surface">代號</th>
              <th className="py-2 pr-3 text-left font-medium sticky left-16 bg-surface">名稱</th>
              {periods.map((p) => (
                <th key={p} className="py-2 px-2 text-center font-medium font-mono">{p}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {tickerList.map(([ticker, info]) => (
              <tr key={ticker} className="border-b border-border/30 hover:bg-surface-hover transition-colors">
                <td className="py-1.5 pr-3 font-mono sticky left-0 bg-surface">
                  <a
                    href={tvUrl(ticker, info.market)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-accent hover:underline"
                  >
                    {ticker}
                  </a>
                </td>
                <td className="py-1.5 pr-3 sticky left-16 bg-surface truncate max-w-24">{info.name}</td>
                {periodMaps.map((map, i) => {
                  const h = map.get(ticker);
                  if (!h) return <td key={periods[i]} className="py-1.5 px-2 text-center text-text-secondary">—</td>;
                  return (
                    <td key={periods[i]} className="py-1.5 px-2 text-center">
                      <span className="font-mono">{h.weight?.toFixed(1)}%</span>
                      <span className="text-text-secondary ml-1">#{h.rank}</span>
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
