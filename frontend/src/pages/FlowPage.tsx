import { useEffect, useMemo, useState } from "react";
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
  market?: string;
  monthly_net: number[]; // net shares per month transition
  total_net: number; // cumulative net shares
  funds: Record<string, FundChange>;
}

interface FlowData {
  periods: string[];
  fund_columns: FundColumn[];
  changes: Record<string, TickerChange>;
}

// TradingView uses 'TWSE:' for TWSE and 'TPEX:' (all caps) for TPEx.
function tvUrl(ticker: string, market: string | undefined): string {
  const prefix = market === "TWSE" ? "TWSE" : "TPEX";
  return `https://tw.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

// 202605 -> "5月"
function monthLabel(period: string): string {
  return `${parseInt(period.slice(4), 10)}月`;
}

const toLots = (shares: number) => Math.round(shares / 1000);

export default function FlowPage() {
  const [data, setData] = useState<FlowData | null>(null);
  const [sortBy, setSortBy] = useState<"activity" | "ticker">("activity");

  useEffect(() => {
    fetch("/data/flow.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  const entries = useMemo(() => {
    if (!data) return [];
    const rows = Object.entries(data.changes).map(([ticker, c]) => ({
      ticker,
      c,
      totalLots: toLots(c.total_net),
      monthlyLots: c.monthly_net.map(toLots),
    }));
    rows.sort((a, b) =>
      sortBy === "activity"
        ? b.totalLots - a.totalLots // most cumulative net buying on top
        : a.ticker.localeCompare(b.ticker),
    );
    return rows;
  }, [data, sortBy]);

  // Shared scales so bars are comparable across rows.
  const maxCum = useMemo(
    () => Math.max(1, ...entries.map((e) => Math.abs(e.totalLots))),
    [entries],
  );
  const maxMon = useMemo(
    () => Math.max(1, ...entries.flatMap((e) => e.monthlyLots.map(Math.abs))),
    [entries],
  );

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const transitions = data.periods.slice(1); // labels for each monthly_net bar

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">資金流向</h2>
      <p className="text-xs text-text-secondary mb-4">
        {monthLabel(data.periods[0])} → {monthLabel(data.periods[data.periods.length - 1])}{" "}
        各基金 Top 10 估算淨流向（張），紅=買超 綠=賣超
      </p>

      <div className="flex gap-2 mb-4">
        <button
          onClick={() => setSortBy("activity")}
          className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
            sortBy === "activity" ? "bg-accent text-white" : "bg-surface-alt text-text-secondary border border-border"
          }`}
        >
          依累計買超排序
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

      {/* Net-flow histogram */}
      <div className="space-y-1 mb-8">
        {/* header */}
        <div className="flex items-center gap-2 text-[11px] text-text-secondary pb-1 border-b border-border">
          <div className="w-14 shrink-0">代號</div>
          <div className="w-20 shrink-0">名稱</div>
          <div className="flex-1 min-w-[120px] text-center">近3月累計淨流向（張）</div>
          <div className="w-16 shrink-0 text-right">累計</div>
          <div className="w-28 shrink-0 text-center">月變化</div>
        </div>

        {entries.map(({ ticker, c, totalLots, monthlyLots }) => {
          const cumPct = (Math.abs(totalLots) / maxCum) * 50; // half-width per side
          return (
            <div key={ticker} className="flex items-center gap-2 py-1 border-b border-border/20">
              <div className="w-14 shrink-0 font-mono text-xs">
                <a
                  href={tvUrl(ticker, c.market)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-accent hover:underline"
                >
                  {ticker}
                </a>
              </div>
              <div className="w-20 shrink-0 truncate text-xs" title={c.ticker_name}>
                <Link to={`/stock/${ticker}`} className="hover:underline">{c.ticker_name}</Link>
              </div>

              {/* diverging cumulative bar */}
              <div className="relative flex-1 min-w-[120px] h-5">
                <div className="absolute left-1/2 top-0 bottom-0 w-px bg-border" />
                {totalLots >= 0 ? (
                  <div
                    className="absolute left-1/2 top-0.5 bottom-0.5 bg-negative/70 rounded-r"
                    style={{ width: `${cumPct}%` }}
                  />
                ) : (
                  <div
                    className="absolute right-1/2 top-0.5 bottom-0.5 bg-positive/70 rounded-l"
                    style={{ width: `${cumPct}%` }}
                  />
                )}
              </div>

              <div
                className={`w-16 shrink-0 text-right font-mono text-xs ${
                  totalLots > 0 ? "text-negative" : totalLots < 0 ? "text-positive" : "text-text-secondary"
                }`}
              >
                {totalLots > 0 ? "+" : ""}
                {totalLots.toLocaleString()}
              </div>

              {/* monthly mini histogram */}
              <div className="w-28 shrink-0 flex items-stretch gap-[3px] h-7">
                {monthlyLots.map((v, i) => {
                  const h = (Math.min(Math.abs(v) / maxMon, 1)) * 50;
                  return (
                    <div
                      key={i}
                      className="relative flex-1"
                      title={`${monthLabel(transitions[i])}: ${v > 0 ? "+" : ""}${v.toLocaleString()}張`}
                    >
                      <div className="absolute left-0 right-0 top-1/2 h-px bg-border/60" />
                      {v >= 0 ? (
                        <div
                          className="absolute left-0 right-0 bg-negative rounded-t-sm"
                          style={{ bottom: "50%", height: `${h}%` }}
                        />
                      ) : (
                        <div
                          className="absolute left-0 right-0 bg-positive rounded-b-sm"
                          style={{ top: "50%", height: `${h}%` }}
                        />
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>

      {/* Per-fund detail (latest transition) */}
      <h3 className="text-sm font-semibold mb-1">各基金明細</h3>
      <p className="text-xs text-text-secondary mb-3">
        {monthLabel(data.periods[data.periods.length - 2])} →{" "}
        {monthLabel(data.periods[data.periods.length - 1])} 各基金估算張數變化
      </p>
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
            {entries.map(({ ticker, c }) => (
              <tr key={ticker} className="border-b border-border/30">
                <td className="py-2 pr-4 font-mono sticky left-0 bg-surface z-10 w-16">
                  <a
                    href={tvUrl(ticker, c.market)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-accent hover:underline"
                  >
                    {ticker}
                  </a>
                </td>
                <td className="py-2 pr-4 sticky left-[4.5rem] bg-surface z-10 w-20 truncate">
                  {c.ticker_name}
                </td>
                {data.fund_columns.map((col) => {
                  const fc = c.funds[col.code];
                  if (!fc || fc.diff == null) {
                    return <td key={col.code} className="py-2 px-3 text-center text-text-secondary">·</td>;
                  }
                  const lots = toLots(fc.diff);
                  const opacity = Math.min(Math.abs(lots) / 100, 1);
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
                      title={`${fc.prev?.toFixed(1) || "—"}% → ${fc.curr?.toFixed(1) || "—"}% (${lots > 0 ? "+" : ""}${lots}張)`}
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
