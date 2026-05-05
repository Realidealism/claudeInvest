import { useEffect, useState } from "react";

interface ScoreRow {
  rank: number;
  ticker: string;
  name: string;
  market: string;
  total_pct: number;
  turnover: number;
  is_new: boolean;
  prev_rank: number | null;
  rank_delta: number | null;
  pct_d1: number | null;
  pct_d2: number | null;
}

interface HistoryItem {
  date: string;
  new_long: number;
  new_short: number;
}

interface ScoresData {
  snapshot_date: string | null;
  long: ScoreRow[];
  short: ScoreRow[];
  history: HistoryItem[];
}

type Side = "long" | "short";

function fmtTurnover(t: number): string {
  if (t >= 1e8) return `${(t / 1e8).toFixed(2)}億`;
  if (t >= 1e4) return `${(t / 1e4).toFixed(0)}萬`;
  return t.toFixed(0);
}

// TradingView uses 'TWSE:' for TWSE and 'TPEX:' (all caps) for TPEx.
function tvUrl(ticker: string, market: string): string {
  const prefix = market === "TPEx" ? "TPEX" : "TWSE";
  return `https://www.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

export default function ScoresPage() {
  const [data, setData] = useState<ScoresData | null>(null);
  const [side, setSide] = useState<Side>("long");

  useEffect(() => {
    fetch("/data/scores.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) {
    return <div className="text-text-secondary text-sm">載入中…</div>;
  }
  if (!data.snapshot_date) {
    return (
      <div className="text-text-secondary text-sm">
        尚無評分快照資料。請先跑 daily_update。
      </div>
    );
  }

  const rows = side === "long" ? data.long : data.short;
  const sideColor = side === "long" ? "text-long-strong" : "text-short-strong";
  const sideLabel = side === "long" ? "做多" : "做空";

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">多空評比</h1>
        <span className="text-xs text-text-secondary">
          快照日：{data.snapshot_date}　·　Top {data.long.length}（多）/ {data.short.length}（空）
        </span>
      </div>
      <p className="text-xs text-text-secondary">
        以 ScoreBoard 三時框合併之 total.{sideLabel} 百分比排序，成交金額為 tie-breaker。
      </p>

      <div className="flex gap-2">
        {(["long", "short"] as Side[]).map((s) => (
          <button
            key={s}
            onClick={() => setSide(s)}
            className={`px-4 py-1.5 text-sm rounded ${
              side === s
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:text-text-primary"
            }`}
          >
            {s === "long" ? "做多 100" : "做空 100"}
          </button>
        ))}
      </div>

      <div className="overflow-x-auto bg-surface-alt border border-border rounded">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="px-2 py-2 w-10">#</th>
              <th className="px-2 py-2">代號</th>
              <th className="px-2 py-2">名稱</th>
              <th className="px-2 py-2 hidden md:table-cell">市場</th>
              <th className="px-2 py-2 text-right">{sideLabel}%</th>
              <th className="px-2 py-2 text-right hidden sm:table-cell">前1日</th>
              <th className="px-2 py-2 text-right hidden sm:table-cell">前2日</th>
              <th className="px-2 py-2 text-right">成交金額</th>
              <th className="px-2 py-2 text-center">變動</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p) => (
              <tr
                key={p.ticker}
                className="border-b border-border/50 hover:bg-surface-hover transition-colors"
              >
                <td className="px-2 py-1.5">{p.rank}</td>
                <td className="px-2 py-1.5 font-mono">
                  <a
                    href={tvUrl(p.ticker, p.market)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-accent hover:underline"
                  >
                    {p.ticker}
                  </a>
                </td>
                <td className="px-2 py-1.5">{p.name}</td>
                <td className="px-2 py-1.5 hidden md:table-cell text-text-secondary">
                  {p.market}
                </td>
                <td className={`px-2 py-1.5 text-right font-mono font-bold ${sideColor}`}>
                  {p.total_pct >= 0 ? "+" : ""}
                  {p.total_pct.toFixed(1)}
                </td>
                <td className="px-2 py-1.5 text-right font-mono text-text-secondary hidden sm:table-cell">
                  {p.pct_d1 !== null
                    ? `${p.pct_d1 >= 0 ? "+" : ""}${p.pct_d1.toFixed(1)}`
                    : "—"}
                </td>
                <td className="px-2 py-1.5 text-right font-mono text-text-secondary hidden sm:table-cell">
                  {p.pct_d2 !== null
                    ? `${p.pct_d2 >= 0 ? "+" : ""}${p.pct_d2.toFixed(1)}`
                    : "—"}
                </td>
                <td className="px-2 py-1.5 text-right font-mono text-text-secondary">
                  {fmtTurnover(p.turnover)}
                </td>
                <td className="px-2 py-1.5 text-center">
                  {p.is_new ? (
                    <span className="px-1.5 py-0.5 rounded text-[10px] bg-blue-500/30 text-blue-300">
                      NEW
                    </span>
                  ) : p.rank_delta !== null && p.rank_delta !== 0 ? (
                    <span className={p.rank_delta > 0 ? "text-green-300" : "text-red-300"}>
                      {p.rank_delta > 0 ? "↑" : "↓"}
                      {Math.abs(p.rank_delta)}
                    </span>
                  ) : (
                    <span className="text-text-secondary">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {data.history.length > 1 && (
        <div className="text-xs text-text-secondary">
          近期快照：
          {data.history.slice(0, 10).map((h) => (
            <span key={h.date} className="ml-2">
              {h.date}（多 NEW {h.new_long} / 空 NEW {h.new_short}）
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
