import { useEffect, useState } from "react";

interface Position {
  ticker: string;
  name: string;
  market: string;
  entry_date: string;
  entry_price: number;
  entry_tier: string;
  current_close: number;
  pnl_pct: number;
  bars_held: number;
  turnover: number;
  defense_price: number | null;
  defense_reason: string | null;
  defense_date: string | null;
}

interface PositionsData {
  snapshot_date: string | null;
  long: Position[];
  short: Position[];
}

type Side = "long" | "short";

const TIER_LABEL: Record<string, string> = {
  pick: "抄底",
  buy: "波段多",
  sell_flee: "空轉多",
  touch: "摸頭",
  sell: "波段空",
  buy_flee: "多轉空",
};

function fmtTurnover(t: number): string {
  if (t >= 1e8) return `${(t / 1e8).toFixed(2)}億`;
  if (t >= 1e4) return `${(t / 1e4).toFixed(0)}萬`;
  return t.toFixed(0);
}

function turnoverClass(t: number): string {
  if (t < 1e7) return "text-text-secondary";
  if (t < 1e8) return "text-yellow-400";
  if (t < 5e8) return "text-orange-400";
  return "text-orange-300 font-bold";
}

function pnlClass(p: number): string {
  if (p > 0) return "text-long-strong";
  if (p < 0) return "text-short-strong";
  return "text-text-secondary";
}

function tvUrl(ticker: string, market: string): string {
  const prefix = market === "TPEx" ? "TPEX" : "TWSE";
  return `https://www.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

function fmtDate(d: string | null): string {
  if (!d) return "—";
  // ISO 'YYYY-MM-DD' → 'M/D'
  const parts = d.split("-");
  if (parts.length !== 3) return d;
  return `${parseInt(parts[1], 10)}/${parseInt(parts[2], 10)}`;
}

export default function PositionsPage() {
  const [data, setData] = useState<PositionsData | null>(null);
  const [side, setSide] = useState<Side>("long");

  useEffect(() => {
    fetch("/data/positions.json")
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
        尚無持倉快照資料。請先跑 daily_update。
      </div>
    );
  }

  const rows = side === "long" ? data.long : data.short;
  const sideLabel = side === "long" ? "做多" : "做空";

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-lg font-bold text-text-primary">策略持倉</h1>
        <span className="text-xs text-text-secondary">
          資料日：{data.snapshot_date}
        </span>
      </div>
      <p className="text-xs text-text-secondary">
        統一策略目前未平倉部位，依當日成交金額排序；防守價為最後一次更新值（保底 / 規則觸發）。
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
            {s === "long"
              ? `做多（${data.long.length}）`
              : `做空（${data.short.length}）`}
          </button>
        ))}
      </div>

      {rows.length === 0 ? (
        <div className="text-xs text-text-secondary px-2 py-3 bg-surface-alt border border-border rounded">
          目前沒有{sideLabel}持倉
        </div>
      ) : (
        <div className="overflow-x-auto bg-surface-alt border border-border rounded">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border text-text-secondary text-left">
                <th className="px-2 py-2 w-10">#</th>
                <th className="px-2 py-2">代號</th>
                <th className="px-2 py-2">名稱</th>
                <th className="px-2 py-2">類型</th>
                <th className="px-2 py-2 text-center">進場日</th>
                <th className="px-2 py-2 text-right hidden md:table-cell">天數</th>
                <th className="px-2 py-2 text-right">進場</th>
                <th className="px-2 py-2 text-right">現價</th>
                <th className="px-2 py-2 text-right">損益%</th>
                <th className="px-2 py-2 text-right">防守</th>
                <th className="px-2 py-2 hidden lg:table-cell">防守理由</th>
                <th className="px-2 py-2 text-right">成交金額</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((p, i) => (
                <tr
                  key={p.ticker}
                  className="border-b border-border/50 hover:bg-surface-hover transition-colors"
                >
                  <td className="px-2 py-1.5">{i + 1}</td>
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
                  <td className="px-2 py-1.5 text-text-secondary">
                    {TIER_LABEL[p.entry_tier] ?? p.entry_tier}
                  </td>
                  <td className="px-2 py-1.5 text-center font-mono text-text-secondary">
                    {fmtDate(p.entry_date)}
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono text-text-secondary hidden md:table-cell">
                    {p.bars_held}
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono text-text-secondary">
                    {p.entry_price.toFixed(2)}
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono">
                    {p.current_close.toFixed(2)}
                  </td>
                  <td className={`px-2 py-1.5 text-right font-mono font-bold ${pnlClass(p.pnl_pct)}`}>
                    {p.pnl_pct >= 0 ? "+" : ""}
                    {p.pnl_pct.toFixed(2)}
                  </td>
                  <td className="px-2 py-1.5 text-right font-mono text-text-secondary">
                    {p.defense_price !== null ? p.defense_price.toFixed(2) : "—"}
                  </td>
                  <td className="px-2 py-1.5 hidden lg:table-cell text-text-secondary">
                    {p.defense_reason ? (
                      <span>
                        {p.defense_reason}
                        {p.defense_date && (
                          <span className="ml-1 text-[10px]">
                            ({fmtDate(p.defense_date)})
                          </span>
                        )}
                      </span>
                    ) : "—"}
                  </td>
                  <td className={`px-2 py-1.5 text-right font-mono ${turnoverClass(p.turnover)}`}>
                    {fmtTurnover(p.turnover)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
