import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

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
  exit_reason?: string | null;
  disposal_status?: string | null;
}

interface PositionsData {
  snapshot_date: string | null;
  snapshot_time?: string | null;
  long: Position[];
  short: Position[];
  exited_long?: Position[];
  exited_short?: Position[];
}

type Tab = "long" | "short" | "exited_long" | "exited_short";
type View = "daily" | "intraday";

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
  const prefix = market === "TWSE" ? "TWSE" : "TPEX";
  return `https://tw.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

// "明日進X確定" / "明日進處置" means today's data already locks tomorrow's
// disposal — louder than "1 次注意即達" (conditional) or "處置中" (already in).
// 20分鐘 = second-time disposal (全件預收, real liquidity killer) — strongest tier.
function disposalClass(s: string | null | undefined): string {
  if (!s) return "text-text-secondary";
  // 🟢 "明天出處置" — bullish (exit imminent), check before 20分盤 since the
  // ongoing-disposal string still names the auction interval
  if (s.includes("🟢")) return "text-emerald-400 font-semibold";
  if (s.includes("20分盤") || s.includes("20分鐘")) return "text-red-200 font-extrabold bg-red-900/50 px-1.5 rounded";
  if (s.includes("🔴")) return "text-red-300 font-extrabold bg-red-900/40 px-1.5 rounded";
  if (s.includes("明日進")) return "text-red-400 font-bold";
  if (s.includes("🟠")) return "text-orange-400 font-semibold";
  return "text-text-secondary";
}

// Compact severity badge next to the stock name, shown only below lg where
// the full disposal column is hidden. Tiers mirror disposalClass.
function DisposalBadge({ status }: { status?: string | null }) {
  if (!status) return null;
  let label = "警示";
  let cls = "bg-surface text-text-secondary border border-border";
  if (status.includes("🟢")) {
    label = "出處置";
    cls = "bg-emerald-900/60 text-emerald-300";
  } else if (status.includes("20分盤") || status.includes("20分鐘")) {
    label = "20分盤";
    cls = "bg-red-900/70 text-red-200 font-extrabold";
  } else if (status.includes("🔴")) {
    label = "處置";
    cls = "bg-red-900/60 text-red-300 font-bold";
  } else if (status.includes("明日進")) {
    label = "明日處置";
    cls = "bg-red-900/50 text-red-400 font-bold";
  } else if (status.includes("🟠")) {
    label = "注意";
    cls = "bg-orange-900/60 text-orange-300";
  }
  return (
    <span className={`ml-1 px-1 py-0.5 text-[10px] rounded whitespace-nowrap align-middle lg:hidden ${cls}`}>
      {label}
    </span>
  );
}

function fmtDate(d: string | null): string {
  if (!d) return "—";
  // ISO 'YYYY-MM-DD' → 'M/D'
  const parts = d.split("-");
  if (parts.length !== 3) return d;
  return `${parseInt(parts[1], 10)}/${parseInt(parts[2], 10)}`;
}

const isExitTab = (t: Tab): boolean => t === "exited_long" || t === "exited_short";

export default function PositionsPage() {
  const [data, setData] = useState<PositionsData | null>(null);
  const [tab, setTab] = useState<Tab>("long");
  const [view, setView] = useState<View>("daily");

  useEffect(() => {
    setData(null);
    const url = view === "intraday" ? "/data/positions_intraday.json" : "/data/positions.json";
    fetch(url)
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, [view]);

  const exitedLong  = data?.exited_long  ?? [];
  const exitedShort = data?.exited_short ?? [];

  const tabLabel: Record<Tab, string> = {
    long: "做多",
    short: "做空",
    exited_long:  "多單出場",
    exited_short: "空單出場",
  };
  const rows: Position[] = data && data.snapshot_date
    ? tab === "long"          ? data.long
    : tab === "short"         ? data.short
    : tab === "exited_long"   ? exitedLong
                              : exitedShort
    : [];
  const intradayTimeLabel = view === "intraday" && data?.snapshot_time
    ? new Date(data.snapshot_time).toLocaleTimeString("zh-TW", {
        hour: "2-digit", minute: "2-digit", hour12: false,
      })
    : null;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-lg font-bold text-text-primary">策略持倉</h1>
        {data?.snapshot_date && (
          <span className="text-xs text-text-secondary">
            資料日：{data.snapshot_date}
            {intradayTimeLabel && <> {intradayTimeLabel}</>}
          </span>
        )}
      </div>
      <p className="text-xs text-text-secondary">
        統一策略目前未平倉部位，依當日成交金額排序；防守價為最後一次更新值（保底 / 規則觸發）。
      </p>

      <div className="flex flex-wrap gap-2">
        {(["daily", "intraday"] as View[]).map((v) => (
          <button
            key={v}
            onClick={() => setView(v)}
            className={`px-3 py-1 text-xs rounded ${
              view === v
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:text-text-primary"
            }`}
          >
            {v === "daily" ? "收盤" : "即時"}
          </button>
        ))}
      </div>

      {!data ? (
        <div className="text-text-secondary text-sm">載入中…</div>
      ) : !data.snapshot_date ? (
        <div className="text-text-secondary text-sm">尚無資料。</div>
      ) : (
      <>
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => setTab("long")}
          className={`px-4 py-1.5 text-sm rounded ${
            tab === "long"
              ? "bg-accent text-white"
              : "bg-surface-alt text-text-secondary hover:text-text-primary"
          }`}
        >
          做多（{data.long.length}）
        </button>
        <button
          onClick={() => setTab("short")}
          className={`px-4 py-1.5 text-sm rounded ${
            tab === "short"
              ? "bg-accent text-white"
              : "bg-surface-alt text-text-secondary hover:text-text-primary"
          }`}
        >
          做空（{data.short.length}）
        </button>
        <button
          onClick={() => setTab("exited_long")}
          className={`px-4 py-1.5 text-sm rounded ${
            tab === "exited_long"
              ? "bg-accent text-white"
              : "bg-surface-alt text-text-secondary hover:text-text-primary"
          }`}
        >
          多單出場（{exitedLong.length}）
        </button>
        <button
          onClick={() => setTab("exited_short")}
          className={`px-4 py-1.5 text-sm rounded ${
            tab === "exited_short"
              ? "bg-accent text-white"
              : "bg-surface-alt text-text-secondary hover:text-text-primary"
          }`}
        >
          空單出場（{exitedShort.length}）
        </button>
      </div>

      {rows.length === 0 ? (
        <div className="text-xs text-text-secondary px-2 py-3 bg-surface-alt border border-border rounded">
          {isExitTab(tab) ? `今日無${tabLabel[tab]}部位` : `目前沒有${tabLabel[tab]}持倉`}
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
                <th className="px-2 py-2 text-right">{isExitTab(tab) ? "出場" : "現價"}</th>
                <th className="px-2 py-2 text-right">損益%</th>
                {isExitTab(tab) ? (
                  <th className="px-2 py-2 hidden lg:table-cell">出場理由</th>
                ) : (
                  <>
                    <th className="px-2 py-2 text-right">防守</th>
                    <th className="px-2 py-2 hidden lg:table-cell">防守理由</th>
                  </>
                )}
                <th className="px-2 py-2 text-right">成交金額</th>
                <th className="px-2 py-2 hidden lg:table-cell">處置/警示</th>
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
                  <td className="px-2 py-1.5">
                    <Link to={`/stock/${p.ticker}`} className="hover:underline">{p.name}</Link>
                    <DisposalBadge status={p.disposal_status} />
                  </td>
                  <td className="px-2 py-1.5 text-text-secondary">
                    {TIER_LABEL[p.entry_tier] ?? p.entry_tier}
                  </td>
                  <td className={`px-2 py-1.5 text-center font-mono ${
                    p.entry_date === data.snapshot_date
                      ? "text-yellow-300 font-bold"
                      : "text-text-secondary"
                  }`}>
                    {fmtDate(p.entry_date)}
                    {p.entry_date === data.snapshot_date && (
                      <span className="ml-1 text-[10px]">今日</span>
                    )}
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
                  {isExitTab(tab) ? (
                    <td className="px-2 py-1.5 hidden lg:table-cell text-text-secondary">
                      {p.exit_reason ?? "—"}
                    </td>
                  ) : (
                    <>
                      <td className={`px-2 py-1.5 text-right font-mono ${
                        p.defense_date === data.snapshot_date
                          ? "text-yellow-300 font-bold"
                          : "text-text-secondary"
                      }`}>
                        {p.defense_price !== null ? p.defense_price.toFixed(2) : "—"}
                      </td>
                      <td className={`px-2 py-1.5 hidden lg:table-cell ${
                        p.defense_date === data.snapshot_date
                          ? "text-yellow-300 font-bold"
                          : "text-text-secondary"
                      }`}>
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
                    </>
                  )}
                  <td className={`px-2 py-1.5 text-right font-mono ${turnoverClass(p.turnover)}`}>
                    {fmtTurnover(p.turnover)}
                  </td>
                  <td
                    className={`px-2 py-1.5 hidden lg:table-cell whitespace-pre-wrap break-words ${disposalClass(p.disposal_status)}`}
                    style={{ maxWidth: 260 }}
                  >
                    {p.disposal_status || "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      </>
      )}
    </div>
  );
}
