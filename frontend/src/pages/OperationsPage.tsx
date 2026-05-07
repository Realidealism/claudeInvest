import { useEffect, useState } from "react";

interface SignalRow {
  ticker: string;
  name: string;
  market: string;
  turnover: number;
}

interface OperationsData {
  snapshot_date: string | null;
  snapshot_time?: string | null;
  signals: Record<string, SignalRow[]>;
}

type SignalKey = "pick" | "touch" | "buy" | "sell" | "buy_flee" | "sell_flee";
type View = "daily" | "intraday";

const SIGNAL_ORDER: SignalKey[] = ["pick", "touch", "buy", "sell", "buy_flee", "sell_flee"];

const SIGNAL_META: Record<SignalKey, { label: string; side: "long" | "short" }> = {
  pick:      { label: "抄底",   side: "long"  },
  touch:     { label: "摸頭",   side: "short" },
  buy:       { label: "波段多", side: "long"  },
  sell:      { label: "波段空", side: "short" },
  buy_flee:  { label: "多轉空", side: "short" },
  sell_flee: { label: "空轉多", side: "long"  },
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

function tvUrl(ticker: string, market: string): string {
  const prefix = market === "TPEx" ? "TPEX" : "TWSE";
  return `https://www.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

export default function OperationsPage() {
  const [data, setData] = useState<OperationsData | null>(null);
  const [filter, setFilter] = useState<SignalKey | "all">("all");
  const [view, setView] = useState<View>("daily");

  useEffect(() => {
    setData(null);
    const url = view === "intraday" ? "/data/operations_intraday.json" : "/data/operations.json";
    fetch(url)
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, [view]);

  const visibleSignals = filter === "all" ? SIGNAL_ORDER : [filter];
  const intradayTimeLabel = view === "intraday" && data?.snapshot_time
    ? new Date(data.snapshot_time).toLocaleTimeString("zh-TW", {
        hour: "2-digit", minute: "2-digit", hour12: false,
      })
    : null;

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline justify-between gap-3">
        <h1 className="text-lg font-bold text-text-primary">操作訊號</h1>
        {data?.snapshot_date && (
          <span className="text-xs text-text-secondary">
            資料日：{data.snapshot_date}
            {intradayTimeLabel && <> {intradayTimeLabel}</>}
          </span>
        )}
      </div>
      <p className="text-xs text-text-secondary">
        每日從訊號工廠 6 個訊號掃出當日觸發的個股，依成交金額遞減排序。
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
            {v === "daily" ? "收盤" : "12:50"}
          </button>
        ))}
      </div>

      {!data ? (
        <div className="text-text-secondary text-sm">載入中…</div>
      ) : !data.snapshot_date ? (
        <div className="text-text-secondary text-sm">
          {view === "intraday"
            ? "尚無盤中訊號資料。請先在 12:50 後跑 intraday_snapshot。"
            : "尚無訊號快照資料。請先跑 daily_update。"}
        </div>
      ) : (
      <>
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => setFilter("all")}
          className={`px-3 py-1 text-xs rounded ${
            filter === "all"
              ? "bg-accent text-white"
              : "bg-surface-alt text-text-secondary hover:text-text-primary"
          }`}
        >
          全部
        </button>
        {SIGNAL_ORDER.map((s) => {
          const meta = SIGNAL_META[s];
          const count = data.signals[s]?.length ?? 0;
          return (
            <button
              key={s}
              onClick={() => setFilter(s)}
              className={`px-3 py-1 text-xs rounded ${
                filter === s
                  ? "bg-accent text-white"
                  : "bg-surface-alt text-text-secondary hover:text-text-primary"
              }`}
            >
              {meta.label}（{count}）
            </button>
          );
        })}
      </div>

      <div className="space-y-6">
        {visibleSignals.map((sig) => {
          const meta = SIGNAL_META[sig];
          const rows = data.signals[sig] ?? [];
          const sideLabel = meta.side === "long" ? "做多" : "做空";
          const sideColor =
            meta.side === "long" ? "text-long-strong" : "text-short-strong";

          return (
            <section key={sig}>
              <div className="flex items-baseline gap-3 mb-2">
                <h2 className={`text-sm font-bold ${sideColor}`}>
                  {meta.label}
                </h2>
                <span className="text-xs text-text-secondary">
                  {sideLabel}　·　{rows.length} 檔
                </span>
              </div>

              {rows.length === 0 ? (
                <div className="text-xs text-text-secondary px-2 py-3 bg-surface-alt border border-border rounded">
                  今日無觸發
                </div>
              ) : (
                <div className="overflow-x-auto bg-surface-alt border border-border rounded">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="border-b border-border text-text-secondary text-left">
                        <th className="px-2 py-2 w-10">#</th>
                        <th className="px-2 py-2">代號</th>
                        <th className="px-2 py-2">名稱</th>
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
                          <td
                            className={`px-2 py-1.5 text-right font-mono ${turnoverClass(p.turnover)}`}
                          >
                            {fmtTurnover(p.turnover)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </section>
          );
        })}
      </div>
      </>
      )}
    </div>
  );
}
