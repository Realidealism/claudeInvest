import { useEffect, useMemo, useState } from "react";

type Side = "long" | "short";
type Format = "int" | "pct" | "ratio3" | null;

interface Column {
  key: string;
  label: string;
  format: Format;
}

interface Strategy {
  key: string;
  label: string;
  side: Side;
  description: string;
  columns: Column[];
}

interface RevenueData {
  generated_at: string | null;
  export_date: string | null;
  available_months: string[];
  strategies: Strategy[];
  data: Record<string, Record<string, Record<string, unknown>[]>>;
}

type SortDir = "asc" | "desc";

function formatCell(value: unknown, fmt: Format): string {
  if (value === null || value === undefined) return "—";
  if (fmt === "int") {
    return Number(value).toLocaleString("en-US");
  }
  if (fmt === "pct") {
    const n = Number(value);
    const sign = n > 0 ? "+" : "";
    return `${sign}${n.toFixed(2)}%`;
  }
  if (fmt === "ratio3") {
    return Number(value).toFixed(3);
  }
  return String(value);
}

function pctClass(value: unknown): string {
  if (value === null || value === undefined) return "text-text-secondary";
  const n = Number(value);
  if (n > 0) return "text-green-300";
  if (n < 0) return "text-red-300";
  return "";
}

function alignClass(fmt: Format): string {
  if (fmt === "int" || fmt === "pct" || fmt === "ratio3") return "text-right";
  return "";
}

// TradingView uses 'TWSE:' for TWSE and 'TPEX:' (all caps) for TPEx.
function tvUrl(ticker: string, market: string): string {
  const prefix = market === "TPEx" ? "TPEX" : "TWSE";
  return `https://www.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

export default function RevenueScreensPage() {
  const [data, setData] = useState<RevenueData | null>(null);
  const [month, setMonth] = useState<string>("");
  const [strategyKey, setStrategyKey] = useState<string>("three_arrows");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  useEffect(() => {
    fetch("/data/revenue_screens.json")
      .then((r) => r.json())
      .then((d: RevenueData) => {
        setData(d);
        if (d.available_months.length > 0) {
          setMonth(d.available_months[0]);
        }
      })
      .catch(console.error);
  }, []);

  const strategy: Strategy | undefined = useMemo(() => {
    return data?.strategies.find((s) => s.key === strategyKey);
  }, [data, strategyKey]);

  const rows = useMemo(() => {
    if (!data || !month || !strategy) return [];
    const all = data.data[month]?.[strategy.key] ?? [];
    const q = search.trim().toLowerCase();
    let filtered = all;
    if (q) {
      filtered = all.filter((r) => {
        const stockId = String(r.stock_id ?? "").toLowerCase();
        const name = String(r.name ?? "").toLowerCase();
        const industry = String(r.industry ?? "").toLowerCase();
        return stockId.includes(q) || name.includes(q) || industry.includes(q);
      });
    }
    if (sortKey) {
      const dir = sortDir === "asc" ? 1 : -1;
      filtered = [...filtered].sort((a, b) => {
        const av = a[sortKey];
        const bv = b[sortKey];
        if (av === null || av === undefined) return 1;
        if (bv === null || bv === undefined) return -1;
        if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
        return String(av).localeCompare(String(bv)) * dir;
      });
    }
    return filtered;
  }, [data, month, strategy, search, sortKey, sortDir]);

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  if (!data) {
    return <div className="text-text-secondary text-sm">載入中…</div>;
  }
  if (data.available_months.length === 0) {
    return (
      <div className="text-text-secondary text-sm">
        尚無月營收選股資料。請先跑 daily_update（月營收公布期間 1–15 日才會更新）。
      </div>
    );
  }

  const longStrategies = data.strategies.filter((s) => s.side === "long");
  const shortStrategies = data.strategies.filter((s) => s.side === "short");

  const monthCounts: Record<string, number> = {};
  for (const ym of data.available_months) {
    monthCounts[ym] = data.data[ym]?.[strategyKey]?.length ?? 0;
  }
  const newInView = rows.filter((r) => Boolean(r.is_new)).length;

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">月營收選股</h1>
        <span className="text-xs text-text-secondary">
          資料月份：{data.available_months.join(" / ")}
        </span>
      </div>

      {/* Strategy tabs */}
      <div className="space-y-2">
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs text-text-secondary mr-1">偏多</span>
          {longStrategies.map((s) => (
            <button
              key={s.key}
              onClick={() => {
                setStrategyKey(s.key);
                setSortKey(null);
              }}
              className={`px-2.5 py-1 text-xs rounded ${
                strategyKey === s.key
                  ? "bg-green-500/30 text-green-200"
                  : "bg-surface-alt text-text-secondary hover:text-text-primary"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs text-text-secondary mr-1">偏空</span>
          {shortStrategies.map((s) => (
            <button
              key={s.key}
              onClick={() => {
                setStrategyKey(s.key);
                setSortKey(null);
              }}
              className={`px-2.5 py-1 text-xs rounded ${
                strategyKey === s.key
                  ? "bg-red-500/30 text-red-200"
                  : "bg-surface-alt text-text-secondary hover:text-text-primary"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </div>

      {/* Description + month + search */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="text-xs text-text-secondary flex-1 min-w-[280px]">
          {strategy?.description}
          {newInView > 0 && (
            <span className="ml-2 text-blue-300">本月新增 {newInView}</span>
          )}
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-xs text-text-secondary">月份</span>
          {data.available_months.map((ym) => (
            <button
              key={ym}
              onClick={() => setMonth(ym)}
              className={`px-2 py-1 text-xs rounded ${
                month === ym
                  ? "bg-accent text-white"
                  : "bg-surface-alt text-text-secondary hover:text-text-primary"
              }`}
            >
              {ym}
              <span className="ml-1 text-text-secondary/70">
                ({monthCounts[ym]})
              </span>
            </button>
          ))}
        </div>
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="搜尋代號 / 名稱 / 產業"
          className="px-2 py-1 text-xs bg-surface-alt border border-border rounded text-text-primary placeholder:text-text-secondary/60 w-48"
        />
      </div>

      {/* Table */}
      <div className="overflow-x-auto bg-surface-alt border border-border rounded">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="px-2 py-2 w-10">#</th>
              {strategy?.columns.map((col) => (
                <th
                  key={col.key}
                  onClick={() => handleSort(col.key)}
                  className={`px-2 py-2 cursor-pointer select-none hover:text-text-primary ${alignClass(col.format)}`}
                >
                  {col.label}
                  {sortKey === col.key && (
                    <span className="ml-1 text-accent">
                      {sortDir === "asc" ? "▲" : "▼"}
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, idx) => (
              <tr
                key={String(r.stock_id ?? idx)}
                className="border-b border-border/50 hover:bg-surface-hover transition-colors"
              >
                <td className="px-2 py-1.5 text-text-secondary">{idx + 1}</td>
                {strategy?.columns.map((col) => {
                  const v = r[col.key];
                  if (col.key === "stock_id") {
                    const ticker = String(v ?? "");
                    const market = String(r.market ?? "TWSE");
                    return (
                      <td key={col.key} className="px-2 py-1.5 font-mono whitespace-nowrap">
                        <a
                          href={tvUrl(ticker, market)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-accent hover:underline"
                        >
                          {ticker}
                        </a>
                        {Boolean(r.is_new) && (
                          <span
                            className="ml-1.5 px-1 py-0.5 rounded text-[9px] bg-blue-500/30 text-blue-300 align-middle"
                            title={data.export_date ? `首次出現於 ${data.export_date}` : "今日新增"}
                          >
                            NEW
                          </span>
                        )}
                      </td>
                    );
                  }
                  const colorCls = col.format === "pct" ? pctClass(v) : "";
                  const monoCls =
                    col.format === "int" || col.format === "pct" || col.format === "ratio3"
                      ? "font-mono"
                      : "";
                  return (
                    <td
                      key={col.key}
                      className={`px-2 py-1.5 ${alignClass(col.format)} ${monoCls} ${colorCls}`}
                    >
                      {formatCell(v, col.format)}
                    </td>
                  );
                })}
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td
                  colSpan={(strategy?.columns.length ?? 0) + 1}
                  className="px-2 py-6 text-center text-text-secondary text-xs"
                >
                  {search ? "查無符合結果" : "本月此策略無命中股票"}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {data.generated_at && (
        <div className="text-xs text-text-secondary">
          產生時間：{data.generated_at}
        </div>
      )}
    </div>
  );
}
