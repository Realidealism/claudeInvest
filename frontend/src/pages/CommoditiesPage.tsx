import { useEffect, useMemo, useState } from "react";
import MultiLineChart, { type SeriesSpec } from "../components/MultiLineChart";
import DataTimestamp from "../components/DataTimestamp";

interface SeriesPoint {
  date:  string;
  close: number | null;
}

interface Quote {
  symbol:      string;
  name:        string;
  category:    string;
  unit:        string;
  dp:          number;
  freq:        string;
  latest:      number | null;
  latest_date: string | null;
  chg_1d:      number | null;
  chg_20d:     number | null;
  chg_60d:     number | null;
  w52_high:    number | null;
  w52_low:     number | null;
  w52_pct:     number | null;
  series:      SeriesPoint[];
}

interface Category {
  key:   string;
  label: string;
}

interface CommoditiesData {
  latest_date: string | null;
  categories:  Category[];
  quotes:      Quote[];
}

// 台股慣例：紅漲綠跌（與美股相反）
function chgClass(v: number | null | undefined): string {
  if (v === null || v === undefined) return "text-text-secondary";
  if (v > 0) return "text-red-400";
  if (v < 0) return "text-emerald-400";
  return "text-text-secondary";
}

// "▲ 1.23%" / "▼ 0.45%" / "—"
function chgText(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  const arrow = v > 0 ? "▲" : v < 0 ? "▼" : "－";
  return `${arrow} ${Math.abs(v).toFixed(2)}%`;
}

function num(v: number | null | undefined, dp: number): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  return v.toFixed(dp);
}

function QuoteCard({
  q, pageLatest, expanded, onToggle,
}: { q: Quote; pageLatest: string | null; expanded: boolean; onToggle: () => void }) {
  const chartSeries: SeriesSpec[] = useMemo(() => [
    { key: "close", label: q.name, color: "#3b82f6", lineWidth: 2 },
  ], [q.name]);

  // 這些序列的更新頻率天差地遠：期貨日更、FBX 週更（週五）、記憶體現貨
  // 成交稀疏時可能數週不動。落後全頁最新日期的，就把它自己的報價日標出來，
  // 否則使用者會把一個三週前的數字讀成今天的價。
  const staleDate = pageLatest && q.latest_date !== pageLatest ? q.latest_date : null;

  // FBX 只在週五公布，所以它相鄰兩點相差一週而非一天。chg_* 是「相隔 N 個
  // 資料點」的變化，單位得跟著序列頻率走，否則週漲跌會被讀成日漲跌。
  const per = q.freq === "weekly" ? "週" : "日";

  // 52 週游標：clamp 0~100，並用 calc 讓圓點在兩端仍完整落在 bar 內
  const pct = q.w52_pct === null || q.w52_pct === undefined || !Number.isFinite(q.w52_pct)
    ? null
    : Math.min(100, Math.max(0, q.w52_pct));
  const DOT = 10; // px

  return (
    <div
      onClick={onToggle}
      className={`bg-surface-alt border rounded p-3 space-y-2 cursor-pointer transition-colors ${
        expanded ? "border-accent" : "border-border hover:border-accent/50"
      } ${expanded ? "sm:col-span-2 lg:col-span-3 xl:col-span-4" : ""}`}
    >
      <div className="flex items-baseline justify-between gap-2">
        <h3 className="text-sm font-bold text-text-primary">{q.name}</h3>
        <span className="text-[10px] text-text-secondary">{q.unit}</span>
      </div>

      <div className="flex flex-wrap items-baseline gap-3">
        <div className="text-2xl font-bold text-text-primary">{num(q.latest, q.dp)}</div>
        <div className={`text-sm font-medium ${chgClass(q.chg_1d)}`}>{chgText(q.chg_1d)}</div>
        {q.freq === "weekly" && (
          <span className="text-[10px] text-text-secondary/70">週變化</span>
        )}
        {staleDate && (
          <span className="text-[10px] text-text-secondary/70">{staleDate} 報價</span>
        )}
      </div>

      <div className="flex gap-4 text-[10px]">
        <span className="text-text-secondary">
          20{per} <span className={chgClass(q.chg_20d)}>{chgText(q.chg_20d)}</span>
        </span>
        <span className="text-text-secondary">
          60{per} <span className={chgClass(q.chg_60d)}>{chgText(q.chg_60d)}</span>
        </span>
      </div>

      <div className="pt-1">
        <div className="relative h-1.5 rounded bg-surface-hover">
          {pct !== null && (
            <div
              className="absolute top-1/2 -translate-y-1/2 rounded-full bg-accent"
              style={{
                width:  `${DOT}px`,
                height: `${DOT}px`,
                left:   `calc(${pct}% - ${(pct / 100) * DOT}px)`,
              }}
            />
          )}
        </div>
        <div className="flex justify-between text-[10px] text-text-secondary mt-1">
          <span>{num(q.w52_low, q.dp)}</span>
          <span className="text-text-secondary/70">
            52週區間{pct !== null ? `　${pct.toFixed(0)}%` : ""}
          </span>
          <span>{num(q.w52_high, q.dp)}</span>
        </div>
      </div>

      {expanded && (
        <div className="pt-2" onClick={(e) => e.stopPropagation()}>
          <div className="text-[11px] text-text-secondary mb-1">
            近 {q.series.length} {per}走勢（{q.unit}）
          </div>
          <MultiLineChart
            data={q.series as unknown as Array<Record<string, number | string | null>>}
            series={chartSeries}
            format="raw"
            height={220}
          />
        </div>
      )}
    </div>
  );
}

export default function CommoditiesPage() {
  const [data, setData] = useState<CommoditiesData | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    fetch("/data/commodities.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary text-sm">載入中…</div>;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">大宗行情</h1>
        <span className="text-xs text-text-secondary">
          國際商品 · 匯率 · 運價
        </span>
      </div>
      <DataTimestamp value={data.latest_date} note="每交易日更新" />

      {/* 分類卡片牆 */}
      {data.categories.map((cat) => {
        const items = data.quotes.filter((q) => q.category === cat.key);
        if (items.length === 0) return null;
        return (
          <div key={cat.key} className="space-y-2">
            <h2 className="text-xs font-bold tracking-wide text-text-secondary pt-1">
              {cat.label}
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
              {items.map((q) => (
                <QuoteCard
                  key={q.symbol}
                  q={q}
                  pageLatest={data.latest_date}
                  expanded={selected === q.symbol}
                  onToggle={() => setSelected(selected === q.symbol ? null : q.symbol)}
                />
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
