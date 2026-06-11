import { useEffect, useState, useCallback, useMemo } from "react";
import MultiLineChart, { type SeriesSpec } from "../components/MultiLineChart";

type Rating = "inverted" | "flat" | "normal" | "steep" | null;

interface SpreadLatest {
  spread: number | null;
  rating: Rating;
  date:   string;
}

interface SpreadBlock {
  label:  string;
  latest: SpreadLatest | null;
}

interface SeriesPoint {
  date:      string;
  dgs10:     number | null;
  dgs2:      number | null;
  dgs3mo:    number | null;
  spread_2y: number | null;
  spread_3m: number | null;
}

interface YieldCurveData {
  latest_date: string | null;
  thresholds:  { flat: number; normal: number; steep: number };
  spread_2y:   SpreadBlock;
  spread_3m:   SpreadBlock;
  series:      SeriesPoint[];
}

// inverted = 紅（衰退預警）/ flat = 橘（接近倒掛）/ normal = 綠 / steep = 青（擴張期）
const RATING_LABEL: Record<string, string> = {
  inverted: "倒掛",
  flat:     "平緩",
  normal:   "正常",
  steep:    "陡峭",
};

function ratingClass(r: Rating): string {
  switch (r) {
    case "inverted": return "text-red-400 bg-red-500/15";
    case "flat":     return "text-orange-400 bg-orange-500/15";
    case "normal":   return "text-emerald-400 bg-emerald-500/15";
    case "steep":    return "text-sky-400 bg-sky-500/15";
    default:         return "text-text-secondary bg-surface-hover";
  }
}

function ratingText(r: Rating): string {
  return r ? (RATING_LABEL[r] ?? r) : "—";
}

function deriveRating(spread: number | null, thr: { flat: number; normal: number; steep: number }): Rating {
  if (spread === null) return null;
  if (spread < thr.flat)   return "inverted";
  if (spread < thr.normal) return "flat";
  if (spread < thr.steep)  return "normal";
  return "steep";
}

function spreadColor(s: number | null, thr: { flat: number; normal: number; steep: number }): string {
  const r = deriveRating(s, thr);
  switch (r) {
    case "inverted": return "text-red-400";
    case "flat":     return "text-orange-400";
    case "normal":   return "text-emerald-400";
    case "steep":    return "text-sky-400";
    default:         return "text-text-primary";
  }
}

const CHART_SERIES: SeriesSpec[] = [
  { key: "ref_zero",   label: "0 (倒掛線)", color: "#ef4444", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "ref_flat",   label: "0.5",        color: "#f59e0b", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "ref_steep",  label: "1.5",        color: "#22d3ee", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "spread_2y",  label: "10Y-2Y",     color: "#3b82f6", lineWidth: 2 },
  { key: "spread_3m",  label: "10Y-3M",     color: "#f97316", lineWidth: 2 },
];

interface CardProps {
  label:   string;
  latest:  SpreadLatest | null;
  thr:     { flat: number; normal: number; steep: number };
  focused: { spread: number | null; rating: Rating; date: string } | null;
}

function SpreadCard({ label, latest, thr, focused }: CardProps) {
  const isHovering = focused !== null;
  const display = isHovering
    ? focused
    : (latest ? { spread: latest.spread, rating: latest.rating, date: latest.date } : null);

  return (
    <div className="bg-surface-alt border border-border rounded p-3 space-y-1">
      <div className="flex items-baseline justify-between">
        <h2 className="text-sm font-bold text-text-primary">{label}</h2>
        {display?.date && (
          <span className="text-[10px] text-text-secondary">{display.date}</span>
        )}
      </div>
      <div className="flex flex-wrap items-center gap-3">
        <div className={`text-3xl font-bold ${spreadColor(display?.spread ?? null, thr)}`}>
          {display?.spread !== null && display?.spread !== undefined
            ? `${display.spread > 0 ? "+" : ""}${display.spread.toFixed(2)}`
            : "—"}
        </div>
        <span className={`px-2 py-0.5 rounded text-xs font-medium ${ratingClass(display?.rating ?? null)}`}>
          {ratingText(display?.rating ?? null)}
        </span>
      </div>
    </div>
  );
}

export default function YieldCurvePage() {
  const [data, setData] = useState<YieldCurveData | null>(null);
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);
  const handleMove = useCallback((idx: number | null) => setHoveredIdx(idx), []);

  useEffect(() => {
    fetch("/data/yield_curve.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  const seriesWithRefs = useMemo(() => {
    if (!data) return [];
    return data.series.map((p) => ({
      ...p,
      ref_zero:  0,
      ref_flat:  data.thresholds.flat,
      ref_steep: data.thresholds.steep,
    }));
  }, [data]);

  if (!data) return <div className="text-text-secondary text-sm">載入中…</div>;

  const lastIdx = data.series.length - 1;
  const focusIdx = hoveredIdx !== null && hoveredIdx >= 0 && hoveredIdx <= lastIdx ? hoveredIdx : lastIdx;
  const focusPoint = data.series[focusIdx];
  const isHovering = hoveredIdx !== null;

  const focused2y = isHovering && focusPoint
    ? { spread: focusPoint.spread_2y, rating: deriveRating(focusPoint.spread_2y, data.thresholds), date: focusPoint.date }
    : null;
  const focused3m = isHovering && focusPoint
    ? { spread: focusPoint.spread_3m, rating: deriveRating(focusPoint.spread_3m, data.thresholds), date: focusPoint.date }
    : null;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">美債殖利率曲線</h1>
        <span className="text-xs text-text-secondary">
          10Y-2Y 與 10Y-3M 利差　·　最新交易日：{data.latest_date ?? "—"}
        </span>
      </div>

      <div className="bg-surface-alt border border-border rounded p-3 text-[11px] text-text-secondary leading-5">
        殖利率倒掛（長天期利率低於短天期）= 經典衰退領先指標。rating 用絕對門檻切：
        <span className="text-red-400">&lt; 0 倒掛</span>　·
        <span className="text-orange-400">0~0.5 平緩</span>　·
        <span className="text-emerald-400">0.5~1.5 正常</span>　·
        <span className="text-sky-400">&gt; 1.5 陡峭</span>。
        資料源 FRED (St. Louis Fed) DGS10 / DGS2 / DGS3MO。
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <SpreadCard label="10Y - 2Y" latest={data.spread_2y.latest} thr={data.thresholds} focused={focused2y} />
        <SpreadCard label="10Y - 3M" latest={data.spread_3m.latest} thr={data.thresholds} focused={focused3m} />
      </div>

      <div className="bg-surface-alt border border-border rounded p-3">
        <div className="text-xs font-medium text-text-primary mb-1">
          過去 1 年利差走勢（dashed = 0 / 0.5 / 1.5 警戒帶）
        </div>
        <MultiLineChart
          data={seriesWithRefs}
          series={CHART_SERIES}
          format="raw"
          height={240}
          onCrosshairMove={handleMove}
          crosshairMarkers={false}
        />
      </div>
    </div>
  );
}
