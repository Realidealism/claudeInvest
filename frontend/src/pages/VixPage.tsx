import { useEffect, useState, useCallback, useMemo } from "react";
import MultiLineChart, { type SeriesSpec } from "../components/MultiLineChart";

type Rating = "calm" | "low" | "elevated" | "panic" | null;

interface VixLatest {
  close:         number | null;
  rating:        Rating;
  // HHMMSS, present only on TW side when the row was written by the
  // MIS poller mid-session. Daily-settled rows have it null/absent.
  intraday_time?: string | null;
}

interface VixThresholds {
  p20: number | null;
  p50: number | null;
  p80: number | null;
}

interface VixSeriesPoint {
  date:  string;
  close: number | null;
}

interface VixSide {
  symbol:     string;
  label:      string;
  source:     string;
  latest:     VixLatest | null;
  thresholds: VixThresholds;
  series:     VixSeriesPoint[];
}

interface VixData {
  latest_date: string | null;
  us: VixSide;
  tw: VixSide;
}

// VIX 跟 F&G 反向：恐慌 (高 VIX) = 機會 = 綠系，平靜 (低 VIX) = 危險（自滿）= 紅系
// 但這裡採「直覺映射」：低 = 安全 = 綠，高 = 警示 = 紅，與多數市場儀表板一致。
const RATING_LABEL: Record<string, string> = {
  calm:     "平靜",
  low:      "偏低",
  elevated: "升高",
  panic:    "恐慌",
};

function ratingClass(r: Rating): string {
  switch (r) {
    case "calm":     return "text-emerald-400 bg-emerald-500/15";
    case "low":      return "text-lime-400 bg-lime-500/15";
    case "elevated": return "text-orange-400 bg-orange-500/15";
    case "panic":    return "text-red-400 bg-red-500/15";
    default:         return "text-text-secondary bg-surface-hover";
  }
}

function ratingText(r: Rating): string {
  return r ? (RATING_LABEL[r] ?? r) : "—";
}

function deriveRating(close: number | null, thr: VixThresholds): Rating {
  if (close === null || thr.p20 === null || thr.p50 === null || thr.p80 === null) return null;
  if (close < thr.p20) return "calm";
  if (close < thr.p50) return "low";
  if (close < thr.p80) return "elevated";
  return "panic";
}

function closeColor(close: number | null, thr: VixThresholds): string {
  const r = deriveRating(close, thr);
  switch (r) {
    case "calm":     return "text-emerald-400";
    case "low":      return "text-lime-400";
    case "elevated": return "text-orange-400";
    case "panic":    return "text-red-400";
    default:         return "text-text-primary";
  }
}

// 4 條 reference dashed (p20 / p50 / p80) + 主指數線
function chartSeries(color: string, sideLabel: string): SeriesSpec[] {
  return [
    { key: "ref_p20", label: "p20", color: "#10b981", dashed: true, lineWidth: 1, hideLastValue: true },
    { key: "ref_p50", label: "p50", color: "#8a8a9a", dashed: true, lineWidth: 1, hideLastValue: true },
    { key: "ref_p80", label: "p80", color: "#ef4444", dashed: true, lineWidth: 1, hideLastValue: true },
    { key: "close",   label: sideLabel, color, lineWidth: 2 },
  ];
}

interface PanelProps {
  side:        VixSide;
  color:       string;
  hoveredIdx:  number | null;
  onMove:      (idx: number | null) => void;
}

function SidePanel({ side, color, hoveredIdx, onMove }: PanelProps) {
  const seriesWithRefs = useMemo(() => {
    const { p20, p50, p80 } = side.thresholds;
    return side.series.map((p) => ({
      ...p,
      ref_p20: p20, ref_p50: p50, ref_p80: p80,
    }));
  }, [side.series, side.thresholds]);

  const lastIdx = side.series.length - 1;
  const focusIdx = hoveredIdx !== null && hoveredIdx >= 0 && hoveredIdx <= lastIdx ? hoveredIdx : lastIdx;
  const focusPoint = side.series[focusIdx];
  const isHovering = hoveredIdx !== null;

  const displayClose = isHovering ? focusPoint?.close ?? null : side.latest?.close ?? null;
  const displayRating: Rating = isHovering
    ? deriveRating(focusPoint?.close ?? null, side.thresholds)
    : side.latest?.rating ?? null;
  const displayDate  = isHovering ? focusPoint?.date : null;

  const { p20, p50, p80 } = side.thresholds;

  // Format intraday_time "HHMMSS" → "HH:MM" for display.
  const intradayLabel = (() => {
    const raw = side.latest?.intraday_time;
    if (!raw || isHovering) return null;
    return `${raw.slice(0, 2)}:${raw.slice(2, 4)}`;
  })();

  return (
    <div className="bg-surface-alt border border-border rounded p-3 space-y-3">
      <div className="flex flex-wrap items-baseline gap-2">
        <h2 className="text-sm font-bold text-text-primary">{side.label}</h2>
        <span className="text-[10px] text-text-secondary">
          {side.symbol}　·　{side.source}
        </span>
        {intradayLabel && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-orange-500/15 text-orange-400 ml-auto">
            盤中 {intradayLabel}
          </span>
        )}
        {!intradayLabel && displayDate && (
          <span className="text-[10px] text-text-secondary ml-auto">{displayDate}</span>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <div className={`text-4xl font-bold ${closeColor(displayClose, side.thresholds)}`}>
          {displayClose !== null ? displayClose.toFixed(2) : "—"}
        </div>
        <div className="flex flex-col gap-1">
          <span className={`px-2 py-0.5 rounded text-xs font-medium inline-block w-fit ${ratingClass(displayRating)}`}>
            {ratingText(displayRating)}
          </span>
          <span className="text-[10px] text-text-secondary">
            252 日百分位門檻：
            <span className="text-emerald-400">{p20 ?? "—"}</span> /
            <span className="text-text-secondary"> {p50 ?? "—"}</span> /
            <span className="text-red-400"> {p80 ?? "—"}</span>
          </span>
        </div>
      </div>

      <MultiLineChart
        data={seriesWithRefs}
        series={chartSeries(color, side.label)}
        format="raw"
        height={200}
        onCrosshairMove={onMove}
        externalCrosshairIdx={hoveredIdx}
        crosshairMarkers={false}
      />
    </div>
  );
}

export default function VixPage() {
  const [data, setData] = useState<VixData | null>(null);
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);
  const handleMove = useCallback((idx: number | null) => setHoveredIdx(idx), []);

  useEffect(() => {
    fetch("/data/vix.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary text-sm">載入中…</div>;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">VIX 波動率指數</h1>
        <span className="text-xs text-text-secondary">
          美股 ^VIX 與台指 VIX（TAIFEX）　·　最新交易日：{data.latest_date ?? "—"}
        </span>
      </div>

      <div className="bg-surface-alt border border-border rounded p-3 text-[11px] text-text-secondary leading-5">
        VIX = 選擇權隱含波動率指數，衡量市場對未來 30 日的預期波動。
        rating 以本指數過去 252 日百分位切段：
        <span className="text-emerald-400">&lt;p20 平靜</span>　·
        <span className="text-lime-400">p20~p50 偏低</span>　·
        <span className="text-orange-400">p50~p80 升高</span>　·
        <span className="text-red-400">&gt;p80 恐慌</span>。
        US ^VIX 取自 CNN F&amp;G volatility 子指標，TW VIX 由 TAIFEX 每日收盤公告。
      </div>

      <div className="grid grid-cols-1 gap-3">
        <SidePanel
          side={data.us}
          color="#3b82f6"
          hoveredIdx={hoveredIdx}
          onMove={handleMove}
        />
        <SidePanel
          side={data.tw}
          color="#f59e0b"
          hoveredIdx={hoveredIdx}
          onMove={handleMove}
        />
      </div>
    </div>
  );
}
