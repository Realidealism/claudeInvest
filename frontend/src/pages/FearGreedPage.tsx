import { useEffect, useState, useCallback, useMemo } from "react";
import MultiLineChart, { type SeriesSpec } from "../components/MultiLineChart";

type Rating = "extreme fear" | "fear" | "neutral" | "greed" | "extreme greed" | string | null;

interface SubBlock {
  score: number | null;
  rating: Rating;
}

interface Latest {
  score: number | null;
  rating: Rating;
  sub: Record<string, SubBlock>;
}

interface SeriesPoint {
  date: string;
  score: number | null;
}

interface FearGreedData {
  latest_date: string | null;
  latest: Latest | null;
  series: SeriesPoint[];
}

const RATING_LABEL: Record<string, string> = {
  "extreme fear":  "極度恐懼",
  "fear":          "恐懼",
  "neutral":       "中立",
  "greed":         "貪婪",
  "extreme greed": "極度貪婪",
};

const SUB_LABEL: Record<string, string> = {
  momentum:   "股價動能",
  strength:   "股價強度",
  breadth:    "市場廣度",
  put_call:   "賣權買權比",
  safe_haven: "避險需求",
  junk_bond:  "垃圾債需求",
  volatility: "市場波動率",
};

const SUB_HINT: Record<string, string> = {
  momentum:   "S&P 500 相對 125 日均線",
  strength:   "52 週新高 vs 新低",
  breadth:    "McClellan 量能累積指標",
  put_call:   "選擇權 P/C ratio",
  safe_haven: "股票 vs 公債 20 日報酬差",
  junk_bond:  "垃圾債 vs 投資級利差",
  volatility: "VIX 與其 50 日均線",
};

// Color by rating — 用「機會 vs 危險」配色：恐懼 = 機會 = 綠系，貪婪 = 危險 = 紅系
function ratingClass(r: Rating): string {
  switch (r) {
    case "extreme fear":  return "text-emerald-400 bg-emerald-500/15";
    case "fear":          return "text-lime-400 bg-lime-500/15";
    case "neutral":       return "text-text-secondary bg-surface-hover";
    case "greed":         return "text-orange-400 bg-orange-500/15";
    case "extreme greed": return "text-red-400 bg-red-500/15";
    default:              return "text-text-secondary bg-surface-hover";
  }
}

function ratingText(r: Rating): string {
  return r ? (RATING_LABEL[r] ?? r) : "—";
}

function scoreColor(s: number | null): string {
  if (s === null) return "text-text-secondary";
  if (s < 25) return "text-emerald-400";
  if (s < 45) return "text-lime-400";
  if (s < 55) return "text-text-primary";
  if (s < 75) return "text-orange-400";
  return "text-red-400";
}

const REF_LINES: SeriesSpec[] = [
  { key: "ref_ef", label: "extreme fear",  color: "#10b981", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "ref_f",  label: "fear",          color: "#84cc16", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "ref_g",  label: "greed",         color: "#f97316", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "ref_eg", label: "extreme greed", color: "#ef4444", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "score",  label: "F&G 主分數",    color: "#3b82f6", lineWidth: 2 },
];

export default function FearGreedPage() {
  const [data, setData] = useState<FearGreedData | null>(null);
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);
  const handleMove = useCallback((idx: number | null) => setHoveredIdx(idx), []);

  useEffect(() => {
    fetch("/data/fear_greed.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  // 把 reference 常值塞進每點，讓 MultiLineChart 自然畫 4 條水平 dashed line。
  const seriesWithRefs = useMemo(() => {
    if (!data) return [];
    return data.series.map((p) => ({
      ...p,
      ref_ef: 25, ref_f: 45, ref_g: 55, ref_eg: 75,
    }));
  }, [data]);

  if (!data) return <div className="text-text-secondary text-sm">載入中…</div>;
  if (!data.latest) return <div className="text-text-secondary text-sm">尚無 CNN F&G 資料。</div>;

  // hover 中用該日 series 點，否則用 latest payload（含 sub）
  const lastIdx = data.series.length - 1;
  const focusIdx = hoveredIdx !== null && hoveredIdx >= 0 && hoveredIdx <= lastIdx ? hoveredIdx : lastIdx;
  const focusPoint = data.series[focusIdx];
  const isHovering = hoveredIdx !== null;

  // 用 hover 那天 series score 推出 rating（rating 在 hover 模式不查 sub）
  const hoveredScore = focusPoint.score;
  const hoveredRating: Rating = (() => {
    const s = hoveredScore;
    if (s === null) return null;
    if (s < 25) return "extreme fear";
    if (s < 45) return "fear";
    if (s < 55) return "neutral";
    if (s < 75) return "greed";
    return "extreme greed";
  })();

  const displayScore  = isHovering ? hoveredScore  : data.latest.score;
  const displayRating = isHovering ? hoveredRating : data.latest.rating;
  const displayDate   = isHovering ? focusPoint.date : data.latest_date;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">CNN 恐懼與貪婪指數</h1>
        <span className="text-xs text-text-secondary">
          美股情緒指標（S&P 500 based）　·　{isHovering ? "" : "最新交易日："}{displayDate}
        </span>
      </div>

      <div className="bg-surface-alt border border-border rounded p-4 flex flex-wrap items-center gap-4">
        <div className={`text-5xl font-bold ${scoreColor(displayScore)}`}>
          {displayScore !== null ? displayScore.toFixed(1) : "—"}
        </div>
        <div className="flex flex-col gap-1">
          <span className={`px-2 py-1 rounded text-sm font-medium inline-block w-fit ${ratingClass(displayRating)}`}>
            {ratingText(displayRating)}
          </span>
          <span className="text-[10px] text-text-secondary">
            0 = 極度恐懼（機會）／ 100 = 極度貪婪（危險）
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {Object.entries(data.latest.sub).map(([slot, blk]) => (
          <div key={slot} className="bg-surface-alt border border-border rounded p-3">
            <div className="text-xs text-text-secondary">{SUB_LABEL[slot] ?? slot}</div>
            <div className="text-base font-bold text-text-primary mt-0.5">
              {blk.score !== null ? blk.score.toLocaleString(undefined, { maximumFractionDigits: 2 }) : "—"}
            </div>
            <span className={`px-1.5 py-0.5 rounded text-[10px] inline-block mt-1 ${ratingClass(blk.rating)}`}>
              {ratingText(blk.rating)}
            </span>
            <div className="text-[10px] text-text-secondary mt-1.5">{SUB_HINT[slot]}</div>
          </div>
        ))}
      </div>

      <div className="bg-surface-alt border border-border rounded p-3">
        <div className="text-xs font-medium text-text-primary mb-1">過去 1 年 F&G 主分數（dashed = 25 / 45 / 55 / 75 區間線）</div>
        <MultiLineChart
          data={seriesWithRefs}
          series={REF_LINES}
          format="raw"
          height={220}
          onCrosshairMove={handleMove}
        />
      </div>
    </div>
  );
}
