import { useEffect, useState, useCallback } from "react";
import MultiLineChart, { type SeriesSpec } from "../components/MultiLineChart";

interface BreadthRow {
  date: string;
  total: number;
  s_up: number; s_dn: number; s_neu: number;
  m_up: number; m_dn: number; m_neu: number;
  l_up: number; l_dn: number; l_neu: number;
  s_trend: number | null;
  m_trend: number | null;
  l_trend: number | null;
  is_intraday?: boolean;
  intraday_time?: string;
}

interface BreadthData {
  latest_date: string | null;
  series: BreadthRow[];
}

// Encoding (analysis/market_breadth.py): -3=bear_exhausting, -2=strong_bear,
// -1=bear, 0=neutral, 1=bull, 2=strong_bull, 3=bull_exhausting.
const TREND_LABELS: Record<number, string> = {
  [-3]: "空方力竭",
  [-2]: "強空",
  [-1]: "偏空",
  0: "中立",
  1: "偏多",
  2: "強多",
  3: "多方力竭",
};

function trendClass(t: number | null): string {
  if (t === null || t === undefined) return "text-text-secondary";
  if (t >= 2) return "text-long-strong";
  if (t === 1) return "text-long-mid";
  if (t === 3) return "text-long-light";
  if (t === -1) return "text-short-mid";
  if (t <= -2) return "text-short-strong";
  if (t === -3) return "text-short-light";
  return "text-text-secondary";
}

const UP_COLOR = "#ef4444";   // 紅 = 多 (台股慣例)
const DN_COLOR = "#10b981";   // 綠 = 空
const NEU_COLOR = "#8a8a9a";  // 灰 = 中立

const SHORT_SERIES: SeriesSpec[] = [
  { key: "s_up",  label: "短排多比", color: UP_COLOR },
  { key: "s_dn",  label: "短排空比", color: DN_COLOR },
  { key: "s_neu", label: "短中立比", color: NEU_COLOR },
];
const MEDIUM_SERIES: SeriesSpec[] = [
  { key: "m_up",  label: "中排多比", color: UP_COLOR },
  { key: "m_dn",  label: "中排空比", color: DN_COLOR },
  { key: "m_neu", label: "中中立比", color: NEU_COLOR },
];
const LONG_SERIES: SeriesSpec[] = [
  { key: "l_up",  label: "長排多比", color: UP_COLOR },
  { key: "l_dn",  label: "長排空比", color: DN_COLOR },
  { key: "l_neu", label: "長中立比", color: NEU_COLOR },
];

export default function BreadthPage() {
  const [data, setData] = useState<BreadthData | null>(null);
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null);

  useEffect(() => {
    fetch("/data/breadth.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  const handleMove = useCallback((idx: number | null) => setHoveredIdx(idx), []);

  if (!data) {
    return <div className="text-text-secondary text-sm">載入中…</div>;
  }
  if (!data.series.length) {
    return <div className="text-text-secondary text-sm">尚無市場排多排空資料。</div>;
  }

  // hoveredIdx 有效時用該點，否則用最後一根（含盤中）
  const lastIdx = data.series.length - 1;
  const focusIdx = hoveredIdx !== null && hoveredIdx >= 0 && hoveredIdx <= lastIdx ? hoveredIdx : lastIdx;
  const latest = data.series[focusIdx];
  const isHovering = hoveredIdx !== null;

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">多空頭排列</h1>
        {latest.is_intraday && !isHovering ? (
          <span className="px-2 py-0.5 rounded text-[10px] bg-amber-500/30 text-amber-300 font-medium">
            盤中估算
            {latest.intraday_time && (
              <>　{new Date(latest.intraday_time).toLocaleTimeString("zh-TW", {
                hour: "2-digit", minute: "2-digit", hour12: false,
              })}</>
            )}
          </span>
        ) : null}
        <span className="text-xs text-text-secondary">
          {isHovering ? "" : (latest.is_intraday ? "今日 " : "最新交易日：")}
          {latest.date}　·　非死魚活躍股 {latest.total.toLocaleString()} 檔
        </span>
      </div>
      <p className="text-xs text-text-secondary">
        排多 = SMA 短/中/長三線多頭排列（含成形中）、排空 = 空頭排列（含成形中）、中立 = 排列不明。分母為非死魚活躍股。
      </p>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {([
          { label: "短期趨勢", t: latest.s_trend, up: latest.s_up, dn: latest.s_dn, neu: latest.s_neu },
          { label: "中期趨勢", t: latest.m_trend, up: latest.m_up, dn: latest.m_dn, neu: latest.m_neu },
          { label: "長期趨勢", t: latest.l_trend, up: latest.l_up, dn: latest.l_dn, neu: latest.l_neu },
        ]).map((card) => (
          <div key={card.label} className="bg-surface-alt border border-border rounded p-3">
            <div className="text-xs text-text-secondary">{card.label}</div>
            <div className={`text-lg font-bold ${trendClass(card.t)}`}>
              {card.t !== null ? TREND_LABELS[card.t] ?? "—" : "—"}
            </div>
            <div className="text-xs text-text-secondary mt-1">
              <span className="text-long-strong">排多 {(card.up * 100).toFixed(1)}%</span>
              {"　"}
              <span className="text-short-strong">排空 {(card.dn * 100).toFixed(1)}%</span>
              {"　"}
              <span className="text-text-secondary">中立 {(card.neu * 100).toFixed(1)}%</span>
            </div>
          </div>
        ))}
      </div>

      <Section title="短期排列比例" series={SHORT_SERIES} data={data.series}
               onMove={handleMove} externalIdx={hoveredIdx} />
      <Section title="中期排列比例" series={MEDIUM_SERIES} data={data.series}
               onMove={handleMove} externalIdx={hoveredIdx} />
      <Section title="長期排列比例" series={LONG_SERIES} data={data.series}
               onMove={handleMove} externalIdx={hoveredIdx} />
    </div>
  );
}

function Section({
  title,
  series,
  data,
  onMove,
  externalIdx,
}: {
  title: string;
  series: SeriesSpec[];
  data: BreadthRow[];
  onMove: (idx: number | null) => void;
  externalIdx: number | null;
}) {
  return (
    <div className="bg-surface-alt border border-border rounded p-2">
      <div className="text-xs font-medium text-text-primary mb-1">{title}</div>
      <MultiLineChart data={data as any} series={series} format="pct" height={140} barSpacing={14}
                      onCrosshairMove={onMove} externalCrosshairIdx={externalIdx} />
    </div>
  );
}
