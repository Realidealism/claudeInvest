import { useEffect, useState, type MouseEvent } from "react";

interface Component {
  key: string;
  name: string;
  hot: number;
  detail: string;
}

interface HistPoint {
  date: string;
  score: number;
  tx: number;
}

interface ThermoData {
  as_of: string | null;
  score: number | null;
  bucket: string | null;
  near_high: boolean;
  pct_from_high: number;
  hi_window: number;
  components: Component[];
  history: HistPoint[];
}

const BUCKET_COLOR: Record<string, string> = {
  冷靜: "#3b82f6",
  溫和: "#22c55e",
  偏緊: "#f59e0b",
  過熱: "#ef4444",
};

// gauge zones: 0-40 冷靜, 40-60 溫和, 60-80 偏緊, 80-100 過熱
const ZONES = [
  { w: 40, c: "#3b82f6" },
  { w: 20, c: "#22c55e" },
  { w: 20, c: "#f59e0b" },
  { w: 20, c: "#ef4444" },
];

function zoneColor(s: number): string {
  if (s >= 80) return "#ef4444";
  if (s >= 60) return "#f59e0b";
  if (s >= 40) return "#22c55e";
  return "#3b82f6";
}

function TrendCharts({ pts }: { pts: HistPoint[] }) {
  const [hi, setHi] = useState<number | null>(null);
  if (pts.length < 2) return null;
  const n = pts.length;
  const H = 130, padT = 8;
  const xOf = (i: number) => (i / (n - 1)) * 1000;
  const cur = hi ?? n - 1;
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => Math.round(f * (n - 1)));
  const onMove = (e: MouseEvent<HTMLDivElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    const f = (e.clientX - r.left) / r.width;
    setHi(Math.max(0, Math.min(n - 1, Math.round(f * (n - 1)))));
  };
  const crosshair = (
    <line x1={xOf(cur)} x2={xOf(cur)} y1={0} y2={H} stroke="currentColor"
      className="text-text-secondary" strokeWidth={1} vectorEffect="non-scaling-stroke" opacity={0.55} />
  );

  // temperature (fixed 0-100)
  const tY = (v: number) => padT + (1 - v / 100) * (H - padT);
  const tLine = pts.map((p, i) => `${i ? "L" : "M"}${xOf(i).toFixed(1)},${tY(p.score).toFixed(1)}`).join(" ");
  // index (auto min-max)
  const txs = pts.map((p) => p.tx);
  const lo = Math.min(...txs), hiv = Math.max(...txs);
  const pY = (v: number) => padT + (1 - (v - lo) / (hiv - lo || 1)) * (H - padT);
  const pLine = pts.map((p, i) => `${i ? "L" : "M"}${xOf(i).toFixed(1)},${pY(p.tx).toFixed(1)}`).join(" ");

  return (
    <div>
      <div className="text-xs text-text-secondary mb-1">
        {pts[cur].date}　溫度 <span className="text-text-primary font-medium tabular-nums" style={{ color: zoneColor(pts[cur].score) }}>{pts[cur].score}</span>
        　加權 <span className="text-text-primary font-medium tabular-nums">{pts[cur].tx.toLocaleString()}</span>
        {hi === null && <span className="ml-1">（最新；滑過看每日）</span>}
      </div>

      {/* temperature */}
      <div className="relative" style={{ height: H }} onMouseLeave={() => setHi(null)} onMouseMove={onMove}>
        <svg viewBox={`0 0 1000 ${H}`} className="w-full" style={{ height: H }} preserveAspectRatio="none">
          <defs>
            <linearGradient id="thermoGrad" gradientUnits="userSpaceOnUse" x1={0} y1={tY(100)} x2={0} y2={tY(0)}>
              <stop offset="0" stopColor="#ef4444" /><stop offset="0.2" stopColor="#ef4444" />
              <stop offset="0.2" stopColor="#f59e0b" /><stop offset="0.4" stopColor="#f59e0b" />
              <stop offset="0.4" stopColor="#22c55e" /><stop offset="0.6" stopColor="#22c55e" />
              <stop offset="0.6" stopColor="#3b82f6" /><stop offset="1" stopColor="#3b82f6" />
            </linearGradient>
          </defs>
          {[60, 80].map((y) => (
            <line key={y} x1={0} x2={1000} y1={tY(y)} y2={tY(y)} stroke="currentColor" className="text-border" strokeWidth={1} vectorEffect="non-scaling-stroke" opacity={0.5} />
          ))}
          <path d={tLine} fill="none" stroke="url(#thermoGrad)" strokeWidth={1.8} vectorEffect="non-scaling-stroke" />
          {crosshair}
          <circle cx={xOf(cur)} cy={tY(pts[cur].score)} r={3.5} fill={zoneColor(pts[cur].score)} vectorEffect="non-scaling-stroke" />
        </svg>
        {[60, 80].map((y) => (
          <span key={y} className="absolute right-0.5 text-[9px] text-text-secondary" style={{ top: tY(y), transform: "translateY(-50%)" }}>{y}</span>
        ))}
        <span className="absolute left-0.5 top-0.5 text-[9px] text-text-secondary">溫度</span>
      </div>

      {/* 加權指數 */}
      <div className="relative mt-1" style={{ height: H }} onMouseLeave={() => setHi(null)} onMouseMove={onMove}>
        <svg viewBox={`0 0 1000 ${H}`} className="w-full" style={{ height: H }} preserveAspectRatio="none">
          <path d={pLine} fill="none" stroke="currentColor" className="text-text-primary" strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
          {crosshair}
          <circle cx={xOf(cur)} cy={pY(pts[cur].tx)} r={3.5} fill="currentColor" className="text-text-primary" vectorEffect="non-scaling-stroke" />
        </svg>
        <span className="absolute right-0.5 text-[9px] text-text-secondary" style={{ top: pY(hiv), transform: "translateY(-50%)" }}>{Math.round(hiv).toLocaleString()}</span>
        <span className="absolute right-0.5 text-[9px] text-text-secondary" style={{ top: pY(lo), transform: "translateY(-50%)" }}>{Math.round(lo).toLocaleString()}</span>
        <span className="absolute left-0.5 top-0.5 text-[9px] text-text-secondary">加權指數</span>
      </div>

      <div className="flex justify-between text-[10px] text-text-secondary mt-1">
        {ticks.map((t, i) => <span key={i}>{pts[t].date}</span>)}
      </div>
    </div>
  );
}

export default function ThermometerPage() {
  const [data, setData] = useState<ThermoData | null>(null);

  useEffect(() => {
    fetch("/data/thermometer.json").then((r) => r.json()).then(setData).catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;
  if (data.score == null) return <div className="text-text-secondary">尚無溫度計資料</div>;

  const color = BUCKET_COLOR[data.bucket ?? ""] ?? "#8a8a9a";

  return (
    <div className="max-w-3xl">
      <h2 className="text-lg font-semibold mb-1">市場溫度計</h2>
      <p className="text-xs text-text-secondary mb-5">
        描述「當前市場多緊繃」的脆弱度儀表——<span className="text-text-primary">不是崩盤預測</span>。
        熱＝槓桿與外資空方定位偏極端、提高警覺；冷＝相對安全。歷史上內生型頂多偏熱，但偏熱不代表即將下跌。資料日 {data.as_of}。
      </p>

      {/* score + bucket */}
      <div className="flex items-end gap-4 mb-3">
        <div className="text-5xl font-bold tabular-nums" style={{ color }}>{data.score}</div>
        <div className="pb-1">
          <span className="px-2 py-0.5 rounded text-sm font-semibold text-white" style={{ backgroundColor: color }}>
            {data.bucket}
          </span>
          <div className="text-xs text-text-secondary mt-1">0–100 緊繃度</div>
        </div>
      </div>

      {/* gauge bar */}
      <div className="relative mb-6">
        <div className="flex h-3 rounded overflow-hidden">
          {ZONES.map((z, i) => (
            <div key={i} style={{ width: `${z.w}%`, backgroundColor: z.c, opacity: 0.5 }} />
          ))}
        </div>
        <div
          className="absolute -top-1 w-0 h-0"
          style={{
            left: `calc(${data.score}% - 5px)`,
            borderLeft: "5px solid transparent",
            borderRight: "5px solid transparent",
            borderTop: `7px solid ${color}`,
          }}
        />
        <div className="flex justify-between text-[10px] text-text-secondary mt-1">
          <span>0 冷靜</span><span>40 溫和</span><span>60 偏緊</span><span>80 過熱</span><span>100</span>
        </div>
      </div>

      {/* 位階 context: 過熱是否發生在高點（會崩的過熱 50% 在高點，回檔中的過熱較少接續大跌） */}
      <div className="mb-6 text-xs">
        <span className="text-text-secondary">位階：</span>
        <span className="tabular-nums">距 {data.hi_window} 日高 {data.pct_from_high}%</span>
        <span className={"ml-2 px-1.5 py-0.5 rounded text-[10px] font-medium " +
          (data.near_high ? "bg-negative/20 text-negative" : "bg-surface-hover text-text-secondary")}>
          {data.near_high ? "近高" : "非高"}
        </span>
        {(data.score ?? 0) >= 60 && (
          <div className="text-text-secondary mt-1">
            {data.near_high
              ? "⚠ 過熱且貼近高點——歷史上這種組合較危險（會崩的過熱有 50% 落在高點）。"
              : "過熱但已離高——屬回檔中的定位極端，歷史上較少接續大跌。"}
          </div>
        )}
      </div>

      {/* components */}
      <div className="space-y-3 mb-6">
        <div className="text-sm font-semibold">組件明細</div>
        {data.components.map((c) => (
          <div key={c.key}>
            <div className="flex items-center justify-between text-sm">
              <span>{c.name}</span>
              <span className="tabular-nums text-text-secondary">{c.hot}</span>
            </div>
            <div className="h-1.5 rounded bg-surface-hover overflow-hidden mt-1">
              <div className="h-full rounded" style={{ width: `${c.hot}%`, backgroundColor: color }} />
            </div>
            <div className="text-xs text-text-secondary mt-1">{c.detail}</div>
          </div>
        ))}
      </div>

      {/* history */}
      <div>
        <div className="text-sm font-semibold mb-1">近一年溫度 vs 加權指數</div>
        <TrendCharts pts={data.history} />
      </div>

      <details className="text-xs text-text-secondary mt-6">
        <summary className="cursor-pointer hover:text-text-primary">方法與限制</summary>
        <ul className="mt-2 space-y-1 list-disc pl-4">
          <li>外資期貨定位：外資臺股期貨淨未平倉在近 120 日的百分位（越淨空越熱）。此為唯一撐過公平偽訊號測試的組件（1.47x）。</li>
          <li>融資水位：融資餘額金額對 55 日均線的乖離（Bollinger z，−2σ→0、均線→50、+2σ→100）。衡量偏離趨勢多少，不受長多水位長期偏高影響。</li>
          <li>選擇權自滿：Put/Call OI 比近 90 日百分位（越低越自滿＝越熱）。單獨弱，但與外資期貨疊加把精確率拉到 2.65x（窗口 60–90 為掃描出的甜蜜點）。</li>
          <li>微台散戶多單：微台散戶淨多佔 OI 百分位（越高越 froth＝越熱）。與外資期貨正交（散戶 vs 外資），但史僅 2024-07 起、樣本短，故降權（0.5）。</li>
          <li>前三組件等權、微台散戶降權平均。★這是狀態描述非計時訊號——歷史上偏熱也常不崩（如 2020 COVID 為外生衝擊，儀表當時僅中性）。</li>
        </ul>
      </details>
    </div>
  );
}
