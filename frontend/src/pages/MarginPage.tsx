import { useEffect, useState } from "react";
import MultiLineChart, { type SeriesSpec } from "../components/MultiLineChart";

interface MarginRow {
  date: string;
  margin_balance: number | null;        // 張
  short_balance:  number | null;        // 張
  margin_balance_value: number | null;  // 仟元
  short_to_margin_pct: number | null;   // %
  net_margin: number | null;            // 張 (buy - sell - repay)
  net_short:  number | null;            // 張 (sell - buy - repay)
  ma60:  number | null;                 // 張
  upper: number | null;                 // ma60 + 2σ
  lower: number | null;                 // ma60 - 2σ
}

interface MarginStats {
  stat_window_days: number;
  ma_window: number;
  margin_balance_pct_1m: number | null;
  margin_balance_pct_6m: number | null;
  margin_balance_pct_3y: number | null;
  margin_balance_z_3y:   number | null;
}

interface MarginData {
  latest_date: string | null;
  stats: MarginStats | null;
  series: MarginRow[];
}

const HEAT_SERIES: SeriesSpec[] = [
  { key: "upper",                label: "+2σ",        color: "#8a8a9a", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "lower",                label: "-2σ",        color: "#8a8a9a", dashed: true, lineWidth: 1, hideLastValue: true },
  { key: "ma60",                 label: "55 日均線",   color: "#f59e0b", lineWidth: 1 },
  { key: "margin_balance_value", label: "融資餘額金額", color: "#ef4444" },
];

const RATIO_SERIES: SeriesSpec[] = [
  { key: "short_to_margin_pct", label: "券資比 (%)", color: "#f59e0b" },
];

const NET_SERIES: SeriesSpec[] = [
  { key: "net_margin", label: "融資 net (張)", color: "#ef4444" },
  { key: "net_short",  label: "融券 net (張)", color: "#10b981" },
];

function fmtLots(v: number | null): string {
  if (v === null) return "—";
  if (Math.abs(v) >= 1e4) return `${(v / 1e4).toFixed(2)}萬張`;
  return `${v.toLocaleString()}張`;
}

// 融資餘額越高市場槓桿越大；當作 "熱度" 分級 → 過高=紅(危險)、過低=灰、中位=普通
function heatTier(p: number | null): { label: string; color: string } {
  if (p === null) return { label: "—", color: "text-text-secondary" };
  if (p < 20) return { label: "過低", color: "text-text-secondary" };
  if (p < 40) return { label: "偏低", color: "text-long-light" };
  if (p < 60) return { label: "中位", color: "text-text-secondary" };
  if (p < 80) return { label: "偏高", color: "text-orange-400" };
  return { label: "過高", color: "text-short-strong" };
}

function fmtKtwd(v: number | null): string {
  if (v === null) return "—";
  // v in 仟元 (NTD thousands). 1 億 = 1e5 仟元, 1 兆 = 1e9 仟元
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)}兆`;
  if (v >= 1e5) return `${(v / 1e5).toFixed(0)}億`;
  if (v >= 1e2) return `${(v / 1e2).toFixed(1)}千萬`;
  return `${v.toLocaleString()}仟`;
}

export default function MarginPage() {
  const [data, setData] = useState<MarginData | null>(null);

  useEffect(() => {
    fetch("/data/margin.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) {
    return <div className="text-text-secondary text-sm">載入中…</div>;
  }
  if (!data.series.length) {
    return <div className="text-text-secondary text-sm">尚無融資融券資料。</div>;
  }

  const latest = data.series[data.series.length - 1];
  const prev = data.series.length >= 2 ? data.series[data.series.length - 2] : null;
  const marginDiff = (latest.margin_balance ?? 0) - (prev?.margin_balance ?? 0);
  const shortDiff  = (latest.short_balance  ?? 0) - (prev?.short_balance  ?? 0);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">融資融券</h1>
        <span className="text-xs text-text-secondary">最新交易日：{data.latest_date}</span>
      </div>
      <p className="text-xs text-text-secondary">
        全市場（TWSE + TPEx）每日融資融券統計。
        融資餘額升 = 散戶積極做多，券資比升 = 空方力量增加。
      </p>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <KpiCard label="融資餘額" main={fmtLots(latest.margin_balance)} diff={marginDiff} diffSuffix="張" diffPositive="red" />
        <KpiCard label="融券餘額" main={fmtLots(latest.short_balance)} diff={shortDiff} diffSuffix="張" diffPositive="green" />
        <KpiCard
          label="券資比"
          main={latest.short_to_margin_pct !== null ? `${latest.short_to_margin_pct.toFixed(2)}%` : "—"}
          diff={
            prev?.short_to_margin_pct !== null && prev?.short_to_margin_pct !== undefined &&
            latest.short_to_margin_pct !== null
              ? +(latest.short_to_margin_pct - prev.short_to_margin_pct).toFixed(2)
              : null
          }
          diffSuffix="pp"
          diffPositive="green"
        />
        <KpiCard label="融資金額" main={fmtKtwd(latest.margin_balance_value)} />
      </div>

      {data.stats && <HeatCard stats={data.stats} />}

      <Section title="融資餘額金額水位（含 55 日均線 + ±2σ）" series={HEAT_SERIES} data={data.series} format="kTwd" />
      <Section title="券資比 (%)" series={RATIO_SERIES} data={data.series} format="raw" />
      <Section title="當日 net 變動（張，正＝增）" series={NET_SERIES} data={data.series} format="int" />
    </div>
  );
}

function HeatCell({ label, pct }: { label: string; pct: number | null }) {
  const tier = heatTier(pct);
  return (
    <div className="flex-1">
      <div className="text-[10px] text-text-secondary">{label}</div>
      <div className={`text-base font-bold ${tier.color}`}>
        {pct !== null ? `${pct.toFixed(0)}` : "—"}
        <span className="text-xs font-normal ml-0.5">%</span>
      </div>
      <div className={`text-[10px] ${tier.color}`}>{tier.label}</div>
    </div>
  );
}

function HeatCard({ stats }: { stats: MarginStats }) {
  return (
    <div className="bg-surface-alt border border-border rounded p-3">
      <div className="flex flex-wrap items-baseline justify-between mb-2">
        <div className="text-sm font-medium text-text-primary">融資水位（百分位）</div>
        <div className="text-[10px] text-text-secondary">
          基準：過去 {stats.stat_window_days} 個交易日
          {stats.margin_balance_z_3y !== null && (
            <>　·　z = {stats.margin_balance_z_3y > 0 ? "+" : ""}{stats.margin_balance_z_3y}σ</>
          )}
        </div>
      </div>
      <div className="flex gap-3">
        <HeatCell label="近 1 個月" pct={stats.margin_balance_pct_1m} />
        <HeatCell label="近 6 個月" pct={stats.margin_balance_pct_6m} />
        <HeatCell label="近 3 年"   pct={stats.margin_balance_pct_3y} />
      </div>
      <div className="text-[10px] text-text-secondary mt-2">
        百分位數 = 今日餘額在該時框內的排名（100 = 區間最高、50 = 中位）。
      </div>
    </div>
  );
}

function KpiCard({
  label,
  main,
  diff,
  diffSuffix,
  diffPositive,
}: {
  label: string;
  main: string;
  diff?: number | null;
  diffSuffix?: string;
  diffPositive?: "red" | "green";
}) {
  let diffEl: React.ReactNode = null;
  if (diff !== undefined && diff !== null && diffSuffix) {
    // 「diffPositive」決定『增加』時用哪色（紅 or 綠）。融資增 → 紅，融券增 → 綠。
    const goingUp = diff > 0;
    const cls =
      diff === 0
        ? "text-text-secondary"
        : goingUp
        ? diffPositive === "green" ? "text-long-strong" : "text-short-strong"
        : diffPositive === "green" ? "text-short-strong" : "text-long-strong";
    diffEl = (
      <span className={`text-xs ${cls}`}>
        {diff > 0 ? "+" : ""}
        {diff.toLocaleString()} {diffSuffix}
      </span>
    );
  }
  return (
    <div className="bg-surface-alt border border-border rounded p-3">
      <div className="text-xs text-text-secondary">{label}</div>
      <div className="text-base font-bold text-text-primary">{main}</div>
      {diffEl && <div>{diffEl}</div>}
    </div>
  );
}

function Section({
  title,
  series,
  data,
  format,
}: {
  title: string;
  series: SeriesSpec[];
  data: MarginRow[];
  format: "int" | "raw" | "pct" | "kTwd";
}) {
  return (
    <div className="bg-surface-alt border border-border rounded p-3">
      <div className="text-sm font-medium text-text-primary mb-2">{title}</div>
      <MultiLineChart data={data as any} series={series} format={format} height={220} />
    </div>
  );
}
