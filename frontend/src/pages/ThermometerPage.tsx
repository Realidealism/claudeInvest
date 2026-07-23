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
  label: string;
  color: string;
  stance: string;
  panic: boolean;
  m_alert: boolean;
  l_alert: boolean;
  lb_alert: boolean;
  tv_alert: boolean;
  tv_n?: number;   // optional: JSON from a pre-rebuild exe won't have it (vn() defaults to 0)
  warn: boolean;
  c: Record<string, number | null>;
}

const WARN_COLOR = "#ec4899";     // 加速惡化警示 (badge card)
const LIMITUP_COLOR = "#a855f7";  // 頂部過熱·漲停 (badge card)

const STANCE_COLOR: Record<string, string> = { 攻擊: "#22c55e", 防守: "#ef4444" };

const COMP_COLOR: Record<string, string> = {
  futures: "#3b82f6",
  margin: "#f59e0b",
};

function MiniChart({ pts, ck, color }: { pts: HistPoint[]; ck: string; color: string }) {
  const [hi, setHi] = useState<number | null>(null);
  const n = pts.length;
  if (n < 2) return null;
  const H = 46, pad = 5;
  const xOf = (i: number) => (i / (n - 1)) * 1000;
  const yOf = (v: number) => pad + (1 - v / 100) * (H - 2 * pad);
  let lastIdx = n - 1;
  while (lastIdx > 0 && pts[lastIdx].c[ck] == null) lastIdx--;
  const cur = hi ?? lastIdx;
  const curVal = pts[cur].c[ck];
  const segs: string[] = [];
  for (let i = 1; i < n; i++) {
    const a = pts[i - 1].c[ck], b = pts[i].c[ck];
    if (a == null || b == null) continue;
    segs.push(`M${xOf(i - 1).toFixed(1)},${yOf(a).toFixed(1)}L${xOf(i).toFixed(1)},${yOf(b).toFixed(1)}`);
  }
  return (
    <div className="relative" style={{ height: H }}
      onMouseLeave={() => setHi(null)}
      onMouseMove={(e) => {
        const r = e.currentTarget.getBoundingClientRect();
        setHi(Math.max(0, Math.min(n - 1, Math.round((e.clientX - r.left) / r.width * (n - 1)))));
      }}>
      <svg viewBox={`0 0 1000 ${H}`} className="w-full" style={{ height: H }} preserveAspectRatio="none">
        {[50].map((y) => (
          <line key={y} x1={0} x2={1000} y1={yOf(y)} y2={yOf(y)} stroke="currentColor" className="text-border" strokeWidth={1} vectorEffect="non-scaling-stroke" opacity={0.4} />
        ))}
        <path d={segs.join(" ")} fill="none" stroke={color} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
        <line x1={xOf(cur)} x2={xOf(cur)} y1={0} y2={H} stroke="currentColor" className="text-text-secondary" strokeWidth={1} vectorEffect="non-scaling-stroke" opacity={0.5} />
        {curVal != null && <circle cx={xOf(cur)} cy={yOf(curVal)} r={2.5} fill={color} vectorEffect="non-scaling-stroke" />}
      </svg>
      {hi != null && (
        <div className="absolute -top-0.5 right-0 text-[10px] text-text-primary bg-surface/90 px-1 rounded">
          {pts[cur].date}　{curVal ?? "—"}
        </div>
      )}
    </div>
  );
}

interface ThermoData {
  as_of: string | null;
  score: number | null;
  bucket: string | null;
  bucket_color: string;
  danger: boolean;
  stance: string;
  stance_color: string;
  stance_reason: string;
  near_high: boolean;
  pct_from_high: number;
  hi_window: number;
  alert: boolean;
  alert_conditions: { name: string; met: boolean }[];
  margin_alert: boolean;
  margin_alert_conditions: { name: string; met: boolean }[];
  limitup_alert: boolean;
  limitup_alert_conditions: { name: string; met: boolean }[];
  limitup_bear_alert: boolean;
  limitup_bear_conditions: { name: string; met: boolean }[];
  top_vote: boolean;
  top_vote_n: number;
  top_vote_k: number;
  top_vote_conditions: { name: string; met: boolean }[];
  warn: boolean;
  warn_conditions: { name: string; met: boolean }[];
  panic: boolean;
  panic_conditions: { name: string; met: boolean }[];
  components: Component[];
  history: HistPoint[];
}

// gauge zones = 定位極端度 intensity (neutral grey → the regime/direction is carried
// by the headline colour, not the gauge, because a high reading is 頂部過熱 near a
// high but 底部超賣 below it).
const ZONES = [
  { w: 40, c: "#3a3a45" },
  { w: 20, c: "#55555f" },
  { w: 20, c: "#70707a" },
  { w: 20, c: "#9a9aa8" },
];

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

  // 四正交過熱投票 (0-4; >=3 → 高點防守). Bars per day, coloured by count.
  const tY = (v: number) => padT + (1 - v / 4) * (H - padT);
  const voteColor = (nn: number) => (nn >= 3 ? "#ef4444" : nn === 2 ? "#f59e0b" : "#8a8a9a");
  const vn = (p: HistPoint) => p.tv_n ?? 0;   // tolerate pre-rebuild JSON without tv_n
  // index (auto min-max)
  const txs = pts.map((p) => p.tx);
  const lo = Math.min(...txs), hiv = Math.max(...txs);
  const pY = (v: number) => padT + (1 - (v - lo) / (hiv - lo || 1)) * (H - padT);

  return (
    <div>
      <div className="text-xs text-text-secondary mb-1">
        {pts[cur].date}　<span className="font-medium" style={{ color: STANCE_COLOR[pts[cur].stance] }}>{pts[cur].stance}</span>
        　過熱投票 <span className="font-medium tabular-nums" style={{ color: voteColor(vn(pts[cur])) }}>{vn(pts[cur])}/4</span>
        {vn(pts[cur]) >= 3 && <span className="font-medium" style={{ color: "#ef4444" }}>　高信念頂部</span>}
        　加權 <span className="text-text-primary font-medium tabular-nums">{pts[cur].tx.toLocaleString()}</span>
        {hi === null && <span className="ml-1">（最新；滑過看每日）</span>}
      </div>

      {/* 四正交過熱投票 0-4 (drives 高點防守) */}
      <div className="relative" style={{ height: H }} onMouseLeave={() => setHi(null)} onMouseMove={onMove}>
        <svg viewBox={`0 0 1000 ${H}`} className="w-full" style={{ height: H }} preserveAspectRatio="none">
          {/* ≥3 = 高點防守 觸發線 */}
          <line x1={0} x2={1000} y1={tY(3)} y2={tY(3)} stroke="#ef4444" strokeWidth={1}
            strokeDasharray="4 3" vectorEffect="non-scaling-stroke" opacity={0.6} />
          {pts.map((p, i) => vn(p) > 0 ? (
            <line key={"v" + i} x1={xOf(i)} x2={xOf(i)} y1={tY(0)} y2={tY(vn(p))}
              stroke={voteColor(vn(p))} strokeWidth={2} vectorEffect="non-scaling-stroke" opacity={0.9} />
          ) : null)}
          {crosshair}
          <circle cx={xOf(cur)} cy={tY(vn(pts[cur]))} r={3} fill={voteColor(vn(pts[cur]))} vectorEffect="non-scaling-stroke" />
        </svg>
        {[1, 2, 3, 4].map((y) => (
          <span key={y} className="absolute right-0.5 text-[9px] text-text-secondary" style={{ top: tY(y), transform: "translateY(-50%)" }}>{y}</span>
        ))}
        <span className="absolute left-0.5 top-0.5 text-[9px] text-text-secondary">過熱投票（0–4；紅虛線＝≥3 高點防守）</span>
      </div>

      {/* 加權指數（線 綠攻·紅防） */}
      <div className="relative mt-1" style={{ height: H }} onMouseLeave={() => setHi(null)} onMouseMove={onMove}>
        <svg viewBox={`0 0 1000 ${H}`} className="w-full" style={{ height: H }} preserveAspectRatio="none">
          {pts.slice(1).map((p, i) => (
            <line key={i} x1={xOf(i)} y1={pY(pts[i].tx)} x2={xOf(i + 1)} y2={pY(p.tx)}
              stroke={STANCE_COLOR[p.stance] ?? "#e5e5e5"} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />
          ))}
          {crosshair}
          <circle cx={xOf(cur)} cy={pY(pts[cur].tx)} r={3.5} fill={STANCE_COLOR[pts[cur].stance] ?? "#e5e5e5"} vectorEffect="non-scaling-stroke" />
        </svg>
        <span className="absolute right-0.5 text-[9px] text-text-secondary" style={{ top: pY(hiv), transform: "translateY(-50%)" }}>{Math.round(hiv).toLocaleString()}</span>
        <span className="absolute right-0.5 text-[9px] text-text-secondary" style={{ top: pY(lo), transform: "translateY(-50%)" }}>{Math.round(lo).toLocaleString()}</span>
        <span className="absolute left-0.5 top-0.5 text-[9px] text-text-secondary">加權指數（線 綠＝攻擊、紅＝防守）</span>
      </div>

      <div className="flex justify-between text-[10px] text-text-secondary mt-1">
        {ticks.map((t, i) => <span key={i}>{pts[t].date}</span>)}
      </div>
    </div>
  );
}

interface LiveStance {
  as_of: string;
  snapshot_time: string;
  is_today: boolean;
  stance: string;
  stance_color: string;
  stance_reason: string;
  short_trend: number;
}

export default function ThermometerPage() {
  const [data, setData] = useState<ThermoData | null>(null);
  const [live, setLive] = useState<LiveStance | null>(null);

  useEffect(() => {
    fetch("/data/thermometer.json").then((r) => r.json()).then(setData).catch(console.error);
    // Live intraday stance (only the 攻防 updates during the session). Missing
    // file / parse errors just leave the close stance in place.
    fetch("/data/thermometer_stance.json").then((r) => (r.ok ? r.json() : null)).then(setLive).catch(() => {});
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;
  if (data.score == null) return <div className="text-text-secondary">尚無溫度計資料</div>;

  const color = data.bucket_color;

  // Use the live stance only when it is strictly newer than the last close
  // reflected in thermometer.json (i.e. an in-session reading). After the
  // daily post-close export catches up, both dates match and we fall back to
  // the close stance (identical value, no misleading "盤中" time tag).
  const showLive = !!live && live.is_today && live.as_of > data.as_of!;
  const stanceValue = showLive ? live!.stance : data.stance;
  const stanceColor = showLive ? live!.stance_color : data.stance_color;
  const stanceReason = showLive ? live!.stance_reason : data.stance_reason;
  const liveTime = showLive ? live!.snapshot_time.slice(11, 16) : null;

  return (
    <div className="max-w-5xl">
     <div className="max-w-3xl">
      <h2 className="text-lg font-semibold mb-1">市場溫度計</h2>
      <p className="text-xs text-text-secondary mb-5">
        以趨勢（攻擊／防守）為主，搭配頂部過熱、恐慌買進兩個 contrarian 訊號。
        <span className="text-text-primary">非崩盤預測</span>，細節見下方「詳細指標」。資料日 {data.as_of}。
      </p>

      {/* 現在建議: 趨勢 + 觸發訊號 兩塊並列 */}
      <div className="mb-5 flex flex-col sm:flex-row gap-3">
        {/* 趨勢 */}
        <div className="flex-1 rounded-lg border p-3 flex items-center gap-3"
          style={{ borderColor: stanceColor, backgroundColor: stanceColor + "1a" }}>
          <span className="text-4xl font-bold" style={{ color: stanceColor }}>{stanceValue}</span>
          <div>
            <div className="text-sm font-semibold">
              現在建議（趨勢）
              {liveTime && (
                <span className="ml-1.5 px-1.5 py-0.5 rounded text-[10px] font-medium bg-surface-hover text-text-secondary">
                  盤中 {liveTime}
                </span>
              )}
            </div>
            <div className="text-xs text-text-secondary">
              {stanceReason}　·　{stanceValue === "防守" ? "短期轉弱，空手觀望" : "短期偏多，可進場"}
            </div>
          </div>
        </div>
        {/* 恐慌買進（觸發才出現） */}
        {data.panic && (
          <div className="flex-1 rounded-lg border p-3 flex items-center gap-3"
            style={{ borderColor: "#facc15", backgroundColor: "#facc151a" }}>
            <span className="text-3xl font-bold" style={{ color: "#facc15" }}>⚡</span>
            <div>
              <div className="text-sm font-semibold" style={{ color: "#facc15" }}>恐慌買進機會</div>
              <div className="text-xs text-text-secondary">深跌＋斷頭急殺＋快殺（V 底設定）。★快崩為主，慢熊仍會接刀，別盲買。</div>
            </div>
          </div>
        )}
        {/* 頂部過熱·外資（觸發才出現） */}
        {data.danger && (
          <div className="flex-1 rounded-lg border p-3 flex items-center gap-3"
            style={{ borderColor: "#ef4444", backgroundColor: "#ef44441a" }}>
            <span className="text-3xl font-bold text-negative">⚠</span>
            <div>
              <div className="text-sm font-semibold text-negative">頂部過熱·外資</div>
              <div className="text-xs text-text-secondary">外資淨空創新低（淨空最重、加碼放空）。★約 82% 假警報。</div>
            </div>
          </div>
        )}
        {/* 頂部過熱·融資（觸發才出現） */}
        {data.margin_alert && (
          <div className="flex-1 rounded-lg border p-3 flex items-center gap-3"
            style={{ borderColor: "#f59e0b", backgroundColor: "#f59e0b1a" }}>
            <span className="text-3xl font-bold" style={{ color: "#f59e0b" }}>⚠</span>
            <div>
              <div className="text-sm font-semibold" style={{ color: "#f59e0b" }}>頂部過熱·融資</div>
              <div className="text-xs text-text-secondary">融資／成交量 布林 z ≥ +1.5σ（槓桿相對量能過熱）。★約 70% 假、獨家抓到 2025-03。</div>
            </div>
          </div>
        )}
        {/* 頂部過熱·漲停（觸發才出現） */}
        {data.limitup_alert && (
          <div className="flex-1 rounded-lg border p-3 flex items-center gap-3"
            style={{ borderColor: LIMITUP_COLOR, backgroundColor: LIMITUP_COLOR + "1a" }}>
            <span className="text-3xl font-bold" style={{ color: LIMITUP_COLOR }}>⚠</span>
            <div>
              <div className="text-sm font-semibold" style={{ color: LIMITUP_COLOR }}>頂部過熱·漲停</div>
              <div className="text-xs text-text-secondary">漲停家數佔比乖離過高＝散戶投機過熱。★時間均勻、抓到 4/5 崩盤峰含 2024-07／2026-02。</div>
            </div>
          </div>
        )}
        {/* 加速惡化警示（觸發才出現）— 頂部過熱且排列尚未翻空但空頭排列急升 */}
        {data.warn && (
          <div className="flex-1 rounded-lg border p-3 flex items-center gap-3"
            style={{ borderColor: WARN_COLOR, backgroundColor: WARN_COLOR + "1a" }}>
            <span className="text-3xl font-bold" style={{ color: WARN_COLOR }}>⚠</span>
            <div>
              <div className="text-sm font-semibold" style={{ color: WARN_COLOR }}>加速惡化警示</div>
              <div className="text-xs text-text-secondary">過熱中、排列尚未翻空，但短空頭排列急升＝可能正要轉弱。★約半數為假，早於攻防翻防守的預警。</div>
            </div>
          </div>
        )}
      </div>

      {/* 詳細指標與訊號（收合） */}
      <details className="mb-4">
        <summary className="cursor-pointer text-sm text-text-secondary hover:text-text-primary mb-3">詳細指標與訊號</summary>

      {/* score + bucket */}
      <div className="flex items-end gap-4 mb-3">
        <div className="text-5xl font-bold tabular-nums" style={{ color }}>{data.score}</div>
        <div className="pb-1">
          <span className="px-2 py-0.5 rounded text-sm font-semibold text-white" style={{ backgroundColor: color }}>
            {data.bucket}
          </span>
          <div className="text-xs text-text-secondary mt-1">定位極端度 {data.score}／100</div>
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
          <span>0 低</span><span>40 中</span><span>60 高</span><span>80 極端</span><span>100</span>
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
      </div>

      {/* 頂部過熱·外資（離散警報，非預測） */}
      <div className={"mb-3 rounded border p-3 " + (data.alert ? "border-negative bg-negative/10" : "border-border")}>
        <div className="flex items-center gap-2 mb-2">
          <span className={"px-2 py-0.5 rounded text-xs font-semibold " +
            (data.alert ? "bg-negative text-white" : "bg-surface-hover text-text-secondary")}>
            {data.alert ? "⚠ 頂部過熱·外資" : "未觸發"}
          </span>
          <span className="text-xs text-text-secondary">
            外資過熱訊號　{data.alert_conditions.filter((c) => c.met).length}/{data.alert_conditions.length} 條件
          </span>
        </div>
        <ul className="text-xs space-y-1">
          {data.alert_conditions.map((c) => (
            <li key={c.name} className={c.met ? "text-text-primary" : "text-text-secondary"}>
              <span className={c.met ? "text-negative" : "text-text-secondary/50"}>{c.met ? "✔" : "✗"}</span>{" "}
              {c.name}
            </li>
          ))}
        </ul>
        <div className="text-xs text-text-secondary mt-2">
          外資淨空創 78 日新低（淨空最重）時亮。
          <span className="text-text-primary">★已拿掉近高限制，下跌途中（如崩盤日）也會亮、非僅頂部</span>，僅供參考。
        </div>
      </div>

      {/* 頂部過熱·融資（獨立第二燈，融資/成交量 布林 z） */}
      <div className={"mb-6 rounded border p-3 " + (data.margin_alert ? "border-[#f59e0b] bg-[#f59e0b]/10" : "border-border")}>
        <div className="flex items-center gap-2 mb-2">
          <span className={"px-2 py-0.5 rounded text-xs font-semibold " +
            (data.margin_alert ? "bg-[#f59e0b] text-black" : "bg-surface-hover text-text-secondary")}>
            {data.margin_alert ? "⚠ 頂部過熱·融資" : "未觸發"}
          </span>
          <span className="text-xs text-text-secondary">
            融資過熱訊號　{(data.margin_alert_conditions ?? []).filter((c) => c.met).length}/{(data.margin_alert_conditions ?? []).length} 條件
          </span>
        </div>
        <ul className="text-xs space-y-1">
          {(data.margin_alert_conditions ?? []).map((c) => (
            <li key={c.name} className={c.met ? "text-text-primary" : "text-text-secondary"}>
              <span className={c.met ? "text-[#f59e0b]" : "text-text-secondary/50"}>{c.met ? "✔" : "✗"}</span>{" "}
              {c.name}
            </li>
          ))}
        </ul>
        <div className="text-xs text-text-secondary mt-2">
          融資餘額／55 日均成交金額 的布林 z ≥ +1.5σ 時亮（先用成交量正規化、再 de-trend，抓槓桿相對量能的尖峰）。
          <span className="text-text-primary">★與外資過熱互補、獨家抓到 2025-03 -23%</span>，但約 70% 假、非時間均勻，僅供參考。
        </div>
      </div>

      {/* 頂部過熱·漲停（第 3 盞獨立燈，漲停佔比 90 日乖離 z） */}
      <div className={"mb-6 rounded border p-3 " + (data.limitup_alert ? "border-[#a855f7] bg-[#a855f7]/10" : "border-border")}>
        <div className="flex items-center gap-2 mb-2">
          <span className={"px-2 py-0.5 rounded text-xs font-semibold " +
            (data.limitup_alert ? "bg-[#a855f7] text-white" : "bg-surface-hover text-text-secondary")}>
            {data.limitup_alert ? "⚠ 頂部過熱·漲停" : "未觸發"}
          </span>
          <span className="text-xs text-text-secondary">
            漲停過熱訊號　{(data.limitup_alert_conditions ?? []).filter((c) => c.met).length}/{(data.limitup_alert_conditions ?? []).length} 條件
          </span>
        </div>
        <ul className="text-xs space-y-1">
          {(data.limitup_alert_conditions ?? []).map((c) => (
            <li key={c.name} className={c.met ? "text-text-primary" : "text-text-secondary"}>
              <span className={c.met ? "text-[#a855f7]" : "text-text-secondary/50"}>{c.met ? "✔" : "✗"}</span>{" "}
              {c.name}
            </li>
          ))}
        </ul>
        <div className="text-xs text-text-secondary mt-2">
          漲停家數佔比對 90 日均線的乖離 z ≥ +1.0σ 時亮（散戶投機過熱）。
          <span className="text-text-primary">★與外資／融資完全正交、時間均勻（9 年 6–7 年有效）、抓到 4/5 崩盤峰（含 2024-07／2026-02，僅漏 2022 慢熊）</span>，lift ~1.5x，仍屬過半假的參考燈。
        </div>
      </div>

      {/* 頂部過熱·漲停配合空方排列（高信心頂：漲停過熱 + 排列翻空） */}
      <div className={"mb-6 rounded border p-3 " + (data.limitup_bear_alert ? "border-[#38bdf8] bg-[#38bdf8]/10" : "border-border")}>
        <div className="flex items-center gap-2 mb-2">
          <span className={"px-2 py-0.5 rounded text-xs font-semibold " +
            (data.limitup_bear_alert ? "bg-[#38bdf8] text-black" : "bg-surface-hover text-text-secondary")}>
            {data.limitup_bear_alert ? "⚠ 漲停過熱＋排列翻空" : "未觸發"}
          </span>
          <span className="text-xs text-text-secondary">
            高信心頂部訊號　{(data.limitup_bear_conditions ?? []).filter((c) => c.met).length}/{(data.limitup_bear_conditions ?? []).length} 條件
          </span>
        </div>
        <ul className="text-xs space-y-1">
          {(data.limitup_bear_conditions ?? []).map((c) => (
            <li key={c.name} className={c.met ? "text-text-primary" : "text-text-secondary"}>
              <span className={c.met ? "text-[#38bdf8]" : "text-text-secondary/50"}>{c.met ? "✔" : "✗"}</span>{" "}
              {c.name}
            </li>
          ))}
        </ul>
        <div className="text-xs text-text-secondary mt-2">
          漲停過熱 AND 排列翻空（short 排列＜0）同時成立才亮＝漲停 froth 出現時廣度已滾落＝出貨型頂部。
          <span className="text-text-primary">★用空方排列過濾漲停燈：亮燈後接大回檔的比例從 24% 升到 41%，且五次崩盤全覆蓋</span>，比單看漲停乾淨許多。
        </div>
      </div>

      {/* 高信念頂部：四正交過熱訊號投票（>=3 of 4） */}
      <div className={"mb-6 rounded border p-3 " + (data.top_vote ? "border-[#f43f5e] bg-[#f43f5e]/10" : "border-border")}>
        <div className="flex items-center gap-2 mb-2">
          <span className={"px-2 py-0.5 rounded text-xs font-semibold " +
            (data.top_vote ? "bg-[#f43f5e] text-white" : "bg-surface-hover text-text-secondary")}>
            {data.top_vote ? "⚠ 高信念頂部" : "未觸發"}
          </span>
          <span className="text-xs text-text-secondary">
            四正交過熱訊號投票　{data.top_vote_n ?? (data.top_vote_conditions ?? []).filter((c) => c.met).length}/4（≥{data.top_vote_k ?? 3} 亮燈）
          </span>
        </div>
        <ul className="text-xs space-y-1">
          {(data.top_vote_conditions ?? []).map((c) => (
            <li key={c.name} className={c.met ? "text-text-primary" : "text-text-secondary"}>
              <span className={c.met ? "text-[#f43f5e]" : "text-text-secondary/50"}>{c.met ? "✔" : "✗"}</span>{" "}
              {c.name}
            </li>
          ))}
        </ul>
        <div className="text-xs text-text-secondary mt-2">
          外資期貨、外資選擇權、漲停、融資 四個互相正交的過熱面向，≥3 個同時繃緊才亮。
          <span className="text-text-primary">★近高日接 ≥8–10% 有感回檔的比例 12.7%→約 59%（4.6x），剔除 2024 單一事件後仍 3.6x、前後半皆成立、抓到 2021／24／26 三波頂；243 組門檻擾動全過穩健檢驗。</span>
          {" "}但屬<span className="text-text-primary">高信念、低 recall</span>：只針對有感回檔（非最深崩盤），且結構上會漏 2020 COVID（外生無堆積）與 2022 慢熊。加碼確認用，不保證見頂。
        </div>
      </div>

      {/* 加速惡化警示（頂部過熱 + 排列未翻空 + 短空頭排列急升，補攻防時滯） */}
      <div className={"mb-6 rounded border p-3 " + (data.warn ? "border-[#f97316] bg-[#f97316]/10" : "border-border")}>
        <div className="flex items-center gap-2 mb-2">
          <span className={"px-2 py-0.5 rounded text-xs font-semibold " +
            (data.warn ? "bg-[#f97316] text-black" : "bg-surface-hover text-text-secondary")}>
            {data.warn ? "⚠ 加速惡化警示" : "未觸發"}
          </span>
          <span className="text-xs text-text-secondary">
            早於攻防的預警　{(data.warn_conditions ?? []).filter((c) => c.met).length}/{(data.warn_conditions ?? []).length} 條件
          </span>
        </div>
        <ul className="text-xs space-y-1">
          {(data.warn_conditions ?? []).map((c) => (
            <li key={c.name} className={c.met ? "text-text-primary" : "text-text-secondary"}>
              <span className={c.met ? "text-[#f97316]" : "text-text-secondary/50"}>{c.met ? "✔" : "✗"}</span>{" "}
              {c.name}
            </li>
          ))}
        </ul>
        <div className="text-xs text-text-secondary mt-2">
          三條件全亮才觸發：頂部過熱燈亮、short 排列還沒翻空（stance 仍攻擊）、但短空頭排列 3 日急升 ≥ +6pp。
          <span className="text-text-primary">★攻防要等排列翻空才轉防守，這個在翻空前就先示警</span>（抓到 2025-02／2026-02 的時滯回檔），但約半數為假，僅供提早警覺。
        </div>
      </div>

      {/* 恐慌買進訊號（V 底 contrarian，快崩限定） */}
      <div className={"mb-6 rounded border p-3 " + (data.panic ? "border-[#facc15] bg-[#facc15]/10" : "border-border")}>
        <div className="flex items-center gap-2 mb-2">
          <span className={"px-2 py-0.5 rounded text-xs font-semibold " +
            (data.panic ? "bg-[#facc15] text-black" : "bg-surface-hover text-text-secondary")}>
            {data.panic ? "恐慌買進" : "未觸發"}
          </span>
          <span className="text-xs text-text-secondary">
            恐慌買進訊號　{data.panic_conditions.filter((c) => c.met).length}/{data.panic_conditions.length} 條件
          </span>
        </div>
        <ul className="text-xs space-y-1">
          {data.panic_conditions.map((c) => (
            <li key={c.name} className={c.met ? "text-text-primary" : "text-text-secondary"}>
              <span className={c.met ? "text-[#facc15]" : "text-text-secondary/50"}>{c.met ? "✔" : "✗"}</span>{" "}
              {c.name}
            </li>
          ))}
        </ul>
        <div className="text-xs text-text-secondary mt-2">
          深跌＋融資斷頭急殺＋快殺＝V 底反彈設定（歷史未來 60 日 +9%／勝 71%）。
          <span className="text-text-primary">★仍以快崩 V 轉為主；慢熊（2018/2022）已大幅過濾但未全消</span>，別盲買。
        </div>
      </div>

      {/* components */}
      <div className="space-y-3 mb-6">
        <div className="text-sm font-semibold">組件明細</div>
        {data.components.map((c) => (
          <div key={c.key}>
            <div className="flex items-center justify-between text-sm">
              <span className="flex items-center gap-1.5">
                <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: COMP_COLOR[c.key] ?? color }} />
                {c.name}
              </span>
              <span className="tabular-nums font-medium" style={{ color: COMP_COLOR[c.key] ?? color }}>{c.hot}</span>
            </div>
            <MiniChart pts={data.history} ck={c.key} color={COMP_COLOR[c.key] ?? color} />
            <div className="text-xs text-text-secondary mt-0.5">{c.detail}</div>
          </div>
        ))}
      </div>

      </details>
     </div>

      {/* history — full width (max-w-5xl) so the sparkline reads wider */}
      <div>
        <div className="text-sm font-semibold mb-1">近一年 四正交過熱投票 vs 加權指數（投票 ≥3＝高信念頂部，觸發高點防守；指數線 綠＝攻擊、紅＝防守）</div>
        <TrendCharts pts={data.history} />
      </div>

     <div className="max-w-3xl">
      <details className="text-xs text-text-secondary mt-6">
        <summary className="cursor-pointer hover:text-text-primary">方法與限制</summary>
        <ul className="mt-2 space-y-1 list-disc pl-4">
          <li>外資期貨定位：外資臺股期貨淨未平倉在近 78 日的百分位（越淨空越熱）。此為唯一撐過公平偽訊號測試的組件（1.47x）。</li>
          <li>融資水位：融資餘額金額對 55 日均線的乖離（Bollinger z，−2σ→0、均線→50、+2σ→100）。衡量偏離趨勢多少，不受長多水位長期偏高影響。</li>
          <li>定位極端度＝上述兩組件等權平均。★這是狀態描述非計時訊號——歷史上偏熱也常不崩（如 2020 COVID 為外生衝擊，儀表當時僅中性）。</li>
          <li>P/C 比與微台散戶已移除：兩者在頂部與底部皆極端（反指標、無方向鑑別力），平均進來只會稀釋分數。真正的操作訊號在儀表之外——頂部過熱看外資期貨創新低、攻防看 OBV＋多空排列、恐慌買進看融資急殺＋深跌。</li>
          <li>攻防進出場（防守＝高點＋向下趨勢；攻擊＝底部＋向上趨勢）：
            <span className="text-text-primary">① 高點防守</span>——高信念頂部（四正交過熱投票 ≥3）一亮就轉防守，<span className="text-text-primary">不必死等排列翻空</span>，進場記下當日指數＝頂位，用價格論點持有：只要指數仍在頂位之下（下跌中）就續守，一旦漲回頂位之上（向上趨勢）才轉攻。這條讓過熱頂（如 2021／2024）提早 17-18 天在高點就防守，危險近高的防守比例 12%→51%。
            <span className="text-text-primary">② 向下趨勢防守</span>——非過熱型下跌（COVID、慢熊，投票抓不到）靠：大台 OBV 轉弱或外資淨空創新低，且短期多空排列翻空 → 轉防守；排列回多方／連續 3 天中性以上 → 轉攻。
            <span className="text-text-primary">③ 底部提早轉攻</span>——外資期貨淨多創 60 日新高且空頭排列升速放緩（占比二階差翻負＝恐慌力竭）時先轉攻，避開 2022 磨熊／2025 關稅崩兩次接刀且保住 2020-03 COVID 底；轉攻後外資淨多仍在近高帶內就維持攻擊（消閃爍）。
            採用驗證：mh=4-8 為 plateau、逐年均勻、long-on-attack 累報酬 +145%→+195% 而最大回撤持平。</li>
        </ul>
      </details>
     </div>
    </div>
  );
}
