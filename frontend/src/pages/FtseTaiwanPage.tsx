import { useEffect, useState } from "react";

interface TxfBlock {
  txf_now:         number | null;
  txf_pct_change:  number | null;
  txf_captured_at: string | null;
}

interface FtseLatest {
  front_contract:    string | null;
  ftse_now:          number | null;
  ftse_base:         number | null;
  pct_change:        number | null;
  taiex_ref_date:    string | null;
  taiex_ref_close:   number | null;
  theoretical_taiex: number | null;
  captured_at:       string | null;
  txf:               TxfBlock | null;
}

interface FtseData {
  latest_date: string | null;
  latest:      FtseLatest | null;
}

// 台股慣例：漲紅、跌綠。
function pctColor(pct: number | null): string {
  if (pct === null) return "text-text-primary";
  if (pct > 0) return "text-red-400";
  if (pct < 0) return "text-emerald-400";
  return "text-text-primary";
}

function fmtPct(pct: number | null): string {
  if (pct === null) return "—";
  return `${pct >= 0 ? "+" : ""}${(pct * 100).toFixed(2)}%`;
}

function fmtNum(v: number | null, dp = 2): string {
  return v === null ? "—" : v.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });
}

// 擷取時間 ISO → 本地 MM/DD HH:MM
function fmtCaptured(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "—";
  return d.toLocaleString("zh-TW", {
    month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false,
  });
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[10px] text-text-secondary">{label}</span>
      <span className="text-sm text-text-primary tabular-nums">{value}</span>
    </div>
  );
}

export default function FtseTaiwanPage() {
  const [data, setData] = useState<FtseData | null>(null);

  useEffect(() => {
    fetch("/data/ftse_taiwan.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary text-sm">載入中…</div>;

  const l = data.latest;
  const txf = l?.txf ?? null;

  // 兩來源估開盤的差距（富台理論 − 台指期夜盤）
  const gap =
    l?.theoretical_taiex != null && txf?.txf_now != null
      ? l.theoretical_taiex - txf.txf_now
      : null;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">夜盤參考 → 估 TAIEX 開盤</h1>
        <span className="text-xs text-text-secondary">
          富台（SGX）/ 台指期夜盤（TAIFEX）　·　更新：{data.latest_date ?? "—"}
        </span>
      </div>

      <div className="bg-surface-alt border border-border rounded p-3 text-[11px] text-text-secondary leading-5">
        台股盤後/放假時，用兩個仍在交易的夜盤期貨估計重新開盤時 TAIEX 大約落點：
        <span className="text-text-primary"> 富台指數（SGX FTSE Taiwan，舊稱摩台）</span>換算理論 TAIEX，
        與<span className="text-text-primary"> 台指期大台 TXF 夜盤</span>（本身即 TAIEX 點數）對照。
        資料源 鉅亨網，約延遲 15 分鐘；兩者與 TAIEX 成分/結算不同，僅供方向參考。
        <span className="text-orange-400"> 注意：台指期（TAIFEX）在台灣放假時整個休市，連假期間只有富台（SGX）會更新。</span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {/* 富台 → 理論 TAIEX */}
        {l ? (
          <div className="bg-surface-alt border border-border rounded p-3 space-y-3">
            <div className="flex flex-wrap items-baseline gap-2">
              <h2 className="text-sm font-bold text-text-primary">理論 TAIEX（富台 SGX）</h2>
              <span className="text-[10px] text-text-secondary">
                {l.front_contract ?? "—"}　·　擷取 {fmtCaptured(l.captured_at)}
              </span>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <div className={`text-4xl font-bold ${pctColor(l.pct_change)}`}>
                {fmtNum(l.theoretical_taiex)}
              </div>
              <span className={`px-2 py-0.5 rounded text-xs font-medium ${pctColor(l.pct_change)} bg-surface-hover`}>
                {fmtPct(l.pct_change)}
              </span>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <Stat label="富台現價（含夜盤）" value={fmtNum(l.ftse_now)} />
              <Stat label="富台基準（台股收盤）" value={fmtNum(l.ftse_base)} />
            </div>
          </div>
        ) : (
          <div className="bg-surface-alt border border-border rounded p-3 text-sm text-text-secondary">
            尚無富台資料。
          </div>
        )}

        {/* 台指期夜盤 */}
        {txf && txf.txf_now != null ? (
          <div className="bg-surface-alt border border-border rounded p-3 space-y-3">
            <div className="flex flex-wrap items-baseline gap-2">
              <h2 className="text-sm font-bold text-text-primary">台指期夜盤（TXF TAIFEX）</h2>
              <span className="text-[10px] text-text-secondary">
                大台連續月　·　擷取 {fmtCaptured(txf.txf_captured_at)}
              </span>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <div className={`text-4xl font-bold ${pctColor(txf.txf_pct_change)}`}>
                {fmtNum(txf.txf_now)}
              </div>
              <span className={`px-2 py-0.5 rounded text-xs font-medium ${pctColor(txf.txf_pct_change)} bg-surface-hover`}>
                {fmtPct(txf.txf_pct_change)}
              </span>
            </div>
            <div className="text-[10px] text-text-secondary">
              台指期本身即 TAIEX 點數，無需換算；放假休市時此值停滯。
            </div>
          </div>
        ) : (
          <div className="bg-surface-alt border border-border rounded p-3 space-y-2">
            <h2 className="text-sm font-bold text-text-primary">台指期夜盤（TXF TAIFEX）</h2>
            <div className="text-sm text-text-secondary">目前無台指期夜盤資料（可能休市）。</div>
          </div>
        )}
      </div>

      {/* 對照基準與差距 */}
      <div className="bg-surface-alt border border-border rounded p-3 grid grid-cols-2 sm:grid-cols-3 gap-3">
        <Stat label="台股最後收盤 TAIEX" value={fmtNum(l?.taiex_ref_close ?? null)} />
        <Stat label="參考日" value={l?.taiex_ref_date ?? "—"} />
        <Stat
          label="兩來源差距（富台 − 台指期）"
          value={gap === null ? "—" : `${gap >= 0 ? "+" : ""}${fmtNum(gap)} 點`}
        />
      </div>
    </div>
  );
}
