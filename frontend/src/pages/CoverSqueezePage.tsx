import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

interface Candidate {
  rank: number;
  ticker: string;
  name: string | null;
  market: string | null;
  last_cover_date: string;
  meeting_date: string | null;
  days_to_cover_date: number;
  short_balance: number;
  avg_volume: number;
  dtc: number;
  close: number | null;
  is_strong: boolean;
  position_state: "long" | "short" | "exited_long" | "exited_short" | "flat" | "na";
}

// Technical-side (統一策略持倉) operation state badge. 台股慣例：多=紅、空=綠。
const POS_BADGE: Record<Candidate["position_state"], { label: string; cls: string }> = {
  long: { label: "多方持倉", cls: "text-long-strong font-medium" },
  short: { label: "空方持倉", cls: "text-short-strong font-medium" },
  exited_long: { label: "多方出場", cls: "text-long-strong/55" },
  exited_short: { label: "空方出場", cls: "text-short-strong/55" },
  flat: { label: "空手", cls: "text-text-secondary/60" },
  na: { label: "—", cls: "text-text-secondary/40" },
};

interface Params {
  window_td: number;
  vol_win_days: number;
  stop_pct: number;
  floor_dtc: number;
  strong_dtc: number;
}

interface SqueezeData {
  as_of: string | null;
  window_end: string | null;
  off_season: boolean;
  candidates: Candidate[];
  next_cover_date: string | null;
  params: Params;
}

function tvUrl(market: string | null, ticker: string): string {
  const prefix = market === "TWSE" ? "TWSE" : "TPEX";
  return `https://tw.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

export default function CoverSqueezePage() {
  const [data, setData] = useState<SqueezeData | null>(null);

  useEffect(() => {
    fetch("/data/cover_squeeze.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const p = data.params;

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">股東會軋空回補</h2>
      <p className="text-xs text-text-secondary mb-2">
        重融券股票須在股東會停止過戶前強制回補，高融券回補比（days-to-cover）個股在回補日前
        約 {p.window_td} 個交易日軋空上漲。回補日約在股東會前兩個月，故旺季為 2–4 月；
        淡季本清單為空。僅列 dtc ≥ {p.floor_dtc}（實證 edge 地板）、以 dtc 排序，
        ★＝dtc ≥ {p.strong_dtc}（實證最強子集）。
      </p>
      <details className="text-xs text-text-secondary mb-4">
        <summary className="cursor-pointer hover:text-text-primary">操作規則與資料說明</summary>
        <ul className="mt-2 space-y-1 list-disc pl-4">
          <li>dtc（融券回補比）＝融券今日餘額 ÷ 近 {p.vol_win_days} 交易日均量；僅計融券（借券不受股東會強制回補約束）。</li>
          <li>進場窗：回補日前約 {p.window_td} 個交易日內；出場：回補日，或觸及 {p.stop_pct}% 保護性停損。</li>
          <li>
            歷史績效（扣 0.4% 成本）：dtc ≥ {p.floor_dtc} 淨 PF 1.58／均 +1.14%；
            dtc ≥ {p.strong_dtc} PF 1.70／+1.26%；
            加 {p.stop_pct}% 停損後 dtc ≥ {p.strong_dtc} PF 1.80／maxL −17.5%。dtc 低於地板者實測為負，不列入。
          </li>
          <li>操作狀態：該股在統一策略（技術面）當日的持倉狀態——多方持倉／空方持倉／今日出場／空手。軋空候選同時為多方持倉＝技術面確認。持倉追蹤自 2026-04-28 起，之前的日期顯示「—」。</li>
          <li>來源：TWSE BFI84U 停券預告表 + TPEx term 前瞻板（每日刷新）＋股東會日期衍生回補日。屬綜合型 overlay，非純技術訊號。</li>
        </ul>
      </details>

      {data.off_season || !data.candidates.length ? (
        <div className="text-text-secondary text-sm border border-border rounded p-4">
          目前無進行中的股東會回補窗（淡季）。
          {data.next_cover_date && (
            <> 下一個已知回補日：<span className="font-medium">{data.next_cover_date}</span>。</>
          )}
          {data.as_of && <div className="text-xs mt-1">資料日：{data.as_of}</div>}
        </div>
      ) : (
        <>
          <div className="text-xs text-text-secondary mb-2">
            資料日 {data.as_of} · 窗內候選 {data.candidates.length} 檔
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="py-2 pr-3 font-medium">排名</th>
                  <th className="py-2 pr-3 font-medium">代號</th>
                  <th className="py-2 pr-3 font-medium">名稱</th>
                  <th className="py-2 pr-3 font-medium">操作狀態</th>
                  <th className="py-2 pr-3 font-medium text-right">dtc</th>
                  <th className="py-2 pr-3 font-medium">回補日</th>
                  <th className="py-2 pr-3 font-medium text-right">距回補</th>
                  <th className="py-2 pr-3 font-medium text-right">融券餘額</th>
                  <th className="py-2 pr-3 font-medium text-right">收盤</th>
                </tr>
              </thead>
              <tbody>
                {data.candidates.map((c) => (
                  <tr key={c.ticker} className="border-b border-border/50 hover:bg-surface-hover">
                    <td className="py-2 pr-3 text-text-secondary">
                      {c.is_strong && <span className="text-accent mr-0.5">★</span>}
                      {c.rank}
                    </td>
                    <td className="py-2 pr-3 font-medium">
                      <a href={tvUrl(c.market, c.ticker)} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
                        {c.ticker}
                      </a>
                    </td>
                    <td className="py-2 pr-3">
                      <Link to={`/stock/${c.ticker}`} className="hover:underline">{c.name ?? "—"}</Link>
                    </td>
                    <td className={"py-2 pr-3 " + POS_BADGE[c.position_state].cls}>
                      {POS_BADGE[c.position_state].label}
                    </td>
                    <td className="py-2 pr-3 text-right font-medium">{c.dtc.toFixed(3)}</td>
                    <td className="py-2 pr-3">{c.last_cover_date}</td>
                    <td className="py-2 pr-3 text-right text-text-secondary">{c.days_to_cover_date}d</td>
                    <td className="py-2 pr-3 text-right text-text-secondary">{c.short_balance.toLocaleString()}</td>
                    <td className="py-2 pr-3 text-right">{c.close ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
