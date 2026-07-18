import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

interface PledgeEvent {
  stock_id: string;
  company_name: string | null;
  insider_role: string | null;
  insider_name: string | null;
  change_date: string | null;
  pledged_shares: number;
  released_shares: number;
  cumulative_pledged: number | null;
  pledgee_name: string | null;
  remark: string | null;
  report_date: string | null;
  type: "pledge" | "release" | "mixed";
}

interface PledgeStats {
  release_events_30d: number;
  release_stocks_30d: number;
  pledge_events_30d: number;
}

interface PledgeData {
  updated_at: string | null;
  events: PledgeEvent[];
  stats: PledgeStats | null;
}

// 身份別欄含 MOPS 代碼前綴 (如 A00020董事長本人)；去掉代碼只留中文職稱。
function cleanRole(role: string | null): string {
  if (!role) return "—";
  return role.replace(/^[^一-鿿]+/, "").trim() || role;
}

// 內部人設質解質事件為「股」為單位；台股習慣以「張」(=1000股) 呈現。
function fmtLots(shares: number): string {
  if (!shares) return "—";
  const lots = shares / 1000;
  if (Math.abs(lots) >= 1e4) return `${(lots / 1e4).toFixed(1)}萬張`;
  return `${lots.toLocaleString(undefined, { maximumFractionDigits: 0 })}張`;
}

type FilterKey = "pledge" | "release";

const FILTER_META: { key: FilterKey; label: string; test: (e: PledgeEvent) => boolean }[] = [
  { key: "pledge",  label: "設質", test: (e) => e.pledged_shares > 0 },
  { key: "release", label: "解質", test: (e) => e.released_shares > 0 },
];

export default function InsiderPledgePage() {
  const [data, setData] = useState<PledgeData | null>(null);
  const [filter, setFilter] = useState<FilterKey>("pledge");

  useEffect(() => {
    fetch("/data/insider_pledge.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) {
    return <div className="text-text-secondary text-sm">載入中…</div>;
  }
  if (!data.events.length) {
    return (
      <div className="space-y-4">
        <h1 className="text-lg font-bold text-text-primary">內部人設質解質</h1>
        <div className="text-text-secondary text-sm">近 180 天尚無設質/解質事件資料。</div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">內部人設質解質</h1>
        {data.updated_at && (
          <span className="text-xs text-text-secondary">更新：{data.updated_at}</span>
        )}
      </div>
      <p className="text-xs text-text-secondary">
        來自公開資訊觀測站「內部人設質解質公告」，事件粒度（每筆質押異動一列）。
        解質＝內部人贖回質押股票（常見於還款或釋出信心）；設質＝以持股向銀行質借。
        近 180 天事件，依異動日期排序。
      </p>

      {data.stats && (
        <div className="grid grid-cols-3 gap-3">
          <StatCard label="近 30 日解質事件" value={data.stats.release_events_30d} />
          <StatCard label="近 30 日解質檔數" value={data.stats.release_stocks_30d} />
          <StatCard label="近 30 日設質事件" value={data.stats.pledge_events_30d} />
        </div>
      )}

      <div className="flex gap-2">
        {FILTER_META.map((f) => {
          const n = data.events.filter(f.test).length;
          const active = filter === f.key;
          return (
            <button
              key={f.key}
              onClick={() => setFilter(f.key)}
              className={`px-3 py-1 rounded text-xs font-medium border transition-colors ${
                active
                  ? "bg-accent text-white border-accent"
                  : "bg-surface-alt text-text-secondary border-border hover:bg-surface-hover"
              }`}
            >
              {f.label}
              <span className={`ml-1 ${active ? "text-white/80" : "text-text-secondary/70"}`}>
                {n}
              </span>
            </button>
          );
        })}
      </div>

      <div className="bg-surface-alt border border-border rounded overflow-x-auto">
        <table className="w-full text-xs whitespace-nowrap">
          <thead>
            <tr className="text-text-secondary border-b border-border">
              <th className="text-left  px-2 py-2">異動日</th>
              <th className="text-left  px-2 py-2">代號</th>
              <th className="text-left  px-2 py-2">名稱</th>
              <th className="text-left  px-2 py-2">身份別</th>
              <th className="text-left  px-2 py-2">姓名</th>
              {filter === "pledge" && <th className="text-right px-2 py-2">設質</th>}
              {filter === "release" && <th className="text-right px-2 py-2">解質</th>}
            </tr>
          </thead>
          <tbody>
            {data.events.filter(FILTER_META.find((f) => f.key === filter)!.test).map((e, i) => {
              return (
                <tr key={i} className="border-b border-border/50 hover:bg-surface-hover">
                  <td className="px-2 py-1.5 text-text-secondary">{e.change_date ?? "—"}</td>
                  <td className="px-2 py-1.5">
                    <Link to={`/stock/${e.stock_id}`} className="text-accent hover:underline">
                      {e.stock_id}
                    </Link>
                  </td>
                  <td className="px-2 py-1.5 text-text-primary">{e.company_name ?? "—"}</td>
                  <td className="px-2 py-1.5 text-text-secondary">{cleanRole(e.insider_role)}</td>
                  <td className="px-2 py-1.5 text-text-primary">{e.insider_name ?? "—"}</td>
                  {filter === "pledge" && (
                    <td className="px-2 py-1.5 text-right text-long-strong">
                      {e.pledged_shares ? fmtLots(e.pledged_shares) : "—"}
                    </td>
                  )}
                  {filter === "release" && (
                    <td className="px-2 py-1.5 text-right text-short-strong">
                      {e.released_shares ? fmtLots(e.released_shares) : "—"}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function StatCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="bg-surface-alt border border-border rounded p-3">
      <div className="text-xs text-text-secondary">{label}</div>
      <div className="text-base font-bold text-text-primary">{value.toLocaleString()}</div>
    </div>
  );
}
