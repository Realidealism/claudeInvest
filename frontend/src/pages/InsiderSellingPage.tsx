import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

interface SellingEvent {
  type: "transfer" | "placement";
  stock_id: string;
  company_name: string | null;
  date: string | null;
  insider_role: string | null;
  insider_name: string | null;
  method: string | null;
  shares: number | null;
  security_kind: string | null;
  period: string | null;
}

interface SellingStats {
  transfer_events_30d: number;
  transfer_stocks_30d: number;
  placement_events_30d: number;
}

interface SellingData {
  updated_at: string | null;
  events: SellingEvent[];
  stats: SellingStats | null;
}

// 身份別欄含 MOPS 代碼前綴 (如 A00020董事本人)；去掉代碼只留中文職稱。
function cleanRole(role: string | null): string {
  if (!role) return "—";
  return role.replace(/^[^一-鿿]+/, "").trim() || role;
}

// 轉讓股數以「股」為單位；台股習慣以「張」(=1000股) 呈現。
function fmtLots(shares: number | null): string {
  if (!shares) return "—";
  const lots = shares / 1000;
  if (Math.abs(lots) >= 1e4) return `${(lots / 1e4).toFixed(1)}萬張`;
  return `${lots.toLocaleString(undefined, { maximumFractionDigits: 0 })}張`;
}

type FilterKey = "transfer" | "placement";

const FILTER_META: { key: FilterKey; label: string }[] = [
  { key: "transfer",  label: "洽特定人轉讓" },
  { key: "placement", label: "私募普通股" },
];

export default function InsiderSellingPage() {
  const [data, setData] = useState<SellingData | null>(null);
  const [filter, setFilter] = useState<FilterKey>("transfer");

  useEffect(() => {
    fetch("/data/insider_selling.json")
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
        <h1 className="text-lg font-bold text-text-primary">內部人賣壓／稀釋</h1>
        <div className="text-text-secondary text-sm">近 90 天尚無洽特定人轉讓／私募普通股事件。</div>
      </div>
    );
  }

  const shown = data.events.filter((e) => e.type === filter);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">內部人賣壓／稀釋</h1>
        {data.updated_at && (
          <span className="text-xs text-text-secondary">更新：{data.updated_at}</span>
        )}
      </div>
      <p className="text-xs text-text-secondary">
        中期空方 avoid／防守 overlay，非隔日訊號。
        <b>洽特定人轉讓</b>＝內部人將整批持股私下轉讓給特定人（事前申報）；歷史上申報後 20 個交易日相對大盤約 -2.5pp、60 日約 -10pp，流動股仍成立。
        <b>私募普通股</b>＝公司對特定人折價發行新股（董事會決議日），稀釋偏空但較弱、多集中小型股。近 90 天事件，依事件日排序。
      </p>

      {data.stats && (
        <div className="grid grid-cols-3 gap-3">
          <StatCard label="近 30 日洽特定人事件" value={data.stats.transfer_events_30d} />
          <StatCard label="近 30 日洽特定人檔數" value={data.stats.transfer_stocks_30d} />
          <StatCard label="近 30 日私募普通股" value={data.stats.placement_events_30d} />
        </div>
      )}

      <div className="flex gap-2">
        {FILTER_META.map((f) => {
          const n = data.events.filter((e) => e.type === f.key).length;
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
              <th className="text-left px-2 py-2">{filter === "placement" ? "決議日" : "申報日"}</th>
              <th className="text-left px-2 py-2">代號</th>
              <th className="text-left px-2 py-2">名稱</th>
              {filter === "transfer" && <th className="text-left  px-2 py-2">身份別</th>}
              {filter === "transfer" && <th className="text-left  px-2 py-2">姓名</th>}
              {filter === "transfer" && <th className="text-right px-2 py-2">轉讓</th>}
              {filter === "placement" && <th className="text-left px-2 py-2">證券種類</th>}
            </tr>
          </thead>
          <tbody>
            {shown.map((e, i) => (
              <tr key={i} className="border-b border-border/50 hover:bg-surface-hover">
                <td className="px-2 py-1.5 text-text-secondary">{e.date ?? "—"}</td>
                <td className="px-2 py-1.5">
                  <Link to={`/stock/${e.stock_id}`} className="text-accent hover:underline">
                    {e.stock_id}
                  </Link>
                </td>
                <td className="px-2 py-1.5 text-text-primary">{e.company_name ?? "—"}</td>
                {filter === "transfer" && (
                  <td className="px-2 py-1.5 text-text-secondary">{cleanRole(e.insider_role)}</td>
                )}
                {filter === "transfer" && (
                  <td className="px-2 py-1.5 text-text-primary">{e.insider_name ?? "—"}</td>
                )}
                {filter === "transfer" && (
                  <td className="px-2 py-1.5 text-right text-short-strong">{fmtLots(e.shares)}</td>
                )}
                {filter === "placement" && (
                  <td className="px-2 py-1.5 text-short-strong">{e.security_kind ?? "—"}</td>
                )}
              </tr>
            ))}
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
