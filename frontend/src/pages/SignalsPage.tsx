import { useEffect, useState } from "react";

interface Signal {
  ticker: string;
  ticker_name: string;
  funds: string[];
  trigger_date: string;
  trigger_period: string;
  weight_change: number | null;
  evidence: Record<string, unknown>;
}

interface SignalsData {
  by_type: Record<string, Signal[]>;
  periods: string[];
}

const SIGNAL_LABELS: Record<string, string> = {
  quarterly_to_monthly_top10: "季報→月報晉升",
  quarterly_dormant_etf_active: "季報潛伏+ETF激活",
  dual_track_entry: "雙軌建倉",
  multi_fund_consensus: "多基金共識",
  consecutive_accumulation: "連續加碼",
  dual_track_accumulation: "雙軌加碼中",
  consensus_formation: "共識形成",
  heavy_position_reduction: "高權重減碼",
  core_exit: "核心出場",
};

const PHASE_COLORS: Record<string, string> = {
  quarterly_to_monthly_top10: "text-positive",
  quarterly_dormant_etf_active: "text-positive",
  dual_track_entry: "text-positive",
  multi_fund_consensus: "text-accent",
  consecutive_accumulation: "text-accent",
  dual_track_accumulation: "text-accent",
  consensus_formation: "text-accent",
  heavy_position_reduction: "text-warning",
  core_exit: "text-negative",
};

export default function SignalsPage() {
  const [data, setData] = useState<SignalsData | null>(null);
  const [activeType, setActiveType] = useState<string>("all");
  const [periodFilter, setPeriodFilter] = useState<string>("all");

  useEffect(() => {
    fetch("/data/signals.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const types = Object.keys(data.by_type);
  const allSignals = activeType === "all"
    ? types.flatMap((t) => data.by_type[t].map((s) => ({ ...s, _type: t })))
    : (data.by_type[activeType] || []).map((s) => ({ ...s, _type: activeType }));

  const filtered = periodFilter === "all"
    ? allSignals
    : allSignals.filter((s) => s.trigger_period === periodFilter);

  // Sort: newest period first, then by ticker
  filtered.sort((a, b) => b.trigger_period.localeCompare(a.trigger_period) || a.ticker.localeCompare(b.ticker));

  return (
    <div>
      <h2 className="text-lg font-semibold mb-4">訊號總覽</h2>

      {/* Signal type tabs */}
      <div className="flex flex-wrap gap-1.5 mb-4">
        <button
          onClick={() => setActiveType("all")}
          className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
            activeType === "all"
              ? "bg-accent text-white"
              : "bg-surface-alt text-text-secondary hover:text-text-primary"
          }`}
        >
          全部 ({allSignals.length})
        </button>
        {types.map((t) => (
          <button
            key={t}
            onClick={() => setActiveType(t)}
            className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
              activeType === t
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:text-text-primary"
            }`}
          >
            {SIGNAL_LABELS[t] || t} ({data.by_type[t].length})
          </button>
        ))}
      </div>

      {/* Period filter */}
      <div className="mb-4">
        <select
          value={periodFilter}
          onChange={(e) => setPeriodFilter(e.target.value)}
          className="bg-surface-alt border border-border rounded px-3 py-1.5 text-sm text-text-primary"
        >
          <option value="all">全部期間</option>
          {data.periods.map((p) => (
            <option key={p} value={p}>{p}</option>
          ))}
        </select>
      </div>

      {/* Signal table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="py-2 pr-4 font-medium">代號</th>
              <th className="py-2 pr-4 font-medium">名稱</th>
              <th className="py-2 pr-4 font-medium">信號</th>
              <th className="py-2 pr-4 font-medium">期間</th>
              <th className="py-2 pr-4 font-medium text-right">權重變化</th>
              <th className="py-2 pr-4 font-medium">涉及基金</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((s, i) => (
              <tr
                key={`${s.ticker}-${s._type}-${s.trigger_period}-${i}`}
                className="border-b border-border/50 hover:bg-surface-hover transition-colors"
              >
                <td className="py-2 pr-4 font-mono">{s.ticker}</td>
                <td className="py-2 pr-4">{s.ticker_name}</td>
                <td className={`py-2 pr-4 ${PHASE_COLORS[s._type] || ""}`}>
                  {SIGNAL_LABELS[s._type] || s._type}
                </td>
                <td className="py-2 pr-4 font-mono text-text-secondary">
                  {s.trigger_period}
                </td>
                <td className="py-2 pr-4 text-right font-mono">
                  {s.weight_change != null ? (
                    <span className={s.weight_change > 0 ? "text-positive" : "text-negative"}>
                      {s.weight_change > 0 ? "+" : ""}
                      {s.weight_change.toFixed(2)}%
                    </span>
                  ) : (
                    <span className="text-text-secondary">—</span>
                  )}
                </td>
                <td className="py-2 pr-4 text-text-secondary text-xs">
                  {s.funds.slice(0, 3).join(", ")}
                  {s.funds.length > 3 && ` +${s.funds.length - 3}`}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {filtered.length === 0 && (
        <p className="text-text-secondary text-center py-8">
          無符合條件的訊號
        </p>
      )}
    </div>
  );
}
