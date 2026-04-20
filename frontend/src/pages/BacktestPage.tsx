import { useEffect, useState } from "react";

interface Trade {
  ticker: string;
  ticker_name: string;
  entry_signal: string;
  entry_period: string;
  entry_date: string;
  entry_price: number;
  exit_signal: string | null;
  exit_period: string | null;
  exit_date: string | null;
  exit_price: number | null;
  return_pct: number | null;
  holding_days: number | null;
}

interface BacktestData {
  metrics: Record<string, number>;
  entry_breakdown: Record<string, { trades: number; win_rate: number; avg_return: number }>;
  trades: Trade[];
}

const SIGNAL_LABELS: Record<string, string> = {
  quarterly_to_monthly_top10: "季報→月報晉升",
  quarterly_dormant_etf_active: "季報潛伏+ETF激活",
  dual_track_entry: "雙軌建倉",
  multi_fund_consensus: "多基金共識",
  consecutive_accumulation: "連續加碼",
  consensus_formation: "共識形成",
  heavy_position_reduction: "高權重減碼",
  core_exit: "核心出場",
};

function MetricCard({ label, value, fmt }: { label: string; value: number | undefined; fmt: string }) {
  if (value == null) return null;
  let display: string;
  if (fmt === "pct") display = `${(value * 100).toFixed(1)}%`;
  else if (fmt === "pct2") display = `${(value * 100).toFixed(2)}%`;
  else if (fmt === "f2") display = value.toFixed(2);
  else if (fmt === "f4") display = value.toFixed(4);
  else display = String(Math.round(value));

  return (
    <div className="bg-surface-alt rounded-lg px-4 py-3 border border-border">
      <div className="text-xs text-text-secondary mb-1">{label}</div>
      <div className="text-lg font-semibold font-mono">{display}</div>
    </div>
  );
}

export default function BacktestPage() {
  const [data, setData] = useState<BacktestData | null>(null);

  useEffect(() => {
    fetch("/data/backtest.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const m = data.metrics;

  return (
    <div>
      <h2 className="text-lg font-semibold mb-4">策略績效</h2>

      {/* Metric cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3 mb-6">
        <MetricCard label="交易數" value={m.total_trades} fmt="int" />
        <MetricCard label="勝率" value={m.win_rate} fmt="pct" />
        <MetricCard label="平均報酬" value={m.avg_return} fmt="pct2" />
        <MetricCard label="平均持有天數" value={m.avg_holding_days} fmt="int" />
        <MetricCard label="最大回撤" value={m.max_drawdown} fmt="pct2" />
        <MetricCard label="Sharpe" value={m.sharpe_ratio} fmt="f2" />
        <MetricCard label="Sortino" value={m.sortino_ratio} fmt="f2" />
        <MetricCard label="Alpha" value={m.alpha} fmt="f4" />
        <MetricCard label="Beta" value={m.beta} fmt="f2" />
        <MetricCard label="IR" value={m.information_ratio} fmt="f2" />
        <MetricCard label="Kelly" value={m.kelly_criterion} fmt="pct" />
      </div>

      {/* Entry signal breakdown */}
      <h3 className="text-sm font-semibold mb-2 text-text-secondary">按進場信號分類</h3>
      <div className="overflow-x-auto mb-6">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="py-2 pr-4 font-medium">信號</th>
              <th className="py-2 pr-4 font-medium text-right">交易數</th>
              <th className="py-2 pr-4 font-medium text-right">勝率</th>
              <th className="py-2 pr-4 font-medium text-right">平均報酬</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(data.entry_breakdown).map(([sig, d]) => (
              <tr key={sig} className="border-b border-border/50">
                <td className="py-2 pr-4">{SIGNAL_LABELS[sig] || sig}</td>
                <td className="py-2 pr-4 text-right font-mono">{d.trades}</td>
                <td className="py-2 pr-4 text-right font-mono">
                  {(d.win_rate * 100).toFixed(1)}%
                </td>
                <td className={`py-2 pr-4 text-right font-mono ${d.avg_return > 0 ? "text-positive" : "text-negative"}`}>
                  {(d.avg_return * 100).toFixed(2)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Trade list */}
      <h3 className="text-sm font-semibold mb-2 text-text-secondary">交易明細</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="py-1.5 pr-3 font-medium">代號</th>
              <th className="py-1.5 pr-3 font-medium">名稱</th>
              <th className="py-1.5 pr-3 font-medium">進場信號</th>
              <th className="py-1.5 pr-3 font-medium">買入日</th>
              <th className="py-1.5 pr-3 font-medium text-right">買價</th>
              <th className="py-1.5 pr-3 font-medium">出場信號</th>
              <th className="py-1.5 pr-3 font-medium">賣出日</th>
              <th className="py-1.5 pr-3 font-medium text-right">賣價</th>
              <th className="py-1.5 pr-3 font-medium text-right">報酬</th>
              <th className="py-1.5 pr-3 font-medium text-right">天數</th>
            </tr>
          </thead>
          <tbody>
            {data.trades.map((t, i) => {
              const ret = t.return_pct != null ? t.return_pct * 100 : null;
              return (
                <tr
                  key={i}
                  className="border-b border-border/30 hover:bg-surface-hover transition-colors"
                >
                  <td className="py-1.5 pr-3 font-mono">{t.ticker}</td>
                  <td className="py-1.5 pr-3">{t.ticker_name}</td>
                  <td className="py-1.5 pr-3 text-text-secondary">
                    {SIGNAL_LABELS[t.entry_signal] || t.entry_signal}
                  </td>
                  <td className="py-1.5 pr-3 font-mono">{t.entry_date}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">
                    {t.entry_price?.toFixed(1)}
                  </td>
                  <td className="py-1.5 pr-3 text-text-secondary">
                    {t.exit_signal ? SIGNAL_LABELS[t.exit_signal] || t.exit_signal : "—"}
                  </td>
                  <td className="py-1.5 pr-3 font-mono">{t.exit_date || "—"}</td>
                  <td className="py-1.5 pr-3 text-right font-mono">
                    {t.exit_price?.toFixed(1) || "—"}
                  </td>
                  <td className={`py-1.5 pr-3 text-right font-mono ${
                    ret != null ? (ret > 0 ? "text-positive" : "text-negative") : ""
                  }`}>
                    {ret != null ? `${ret > 0 ? "+" : ""}${ret.toFixed(1)}%` : "—"}
                  </td>
                  <td className="py-1.5 pr-3 text-right font-mono">
                    {t.holding_days ?? "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
