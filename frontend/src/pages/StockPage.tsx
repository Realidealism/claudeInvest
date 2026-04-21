import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import CandlestickChart from "../components/CandlestickChart";

interface FundHolding {
  code: string;
  name: string;
  weight: number;
  rank: number;
  period: string;
}

interface EtfHolding {
  etf_id: string;
  weight: number;
  trade_date: string;
}

interface Signal {
  signal_type: string;
  trigger_period: string;
  funds: string[];
  weight_change: number | null;
}

interface StockEntry {
  ticker_name: string;
  fund_holdings: FundHolding[];
  etf_holdings: EtfHolding[];
  signals: Signal[];
}

interface StocksData {
  stocks: Record<string, StockEntry>;
  latest_monthly: string;
}

interface OHLCV {
  t: string;
  o: number | null;
  h: number | null;
  l: number | null;
  c: number | null;
  v: number;
}

const SHORT_SIGNALS = new Set(["heavy_position_reduction", "core_exit", "etf_multi_exit", "etf_consecutive_reduction", "etf_abnormal_exit"]);

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
  etf_multi_consensus: "多ETF共識",
  etf_consecutive_accumulation: "ETF連續加碼",
  etf_abnormal_position: "ETF異常建倉",
  etf_multi_exit: "多ETF共識退場",
  etf_consecutive_reduction: "ETF連續減碼",
  etf_abnormal_exit: "ETF異常減倉",
};

export default function StockPage() {
  const { ticker } = useParams<{ ticker: string }>();
  const [data, setData] = useState<StocksData | null>(null);
  const [priceData, setPriceData] = useState<OHLCV[]>([]);

  useEffect(() => {
    fetch("/data/stocks.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  useEffect(() => {
    if (!ticker) return;
    fetch("/data/prices.json")
      .then((r) => r.json())
      .then((all: Record<string, OHLCV[]>) => setPriceData(all[ticker] || []));
  }, [ticker]);

  if (!data || !ticker) return <div className="text-text-secondary">Loading...</div>;

  const stock = data.stocks[ticker];
  if (!stock) return <div className="text-negative">找不到股票: {ticker}</div>;

  // Group fund holdings by period
  const byPeriod: Record<string, FundHolding[]> = {};
  for (const h of stock.fund_holdings) {
    byPeriod[h.period] = byPeriod[h.period] || [];
    byPeriod[h.period].push(h);
  }
  const periods = Object.keys(byPeriod).sort().reverse();

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">
        {ticker} {stock.ticker_name}
      </h2>
      <p className="text-xs text-text-secondary mb-4">月報最新: {data.latest_monthly}</p>

      {/* K-line chart */}
      {priceData.length > 0 && (
        <div className="bg-surface-alt border border-border rounded-lg p-4 mb-6">
          <h3 className="text-sm font-semibold mb-3 text-text-secondary">K 線走勢（近 12 個月）</h3>
          <CandlestickChart
            data={priceData}
            signals={stock.signals
              .map((s) => {
                // Convert period like "202603M" or "202603" to a date string
                const ym = s.trigger_period.replace(/M$/, "");
                const target = `${ym.slice(0, 4)}-${ym.slice(4, 6)}-15`;
                // Find nearest actual trading date
                const dates = priceData.map((p) => p.t);
                const nearest = dates.find((d) => d >= target) || dates[dates.length - 1];
                return {
                  date: nearest,
                  type: SHORT_SIGNALS.has(s.signal_type) ? "short" as const : "long" as const,
                  label: SIGNAL_LABELS[s.signal_type] || s.signal_type,
                };
              })
              .filter((s) => s.date)
            }
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Fund holdings across periods */}
        <div className="bg-surface-alt border border-border rounded-lg p-4">
          <h3 className="text-sm font-semibold mb-3 text-text-secondary">基金持有紀錄</h3>
          {periods.map((period) => (
            <div key={period} className="mb-3">
              <div className="text-xs font-mono text-text-secondary mb-1">{period}</div>
              {byPeriod[period].map((h) => (
                <div key={h.code} className="flex items-center gap-2 text-xs mb-0.5">
                  <Link to={`/fund/${h.code}`} className="w-40 truncate text-accent hover:underline">
                    {h.name}
                  </Link>
                  <div className="flex-1 h-3 bg-surface rounded overflow-hidden">
                    <div
                      className="h-full bg-accent/50 rounded"
                      style={{ width: `${Math.min((h.weight || 0) / 15 * 100, 100)}%` }}
                    />
                  </div>
                  <span className="w-14 text-right font-mono">{h.weight?.toFixed(1)}%</span>
                  <span className="w-8 text-right text-text-secondary">#{h.rank}</span>
                </div>
              ))}
            </div>
          ))}
        </div>

        {/* Signals */}
        <div>
          {stock.signals.length > 0 && (
            <div className="bg-surface-alt border border-border rounded-lg p-4 mb-4">
              <h3 className="text-sm font-semibold mb-3 text-text-secondary">觸發訊號</h3>
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border text-text-secondary">
                    <th className="py-1 pr-2 text-left font-medium">期間</th>
                    <th className="py-1 pr-2 text-left font-medium">訊號</th>
                    <th className="py-1 pr-2 text-left font-medium">涉及基金/ETF</th>
                  </tr>
                </thead>
                <tbody>
                  {stock.signals.map((s, i) => (
                    <tr key={i} className="border-b border-border/30">
                      <td className="py-1.5 pr-2 font-mono">{s.trigger_period}</td>
                      <td className="py-1.5 pr-2">{SIGNAL_LABELS[s.signal_type] || s.signal_type}</td>
                      <td className="py-1.5 pr-2 text-text-secondary">
                        {s.funds.slice(0, 3).join(", ")}
                        {s.funds.length > 3 && ` +${s.funds.length - 3}`}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* ETF holdings */}
          {stock.etf_holdings.length > 0 && (
            <div className="bg-surface-alt border border-border rounded-lg p-4">
              <h3 className="text-sm font-semibold mb-3 text-text-secondary">ETF 持有</h3>
              {stock.etf_holdings.map((h, i) => (
                <div key={i} className="flex items-center gap-2 text-xs mb-1">
                  <Link to={`/fund/${h.etf_id}`} className="w-16 font-mono text-accent hover:underline">
                    {h.etf_id}
                  </Link>
                  <span className="w-24 font-mono text-text-secondary">{h.trade_date}</span>
                  <span className="font-mono">{(h.weight * 100).toFixed(2)}%</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
