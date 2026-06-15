import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

interface FundInfo {
  code: string;
  name: string;
}

interface FundsData {
  funds: FundInfo[];
}

interface Signal {
  ticker: string;
  ticker_name: string;
  market?: string;
  funds: string[];
  trigger_date: string;
  trigger_period: string;
  weight_change: number | null;
}

// TradingView uses 'TWSE:' for TWSE and 'TPEX:' (all caps) for TPEx.
function tvUrl(ticker: string, market: string | undefined): string {
  const prefix = market === "TWSE" ? "TWSE" : "TPEX";
  return `https://tw.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

interface SignalsData {
  by_type: Record<string, Signal[]>;
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
  etf_multi_consensus: "多ETF共識",
  etf_consecutive_accumulation: "ETF連續加碼",
  etf_abnormal_position: "ETF異常建倉",
  etf_multi_exit: "多ETF共識退場",
  etf_consecutive_reduction: "ETF連續減碼",
  etf_abnormal_exit: "ETF異常減倉",
};

const SHORT_SIGNALS = new Set([
  "heavy_position_reduction", "core_exit",
  "etf_multi_exit", "etf_consecutive_reduction", "etf_abnormal_exit",
]);

interface FlatSignal extends Signal {
  signal_type: string;
}

export default function SearchPage() {
  const [data, setData] = useState<SignalsData | null>(null);
  const [fundMap, setFundMap] = useState<Record<string, string>>({});
  const [query, setQuery] = useState("");

  useEffect(() => {
    fetch("/data/signals.json")
      .then((r) => r.json())
      .then(setData);
    fetch("/data/funds.json")
      .then((r) => r.json())
      .then((d: FundsData) => {
        const m: Record<string, string> = {};
        for (const f of d.funds) m[f.name] = f.code;
        setFundMap(m);
      });
  }, []);

  // Flatten all signals with their type
  const allSignals = useMemo(() => {
    if (!data) return [];
    const flat: FlatSignal[] = [];
    for (const [type, signals] of Object.entries(data.by_type)) {
      for (const s of signals) {
        flat.push({ ...s, signal_type: type });
      }
    }
    return flat;
  }, [data]);

  // Filter by query
  const results = useMemo(() => {
    const q = query.trim();
    if (!q) return [];
    const lower = q.toLowerCase();
    return allSignals.filter(
      (s) => s.ticker.includes(q) || s.ticker_name.toLowerCase().includes(lower)
    );
  }, [query, allSignals]);

  // Group by ticker
  const grouped = useMemo(() => {
    const map: Record<string, { name: string; market?: string; signals: FlatSignal[] }> = {};
    for (const s of results) {
      if (!map[s.ticker]) {
        map[s.ticker] = { name: s.ticker_name, market: s.market, signals: [] };
      }
      map[s.ticker].signals.push(s);
    }
    // Sort signals within each ticker: newest first
    for (const v of Object.values(map)) {
      v.signals.sort((a, b) => b.trigger_date.localeCompare(a.trigger_date));
    }
    return map;
  }, [results]);

  const tickers = Object.keys(grouped).sort();

  return (
    <div>
      <h2 className="text-lg font-semibold mb-4">訊號查詢</h2>

      <div className="mb-6">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="輸入股票代號或名稱..."
          className="w-full max-w-md bg-surface-alt border border-border rounded-lg px-4 py-2.5 text-sm text-text-primary placeholder:text-text-secondary focus:outline-none focus:border-accent"
          autoFocus
        />
      </div>

      {query.trim() && tickers.length === 0 && (
        <p className="text-text-secondary">無符合條件的訊號</p>
      )}

      {tickers.map((ticker) => {
        const { name, market, signals } = grouped[ticker];
        return (
          <div key={ticker} className="mb-6">
            <h3 className="text-sm font-semibold mb-2">
              <a
                href={tvUrl(ticker, market)}
                target="_blank"
                rel="noopener noreferrer"
                className="text-accent hover:underline font-mono"
              >
                {ticker}
              </a>
              <span className="ml-2 text-text-primary">{name}</span>
              <span className="ml-2 text-text-secondary font-normal">({signals.length} 筆)</span>
            </h3>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-text-secondary text-left">
                    <th className="py-1.5 pr-4 font-medium">信號</th>
                    <th className="py-1.5 pr-4 font-medium">日期</th>
                    <th className="py-1.5 pr-4 font-medium text-right">權重變化</th>
                    <th className="py-1.5 pr-4 font-medium">涉及基金/ETF</th>
                  </tr>
                </thead>
                <tbody>
                  {signals.map((s, i) => {
                    const isShort = SHORT_SIGNALS.has(s.signal_type);
                    return (
                      <tr key={i} className="border-b border-border/30 hover:bg-surface-hover transition-colors">
                        <td className={`py-1.5 pr-4 ${isShort ? "text-short-strong" : "text-long-strong"}`}>
                          <span className="mr-1">{isShort ? "▼" : "▲"}</span>
                          {SIGNAL_LABELS[s.signal_type] || s.signal_type}
                        </td>
                        <td className="py-1.5 pr-4 font-mono text-text-secondary">{s.trigger_date}</td>
                        <td className="py-1.5 pr-4 text-right font-mono">
                          {s.weight_change != null ? (
                            <span className={s.weight_change > 0 ? "text-negative" : "text-positive"}>
                              {s.weight_change > 0 ? "+" : ""}{s.weight_change.toFixed(2)}%
                            </span>
                          ) : "—"}
                        </td>
                        <td className="py-1.5 pr-4 text-xs">
                          {s.funds.slice(0, 3).map((name, j) => {
                            const code = fundMap[name];
                            return (
                              <span key={j}>
                                {j > 0 && ", "}
                                {code ? (
                                  <Link to={`/fund/${code}`} className="text-accent hover:underline">{name}</Link>
                                ) : (
                                  <span className="text-text-secondary">{name}</span>
                                )}
                              </span>
                            );
                          })}
                          {s.funds.length > 3 && <span className="text-text-secondary"> +{s.funds.length - 3}</span>}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}

      {!query.trim() && (
        <p className="text-text-secondary text-sm">輸入代號或名稱後即時顯示該股歷史訊號</p>
      )}
    </div>
  );
}
