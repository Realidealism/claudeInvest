import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

interface Valuation {
  method: string | null;
  multiple: number | null;
  band: string | null;
  upside_pct: number | null;
  decision: string | null;
}

interface Pick {
  rank: number;
  ticker: string;
  name: string;
  industry: string | null;
  score: number;
  grade: string;
  rules: Record<string, boolean | null>;
  valuation: Valuation;
  is_new: boolean;
  prev_rank: number | null;
  rank_delta: number | null;
}

interface HistoryItem {
  date: string;
  top_n: number;
  new_count: number;
}

interface HermitData {
  snapshot_date: string | null;
  picks: Pick[];
  history: HistoryItem[];
}

const RULE_NAMES: Record<string, string> = {
  F1: "獲利成長",
  F2: "營收成長",
  F3: "毛利率連升",
  F4: "存貨天數",
  F5: "負債比",
  F6: "FCF 健康",
  F7: "月營收動能",
  F8: "季營收動能",
};

const GRADE_BG: Record<string, string> = {
  A: "bg-green-500/20 text-green-300",
  B: "bg-yellow-500/20 text-yellow-300",
  C: "bg-orange-500/20 text-orange-300",
  D: "bg-red-500/20 text-red-300",
};

const DECISION_BG: Record<string, string> = {
  BUY: "bg-green-500/20 text-green-300",
  HOLD: "bg-yellow-500/20 text-yellow-300",
  SELL: "bg-red-500/20 text-red-300",
};

export default function HermitPage() {
  const [data, setData] = useState<HermitData | null>(null);
  const [decisionFilter, setDecisionFilter] = useState<string>("ALL");
  const [gradeFilter, setGradeFilter] = useState<string>("ALL");
  const [showOnlyNew, setShowOnlyNew] = useState(false);

  useEffect(() => {
    fetch("/data/hermit.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  const filtered = useMemo(() => {
    if (!data) return [];
    return data.picks.filter((p) => {
      if (decisionFilter !== "ALL" && p.valuation.decision !== decisionFilter) return false;
      if (gradeFilter !== "ALL" && p.grade !== gradeFilter) return false;
      if (showOnlyNew && !p.is_new) return false;
      return true;
    });
  }, [data, decisionFilter, gradeFilter, showOnlyNew]);

  if (!data) {
    return (
      <div className="text-text-secondary text-sm">載入中…</div>
    );
  }
  if (data.picks.length === 0) {
    return (
      <div className="text-text-secondary text-sm">
        尚無 hermit_stock 快照資料。請先跑 daily_update。
      </div>
    );
  }

  const counts = {
    total: data.picks.length,
    new: data.picks.filter((p) => p.is_new).length,
    buy: data.picks.filter((p) => p.valuation.decision === "BUY").length,
    hold: data.picks.filter((p) => p.valuation.decision === "HOLD").length,
    sell: data.picks.filter((p) => p.valuation.decision === "SELL").length,
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">贏勢股篩選</h1>
        <span className="text-xs text-text-secondary">
          快照日：{data.snapshot_date}　·　Top {counts.total}　·
          BUY {counts.buy}　HOLD {counts.hold}　SELL {counts.sell}　·
          NEW {counts.new}
        </span>
      </div>
      <p className="text-xs text-text-secondary">
        策略：gate F6+F7+F8 / Top-50 / score floor=3。BUY = 潛在報酬 ≥ +20%，SELL = 潛在報酬 ≤ 0%。
      </p>

      {/* Filters */}
      <div className="flex flex-wrap gap-2 items-center">
        <span className="text-xs text-text-secondary">決策</span>
        {["ALL", "BUY", "HOLD", "SELL"].map((d) => (
          <button
            key={d}
            onClick={() => setDecisionFilter(d)}
            className={`px-3 py-1 text-xs rounded ${
              decisionFilter === d
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:text-text-primary"
            }`}
          >
            {d}
          </button>
        ))}
        <span className="text-xs text-text-secondary ml-3">評等</span>
        {["ALL", "A", "B", "C"].map((g) => (
          <button
            key={g}
            onClick={() => setGradeFilter(g)}
            className={`px-3 py-1 text-xs rounded ${
              gradeFilter === g
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:text-text-primary"
            }`}
          >
            {g}
          </button>
        ))}
        <label className="flex items-center gap-1.5 text-xs text-text-secondary ml-3 cursor-pointer">
          <input
            type="checkbox"
            checked={showOnlyNew}
            onChange={(e) => setShowOnlyNew(e.target.checked)}
            className="accent-accent"
          />
          只看 NEW
        </label>
      </div>

      {/* Table */}
      <div className="overflow-x-auto bg-surface-alt border border-border rounded">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="px-2 py-2 w-10">#</th>
              <th className="px-2 py-2">代號</th>
              <th className="px-2 py-2">名稱</th>
              <th className="px-2 py-2 hidden md:table-cell">產業</th>
              <th className="px-2 py-2 text-center">分數</th>
              <th className="px-2 py-2 text-center">評等</th>
              <th className="px-2 py-2 hidden lg:table-cell">8 規則</th>
              <th className="px-2 py-2 text-center">方法</th>
              <th className="px-2 py-2 text-right">倍數</th>
              <th className="px-2 py-2 hidden lg:table-cell">區間</th>
              <th className="px-2 py-2 text-right">潛在%</th>
              <th className="px-2 py-2 text-center">決策</th>
              <th className="px-2 py-2 text-center">變動</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((p) => (
              <tr
                key={p.ticker}
                className="border-b border-border/50 hover:bg-surface-hover transition-colors"
              >
                <td className="px-2 py-1.5">{p.rank}</td>
                <td className="px-2 py-1.5 font-mono">
                  <Link to={`/stock/${p.ticker}`} className="text-accent hover:underline">
                    {p.ticker}
                  </Link>
                </td>
                <td className="px-2 py-1.5">{p.name}</td>
                <td className="px-2 py-1.5 hidden md:table-cell text-text-secondary">
                  {p.industry || "—"}
                </td>
                <td className="px-2 py-1.5 text-center font-bold">{p.score}/8</td>
                <td className="px-2 py-1.5 text-center">
                  <span className={`px-1.5 py-0.5 rounded text-xs ${GRADE_BG[p.grade] || ""}`}>
                    {p.grade}
                  </span>
                </td>
                <td className="px-2 py-1.5 hidden lg:table-cell">
                  <div className="flex gap-0.5" title={Object.entries(p.rules).map(([k,v]) => `${k}(${RULE_NAMES[k]}):${v?'✓':v===false?'✗':'?'}`).join('  ')}>
                    {Object.entries(p.rules).map(([k, v]) => (
                      <span
                        key={k}
                        className={`w-3.5 h-3.5 inline-flex items-center justify-center text-[9px] rounded-sm ${
                          v === true
                            ? "bg-green-500/30 text-green-300"
                            : v === false
                            ? "bg-red-500/30 text-red-300"
                            : "bg-text-secondary/20 text-text-secondary"
                        }`}
                      >
                        {k.slice(1)}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="px-2 py-1.5 text-center">{p.valuation.method || "—"}</td>
                <td className="px-2 py-1.5 text-right font-mono">
                  {p.valuation.multiple !== null ? p.valuation.multiple.toFixed(2) : "—"}
                </td>
                <td className="px-2 py-1.5 hidden lg:table-cell text-text-secondary">
                  {p.valuation.band || "—"}
                </td>
                <td
                  className={`px-2 py-1.5 text-right font-mono ${
                    p.valuation.upside_pct !== null
                      ? p.valuation.upside_pct > 0
                        ? "text-green-300"
                        : "text-red-300"
                      : "text-text-secondary"
                  }`}
                >
                  {p.valuation.upside_pct !== null
                    ? `${p.valuation.upside_pct > 0 ? "+" : ""}${p.valuation.upside_pct.toFixed(1)}%`
                    : "—"}
                </td>
                <td className="px-2 py-1.5 text-center">
                  {p.valuation.decision && (
                    <span
                      className={`px-1.5 py-0.5 rounded text-xs ${
                        DECISION_BG[p.valuation.decision] || ""
                      }`}
                    >
                      {p.valuation.decision}
                    </span>
                  )}
                </td>
                <td className="px-2 py-1.5 text-center">
                  {p.is_new ? (
                    <span className="px-1.5 py-0.5 rounded text-[10px] bg-blue-500/30 text-blue-300">
                      NEW
                    </span>
                  ) : p.rank_delta !== null && p.rank_delta !== 0 ? (
                    <span
                      className={
                        p.rank_delta > 0 ? "text-green-300" : "text-red-300"
                      }
                    >
                      {p.rank_delta > 0 ? "↑" : "↓"}
                      {Math.abs(p.rank_delta)}
                    </span>
                  ) : (
                    <span className="text-text-secondary">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* History sidebar */}
      {data.history.length > 1 && (
        <div className="text-xs text-text-secondary">
          近期快照：
          {data.history.slice(0, 10).map((h) => (
            <span key={h.date} className="ml-2">
              {h.date}（NEW {h.new_count}）
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
