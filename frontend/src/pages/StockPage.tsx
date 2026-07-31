import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";

// ---- Interfaces copied from existing pages (kept consistent, no shared layer) ----

// ScoresPage.tsx
interface ScoreRow {
  rank: number;
  ticker: string;
  name: string;
  market: string;
  total_pct: number;
  turnover: number;
}

interface ScoresData {
  snapshot_date: string | null;
  long: ScoreRow[];
  short: ScoreRow[];
}

// OperationsPage.tsx
interface SignalRow {
  ticker: string;
  name: string;
  market: string;
  turnover: number;
  streak?: number;
}

interface OperationsData {
  snapshot_date: string | null;
  signals: Record<string, SignalRow[]>;
}

type SignalKey = "pick" | "touch" | "buy" | "sell" | "buy_flee" | "sell_flee";

const SIGNAL_ORDER: SignalKey[] = ["pick", "touch", "buy", "sell", "buy_flee", "sell_flee"];

const SIGNAL_META: Record<SignalKey, { label: string; side: "long" | "short" }> = {
  pick:      { label: "抄底",   side: "long"  },
  touch:     { label: "摸頭",   side: "short" },
  buy:       { label: "波段多", side: "long"  },
  sell:      { label: "波段空", side: "short" },
  buy_flee:  { label: "多轉空", side: "short" },
  sell_flee: { label: "空轉多", side: "long"  },
};

// PositionsPage.tsx
interface Position {
  ticker: string;
  name: string;
  market: string;
  entry_date: string;
  entry_price: number;
  entry_tier: string;
  current_close: number;
  pnl_pct: number;
  bars_held: number;
  turnover: number;
  defense_price: number | null;
  defense_reason: string | null;
  defense_date: string | null;
  exit_reason?: string | null;
  disposal_status?: string | null;
}

interface PositionsData {
  snapshot_date: string | null;
  long: Position[];
  short: Position[];
  exited_long?: Position[];
  exited_short?: Position[];
}

const TIER_LABEL: Record<string, string> = {
  pick: "抄底",
  buy: "波段多",
  sell_flee: "空轉多",
  touch: "摸頭",
  sell: "波段空",
  buy_flee: "多轉空",
};

// SignalsPage.tsx
interface FundSignal {
  ticker: string;
  ticker_name: string;
  market?: string;
  funds: string[];
  trigger_date: string;
  trigger_period: string;
}

interface FundSignalsData {
  by_type: Record<string, FundSignal[]>;
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

// funds.json
interface FundInfo {
  code: string;
  name: string;
  fund_type: string;
}

interface HoldingRow {
  ticker: string;
  ticker_name: string;
  market?: string;
  rank: number | null;
  weight: number | null;
  shares: number | null;
}

interface FundsData {
  funds: FundInfo[];
  holdings: Record<string, { monthly?: Record<string, HoldingRow[]> }>;
  latest_monthly: string | null;
}

// flow.json
interface FlowFundEntry {
  curr: number | null;
  prev: number | null;
  diff: number;
}

interface FlowChange {
  ticker_name: string;
  market: string;
  monthly_net: number[];
  total_net: number;
  funds: Record<string, FlowFundEntry>;
}

interface FlowData {
  periods: string[];
  fund_columns: { code: string; name: string }[];
  changes: Record<string, FlowChange>;
}

// chip_picks.json
interface ChipRow {
  rank: number;
  ticker: string;
  name: string;
  market: string;
}

interface ChipPicksData {
  weeks: { date: string; long: ChipRow[]; short: ChipRow[] }[];
}

// HermitPage.tsx
interface Valuation {
  method: string | null;
  multiple: number | null;
  band: string | null;
  upside_pct: number | null;
  decision: string | null;
}

interface HermitPick {
  rank: number;
  ticker: string;
  name: string;
  market?: string;
  industry: string | null;
  score: number;
  grade: string;
  valuation: Valuation;
  is_new: boolean;
}

interface HermitData {
  snapshot_date: string | null;
  picks: HermitPick[];
}

// revenue_screens.json — root: strategies[] (metadata) + data[month][strategy_key][]
interface RevenueStrategy {
  key: string;
  label: string;
  side: string;
}

interface RevenueRow {
  stock_id: string;
  name: string;
  market?: string;
}

interface RevenueData {
  strategies: RevenueStrategy[];
  data: Record<string, Record<string, RevenueRow[]>>;
}

// ---- Per-section hit shapes ----

// 相關原物料。sign 是「這個商品上漲對本檔的方向」：成本端 -1、售價端與
// 庫存端 +1，由 db/commodity_links.py 的角色決定。
interface CommodityLink {
  symbol: string;
  name:   string;
  unit:   string;
  dp:     number;
  latest: number | null;
  chg_1:  number | null;
  role:   string;
  sign:   number;
  note:   string;
}

interface StockCommoditiesData {
  latest_date: string | null;
  by_stock:    Record<string, CommodityLink[]>;
}

interface ScoreHit { side: "long" | "short"; rank: number; total_pct: number; turnover: number }
interface OpHit { signal: SignalKey; streak?: number; turnover: number }
interface PosHit { bucket: "long" | "short" | "exited_long" | "exited_short"; row: Position }
interface FundSigHit { type: string; sig: FundSignal }
interface HoldingHit { fundName: string; period: string; weight: number | null; rank: number | null }
interface ChipHit { date: string; side: "long" | "short"; rank: number }
interface RevenueHit { month: string; strategyLabel: string; side: string }

type Section<T> =
  | { status: "loading" }
  | { status: "error" }
  | { status: "ok"; hits: T };

const LOADING: { status: "loading" } = { status: "loading" };

function fmtTurnover(t: number): string {
  if (t >= 1e8) return `${(t / 1e8).toFixed(2)}億`;
  if (t >= 1e4) return `${(t / 1e4).toFixed(0)}萬`;
  return t.toFixed(0);
}

function fmtShares(n: number): string {
  if (Math.abs(n) >= 1e4) return `${(n / 1e3).toFixed(0)}張`;
  return `${n}股`;
}

function pnlClass(p: number): string {
  if (p > 0) return "text-long-strong";
  if (p < 0) return "text-short-strong";
  return "text-text-secondary";
}

// TradingView uses 'TWSE:' for TWSE and 'TPEX:' (all caps) for TPEx.
// Default to TWSE when the market is unknown.
function tvUrl(ticker: string, market: string | undefined): string {
  const prefix = !market || market === "TWSE" ? "TWSE" : "TPEX";
  return `https://tw.tradingview.com/chart/?symbol=${prefix}:${ticker}`;
}

function sideLabel(side: "long" | "short"): string {
  return side === "long" ? "做多" : "做空";
}

function sideClass(side: "long" | "short"): string {
  return side === "long" ? "text-long-strong" : "text-short-strong";
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="bg-surface-alt border border-border rounded p-3">
      <h2 className="text-sm font-bold text-text-primary mb-2">{title}</h2>
      {children}
    </section>
  );
}

function SectionBody<T>({
  section,
  isEmpty,
  children,
}: {
  section: Section<T>;
  isEmpty: (hits: T) => boolean;
  children: (hits: T) => React.ReactNode;
}) {
  if (section.status === "loading")
    return <div className="text-xs text-text-secondary">載入中…</div>;
  if (section.status === "error")
    return <div className="text-xs text-text-secondary">載入失敗</div>;
  if (isEmpty(section.hits))
    return <div className="text-xs text-text-secondary">未上榜</div>;
  return <>{children(section.hits)}</>;
}

export default function StockPage() {
  const params = useParams<{ ticker: string }>();
  const ticker = (params.ticker ?? "").trim();

  const [meta, setMeta] = useState<{ name: string; market?: string } | null>(null);

  const [scores, setScores] = useState<Section<ScoreHit[]>>(LOADING);
  const [ops, setOps] = useState<Section<OpHit[]>>(LOADING);
  const [positions, setPositions] = useState<Section<PosHit[]>>(LOADING);
  const [fundSigs, setFundSigs] = useState<Section<FundSigHit[]>>(LOADING);
  const [holdings, setHoldings] = useState<Section<HoldingHit[]>>(LOADING);
  const [flow, setFlow] = useState<Section<{ change: FlowChange; periods: string[] } | null>>(LOADING);
  const [chips, setChips] = useState<Section<ChipHit[]>>(LOADING);
  const [hermit, setHermit] = useState<Section<HermitPick | null>>(LOADING);
  const [revenue, setRevenue] = useState<Section<RevenueHit[]>>(LOADING);
  const [commodities, setCommodities] = useState<Section<CommodityLink[]>>(LOADING);

  useEffect(() => {
    if (!ticker) return;
    setMeta(null);
    setScores(LOADING);
    setOps(LOADING);
    setPositions(LOADING);
    setFundSigs(LOADING);
    setHoldings(LOADING);
    setFlow(LOADING);
    setChips(LOADING);
    setHermit(LOADING);
    setRevenue(LOADING);
    setCommodities(LOADING);

    let cancelled = false;

    const reportMeta = (name: string | undefined, market: string | undefined) => {
      if (!name || cancelled) return;
      setMeta((prev) => prev ?? { name, market });
    };

    const fetchJson = async <T,>(url: string): Promise<T> => {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`${url}: ${r.status}`);
      return r.json();
    };

    const guard = <T,>(set: (s: Section<T>) => void, fn: () => Promise<T>) =>
      fn()
        .then((hits) => { if (!cancelled) set({ status: "ok", hits }); })
        .catch((e) => { console.error(e); if (!cancelled) set({ status: "error" }); });

    // 1. Scores
    guard(setScores, async () => {
      const d = await fetchJson<ScoresData>("/data/scores.json");
      const hits: ScoreHit[] = [];
      for (const side of ["long", "short"] as const) {
        const row = (d[side] ?? []).find((r) => r.ticker === ticker);
        if (row) {
          hits.push({ side, rank: row.rank, total_pct: row.total_pct, turnover: row.turnover });
          reportMeta(row.name, row.market);
        }
      }
      return hits;
    });

    // 2. Operations
    guard(setOps, async () => {
      const d = await fetchJson<OperationsData>("/data/operations.json");
      const hits: OpHit[] = [];
      for (const sig of SIGNAL_ORDER) {
        const row = (d.signals[sig] ?? []).find((r) => r.ticker === ticker);
        if (row) {
          hits.push({ signal: sig, streak: row.streak, turnover: row.turnover });
          reportMeta(row.name, row.market);
        }
      }
      return hits;
    });

    // 3. Positions
    guard(setPositions, async () => {
      const d = await fetchJson<PositionsData>("/data/positions.json");
      const hits: PosHit[] = [];
      const buckets = ["long", "short", "exited_long", "exited_short"] as const;
      for (const bucket of buckets) {
        for (const row of d[bucket] ?? []) {
          if (row.ticker === ticker) {
            hits.push({ bucket, row });
            reportMeta(row.name, row.market);
          }
        }
      }
      return hits;
    });

    // 4. Fund chip signals
    guard(setFundSigs, async () => {
      const d = await fetchJson<FundSignalsData>("/data/signals.json");
      const hits: FundSigHit[] = [];
      for (const [type, list] of Object.entries(d.by_type ?? {})) {
        for (const sig of list) {
          if (sig.ticker === ticker) {
            hits.push({ type, sig });
            reportMeta(sig.ticker_name, sig.market);
          }
        }
      }
      hits.sort((a, b) => b.sig.trigger_date.localeCompare(a.sig.trigger_date));
      return hits;
    });

    // 5. Fund holdings (latest period per fund)
    guard(setHoldings, async () => {
      const d = await fetchJson<FundsData>("/data/funds.json");
      const hits: HoldingHit[] = [];
      for (const f of d.funds ?? []) {
        const monthly = d.holdings?.[f.code]?.monthly;
        if (!monthly) continue;
        const keys = Object.keys(monthly);
        if (keys.length === 0) continue;
        // Fund-type uses the root latest_monthly period; ETF-type monthly keys
        // are date strings, take that fund's own max key. Fall back to own max.
        let key: string;
        if (f.fund_type !== "etf" && d.latest_monthly && monthly[String(d.latest_monthly)]) {
          key = String(d.latest_monthly);
        } else {
          key = keys.sort()[keys.length - 1];
        }
        const row = (monthly[key] ?? []).find((r) => r.ticker === ticker);
        if (row) {
          hits.push({ fundName: f.name, period: key, weight: row.weight, rank: row.rank });
          reportMeta(row.ticker_name, row.market);
        }
      }
      hits.sort((a, b) => (b.weight ?? 0) - (a.weight ?? 0));
      return hits;
    });

    // 6. Flow
    guard(setFlow, async () => {
      const d = await fetchJson<FlowData>("/data/flow.json");
      const change = d.changes?.[ticker];
      if (!change) return null;
      reportMeta(change.ticker_name, change.market);
      return { change, periods: d.periods ?? [] };
    });

    // 7. Chip picks
    guard(setChips, async () => {
      const d = await fetchJson<ChipPicksData>("/data/chip_picks.json");
      const hits: ChipHit[] = [];
      for (const week of d.weeks ?? []) {
        for (const side of ["long", "short"] as const) {
          const row = (week[side] ?? []).find((r) => r.ticker === ticker);
          if (row) {
            hits.push({ date: week.date, side, rank: row.rank });
            reportMeta(row.name, row.market);
          }
        }
      }
      hits.sort((a, b) => b.date.localeCompare(a.date));
      return hits;
    });

    // 8. Hermit
    guard(setHermit, async () => {
      const d = await fetchJson<HermitData>("/data/hermit.json");
      const pick = (d.picks ?? []).find((p) => p.ticker === ticker) ?? null;
      if (pick) reportMeta(pick.name, pick.market);
      return pick;
    });

    // 9. Revenue screens (note: matched by stock_id, not ticker)
    guard(setRevenue, async () => {
      const d = await fetchJson<RevenueData>("/data/revenue_screens.json");
      const labelMap: Record<string, RevenueStrategy> = {};
      for (const s of d.strategies ?? []) labelMap[s.key] = s;
      const hits: RevenueHit[] = [];
      for (const [month, byStrategy] of Object.entries(d.data ?? {})) {
        for (const [key, rows] of Object.entries(byStrategy ?? {})) {
          const row = (rows ?? []).find((r) => r.stock_id === ticker);
          if (row) {
            hits.push({
              month,
              strategyLabel: labelMap[key]?.label ?? key,
              side: labelMap[key]?.side ?? "",
            });
            reportMeta(row.name, row.market);
          }
        }
      }
      hits.sort((a, b) => b.month.localeCompare(a.month));
      return hits;
    });

    // 10. 相關原物料
    guard(setCommodities, async () => {
      const d = await fetchJson<StockCommoditiesData>("/data/stock_commodities.json");
      return d.by_stock?.[ticker] ?? [];
    });

    return () => { cancelled = true; };
  }, [ticker]);

  const sections: Section<unknown>[] = [
    scores, ops, positions, fundSigs, holdings, flow, chips, hermit, revenue,
    commodities,
  ];
  const allSettled = sections.every((s) => s.status !== "loading");
  const anyHit =
    (scores.status === "ok" && scores.hits.length > 0) ||
    (ops.status === "ok" && ops.hits.length > 0) ||
    (positions.status === "ok" && positions.hits.length > 0) ||
    (fundSigs.status === "ok" && fundSigs.hits.length > 0) ||
    (holdings.status === "ok" && holdings.hits.length > 0) ||
    (flow.status === "ok" && flow.hits !== null) ||
    (chips.status === "ok" && chips.hits.length > 0) ||
    (hermit.status === "ok" && hermit.hits !== null) ||
    (revenue.status === "ok" && revenue.hits.length > 0);

  if (!ticker) {
    return <div className="text-text-secondary text-sm">無效的股票代號。</div>;
  }

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">
          <span className="font-mono">{ticker}</span>
          {meta?.name && <span className="ml-2">{meta.name}</span>}
        </h1>
        {allSettled && !anyHit && (
          <span className="text-xs text-text-secondary">各系統均未上榜</span>
        )}
        <a
          href={tvUrl(ticker, meta?.market)}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-accent hover:underline"
        >
          TradingView ↗
        </a>
      </div>

      {/* 1. 多空評比 */}
      <Card title="多空評比">
        <SectionBody section={scores} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="px-2 py-1.5">側別</th>
                  <th className="px-2 py-1.5 text-right">排名</th>
                  <th className="px-2 py-1.5 text-right">總分%</th>
                  <th className="px-2 py-1.5 text-right">成交金額</th>
                </tr>
              </thead>
              <tbody>
                {hits.map((h) => (
                  <tr key={h.side} className="border-b border-border/50">
                    <td className={`px-2 py-1.5 font-bold ${sideClass(h.side)}`}>{sideLabel(h.side)}</td>
                    <td className="px-2 py-1.5 text-right font-mono">{h.rank}</td>
                    <td className="px-2 py-1.5 text-right font-mono">
                      {h.total_pct >= 0 ? "+" : ""}{h.total_pct.toFixed(1)}
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono">{fmtTurnover(h.turnover)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </SectionBody>
      </Card>

      {/* 2. 操作訊號 */}
      <Card title="操作訊號">
        <SectionBody section={ops} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="px-2 py-1.5">訊號</th>
                  <th className="px-2 py-1.5">方向</th>
                  <th className="px-2 py-1.5 text-right">連續</th>
                  <th className="px-2 py-1.5 text-right">成交金額</th>
                </tr>
              </thead>
              <tbody>
                {hits.map((h) => {
                  const m = SIGNAL_META[h.signal];
                  return (
                    <tr key={h.signal} className="border-b border-border/50">
                      <td className={`px-2 py-1.5 font-bold ${sideClass(m.side)}`}>{m.label}</td>
                      <td className="px-2 py-1.5 text-text-secondary">{sideLabel(m.side)}</td>
                      <td className="px-2 py-1.5 text-right font-mono">{h.streak ?? 1}d</td>
                      <td className="px-2 py-1.5 text-right font-mono">{fmtTurnover(h.turnover)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </SectionBody>
      </Card>

      {/* 3. 策略持倉 */}
      <Card title="策略持倉">
        <SectionBody section={positions} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border text-text-secondary text-left">
                    <th className="px-2 py-1.5">方向</th>
                    <th className="px-2 py-1.5">類型</th>
                    <th className="px-2 py-1.5 text-center">進場日</th>
                    <th className="px-2 py-1.5 text-right">進場</th>
                    <th className="px-2 py-1.5 text-right">損益%</th>
                    <th className="px-2 py-1.5 text-right">防守</th>
                    <th className="px-2 py-1.5">防守理由</th>
                    <th className="px-2 py-1.5">處置/警示</th>
                  </tr>
                </thead>
                <tbody>
                  {hits.map((h, i) => {
                    const bucketLabel: Record<PosHit["bucket"], string> = {
                      long: "做多", short: "做空", exited_long: "多單出場", exited_short: "空單出場",
                    };
                    const isLongSide = h.bucket === "long" || h.bucket === "exited_long";
                    return (
                      <tr key={i} className="border-b border-border/50">
                        <td className={`px-2 py-1.5 font-bold ${isLongSide ? "text-long-strong" : "text-short-strong"}`}>
                          {bucketLabel[h.bucket]}
                        </td>
                        <td className="px-2 py-1.5 text-text-secondary">
                          {TIER_LABEL[h.row.entry_tier] ?? h.row.entry_tier}
                        </td>
                        <td className="px-2 py-1.5 text-center font-mono text-text-secondary">{h.row.entry_date}</td>
                        <td className="px-2 py-1.5 text-right font-mono">{h.row.entry_price.toFixed(2)}</td>
                        <td className={`px-2 py-1.5 text-right font-mono font-bold ${pnlClass(h.row.pnl_pct)}`}>
                          {h.row.pnl_pct >= 0 ? "+" : ""}{h.row.pnl_pct.toFixed(2)}
                        </td>
                        <td className="px-2 py-1.5 text-right font-mono text-text-secondary">
                          {h.row.defense_price !== null ? h.row.defense_price.toFixed(2) : "—"}
                        </td>
                        <td className="px-2 py-1.5 text-text-secondary">{h.row.defense_reason ?? "—"}</td>
                        <td className="px-2 py-1.5 text-text-secondary">{h.row.disposal_status || "—"}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </SectionBody>
      </Card>

      {/* 4. 基金籌碼訊號 */}
      <Card title="基金籌碼訊號">
        <SectionBody section={fundSigs} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="px-2 py-1.5">訊號類型</th>
                  <th className="px-2 py-1.5">觸發期間</th>
                  <th className="px-2 py-1.5">基金</th>
                </tr>
              </thead>
              <tbody>
                {hits.map((h, i) => (
                  <tr key={i} className="border-b border-border/50">
                    <td className="px-2 py-1.5">{SIGNAL_LABELS[h.type] ?? h.type}</td>
                    <td className="px-2 py-1.5 font-mono text-text-secondary">{h.sig.trigger_period}</td>
                    <td className="px-2 py-1.5 text-text-secondary">{h.sig.funds.join("、")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </SectionBody>
      </Card>

      {/* 5. 基金持股 */}
      <Card title="基金持股（最新期）">
        <SectionBody section={holdings} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="px-2 py-1.5">基金</th>
                  <th className="px-2 py-1.5">期間</th>
                  <th className="px-2 py-1.5 text-right">權重%</th>
                  <th className="px-2 py-1.5 text-right">排名</th>
                </tr>
              </thead>
              <tbody>
                {hits.map((h, i) => (
                  <tr key={i} className="border-b border-border/50">
                    <td className="px-2 py-1.5">{h.fundName}</td>
                    <td className="px-2 py-1.5 font-mono text-text-secondary">{h.period}</td>
                    <td className="px-2 py-1.5 text-right font-mono">
                      {h.weight !== null ? h.weight.toFixed(2) : "—"}
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono text-text-secondary">{h.rank ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </SectionBody>
      </Card>

      {/* 6. 資金流向 */}
      <Card title="資金流向">
        <SectionBody section={flow} isEmpty={(h) => h === null}>
          {(hit) => {
            const { change, periods } = hit!;
            // monthly_net[i] is the transition periods[i] -> periods[i+1]
            // (same convention as FlowPage: labels = periods.slice(1)).
            const transitions = periods.slice(1);
            return (
              <div className="space-y-2">
                <div className="text-xs">
                  累計淨買賣超：
                  <span className={`ml-1 font-mono font-bold ${pnlClass(change.total_net)}`}>
                    {change.total_net > 0 ? "+" : ""}{fmtShares(change.total_net)}
                  </span>
                </div>
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-border text-text-secondary text-left">
                      {transitions.map((p) => (
                        <th key={p} className="px-2 py-1.5 text-right font-mono">{p}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      {transitions.map((p, i) => {
                        const v = change.monthly_net[i] ?? 0;
                        return (
                          <td key={p} className={`px-2 py-1.5 text-right font-mono ${pnlClass(v)}`}>
                            {v > 0 ? "+" : ""}{fmtShares(v)}
                          </td>
                        );
                      })}
                    </tr>
                  </tbody>
                </table>
              </div>
            );
          }}
        </SectionBody>
      </Card>

      {/* 7. 集保選股 */}
      <Card title="集保選股">
        <SectionBody section={chips} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="px-2 py-1.5">週</th>
                  <th className="px-2 py-1.5">側別</th>
                  <th className="px-2 py-1.5 text-right">排名</th>
                </tr>
              </thead>
              <tbody>
                {hits.map((h, i) => (
                  <tr key={i} className="border-b border-border/50">
                    <td className="px-2 py-1.5 font-mono text-text-secondary">{h.date}</td>
                    <td className={`px-2 py-1.5 font-bold ${sideClass(h.side)}`}>{sideLabel(h.side)}</td>
                    <td className="px-2 py-1.5 text-right font-mono">{h.rank}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </SectionBody>
      </Card>

      {/* 8. 贏勢股 */}
      <Card title="贏勢股">
        <SectionBody section={hermit} isEmpty={(h) => h === null}>
          {(pick) => (
            <div className="text-xs space-y-1">
              <div>
                評等 <span className="font-bold">{pick!.grade}</span>
                <span className="ml-3">分數 <span className="font-mono">{pick!.score}</span></span>
                <span className="ml-3">排名 <span className="font-mono">{pick!.rank}</span></span>
                {pick!.is_new && (
                  <span className="ml-2 px-1.5 py-0.5 rounded text-[10px] bg-blue-500/30 text-blue-300">NEW</span>
                )}
              </div>
              <div className="text-text-secondary">
                {pick!.industry ?? "—"}
                <span className="ml-3">
                  估值決策 {pick!.valuation.decision ?? "—"}
                  {pick!.valuation.upside_pct !== null && (
                    <span className={`ml-1 font-mono ${pnlClass(pick!.valuation.upside_pct)}`}>
                      ({pick!.valuation.upside_pct >= 0 ? "+" : ""}{pick!.valuation.upside_pct.toFixed(1)}%)
                    </span>
                  )}
                </span>
              </div>
            </div>
          )}
        </SectionBody>
      </Card>

      {/* 9. 月營收選股 */}
      <Card title="月營收選股">
        <SectionBody section={revenue} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="px-2 py-1.5">月份</th>
                  <th className="px-2 py-1.5">策略</th>
                  <th className="px-2 py-1.5">方向</th>
                </tr>
              </thead>
              <tbody>
                {hits.map((h, i) => (
                  <tr key={i} className="border-b border-border/50">
                    <td className="px-2 py-1.5 font-mono text-text-secondary">{h.month}</td>
                    <td className="px-2 py-1.5">{h.strategyLabel}</td>
                    <td className={`px-2 py-1.5 font-bold ${h.side === "long" ? "text-long-strong" : h.side === "short" ? "text-short-strong" : "text-text-secondary"}`}>
                      {h.side === "long" ? "做多" : h.side === "short" ? "做空" : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </SectionBody>
      </Card>

      {/* 10. 相關原物料 */}
      <Card title="相關原物料">
        <SectionBody section={commodities} isEmpty={(h) => h.length === 0}>
          {(hits) => (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-text-secondary text-left">
                  <th className="px-2 py-1.5">商品</th>
                  <th className="px-2 py-1.5">角色</th>
                  <th className="px-2 py-1.5 text-right">報價</th>
                  <th className="px-2 py-1.5 text-right">日變化</th>
                </tr>
              </thead>
              <tbody>
                {hits.map((c) => (
                  <tr key={c.symbol} className="border-b border-border/50" title={c.note}>
                    <td className="px-2 py-1.5">
                      {c.name}
                      <span className="ml-2 text-[10px] text-text-secondary">{c.unit}</span>
                    </td>
                    {/* 角色的顏色是「這個商品漲對本檔是好是壞」，不是商品自己的漲跌 */}
                    <td className={`px-2 py-1.5 font-bold ${c.sign > 0 ? "text-long-strong" : "text-short-strong"}`}>
                      {c.role}
                    </td>
                    <td className="px-2 py-1.5 text-right font-mono">
                      {c.latest === null ? "—" : c.latest.toFixed(c.dp)}
                    </td>
                    <td className={`px-2 py-1.5 text-right font-mono ${
                      c.chg_1 === null ? "text-text-secondary" : pnlClass(c.chg_1)
                    }`}>
                      {c.chg_1 === null ? "—" : `${c.chg_1 >= 0 ? "+" : ""}${c.chg_1.toFixed(2)}%`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </SectionBody>
      </Card>
    </div>
  );
}
