import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import DataTimestamp from "../components/DataTimestamp";

interface LinkedStock {
  id:   string;
  name: string;
  role: string;   // 成本端 / 售價端 / 庫存端
  sign: number;   // 商品漲對這檔的方向：+1 利多 / -1 利空
  note: string;
}

interface Quote {
  symbol:      string;
  name:        string;
  category:    string;
  unit:        string;
  dp:          number;
  freq:        string;
  tv:          string | null;
  latest:      number | null;
  latest_date: string | null;
  chg_1:       number | null;
  chg_mid:     number | null;
  chg_long:    number | null;
  n_mid:       number;
  n_long:      number;
  w52_high:    number | null;
  w52_low:     number | null;
  w52_pct:     number | null;
  stocks:      LinkedStock[];
}

interface Category {
  key:   string;
  label: string;
}

interface CommoditiesData {
  latest_date: string | null;
  categories:  Category[];
  quotes:      Quote[];
}

// 台股慣例：紅漲綠跌（與美股相反）
function chgClass(v: number | null | undefined): string {
  if (v === null || v === undefined) return "text-text-secondary";
  if (v > 0) return "text-red-400";
  if (v < 0) return "text-emerald-400";
  return "text-text-secondary";
}

// "▲ 1.23%" / "▼ 0.45%" / "—"
function chgText(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  const arrow = v > 0 ? "▲" : v < 0 ? "▼" : "－";
  return `${arrow} ${Math.abs(v).toFixed(2)}%`;
}

function num(v: number | null | undefined, dp: number): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "—";
  return v.toFixed(dp);
}

// 相關個股。按角色分組，因為同一個商品對上下游的方向是相反的——熱軋鋼捲漲，
// 中鋼(賣)受惠、燁輝(買)受害，這正是純相關係數表達不出來的東西。角色標籤照
// 台股慣例上色：利多紅、利空綠。
const ROLE_ORDER = ["售價端", "庫存端", "成本端"];

function LinkedStocks({ stocks }: { stocks: LinkedStock[] }) {
  const groups = ROLE_ORDER
    .map((role) => ({ role, items: stocks.filter((s) => s.role === role) }))
    .filter((g) => g.items.length > 0);

  return (
    <div className="pt-2 mt-1 border-t border-border/60 space-y-1">
      {groups.map((g) => (
        <div key={g.role} className="flex gap-2 text-[11px]">
          <span
            className={`shrink-0 font-medium ${
              g.items[0].sign > 0 ? "text-red-400" : "text-emerald-400"
            }`}
            title={g.items[0].sign > 0 ? "此商品上漲對這些個股偏利多" : "此商品上漲對這些個股偏利空"}
          >
            {g.role}
          </span>
          <span className="flex flex-wrap gap-x-2 gap-y-0.5">
            {g.items.map((s) => (
              <Link
                key={s.id}
                to={`/stock/${s.id}`}
                title={s.note}
                className="text-text-secondary hover:text-text-primary hover:underline"
              >
                {s.name}
              </Link>
            ))}
          </span>
        </div>
      ))}
    </div>
  );
}

function QuoteCard({ q, pageLatest }: { q: Quote; pageLatest: string | null }) {
  // 這些序列的更新頻率天差地遠：期貨日更、FBX 週更（週五）、記憶體現貨
  // 成交稀疏時可能數週不動。落後全頁最新日期的，就把它自己的報價日標出來，
  // 否則使用者會把一個三週前的數字讀成今天的價。比全頁新的（比特幣、外匯的
  // 週日 tick）不算落後，不標。
  const staleDate =
    pageLatest && q.latest_date && q.latest_date < pageLatest ? q.latest_date : null;

  // FBX 只在週五公布、面板報價月更，所以它們相鄰兩點不是相差一天。chg_* 是
  // 「相隔 N 個資料點」的變化，單位得跟著序列頻率走，否則週/月漲跌會被讀成
  // 日漲跌。回看幾個點由 export 端依頻率決定（n_mid / n_long）。
  const per = q.freq === "weekly" ? "週" : q.freq === "monthly" ? "月" : "日";

  // 52 週游標：clamp 0~100，並用 calc 讓圓點在兩端仍完整落在 bar 內
  const pct = q.w52_pct === null || q.w52_pct === undefined || !Number.isFinite(q.w52_pct)
    ? null
    : Math.min(100, Math.max(0, q.w52_pct));
  const DOT = 10; // px

  // 我們只存收盤價，K 棒在 TradingView 那邊看。運價與記憶體現貨不是掛牌
  // 商品，沒有 TradingView 符號可連，卡片就維持不可點。
  const href = q.tv
    ? `https://tw.tradingview.com/chart/?symbol=${encodeURIComponent(q.tv)}`
    : null;

  const body = (
    <>
      <div className="flex items-baseline justify-between gap-2">
        <h3 className="text-sm font-bold text-text-primary">{q.name}</h3>
        <span className="text-[10px] text-text-secondary">{q.unit}</span>
      </div>

      <div className="flex flex-wrap items-baseline gap-3">
        <div className="text-2xl font-bold text-text-primary">{num(q.latest, q.dp)}</div>
        <div className={`text-sm font-medium ${chgClass(q.chg_1)}`}>{chgText(q.chg_1)}</div>
        {q.freq !== "daily" && (
          <span className="text-[10px] text-text-secondary/70">{per}變化</span>
        )}
        {staleDate && (
          <span className="text-[10px] text-text-secondary/70">{staleDate} 報價</span>
        )}
      </div>

      <div className="flex gap-4 text-[10px]">
        <span className="text-text-secondary">
          {q.n_mid}{per} <span className={chgClass(q.chg_mid)}>{chgText(q.chg_mid)}</span>
        </span>
        <span className="text-text-secondary">
          {q.n_long}{per} <span className={chgClass(q.chg_long)}>{chgText(q.chg_long)}</span>
        </span>
      </div>

      <div className="pt-1">
        <div className="relative h-1.5 rounded bg-surface-hover">
          {pct !== null && (
            <div
              className="absolute top-1/2 -translate-y-1/2 rounded-full bg-accent"
              style={{
                width:  `${DOT}px`,
                height: `${DOT}px`,
                left:   `calc(${pct}% - ${(pct / 100) * DOT}px)`,
              }}
            />
          )}
        </div>
        <div className="flex justify-between text-[10px] text-text-secondary mt-1">
          <span>{num(q.w52_low, q.dp)}</span>
          <span className="text-text-secondary/70">
            52週區間{pct !== null ? `　${pct.toFixed(0)}%` : ""}
          </span>
          <span>{num(q.w52_high, q.dp)}</span>
        </div>
      </div>
    </>
  );

  // 相關個股要連到我們自己的個股頁，所以不能包在 TradingView 那個 <a> 裡面
  // （巢狀 anchor 是無效 HTML）。外層改成 div，報價本體才是連結。
  return (
    <div
      className={`bg-surface-alt border border-border rounded p-3${
        href ? " transition-colors hover:border-accent" : ""
      }`}
    >
      {href ? (
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          title={`在 TradingView 開啟 ${q.tv}`}
          className="block space-y-2 cursor-pointer"
        >
          {body}
        </a>
      ) : (
        <div className="space-y-2">{body}</div>
      )}
      {q.stocks?.length > 0 && <LinkedStocks stocks={q.stocks} />}
    </div>
  );
}

export default function CommoditiesPage() {
  const [data, setData] = useState<CommoditiesData | null>(null);

  useEffect(() => {
    fetch("/data/commodities.json")
      .then((r) => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary text-sm">載入中…</div>;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-baseline gap-3">
        <h1 className="text-lg font-bold text-text-primary">大宗行情</h1>
        <span className="text-xs text-text-secondary">
          國際商品 · 匯率 · 運價（點卡片看 TradingView 走勢）
        </span>
      </div>
      <DataTimestamp value={data.latest_date} note="每交易日更新" />

      {/* 分類卡片牆 */}
      {data.categories.map((cat) => {
        const items = data.quotes.filter((q) => q.category === cat.key);
        if (items.length === 0) return null;
        return (
          <div key={cat.key} className="space-y-2">
            <h2 className="text-xs font-bold tracking-wide text-text-secondary pt-1">
              {cat.label}
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
              {items.map((q) => (
                <QuoteCard key={q.symbol} q={q} pageLatest={data.latest_date} />
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}
