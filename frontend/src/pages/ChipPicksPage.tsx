import { useEffect, useState } from "react";

interface Pick {
  rank: number;
  ticker: string;
  name: string | null;
  market: string | null;
  d_big: number;
  d_retail: number;
  d_holders: number;
  ratio: number;
}

interface Week {
  date: string;
  picks: Pick[];
}

interface ChipPicksData {
  latest_date: string | null;
  weeks: Week[];
}

export default function ChipPicksPage() {
  const [data, setData] = useState<ChipPicksData | null>(null);
  const [week, setWeek] = useState<string | null>(null);

  useEffect(() => {
    fetch("/data/chip_picks.json")
      .then((r) => r.json())
      .then((d: ChipPicksData) => {
        setData(d);
        setWeek(d.weeks[0]?.date ?? null);
      })
      .catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;
  if (!data.weeks.length)
    return <div className="text-text-secondary">尚無選股資料</div>;

  const current = data.weeks.find((w) => w.date === week) ?? data.weeks[0];

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">集保選股</h2>
      <p className="text-xs text-text-secondary mb-4">
        每週取「大戶持股增 + 散戶持股減 + 千張大戶人數增」三維共識前 20 名（普通股，4 週變化）。
        集保歷史僅約 14 個月、樣本偏小，結論信心度低，僅供方向參考。
      </p>

      <div className="flex flex-wrap gap-1.5 mb-4">
        {data.weeks.map((w) => (
          <button
            key={w.date}
            onClick={() => setWeek(w.date)}
            className={
              "px-3 py-1 rounded text-xs font-medium " +
              (w.date === current.date
                ? "bg-accent text-white"
                : "bg-surface-alt text-text-secondary hover:bg-surface-hover")
            }
          >
            {w.date}
          </button>
        ))}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="py-2 pr-4 font-medium">排名</th>
              <th className="py-2 pr-4 font-medium">代號</th>
              <th className="py-2 pr-4 font-medium">名稱</th>
              <th className="py-2 pr-4 font-medium">市場</th>
              <th className="py-2 pr-4 font-medium text-right">大戶增</th>
              <th className="py-2 pr-4 font-medium text-right">散戶減</th>
              <th className="py-2 pr-4 font-medium text-right">千張人數增</th>
              <th className="py-2 pr-4 font-medium text-right">千張比例</th>
            </tr>
          </thead>
          <tbody>
            {current.picks.map((p) => (
              <tr
                key={p.ticker}
                className="border-b border-border/50 hover:bg-surface-hover"
              >
                <td className="py-2 pr-4 text-text-secondary">{p.rank}</td>
                <td className="py-2 pr-4 font-medium">{p.ticker}</td>
                <td className="py-2 pr-4">{p.name ?? "—"}</td>
                <td className="py-2 pr-4 text-text-secondary">{p.market ?? "—"}</td>
                <td className="py-2 pr-4 text-right">{p.d_big.toFixed(2)}</td>
                <td className="py-2 pr-4 text-right">{p.d_retail.toFixed(2)}</td>
                <td className="py-2 pr-4 text-right">{p.d_holders}</td>
                <td className="py-2 pr-4 text-right">{p.ratio.toFixed(2)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
