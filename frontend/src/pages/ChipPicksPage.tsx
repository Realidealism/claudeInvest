import { useEffect, useState } from "react";

interface Pick {
  rank: number;
  ticker: string;
  name: string | null;
  market: string | null;
}

interface Week {
  date: string;
  long: Pick[];
  short: Pick[];
}

interface ChipPicksData {
  latest_date: string | null;
  weeks: Week[];
}

function tvUrl(market: string | null, ticker: string): string | null {
  const ex = market === "TWSE" ? "TWSE" : market === "TPEx" ? "TPEX" : null;
  return ex ? `https://www.tradingview.com/chart/?symbol=${ex}:${ticker}` : null;
}

function PicksTable({ title, picks }: { title: string; picks: Pick[] }) {
  return (
    <div>
      <h3 className="text-sm font-semibold mb-2">{title}</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="py-2 pr-4 font-medium">排名</th>
              <th className="py-2 pr-4 font-medium">代號</th>
              <th className="py-2 pr-4 font-medium">名稱</th>
            </tr>
          </thead>
          <tbody>
            {picks.map((p) => {
              const url = tvUrl(p.market, p.ticker);
              return (
                <tr
                  key={p.ticker}
                  className="border-b border-border/50 hover:bg-surface-hover"
                >
                  <td className="py-2 pr-4 text-text-secondary">{p.rank}</td>
                  <td className="py-2 pr-4 font-medium">
                    {url ? (
                      <a
                        href={url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-accent hover:underline"
                      >
                        {p.ticker}
                      </a>
                    ) : (
                      p.ticker
                    )}
                  </td>
                  <td className="py-2 pr-4">{p.name ?? "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
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
        依集保股權變化選股。做多＝籌碼集中（大戶增、散戶減、千張人數增）；做空＝籌碼擴散（反向）。
        每週各取前 30 名。集保歷史僅約 14 個月、樣本偏小，做空側訊號較弱，僅供方向參考。
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

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <PicksTable title="做多標的" picks={current.long} />
        <PicksTable title="做空標的" picks={current.short} />
      </div>
    </div>
  );
}
