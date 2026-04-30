import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

interface Fund {
  id: number;
  code: string;
  name: string;
  fund_type: string;
  company: string;
  manager_name: string | null;
}

interface FundsData {
  funds: Fund[];
  latest_monthly: string;
  latest_quarterly: string;
}

export default function FundsPage() {
  const [data, setData] = useState<FundsData | null>(null);

  useEffect(() => {
    fetch("/data/funds.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const activeFunds = data.funds.filter((f) => f.fund_type === "fund");
  const etfs = data.funds.filter((f) => f.fund_type === "etf");

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">基金分析</h2>
      <p className="text-xs text-text-secondary mb-4">
        月報最新: {data.latest_monthly} / 季報最新: {data.latest_quarterly}
      </p>

      <h3 className="text-sm font-semibold mb-2 text-text-secondary">主動式基金 ({activeFunds.length})</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 mb-6">
        {activeFunds.map((f) => (
          <Link
            key={f.code}
            to={`/fund/${f.code}`}
            className="bg-surface-alt border border-border rounded-lg px-4 py-3 hover:border-accent/50 transition-colors"
          >
            <div className="font-medium">{f.name}</div>
            <div className="text-xs text-text-secondary mt-1">
              {f.company} / {f.manager_name || "—"} / {f.code}
            </div>
          </Link>
        ))}
      </div>

      <h3 className="text-sm font-semibold mb-2 text-text-secondary">主動式 ETF ({etfs.length})</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
        {etfs.map((f) => (
          <Link
            key={f.code}
            to={`/fund/${f.code}`}
            className="bg-surface-alt border border-border rounded-lg px-4 py-3 hover:border-accent/50 transition-colors"
          >
            <div className="font-medium">{f.name}</div>
            <div className="text-xs text-text-secondary mt-1">
              {f.company} / {f.manager_name || "—"} / {f.code}
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
