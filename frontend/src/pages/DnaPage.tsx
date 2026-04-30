import { useEffect, useState } from "react";

interface FundDna {
  code: string;
  name: string;
  company: string;
  manager: string;
  fund_type: string;
  avg_concentration: number;
  avg_turnover: number;
}

interface DnaData {
  funds: FundDna[];
  periods: string[];
}

export default function DnaPage() {
  const [data, setData] = useState<DnaData | null>(null);

  useEffect(() => {
    fetch("/data/dna.json")
      .then((r) => r.json())
      .then(setData);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const maxConc = Math.max(...data.funds.map((f) => f.avg_concentration));
  const maxTurn = Math.max(...data.funds.map((f) => f.avg_turnover));

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">經理人風格 DNA</h2>
      <p className="text-xs text-text-secondary mb-4">
        集中度 = Top 3 平均權重，換手率 = 相鄰期間 Top 10 異動比例（{data.periods.length} 期資料）
      </p>

      {/* Scatter-like bar comparison */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Concentration ranking */}
        <div className="bg-surface-alt border border-border rounded-lg p-4">
          <h3 className="text-sm font-semibold mb-3 text-text-secondary">集中度排名（Top 3 權重平均）</h3>
          <div className="space-y-1.5">
            {[...data.funds]
              .sort((a, b) => b.avg_concentration - a.avg_concentration)
              .map((f) => (
                <div key={f.code} className="flex items-center gap-2 text-xs">
                  <span className="w-28 truncate">{f.name}</span>
                  <span className="w-16 text-text-secondary truncate">{f.manager}</span>
                  <div className="flex-1 h-4 bg-surface rounded overflow-hidden">
                    <div
                      className="h-full bg-accent/60 rounded"
                      style={{ width: `${(f.avg_concentration / maxConc) * 100}%` }}
                    />
                  </div>
                  <span className="w-14 text-right font-mono">{f.avg_concentration.toFixed(1)}%</span>
                </div>
              ))}
          </div>
        </div>

        {/* Turnover ranking */}
        <div className="bg-surface-alt border border-border rounded-lg p-4">
          <h3 className="text-sm font-semibold mb-3 text-text-secondary">換手率排名（相鄰期 Top 10 異動比）</h3>
          <div className="space-y-1.5">
            {[...data.funds]
              .sort((a, b) => b.avg_turnover - a.avg_turnover)
              .map((f) => (
                <div key={f.code} className="flex items-center gap-2 text-xs">
                  <span className="w-28 truncate">{f.name}</span>
                  <span className="w-16 text-text-secondary truncate">{f.manager}</span>
                  <div className="flex-1 h-4 bg-surface rounded overflow-hidden">
                    <div
                      className="h-full bg-warning/60 rounded"
                      style={{ width: `${(f.avg_turnover / maxTurn) * 100}%` }}
                    />
                  </div>
                  <span className="w-14 text-right font-mono">{(f.avg_turnover * 100).toFixed(0)}%</span>
                </div>
              ))}
          </div>
        </div>
      </div>

      {/* Full table */}
      <div className="mt-6 bg-surface-alt border border-border rounded-lg p-4">
        <h3 className="text-sm font-semibold mb-3 text-text-secondary">完整比較</h3>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-text-secondary text-left">
              <th className="py-2 pr-4 font-medium">基金</th>
              <th className="py-2 pr-4 font-medium">經理人</th>
              <th className="py-2 pr-4 font-medium">公司</th>
              <th className="py-2 pr-4 font-medium text-right">集中度</th>
              <th className="py-2 pr-4 font-medium text-right">換手率</th>
              <th className="py-2 font-medium text-center">風格</th>
            </tr>
          </thead>
          <tbody>
            {data.funds.map((f) => {
              const style =
                f.avg_concentration > 25 && f.avg_turnover < 0.5
                  ? "集中持有"
                  : f.avg_concentration < 20 && f.avg_turnover > 0.6
                  ? "分散輪動"
                  : f.avg_turnover > 0.7
                  ? "高換手"
                  : "均衡";
              return (
                <tr key={f.code} className="border-b border-border/50">
                  <td className="py-2 pr-4">{f.name}</td>
                  <td className="py-2 pr-4 text-text-secondary">{f.manager}</td>
                  <td className="py-2 pr-4 text-text-secondary">{f.company}</td>
                  <td className="py-2 pr-4 text-right font-mono">{f.avg_concentration.toFixed(1)}%</td>
                  <td className="py-2 pr-4 text-right font-mono">{(f.avg_turnover * 100).toFixed(0)}%</td>
                  <td className="py-2 text-center text-xs">
                    <span className={`px-2 py-0.5 rounded ${
                      style === "集中持有" ? "bg-accent/10 text-accent" :
                      style === "高換手" ? "bg-warning/10 text-warning" :
                      style === "分散輪動" ? "bg-negative/10 text-negative" :
                      "bg-surface text-text-secondary"
                    }`}>{style}</span>
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
