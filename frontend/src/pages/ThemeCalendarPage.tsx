import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

interface Theme {
  key: string;
  name: string;
  note: string;
  link: string | null;
  color: string;
  months: number[]; // 12 ints: 0=off, 1=注意, 2=活躍
  live: { label: string; count: number } | null;
}

interface CalData {
  year: number;
  current_month: number; // 1-12
  as_of: string;
  themes: Theme[];
}

const MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

function ThemeName({ t }: { t: Theme }) {
  const dot = (
    <span className="inline-block w-2 h-2 rounded-full mr-2 shrink-0" style={{ backgroundColor: t.color }} />
  );
  return t.link ? (
    <Link to={t.link} className="flex items-center hover:underline">{dot}{t.name}</Link>
  ) : (
    <span className="flex items-center">{dot}{t.name}</span>
  );
}

export default function ThemeCalendarPage() {
  const [data, setData] = useState<CalData | null>(null);

  useEffect(() => {
    fetch("/data/theme_calendar.json").then((r) => r.json()).then(setData).catch(console.error);
  }, []);

  if (!data) return <div className="text-text-secondary">Loading...</div>;

  const cm = data.current_month;
  const focus = data.themes.filter((t) => t.months[cm - 1] > 0);

  return (
    <div>
      <h2 className="text-lg font-semibold mb-1">題材行事曆</h2>
      <p className="text-xs text-text-secondary mb-4">
        以月為單位的季節題材提醒。色帶＝該題材的注意（淡）與活躍（濃）窗口，當月以外框標示。點題材名可跳到對應清單。
      </p>

      {/* 本月焦點 */}
      <div className="mb-6 rounded border border-border bg-surface-alt p-3">
        <div className="text-sm font-semibold mb-2">本月焦點 · {data.year} 年 {cm} 月</div>
        {focus.length === 0 ? (
          <div className="text-xs text-text-secondary">本月無特別季節題材提醒。</div>
        ) : (
          <ul className="space-y-2">
            {focus.map((t) => (
              <li key={t.key} className="text-xs">
                <div className="flex items-center gap-2">
                  <span
                    className="px-1.5 py-0.5 rounded text-[10px] font-medium shrink-0"
                    style={{
                      backgroundColor: t.color,
                      opacity: t.months[cm - 1] === 2 ? 1 : 0.45,
                      color: "#fff",
                    }}
                  >
                    {t.months[cm - 1] === 2 ? "活躍" : "注意"}
                  </span>
                  <ThemeName t={t} />
                  {t.live && (
                    <span className="text-accent font-medium">· {t.live.label}</span>
                  )}
                </div>
                <div className="text-text-secondary mt-0.5 pl-1">{t.note}</div>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* 年度季節帶 */}
      <div className="overflow-x-auto">
        <table className="text-sm border-separate" style={{ borderSpacing: 0 }}>
          <thead>
            <tr className="text-text-secondary">
              <th className="text-left font-medium py-1 pr-3 sticky left-0 bg-surface z-10">題材</th>
              {MONTHS.map((m) => (
                <th
                  key={m}
                  className={
                    "font-medium w-9 text-center py-1 " +
                    (m === cm ? "text-accent" : "")
                  }
                >
                  {m}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.themes.map((t) => (
              <tr key={t.key}>
                <td className="py-1.5 pr-3 whitespace-nowrap sticky left-0 bg-surface z-10">
                  <ThemeName t={t} />
                </td>
                {t.months.map((lv, i) => (
                  <td
                    key={i}
                    className={"px-0.5 py-1.5 " + (i + 1 === cm ? "bg-surface-hover" : "")}
                  >
                    <div
                      className="h-4 rounded-sm"
                      style={{
                        backgroundColor: lv > 0 ? t.color : "transparent",
                        opacity: lv === 2 ? 0.9 : lv === 1 ? 0.32 : 0,
                      }}
                    />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex items-center gap-4 mt-3 text-xs text-text-secondary">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-4 h-3 rounded-sm bg-text-secondary" style={{ opacity: 0.32 }} />注意
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-4 h-3 rounded-sm bg-text-secondary" style={{ opacity: 0.9 }} />活躍
        </span>
        <span>直欄高亮＝本月</span>
      </div>
    </div>
  );
}
