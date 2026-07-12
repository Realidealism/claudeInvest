import { useEffect, useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";

const NAV_GROUPS = [
  {
    key: "market",
    label: "市場面",
    items: [
      { to: "/breadth",    label: "多空頭排列" },
      { to: "/margin",     label: "融資融券" },
      { to: "/vix",         label: "VIX 波動率指數" },
      { to: "/fear-greed",  label: "CNN 恐懼貪婪指數" },
      { to: "/yield-curve", label: "美債殖利率曲線" },
      { to: "/ftse-taiwan", label: "夜盤參考" },
      { to: "/commodities", label: "大宗行情" },
    ],
  },
  {
    key: "chips",
    label: "籌碼面",
    items: [
      { to: "/signals",    label: "訊號總覽" },
      { to: "/funds",      label: "基金分析" },
      { to: "/dual-track", label: "雙軌比對" },
      { to: "/flow",       label: "資金流向" },
      { to: "/chip-picks", label: "集保選股" },
    ],
  },
  {
    key: "fundamentals",
    label: "基本面",
    items: [
      { to: "/hermit",  label: "贏勢股篩選" },
      { to: "/revenue", label: "月營收選股" },
    ],
  },
  {
    key: "technicals",
    label: "技術面",
    items: [
      { to: "/scores",     label: "多空評比" },
      { to: "/operations", label: "操作訊號" },
      { to: "/positions",  label: "策略持倉" },
    ],
  },
] as const;

const COLLAPSE_KEY = "nav-collapsed-groups";

// Default: collapse every group except the one containing the current
// page, so first-time users land on a tidy sidebar that already shows
// related pages. Subsequent toggles persist via localStorage.
function initialCollapsed(currentPath: string): Set<string> {
  try {
    const raw = localStorage.getItem(COLLAPSE_KEY);
    if (raw) return new Set(JSON.parse(raw) as string[]);
  } catch {
    // ignore parse / storage errors
  }
  const activeKey = NAV_GROUPS.find((g) =>
    g.items.some((it) => it.to === currentPath)
  )?.key;
  return new Set(NAV_GROUPS.filter((g) => g.key !== activeKey).map((g) => g.key));
}

function NavItems({ onClick }: { onClick?: () => void }) {
  const { pathname } = useLocation();
  const [collapsed, setCollapsed] = useState<Set<string>>(() => initialCollapsed(pathname));

  useEffect(() => {
    try {
      localStorage.setItem(COLLAPSE_KEY, JSON.stringify([...collapsed]));
    } catch {
      // ignore storage errors
    }
  }, [collapsed]);

  const toggle = (key: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <ul className="flex-1 py-2 overflow-y-auto">
      {NAV_GROUPS.map((group) => {
        const isCollapsed = collapsed.has(group.key);
        return (
          <li key={group.key} className="mb-1">
            <button
              type="button"
              onClick={() => toggle(group.key)}
              className="w-full flex items-center justify-between px-4 py-1.5 text-xs font-bold tracking-wide text-text-secondary hover:text-text-primary"
            >
              <span>{group.label}</span>
              <svg
                width="10"
                height="10"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                className={`transition-transform ${isCollapsed ? "" : "rotate-90"}`}
              >
                <path d="M9 18l6-6-6-6" />
              </svg>
            </button>
            {!isCollapsed && (
              <ul>
                {group.items.map(({ to, label }) => (
                  <li key={to}>
                    <NavLink
                      to={to}
                      onClick={onClick}
                      className={({ isActive }) =>
                        `block pl-7 pr-4 py-2 text-sm transition-colors ${
                          isActive
                            ? "bg-accent/10 text-accent font-medium border-r-2 border-accent"
                            : "text-text-secondary hover:bg-surface-hover hover:text-text-primary"
                        }`
                      }
                    >
                      {label}
                    </NavLink>
                  </li>
                ))}
              </ul>
            )}
          </li>
        );
      })}
    </ul>
  );
}

export default function Layout() {
  const [open, setOpen] = useState(false);

  return (
    <div className="flex h-screen">
      {/* Desktop sidebar */}
      <nav className="hidden md:flex w-52 shrink-0 border-r border-border bg-surface-alt flex-col">
        <div className="px-4 py-5 border-b border-border">
          <h1 className="text-sm font-bold tracking-wide text-text-primary">
            個股策略中心
          </h1>
          <p className="text-xs text-text-secondary mt-0.5">TW Equity Strategy Hub</p>
        </div>
        <NavItems />
        <div className="px-4 py-3 text-xs text-text-secondary border-t border-border">
          v0.1
        </div>
      </nav>

      {/* Mobile overlay */}
      {open && (
        <div
          className="fixed inset-0 bg-black/50 z-40 md:hidden"
          onClick={() => setOpen(false)}
        />
      )}

      {/* Mobile sidebar drawer */}
      <nav
        className={`fixed top-0 left-0 h-full w-52 bg-surface-alt border-r border-border z-50 flex flex-col transition-transform duration-200 md:hidden ${
          open ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="px-4 py-5 border-b border-border flex items-center justify-between">
          <div>
            <h1 className="text-sm font-bold tracking-wide text-text-primary">
              個股策略中心
            </h1>
            <p className="text-xs text-text-secondary mt-0.5">TW Equity Strategy Hub</p>
          </div>
          <button
            onClick={() => setOpen(false)}
            className="text-text-secondary hover:text-text-primary p-1"
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M18 6L6 18M6 6l12 12" />
            </svg>
          </button>
        </div>
        <NavItems onClick={() => setOpen(false)} />
        <div className="px-4 py-3 text-xs text-text-secondary border-t border-border">
          v0.1
        </div>
      </nav>

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Mobile header */}
        <header className="md:hidden flex items-center gap-3 px-4 py-3 border-b border-border bg-surface-alt shrink-0">
          <button
            onClick={() => setOpen(true)}
            className="text-text-secondary hover:text-text-primary p-1"
          >
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M3 12h18M3 6h18M3 18h18" />
            </svg>
          </button>
          <h1 className="text-sm font-bold text-text-primary">個股策略中心</h1>
        </header>

        <main className="flex-1 overflow-y-auto p-4 md:p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
