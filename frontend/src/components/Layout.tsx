import { useState } from "react";
import { NavLink, Outlet } from "react-router-dom";

const NAV = [
  { to: "/signals", label: "訊號總覽" },
  { to: "/backtest", label: "策略績效" },
  { to: "/funds", label: "基金分析" },
  { to: "/dual-track", label: "雙軌比對" },
  { to: "/timeline", label: "月季交叉" },
  { to: "/dna", label: "經理人DNA" },
  { to: "/flow", label: "資金流向" },
  { to: "/search", label: "訊號查詢" },
  { to: "/hermit", label: "贏勢股篩選" },
  { to: "/scores", label: "多空評比" },
] as const;

function NavItems({ onClick }: { onClick?: () => void }) {
  return (
    <ul className="flex-1 py-2">
      {NAV.map(({ to, label }) => (
        <li key={to}>
          <NavLink
            to={to}
            onClick={onClick}
            className={({ isActive }) =>
              `block px-4 py-2 text-sm transition-colors ${
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
            持股交叉比對
          </h1>
          <p className="text-xs text-text-secondary mt-0.5">Fund Holdings X-Ref</p>
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
              持股交叉比對
            </h1>
            <p className="text-xs text-text-secondary mt-0.5">Fund Holdings X-Ref</p>
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
          <h1 className="text-sm font-bold text-text-primary">持股交叉比對</h1>
        </header>

        <main className="flex-1 overflow-y-auto p-4 md:p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
