import { NavLink, Outlet } from "react-router-dom";

const NAV = [
  { to: "/signals", label: "訊號總覽" },
  { to: "/backtest", label: "策略績效" },
  { to: "/funds", label: "基金分析" },
  { to: "/dual-track", label: "雙軌比對" },
] as const;

export default function Layout() {
  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <nav className="w-52 shrink-0 border-r border-border bg-surface-alt flex flex-col">
        <div className="px-4 py-5 border-b border-border">
          <h1 className="text-sm font-bold tracking-wide text-text-primary">
            持股交叉比對
          </h1>
          <p className="text-xs text-text-secondary mt-0.5">Fund Holdings X-Ref</p>
        </div>
        <ul className="flex-1 py-2">
          {NAV.map(({ to, label }) => (
            <li key={to}>
              <NavLink
                to={to}
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
        <div className="px-4 py-3 text-xs text-text-secondary border-t border-border">
          v0.1
        </div>
      </nav>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto p-6">
        <Outlet />
      </main>
    </div>
  );
}
