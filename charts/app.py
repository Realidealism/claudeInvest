"""
Dash application for interactive candlestick charting.

Run with:
    python -m charts.app

Features:
- Candlestick chart with volume subplot
- Drawing tools (line, rectangle, circle, eraser)
- Signal markers from analysis modules
- SMA overlay lines
- Stock selector and date range picker
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import numpy as np
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State

from db.connection import get_cursor


def _load_watchlist() -> dict[str, list[dict]]:
    """Load watchlist groups with stock names and latest price from database."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT w.group_name, w.stock_id,
                   COALESCE(s.name, i.name, w.stock_id) AS name,
                   COALESCE(dp.close_price, ip.close_price) AS close_price,
                   COALESCE(dp.change, ip.change) AS change,
                   COALESCE(dp.change_pct, ip.change_pct) AS change_pct
            FROM tw.watchlist w
            LEFT JOIN tw.stocks s ON s.stock_id = w.stock_id
            LEFT JOIN tw.indices i ON i.index_id = w.stock_id
            LEFT JOIN LATERAL (
                SELECT close_price, change, change_pct
                FROM tw.daily_prices d
                WHERE d.stock_id = w.stock_id
                ORDER BY d.trade_date DESC LIMIT 1
            ) dp ON true
            LEFT JOIN LATERAL (
                SELECT close_price, change, change_pct
                FROM tw.index_prices ix
                WHERE ix.index_id = w.stock_id
                ORDER BY ix.trade_date DESC LIMIT 1
            ) ip ON true
            ORDER BY w.group_name, w.sort_order
            """
        )
        rows = cur.fetchall()
    result: dict[str, list[dict]] = {}
    for r in rows:
        close = r["close_price"]
        chg = r["change"]
        chg_pct = r["change_pct"]
        result.setdefault(r["group_name"], []).append({
            "id": r["stock_id"],
            "name": r["name"],
            "close": float(close) if close is not None else None,
            "change": float(chg) if chg is not None else None,
            "change_pct": float(chg_pct) if chg_pct is not None else None,
        })
    return result if result else {"自選股": []}


def _add_watchlist_item(group: str, stock_id: str) -> None:
    """Add a stock to the watchlist in the database."""
    with get_cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 AS next_ord "
            "FROM tw.watchlist WHERE group_name = %s",
            (group,),
        )
        next_ord = cur.fetchone()["next_ord"]
        cur.execute(
            "INSERT INTO tw.watchlist (group_name, stock_id, sort_order) "
            "VALUES (%s, %s, %s) ON CONFLICT (group_name, stock_id) DO NOTHING",
            (group, stock_id, next_ord),
        )


def _remove_watchlist_item(stock_id: str) -> None:
    """Remove a stock from all watchlist groups."""
    with get_cursor() as cur:
        cur.execute("DELETE FROM tw.watchlist WHERE stock_id = %s", (stock_id,))




def _load_signal_setting(key: str, default):
    """Load a signal setting from the database."""
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT value FROM tw.signal_settings WHERE key = %s", (key,))
        row = cur.fetchone()
    if row is None:
        return default
    return json.loads(row["value"])


def _save_signal_setting(key: str, value) -> None:
    """Save a signal setting to the database."""
    val_json = json.dumps(value, ensure_ascii=False)
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO tw.signal_settings (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """,
            (key, val_json),
        )


from analysis.close import calculate_close
from analysis.candle import calculate_candle
from analysis.volume import calculate_volume
from analysis.wave import calculate_wave
from charts.candlestick import build_candlestick_figure, DRAWING_BUTTONS
from charts.signals import (
    get_signal_categories,
    generate_markers,
    SIGNAL_DEFS,
)


# ── Data fetching ──────────────────────────────────────────────────────────


def fetch_price_data(
    stock_id: str, start_date: str, end_date: str,
) -> dict:
    """
    Fetch OHLCV data from tw.daily_prices or tw.index_prices.

    Special IDs 'TAIEX' and 'TPEx' fetch from the index table.
    Returns dict with keys: dates, open, high, low, close, turnover, name.
    Returns None if no data found.
    """
    is_index = stock_id in ("TAIEX", "TPEx")

    with get_cursor(commit=False) as cur:
        if is_index:
            cur.execute(
                "SELECT name FROM tw.indices WHERE index_id = %s",
                (stock_id,),
            )
            row = cur.fetchone()
            display_name = row["name"] if row else stock_id

            cur.execute(
                """
                SELECT trade_date, open_price, high_price, low_price,
                       close_price, turnover
                FROM tw.index_prices
                WHERE index_id = %s
                  AND trade_date BETWEEN %s AND %s
                  AND close_price IS NOT NULL
                ORDER BY trade_date
                """,
                (stock_id, start_date, end_date),
            )
        else:
            cur.execute(
                "SELECT name FROM tw.stocks WHERE stock_id = %s",
                (stock_id,),
            )
            row = cur.fetchone()
            display_name = row["name"] if row else stock_id

            cur.execute(
                """
                SELECT trade_date, open_price, high_price, low_price,
                       close_price, volume, turnover
                FROM tw.daily_prices
                WHERE stock_id = %s
                  AND trade_date BETWEEN %s AND %s
                  AND close_price IS NOT NULL
                ORDER BY trade_date
                """,
                (stock_id, start_date, end_date),
            )
        rows = cur.fetchall()

    if not rows:
        return None

    result = {
        "dates": [r["trade_date"].strftime("%Y-%m-%d") for r in rows],
        "open": np.array([float(r["open_price"]) for r in rows], dtype=np.float32),
        "high": np.array([float(r["high_price"]) for r in rows], dtype=np.float32),
        "low": np.array([float(r["low_price"]) for r in rows], dtype=np.float32),
        "close": np.array([float(r["close_price"]) for r in rows], dtype=np.float32),
        "name": display_name,
        "is_index": is_index,
    }

    if is_index:
        result["sub_value"] = np.array([float(r["turnover"] or 0) for r in rows], dtype=np.float32)
    else:
        result["sub_value"] = np.array([float(r["volume"] or 0) for r in rows], dtype=np.float32)
        result["turnover"] = np.array([float(r["turnover"] or 0) for r in rows], dtype=np.float32)

    return result


# ── Signal checkbox builder ───────────────────────────────────────────────


_CATEGORY_LABELS = {
    "candle": "K棒",
    "trigger": "觸發",
    "creep": "爬行",
    "volume": "成交量",
}


def _build_signal_checklist() -> html.Div:
    """Build grouped checkboxes for signal selection."""
    categories = get_signal_categories()
    children = []
    for cat_name, defs in categories.items():
        display_name = _CATEGORY_LABELS.get(cat_name, cat_name)
        options = [{"label": f" {d.label}", "value": d.key} for d in defs]
        children.append(
            html.Div([
                html.H4(display_name, style={"margin": "8px 0 4px 0", "color": "#aaa"}),
                dcc.Checklist(
                    id=f"signals-{cat_name}",
                    options=options,
                    value=[],
                    inline=True,
                    style={"fontSize": "13px", "color": "#eee"},
                    inputStyle={"marginRight": "4px", "marginLeft": "10px"},
                ),
            ])
        )
    return html.Div(children)


# ── Dash App ───────────────────────────────────────────────────────────────


app = dash.Dash(
    __name__,
    title="K Line Chart",
    update_title=None,
    suppress_callback_exceptions=True,
)

app.index_string = """<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body { background-color: #1e1e1e; }
            label { color: #eee !important; }
            .watchlist-item:hover { background-color: #333 !important; }
            .watchlist-item:hover span:last-child { color: #ef5350 !important; }
            .sidebar-tab:hover { color: #ccc !important; }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
        <script>
        // Trace index mapping (must match build_candlestick_figure order)
        // 0: Candlestick, 1-10: MA, 11: Volume, 12-31: Signals, 32-35: Wave
        var _maIndices = {
            '3':1, '5':2, '8':3, '13':4, '21':5, '34':6, '55':7, '89':8, '144':9, '233':10
        };
        var _sigKeys = ['jump','squat','short_hl','medium_hl','long_hl','red_long','black_long',
            'upper_shadow','lower_shadow','trigger_high1','trigger_low1','trigger_high2',
            'trigger_low2','trigger_high3','trigger_low3','creep_high1','creep_low1',
            'vol_burst','vol_sleep','vol_flood'];
        var _sigBaseIdx = 12;
        var _waveSlots = [
            {name:'Wave', group:'wave', idx:32},
            {name:'浪瀑(多)', group:'wf', idx:33},
            {name:'浪瀑(空)', group:'wf', idx:34},
            {name:'浪溝', group:'wf', idx:35}
        ];

        function _getGraph() {
            return document.querySelector('#chart .js-plotly-plot');
        }

        function _restyle(traceIdx, vis) {
            var g = _getGraph();
            if (!g || !g.data || traceIdx >= g.data.length) return;
            // Only restyle if trace has data
            var hasData = g.data[traceIdx].x && g.data[traceIdx].x.length > 0;
            Plotly.restyle(g, {visible: [hasData && vis]}, [traceIdx]);
        }

        // MA toggle
        document.addEventListener('click', function(e) {
            var c = document.getElementById('ma-select');
            if (!c || !c.contains(e.target)) return;
            setTimeout(function() {
                var checked = new Set();
                c.querySelectorAll('input').forEach(function(inp) {
                    if (inp.checked) checked.add(inp.value);
                });
                Object.keys(_maIndices).forEach(function(p) {
                    _restyle(_maIndices[p], checked.has(p));
                });
            }, 50);
        });

        // Signal toggle
        document.addEventListener('click', function(e) {
            var modal = document.getElementById('signal-modal-backdrop');
            if (!modal) return;
            // Check if click is inside any signals- checklist
            var found = false;
            _sigKeys.forEach(function() {}); // just need to check container
            var checklists = modal.querySelectorAll('[id^="signals-"]');
            checklists.forEach(function(cl) {
                if (cl.contains(e.target)) found = true;
            });
            if (!found) return;
            setTimeout(function() {
                var enabled = new Set();
                checklists.forEach(function(cl) {
                    cl.querySelectorAll('input').forEach(function(inp) {
                        if (inp.checked) enabled.add(inp.value);
                    });
                });
                _sigKeys.forEach(function(key, i) {
                    _restyle(_sigBaseIdx + i, enabled.has(key));
                });
            }, 50);
        });

        // Wave toggle
        document.addEventListener('click', function(e) {
            var c = document.getElementById('wave-select');
            if (!c || !c.contains(e.target)) return;
            setTimeout(function() {
                var checked = new Set();
                c.querySelectorAll('input').forEach(function(inp) {
                    if (inp.checked) checked.add(inp.value);
                });
                _waveSlots.forEach(function(w) {
                    _restyle(w.idx, checked.has(w.group));
                });
            }, 50);
        });

        // Delete key removes selected shape
        document.addEventListener('keydown', function(e) {
            if (e.key !== 'Delete') return;
            var graphDiv = document.querySelector('#chart .js-plotly-plot');
            if (!graphDiv || !graphDiv.layout || !graphDiv.layout.shapes) return;
            var shapes = graphDiv.layout.shapes;
            var activeIdx = -1;
            for (var i = 0; i < shapes.length; i++) {
                if (shapes[i].editable === true) {
                    activeIdx = i;
                    break;
                }
            }
            if (activeIdx === -1) return;
            var newShapes = shapes.filter(function(_, idx) { return idx !== activeIdx; });
            Plotly.relayout(graphDiv, {shapes: newShapes});
        });

        </script>
    </body>
</html>"""

_input_style = {
    "padding": "6px 10px",
    "backgroundColor": "#333", "color": "#eee",
    "border": "1px solid #555", "borderRadius": "4px",
}

app.layout = html.Div(
    style={"backgroundColor": "#1e1e1e", "minHeight": "100vh", "padding": "16px",
           "display": "flex", "gap": "12px"},
    children=[
        # fires once after page load to trigger DB read
        dcc.Interval(id="init-interval", interval=100, max_intervals=1),
        # ── Main content (left) ──
        html.Div(style={"flex": "1", "minWidth": "0"}, children=[
            # -- Header controls --
            html.Div(
                style={"display": "flex", "gap": "12px", "alignItems": "center",
                       "marginBottom": "12px", "flexWrap": "wrap"},
                children=[
                    dcc.Input(
                        id="stock-input",
                        type="text",
                        placeholder="2330 / TAIEX / TPEx",
                        value="2330",
                        debounce=True,
                        style={**_input_style, "width": "120px"},
                    ),
                    dcc.Input(
                        id="start-date",
                        type="text",
                        placeholder="Start (YYYY-MM-DD)",
                        value=(date.today() - timedelta(days=180)).strftime("%Y-%m-%d"),
                        debounce=True,
                        style={**_input_style, "width": "130px"},
                    ),
                    html.Span("~", style={"color": "#888"}),
                    dcc.Input(
                        id="end-date",
                        type="text",
                        placeholder="End (YYYY-MM-DD)",
                        value=date.today().strftime("%Y-%m-%d"),
                        debounce=True,
                        style={**_input_style, "width": "130px"},
                    ),
                    html.Button(
                        "Load",
                        id="load-btn",
                        n_clicks=0,
                        style={
                            "padding": "6px 20px", "backgroundColor": "#2196f3",
                            "color": "white", "border": "none", "borderRadius": "4px",
                            "cursor": "pointer",
                        },
                    ),
                    html.Button(
                        "訊號設定",
                        id="signal-modal-open",
                        n_clicks=0,
                        style={
                            "padding": "6px 16px", "backgroundColor": "#555",
                            "color": "#eee", "border": "none", "borderRadius": "4px",
                            "cursor": "pointer", "fontSize": "13px",
                        },
                    ),
                ],
            ),

            # -- Signal settings modal (hidden by default) --
            html.Div(
                id="signal-modal-backdrop",
                style={"display": "none", "position": "fixed", "top": "0", "left": "0",
                       "width": "100vw", "height": "100vh",
                       "backgroundColor": "rgba(0,0,0,0.5)", "zIndex": "1000",
                       "justifyContent": "center", "alignItems": "center"},
                children=[
                    html.Div(
                        style={"backgroundColor": "#2a2a2a", "borderRadius": "8px",
                               "padding": "20px", "width": "560px", "maxHeight": "80vh",
                               "overflowY": "auto", "position": "relative",
                               "border": "1px solid #555"},
                        children=[
                            # Modal header
                            html.Div(
                                style={"display": "flex", "justifyContent": "space-between",
                                       "alignItems": "center", "marginBottom": "16px"},
                                children=[
                                    html.H3("訊號設定", style={"color": "#eee", "margin": "0"}),
                                    html.Button(
                                        "✕",
                                        id="signal-modal-close",
                                        n_clicks=0,
                                        style={"background": "none", "border": "none",
                                               "color": "#aaa", "fontSize": "18px",
                                               "cursor": "pointer"},
                                    ),
                                ],
                            ),
                            # MA section
                            html.Div([
                                html.H4("均線 (MA)", style={"color": "#aaa", "margin": "0 0 6px 0"}),
                                dcc.Checklist(
                                    id="ma-select",
                                    options=[
                                        {"label": "3", "value": "3"},
                                        {"label": "5", "value": "5"},
                                        {"label": "8", "value": "8"},
                                        {"label": "13", "value": "13"},
                                        {"label": "21", "value": "21"},
                                        {"label": "34", "value": "34"},
                                        {"label": "55", "value": "55"},
                                        {"label": "89", "value": "89"},
                                        {"label": "144", "value": "144"},
                                        {"label": "233", "value": "233"},
                                    ],
                                    value=["5", "21", "55"],
                                    inline=True,
                                    style={"fontSize": "13px", "color": "#eee"},
                                    inputStyle={"marginRight": "6px", "marginLeft": "12px"},
                                    labelStyle={"display": "inline-flex", "alignItems": "center",
                                                "gap": "4px", "marginRight": "8px"},
                                ),
                            ], style={"marginBottom": "16px"}),
                            # Wave section
                            html.Div([
                                html.H4("波浪", style={"color": "#aaa", "margin": "0 0 6px 0"}),
                                dcc.Checklist(
                                    id="wave-select",
                                    options=[
                                        {"label": "波浪線", "value": "wave"},
                                        {"label": "浪瀑/浪溝", "value": "wf"},
                                    ],
                                    value=[],
                                    inline=True,
                                    style={"fontSize": "13px", "color": "#eee"},
                                    inputStyle={"marginRight": "6px", "marginLeft": "12px"},
                                    labelStyle={"display": "inline-flex", "alignItems": "center",
                                                "gap": "4px", "marginRight": "8px"},
                                ),
                            ], style={"marginBottom": "16px"}),
                            # Signal markers section
                            html.Div([
                                html.H4("訊號標記", style={"color": "#aaa", "margin": "0 0 6px 0"}),
                                _build_signal_checklist(),
                            ]),
                        ],
                    ),
                ],
            ),

            # -- Chart --
            dcc.Graph(
                id="chart",
                config={
                    "modeBarButtonsToAdd": DRAWING_BUTTONS,
                    "displayModeBar": True,
                    "scrollZoom": True,
                },
                style={"height": "700px"},
            ),

            # -- Store for drawn shapes persistence --
            dcc.Store(id="shapes-store", data=[]),

            # -- Status --
            html.Div(id="status-msg",
                      style={"color": "#888", "marginTop": "8px", "fontSize": "12px"}),
        ]),

        # ── Right sidebar with tabs ──
        html.Div(
            style={
                "width": "220px", "flexShrink": "0",
                "backgroundColor": "#252525", "borderRadius": "6px",
                "overflowY": "auto", "maxHeight": "90vh",
                "display": "flex", "flexDirection": "column",
            },
            children=[
                # Tab headers
                html.Div(
                    id="sidebar-tab-headers",
                    style={"display": "flex", "borderBottom": "1px solid #444"},
                    children=[
                        html.Div("自選股", id="tab-watchlist-btn", n_clicks=0,
                                 className="sidebar-tab active-tab",
                                 style={"flex": "1", "textAlign": "center",
                                        "padding": "8px 0", "cursor": "pointer",
                                        "fontSize": "13px", "color": "#eee",
                                        "borderBottom": "2px solid #2196f3"}),
                        html.Div("庫存", id="tab-inventory-btn", n_clicks=0,
                                 className="sidebar-tab",
                                 style={"flex": "1", "textAlign": "center",
                                        "padding": "8px 0", "cursor": "pointer",
                                        "fontSize": "13px", "color": "#888",
                                        "borderBottom": "2px solid transparent"}),
                        html.Div("選股結果", id="tab-screener-btn", n_clicks=0,
                                 className="sidebar-tab",
                                 style={"flex": "1", "textAlign": "center",
                                        "padding": "8px 0", "cursor": "pointer",
                                        "fontSize": "13px", "color": "#888",
                                        "borderBottom": "2px solid transparent"}),
                    ],
                ),

                # Tab content: 自選股
                html.Div(
                    id="tab-watchlist-content",
                    style={"padding": "12px", "display": "block"},
                    children=[
                        # Pinned: TAIEX
                        html.Div(
                            id="pinned-taiex",
                            n_clicks=0,
                            className="watchlist-item",
                            style={"padding": "6px 8px", "marginBottom": "8px",
                                   "borderRadius": "4px", "cursor": "pointer",
                                   "borderBottom": "1px solid #444"},
                        ),
                        # Add stock input
                        html.Div(
                            style={"display": "flex", "gap": "4px", "marginBottom": "10px"},
                            children=[
                                dcc.Input(
                                    id="watchlist-add-input",
                                    type="text",
                                    placeholder="代碼",
                                    debounce=True,
                                    style={**_input_style, "width": "100px", "fontSize": "13px"},
                                ),
                                html.Button(
                                    "+",
                                    id="watchlist-add-btn",
                                    n_clicks=0,
                                    style={
                                        "padding": "4px 10px", "backgroundColor": "#4caf50",
                                        "color": "white", "border": "none",
                                        "borderRadius": "4px", "cursor": "pointer",
                                        "fontSize": "14px",
                                    },
                                ),
                            ],
                        ),
                        html.Div(id="watchlist-container"),
                        dcc.Store(id="watchlist-store"),
                    ],
                ),

                # Tab content: 庫存
                html.Div(
                    id="tab-inventory-content",
                    style={"padding": "12px", "display": "none"},
                    children=[
                        html.Div(id="inventory-container", children=[
                            html.Div("尚未連接券商 API",
                                     style={"color": "#666", "fontSize": "13px",
                                            "textAlign": "center", "marginTop": "20px"}),
                        ]),
                    ],
                ),

                # Tab content: 選股結果 (placeholder)
                html.Div(
                    id="tab-screener-content",
                    style={"padding": "12px", "display": "none"},
                    children=[
                        html.Div(id="screener-container", children=[
                            html.Div("尚未連接選股系統",
                                     style={"color": "#666", "fontSize": "13px",
                                            "textAlign": "center", "marginTop": "20px"}),
                        ]),
                    ],
                ),
            ],
        ),
    ],
)


# ── Callbacks ──────────────────────────────────────────────────────────────


def _get_index_quote(index_id: str) -> dict:
    """Fetch latest price info for an index."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT i.name, p.close_price, p.change, p.change_pct
            FROM tw.indices i
            LEFT JOIN LATERAL (
                SELECT close_price, change, change_pct
                FROM tw.index_prices ix
                WHERE ix.index_id = i.index_id
                ORDER BY ix.trade_date DESC LIMIT 1
            ) p ON true
            WHERE i.index_id = %s
            """,
            (index_id,),
        )
        row = cur.fetchone()
    if not row:
        return {"name": index_id, "close": None, "change": None, "change_pct": None}
    return {
        "name": row["name"],
        "close": float(row["close_price"]) if row["close_price"] else None,
        "change": float(row["change"]) if row["change"] else None,
        "change_pct": float(row["change_pct"]) if row["change_pct"] else None,
    }


def _render_pinned_index(q: dict) -> list:
    """Build children for pinned TAIEX display."""
    chg = q.get("change")
    if chg is not None and chg > 0:
        color = "#ef5350"
        sign = "+"
    elif chg is not None and chg < 0:
        color = "#26a69a"
        sign = ""
    else:
        color = "#888"
        sign = ""
    close_str = f"{q['close']:,.2f}" if q["close"] is not None else "-"
    chg_str = f"{sign}{chg:,.2f}" if chg is not None else "-"
    pct_str = f"{sign}{q['change_pct']:,.2f}%" if q.get("change_pct") is not None else "-"
    return [
        html.Div(f"TAIEX {q['name']}", style={"color": "#ffd54f", "fontSize": "13px"}),
        html.Div(
            style={"display": "flex", "justifyContent": "space-between",
                   "marginTop": "2px", "fontSize": "12px"},
            children=[
                html.Span(close_str, style={"color": color}),
                html.Span(chg_str, style={"color": color}),
                html.Span(pct_str, style={"color": color}),
            ],
        ),
    ]


def _load_inventory() -> list[dict]:
    """Load inventory positions from database."""
    with get_cursor(commit=False) as cur:
        cur.execute(
            """
            SELECT symbol, symbol_name, current_quantity,
                   unrealized_pnl, unrealized_pnl_rate
            FROM tw.inventory
            WHERE current_quantity > 0
            ORDER BY symbol
            """
        )
        return [dict(r) for r in cur.fetchall()]


def _render_inventory(positions: list[dict]) -> list:
    """Build inventory list UI."""
    if not positions:
        return [html.Div("尚未連接券商 API",
                         style={"color": "#666", "fontSize": "13px",
                                "textAlign": "center", "marginTop": "20px"})]
    items = []
    for pos in positions:
        pnl = pos.get("unrealized_pnl")
        pnl_rate = pos.get("unrealized_pnl_rate")
        if pnl is not None and float(pnl) > 0:
            color = "#ef5350"
            sign = "+"
        elif pnl is not None and float(pnl) < 0:
            color = "#26a69a"
            sign = ""
        else:
            color = "#888"
            sign = ""
        pnl_str = f"{sign}{float(pnl):,.0f}" if pnl is not None else "-"
        rate_str = f"{sign}{float(pnl_rate):,.2f}%" if pnl_rate is not None else "-"
        qty = pos.get("current_quantity", 0)
        items.append(
            html.Div(
                style={"padding": "5px 6px", "borderRadius": "4px",
                       "cursor": "pointer", "marginBottom": "2px"},
                className="watchlist-item",
                id={"type": "inventory-stock", "index": pos["symbol"]},
                n_clicks=0,
                children=[
                    html.Div(
                        style={"display": "flex", "justifyContent": "space-between"},
                        children=[
                            html.Span(f"{pos['symbol']} {pos.get('symbol_name', '')}",
                                      style={"color": "#eee", "fontSize": "13px"}),
                            html.Span(f"{qty:,} 股",
                                      style={"color": "#aaa", "fontSize": "12px"}),
                        ],
                    ),
                    html.Div(
                        style={"display": "flex", "justifyContent": "space-between",
                               "marginTop": "2px", "fontSize": "12px"},
                        children=[
                            html.Span(pnl_str, style={"color": color}),
                            html.Span(rate_str, style={"color": color}),
                        ],
                    ),
                ],
            )
        )
    return items


# Load watchlist + TAIEX quote + signal settings + inventory from DB on page load
@app.callback(
    Output("watchlist-store", "data", allow_duplicate=True),
    Output("pinned-taiex", "children"),
    Output("inventory-container", "children"),
    Output("ma-select", "value"),
    Output("wave-select", "value"),
    *[Output(f"signals-{cat}", "value") for cat in get_signal_categories()],
    Input("init-interval", "n_intervals"),
    prevent_initial_call=True,
)
def load_settings_from_db(n):
    wl = _load_watchlist()
    taiex = _render_pinned_index(_get_index_quote("TAIEX"))
    inventory = _render_inventory(_load_inventory())
    ma = _load_signal_setting("ma_select", ["5", "21", "55"])
    wave = _load_signal_setting("wave_select", [])
    cats = list(get_signal_categories().keys())
    signal_vals = [_load_signal_setting(f"signals_{cat}", []) for cat in cats]
    return (wl, taiex, inventory, ma, wave, *signal_vals)


# Toggle signal settings modal
@app.callback(
    Output("signal-modal-backdrop", "style"),
    Input("signal-modal-open", "n_clicks"),
    Input("signal-modal-close", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_signal_modal(open_clicks, close_clicks):
    _modal_hide = {"display": "none", "position": "fixed", "top": "0", "left": "0",
                   "width": "100vw", "height": "100vh",
                   "backgroundColor": "rgba(0,0,0,0.5)", "zIndex": "1000",
                   "justifyContent": "center", "alignItems": "center"}
    _modal_show = {**_modal_hide, "display": "flex"}
    if dash.ctx.triggered_id == "signal-modal-open":
        return _modal_show
    return _modal_hide


# Save signal settings to DB whenever they change
_save_signal_inputs = [
    Input("ma-select", "value"),
    Input("wave-select", "value"),
]
for _cat in get_signal_categories():
    _save_signal_inputs.append(Input(f"signals-{_cat}", "value"))


@app.callback(
    Output("signal-modal-backdrop", "className"),  # dummy output
    *_save_signal_inputs,
    prevent_initial_call=True,
)
def save_signal_settings(ma_val, wave_val, *signal_vals):
    _save_signal_setting("ma_select", ma_val or [])
    _save_signal_setting("wave_select", wave_val or [])
    cats = list(get_signal_categories().keys())
    for cat, val in zip(cats, signal_vals):
        _save_signal_setting(f"signals_{cat}", val or [])
    return dash.no_update


# Build dynamic inputs for signal checklists
_signal_states = []
for _cat in get_signal_categories():
    _signal_states.append(State(f"signals-{_cat}", "value"))


# Callback: Sidebar tab switching
@app.callback(
    Output("tab-watchlist-content", "style"),
    Output("tab-inventory-content", "style"),
    Output("tab-screener-content", "style"),
    Output("tab-watchlist-btn", "style"),
    Output("tab-inventory-btn", "style"),
    Output("tab-screener-btn", "style"),
    Input("tab-watchlist-btn", "n_clicks"),
    Input("tab-inventory-btn", "n_clicks"),
    Input("tab-screener-btn", "n_clicks"),
    prevent_initial_call=True,
)
def switch_sidebar_tab(wl_clicks, inv_clicks, sc_clicks):
    active_style = {"flex": "1", "textAlign": "center", "padding": "8px 0",
                    "cursor": "pointer", "fontSize": "13px", "color": "#eee",
                    "borderBottom": "2px solid #2196f3"}
    inactive_style = {"flex": "1", "textAlign": "center", "padding": "8px 0",
                      "cursor": "pointer", "fontSize": "13px", "color": "#888",
                      "borderBottom": "2px solid transparent"}
    show = {"padding": "12px", "display": "block"}
    hide = {"padding": "12px", "display": "none"}

    triggered = dash.ctx.triggered_id
    if triggered == "tab-inventory-btn":
        return hide, show, hide, inactive_style, active_style, inactive_style
    if triggered == "tab-screener-btn":
        return hide, hide, show, inactive_style, inactive_style, active_style
    return show, hide, hide, active_style, inactive_style, inactive_style


# Wave visibility is controlled by JS in index_string


# Callback 1: Save user-drawn shapes when relayout fires
@app.callback(
    Output("shapes-store", "data"),
    Input("chart", "relayoutData"),
    State("shapes-store", "data"),
    prevent_initial_call=True,
)
def save_shapes(relayout_data, stored_shapes):
    if relayout_data and "shapes" in relayout_data:
        return relayout_data["shapes"]
    return dash.no_update


# Callback 2: Load data and build chart (only fires on data change)
@app.callback(
    Output("chart", "figure"),
    Output("status-msg", "children"),
    Input("load-btn", "n_clicks"),
    Input("stock-input", "value"),
    Input("start-date", "value"),
    Input("end-date", "value"),
    State("ma-select", "value"),
    State("wave-select", "value"),
    *_signal_states,
    State("shapes-store", "data"),
    prevent_initial_call=True,
)
def update_chart(n_clicks, stock_id, start_date, end_date,
                 ma_select, wave_select, *args):
    n_signal_cats = len(get_signal_categories())
    signal_values = list(args[:n_signal_cats])
    stored_shapes = args[n_signal_cats]

    if not stock_id or not start_date or not end_date:
        return dash.no_update, "Please enter stock ID and date range."

    # Fetch data
    data = fetch_price_data(stock_id, start_date, end_date)
    if data is None:
        empty_fig = build_candlestick_figure(
            [], np.array([]), np.array([]), np.array([]),
            np.array([]), np.array([]),
        )
        return empty_fig, f"No data found for {stock_id}."

    # Run analysis
    analysis_results = {}
    try:
        analysis_results["candle"] = calculate_candle(
            data["open"], data["high"], data["low"], data["close"],
        )
        analysis_results["volume"] = calculate_volume(data["sub_value"])
        analysis_results["close"] = calculate_close(data["close"])
    except Exception as e:
        pass  # Partial analysis is ok, signals just won't show

    # Generate ALL signal markers (visibility controlled by JS)
    all_signal_keys = [sd.key for sd in SIGNAL_DEFS]
    markers = generate_markers(
        data["dates"], data["high"], data["low"],
        analysis_results, all_signal_keys,
    )

    # Build ALL SMA lines (visibility controlled by JS)
    sma_lines = {}
    if "close" in analysis_results:
        close_result = analysis_results["close"]
        for period in [3, 5, 8, 13, 21, 34, 55, 89, 144, 233]:
            if period in close_result.ma.sma:
                sma_lines[f"MA{period}"] = close_result.ma.sma[period]

    # Build wave lines (always computed, visibility controlled by JS)
    wave_lines = []
    if "candle" in analysis_results and "close" in analysis_results:
        try:
            candle_r = analysis_results["candle"]
            close_r = analysis_results["close"]
            vol_r = analysis_results.get("volume")
            wave_r = calculate_wave(
                data["open"], data["high"], data["low"], data["close"],
                candle_r, close_r.bs,
                volume=data["sub_value"] if vol_r else None,
            )
            wl = wave_r.waves
            dates = data["dates"]

            if wl.count() >= 2:
                w_dates = [dates[idx] for idx in wl.day_idx]
                w_prices = list(wl.tip)
                wave_lines.append({
                    "dates": w_dates,
                    "prices": w_prices,
                    "name": "Wave",
                    "color": "#ffd54f",
                })

                wf_groups = {
                    "浪瀑(多)": {"dates": [], "prices": [], "color": "#ef5350"},
                    "浪瀑(空)": {"dates": [], "prices": [], "color": "#26a69a"},
                    "浪溝": {"dates": [], "prices": [], "color": "#9e9e9e"},
                }
                for i in range(wl.count()):
                    idx = wl.day_idx[i]
                    if wl.waterfall[i]:
                        key = "浪瀑(多)" if wl.wave[i] else "浪瀑(空)"
                        wf_groups[key]["dates"].append(dates[idx])
                        wf_groups[key]["prices"].append(wl.tip[i])
                    if wl.water_ditch[i]:
                        wf_groups["浪溝"]["dates"].append(dates[idx])
                        wf_groups["浪溝"]["prices"].append(wl.tip[i])
                for name, grp in wf_groups.items():
                    if grp["dates"]:
                        wave_lines.append({
                            "dates": grp["dates"],
                            "prices": grp["prices"],
                            "name": name,
                            "color": grp["color"],
                        })
        except Exception:
            pass

    # Collect currently enabled signals for initial visibility
    enabled = set()
    for sv in signal_values:
        if sv:
            enabled.update(sv)

    # Build figure with ALL data pre-loaded, visibility set by current state
    fig = build_candlestick_figure(
        dates=data["dates"],
        open_=data["open"],
        high=data["high"],
        low=data["low"],
        close=data["close"],
        sub_value=data["sub_value"],
        is_index=data["is_index"],
        signals=markers,
        all_signal_defs=SIGNAL_DEFS,
        enabled_signals=enabled,
        sma_lines=sma_lines,
        enabled_ma=set(ma_select) if ma_select else set(),
        wave_lines=wave_lines if wave_lines else None,
        wave_visible=set(wave_select) if wave_select else set(),
        title=f"{stock_id} {data['name']}",
    )

    # Restore user-drawn shapes
    if stored_shapes:
        fig.update_layout(shapes=stored_shapes)

    n_days = len(data["dates"])
    status = f"Loaded {n_days} trading days | {len(markers)} signal markers"
    return fig, status


# ── Watchlist callbacks ────────────────────────────────────────────────────


@app.callback(
    Output("watchlist-store", "data"),
    Output("watchlist-add-input", "value"),
    Input("watchlist-add-btn", "n_clicks"),
    State("watchlist-add-input", "value"),
    State("watchlist-store", "data"),
    prevent_initial_call=True,
)
def add_to_watchlist(n_clicks, new_stock, wl_data):
    if not new_stock or not new_stock.strip():
        return dash.no_update, ""
    stock = new_stock.strip().upper()
    group = "自選股"
    _add_watchlist_item(group, stock)
    return _load_watchlist(), ""


@app.callback(
    Output("watchlist-container", "children"),
    Input("watchlist-store", "data"),
)
def render_watchlist(wl_data):
    if not wl_data:
        return html.Div("(empty)", style={"color": "#666", "fontSize": "13px"})

    items = []
    for group, stocks in wl_data.items():
        for entry in stocks:
            if isinstance(entry, dict):
                stock_id = entry["id"]
                stock_name = entry.get("name", stock_id)
                close = entry.get("close")
                chg = entry.get("change")
                chg_pct = entry.get("change_pct")
            else:
                stock_id, stock_name = entry, entry
                close = chg = chg_pct = None

            # Color: red for up, green for down (TW convention)
            if chg is not None and chg > 0:
                price_color = "#ef5350"
                sign = "+"
            elif chg is not None and chg < 0:
                price_color = "#26a69a"
                sign = ""
            else:
                price_color = "#888"
                sign = ""

            close_str = f"{close:,.2f}" if close is not None else "-"
            chg_str = f"{sign}{chg:,.2f}" if chg is not None else "-"
            pct_str = f"{sign}{chg_pct:,.2f}%" if chg_pct is not None else "-"

            items.append(
                html.Div(
                    style={"padding": "5px 6px", "borderRadius": "4px",
                           "cursor": "pointer", "marginBottom": "2px"},
                    className="watchlist-item",
                    id={"type": "watchlist-stock", "index": stock_id},
                    n_clicks=0,
                    children=[
                        # Row 1: stock id + name ... x button
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-between",
                                   "alignItems": "center"},
                            children=[
                                html.Span(
                                    f"{stock_id} {stock_name}",
                                    style={"color": "#eee", "fontSize": "13px"},
                                ),
                                html.Span(
                                    "x",
                                    id={"type": "watchlist-remove", "index": stock_id},
                                    n_clicks=0,
                                    style={"color": "#888", "cursor": "pointer",
                                           "fontSize": "11px", "padding": "0 2px"},
                                ),
                            ],
                        ),
                        # Row 2: price | change | change%
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-between",
                                   "marginTop": "2px", "fontSize": "12px"},
                            children=[
                                html.Span(close_str, style={"color": price_color}),
                                html.Span(chg_str, style={"color": price_color}),
                                html.Span(pct_str, style={"color": price_color}),
                            ],
                        ),
                    ],
                )
            )
    return items


# Click pinned TAIEX -> update stock-input
@app.callback(
    Output("stock-input", "value", allow_duplicate=True),
    Input("pinned-taiex", "n_clicks"),
    prevent_initial_call=True,
)
def select_taiex(n_clicks):
    if n_clicks:
        return "TAIEX"
    return dash.no_update


# Click stock in watchlist -> update stock-input
@app.callback(
    Output("stock-input", "value", allow_duplicate=True),
    Input({"type": "watchlist-stock", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def select_watchlist_stock(n_clicks_list):
    if not n_clicks_list or not any(n_clicks_list):
        return dash.no_update
    if not dash.ctx.triggered_id:
        return dash.no_update
    triggered = dash.ctx.triggered_id
    if isinstance(triggered, dict) and triggered.get("type") == "watchlist-stock":
        return triggered["index"]
    return dash.no_update


# Remove stock from watchlist
@app.callback(
    Output("watchlist-store", "data", allow_duplicate=True),
    Input({"type": "watchlist-remove", "index": dash.ALL}, "n_clicks"),
    State("watchlist-store", "data"),
    prevent_initial_call=True,
)
def remove_from_watchlist(n_clicks_list, wl_data):
    # Guard: ignore if no real click occurred (page load can fire this)
    if not n_clicks_list or not any(n_clicks_list):
        return dash.no_update
    if not dash.ctx.triggered_id:
        return dash.no_update
    triggered = dash.ctx.triggered_id
    if isinstance(triggered, dict) and triggered.get("type") == "watchlist-remove":
        stock = triggered["index"]
        _remove_watchlist_item(stock)
        return _load_watchlist()
    return dash.no_update


# Click inventory stock -> update stock-input
@app.callback(
    Output("stock-input", "value", allow_duplicate=True),
    Input({"type": "inventory-stock", "index": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def select_inventory_stock(n_clicks_list):
    if not n_clicks_list or not any(n_clicks_list):
        return dash.no_update
    if not dash.ctx.triggered_id:
        return dash.no_update
    triggered = dash.ctx.triggered_id
    if isinstance(triggered, dict) and triggered.get("type") == "inventory-stock":
        return triggered["index"]
    return dash.no_update


# ── Entry point ────────────────────────────────────────────────────────────


def main():
    import webbrowser
    port = 8050
    print(f"Starting chart server at http://127.0.0.1:{port}")
    webbrowser.open(f"http://127.0.0.1:{port}")
    app.run(debug=False, port=port)


if __name__ == "__main__":
    main()
