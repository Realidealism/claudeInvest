"""季節題材行事曆 (綜合型 event calendar).

Recurring, time-of-year themes worth watching, each as a 12-month band: level 0
= off, 1 = 注意 (lead-in, start watching), 2 = 活躍 (window open). Windows are
grounded in the data (short_cover_calendar / dividend_calendar month histograms),
not guessed. AGM squeeze also carries a live count so 本月焦點 reflects the actual
upcoming cover density; other themes are static seasonal reminders for now.

The registry is the extension point — append a theme dict to THEMES to add a
reminder; wire a live-enrichment branch in build_calendar() if it has a data feed.
"""

from __future__ import annotations

from datetime import date, timedelta

# months: 12 ints (Jan..Dec), 0=off / 1=注意 / 2=活躍.
THEMES = [
    {
        "key": "agm_squeeze",
        "name": "股東會軋空回補",
        "note": "重融券股在股東會停過戶前強制回補，高 dtc 個股回補日前軋空；回補潮集中 3–4 月，2 月起注意。",
        "link": "/cover-squeeze",
        "color": "#3b82f6",
        "months": [0, 1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0],
    },
    {
        "key": "ex_dividend",
        "name": "除權息旺季",
        "note": "除權息除息日集中 7–8 月，6 月起注意；觀察填權息行情與除息前融券最後回補。",
        "link": None,
        "color": "#f59e0b",
        "months": [0, 0, 0, 0, 0, 1, 2, 2, 0, 0, 0, 0],
    },
    {
        "key": "financial_reports",
        "name": "財報公布季",
        "note": "季報截止：年報 3/31、Q1 5/15、Q2 8/14、Q3 11/14；財報行情與地雷觀察。",
        "link": None,
        "color": "#a855f7",
        "months": [0, 0, 2, 0, 2, 0, 0, 2, 0, 0, 2, 0],
    },
    {
        "key": "trust_window_dressing",
        "name": "投信作帳行情",
        "note": "投信季底衝績效拉抬重壓股，季底 3/6/9/12 月；配投信買賣超（trust_net）觀察連買股。",
        "link": None,
        "color": "#14b8a6",
        "months": [0, 0, 2, 0, 0, 2, 0, 0, 2, 0, 0, 2],
    },
    {
        "key": "index_rebalance",
        "name": "指數成分股調整",
        "note": "MSCI 半年度（5/11 月）最大、季度（2/8 月）；台灣50／高股息 ETF 季調（3/6/9/12 月）；被動資金於生效日前後買賣。",
        "link": None,
        "color": "#ec4899",
        "months": [0, 1, 0, 0, 2, 0, 0, 1, 0, 0, 2, 0],
    },
    {
        "key": "year_end_dressing",
        "name": "年底作帳＋元月行情",
        "note": "投信／集團年底作帳（11–12 月）＋元月資金行情（1 月）；重壓股與強勢股年底延續。",
        "link": None,
        "color": "#6366f1",
        "months": [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2],
    },
    {
        "key": "lunar_new_year",
        "name": "農曆年封關／開紅盤",
        "note": "農曆年封關前量縮觀望、開紅盤日紅盤效應；日期依農曆浮動（約 1 月下旬–2 月上旬）。",
        "link": None,
        "color": "#f43f5e",
        "months": [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    },
]


def _agm_live(cur, today: date) -> dict | None:
    """Upcoming 股東會 forced-cover density in the next 45 days (None if off-season)."""
    cur.execute(
        """SELECT COUNT(DISTINCT stock_id) n FROM tw.short_cover_calendar
           WHERE reason = %s AND last_cover_date >= %s AND last_cover_date <= %s""",
        ("股東會", today, today + timedelta(days=45)),
    )
    n = cur.fetchone()["n"]
    return {"label": f"未來 45 天 {n} 檔股東會回補", "count": n} if n else None


def build_calendar(cur, today: date | None = None) -> dict:
    today = today or date.today()
    live_builders = {"agm_squeeze": _agm_live}

    themes = []
    for t in THEMES:
        live = live_builders[t["key"]](cur, today) if t["key"] in live_builders else None
        themes.append({**t, "live": live})

    return {"year": today.year, "current_month": today.month,
            "as_of": today, "themes": themes}


if __name__ == "__main__":
    from db.connection import get_cursor

    with get_cursor(commit=False) as cur:
        cal = build_calendar(cur)
    print(f"year={cal['year']} current_month={cal['current_month']}")
    names = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
    print("            " + " ".join(f"{m:>2}" for m in names))
    for t in cal["themes"]:
        band = " ".join(("██" if lv == 2 else "░░" if lv == 1 else "  ") for lv in t["months"])
        live = f"  <{t['live']['label']}>" if t["live"] else ""
        print(f"{t['name']:<10} {band}{live}")
