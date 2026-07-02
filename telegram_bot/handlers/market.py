"""`/market` — 市場面概況：台股寬度、籌碼、風險情緒、趨勢變化。

Reads the dashboard JSON snapshots refreshed by the (now automated)
intraday / EOD pipeline — no DB access. breadth.json is required; the
auxiliary sentiment/chip sources (margin / fear_greed / vix / yield_curve)
degrade gracefully — a missing file or key just drops that line.

breadth.json's latest row carries `is_intraday` while the session is open,
in which case its figures are live estimates. The macro sources publish on
their own cadence; lines whose date lags the breadth date are tagged.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from telegram_bot.auth import restricted

logger = logging.getLogger(__name__)

_TPE = timezone(timedelta(hours=8))
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DATA_DIR = _REPO_ROOT / "frontend" / "public" / "data"

# Trend code → (label, emoji). Codes match analysis.market_breadth.TREND_CODE
# (json stores the sort_normal variant, range -2..2; no exhausting ±3).
_TREND_LABEL: dict[int, tuple[str, str]] = {
    2: ("明確多", "🔥"),
    1: ("偏多", "🔴"),
    0: ("中性", "➖"),
    -1: ("偏空", "🟢"),
    -2: ("明確空", "🧊"),
}

_SCOPES = (("s", "短線"), ("m", "中線"), ("l", "長線"))

_FG_LABEL = {
    "extreme fear": ("極度恐懼", "😱"),
    "fear": ("恐懼", "😨"),
    "neutral": ("中性", "😐"),
    "greed": ("貪婪", "😋"),
    "extreme greed": ("極度貪婪", "🤑"),
}
# VIX ratings (export.generate._vix_rating): calm < p20, low < p50,
# elevated < p80, panic ≥ p80. Faces ascend calm → panic.
_VIX_LABEL = {
    "calm": ("平靜", "😋"),
    "low": ("偏低", "😐"),
    "elevated": ("偏高", "😨"),
    "panic": ("恐慌", "😱"),
}


def _weekday_zh(d: date) -> str:
    return "週" + "一二三四五六日"[d.weekday()]


def _pct_cell(prefix: str, cur_frac, prev_frac) -> str:
    """'漲48%↑' — integer pct with a thin up/down arrow vs the previous day
    (compared on the displayed rounded value; equal → no arrow)."""
    cur = round((cur_frac or 0) * 100)
    if prev_frac is None:
        return f"{prefix}{cur}%"
    prev = round((prev_frac or 0) * 100)
    mark = "▲" if cur > prev else ("▼" if cur < prev else "")
    return f"{prefix}{cur}%{mark}"


def _load(name: str) -> dict | None:
    try:
        with (_DATA_DIR / name).open(encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _date_suffix(ind_date: str | None, base_date: str) -> str:
    """' (MM/DD)' when the indicator's date lags the breadth date, else ''."""
    if not ind_date or ind_date == base_date:
        return ""
    return f" ({ind_date[5:].replace('-', '/')})"


# ── Section builders ──────────────────────────────────────────────────────


def _build_chips(base_date: str) -> list[str] | None:
    """台股融資融券 from margin.json (張 → 萬張)."""
    d = _load("margin.json")
    if not d:
        return None
    try:
        ser = d["series"]
        s = ser[-1]
        prev = ser[-2] if len(ser) >= 2 else None
        st = d["stats"]
        val = s["margin_balance_value"] / 100000  # 千元 → 億元
        ratio = s["short_to_margin_pct"]
        pct = st["margin_balance_pct_3y"]
        dval = (s["margin_balance_value"] - prev["margin_balance_value"]) / 100000 if prev else None
    except (KeyError, IndexError, TypeError):
        return None
    emoji = "⚠️" if (pct >= 90 or pct <= 10) else ""
    suf = _date_suffix(d.get("latest_date"), base_date)
    day = f"　單日 {dval:+,.0f}億" if dval is not None else ""
    return [
        f"🔧 台股籌碼{suf}",
        f"  融資  {val:,.0f}億・3年分位{pct:.1f}% {emoji}{day}".rstrip(),
        f"  券資比  {ratio:.2f}%",
    ]


_TX_LABEL = {"long": "做多中", "short": "做空中", "flat": "空手"}
# entry_tier code → 中文 (mirror unified factory tier names).
_TX_TIER = {
    "pick": "抄底", "buy": "波段多", "sell_flee": "空翻多",
    "touch": "摸頭", "sell": "波段空", "buy_flee": "多翻空",
}


def _defense_emoji(pos: dict | None) -> str:
    """Defense-proximity bands, mirroring the watchlist one-line lead emoji
    (telegram_bot.handlers.score): ⚪ no position, 🟢 >5% from defense,
    🟡 ≤5%, 🟠 ≤3%, 🔴 ≤1%, ❌ already exited."""
    if pos is None:
        return "⚪"
    if pos.get("is_exited"):
        return "❌"
    defense = pos.get("defense_price")
    current = pos.get("current_close")
    if defense is None or current is None or float(defense) == 0:
        return "🟢"
    dist = abs(float(current) - float(defense)) / float(defense)
    if dist <= 0.01:
        return "🔴"
    if dist <= 0.03:
        return "🟠"
    if dist <= 0.05:
        return "🟡"
    return "🟢"


def _build_futures(base_date: str) -> list[str] | None:
    """大台期貨統一狀態 from futures_tx.json (做多/做空/空手 + 進場/防守)."""
    d = _load("futures_tx.json")
    if not d:
        return None
    try:
        state = d["state"]
        pos = d.get("position")
        label = _TX_LABEL.get(state, state)
        emoji = _defense_emoji(pos if state in ("long", "short") else None)
        td = d.get("trade_date", "")
        tag = "盤中" if d.get("is_intraday") else "收盤"
        close = d.get("current_close")
        suf = _date_suffix(td, base_date)
        head = f"📈 大台期貨訊號（{tag}{suf}）".rstrip()
        if close is not None:
            head += f"　{close:,.0f}"
        lines = [head]

        if state in ("long", "short") and pos:
            tier = _TX_TIER.get(pos.get("entry_tier"), pos.get("entry_tier") or "")
            ep = pos.get("entry_price")
            pnl = pos.get("pnl_pct")
            bars = pos.get("bars_held")
            detail = f"  統一狀態：{label} {emoji}"
            seg = []
            if tier:
                seg.append(f"{tier}進場")
            if ep is not None:
                seg.append(f"@{ep:,.0f}")
            if pnl is not None:
                seg.append(f"{pnl:+.1f}%")
            if bars is not None:
                seg.append(f"持有{bars}日")
            if seg:
                detail += "（" + "・".join(seg) + "）"
            lines.append(detail)
            dp = pos.get("defense_price")
            if dp is not None:
                move = ""
                if pos.get("defense_moved"):
                    arrow = "⬆️上移" if pos.get("side") == "long" else "⬇️下移"
                    prev = pos.get("defense_prev")
                    delta = f" +{abs(dp - prev):,.0f}" if prev is not None else ""
                    move = f"　{arrow}{delta}"
                lines.append(f"  防守價  {dp:,.0f}{move}")
        else:
            note = ""
            if pos and pos.get("is_exited"):
                emoji = _defense_emoji(pos)  # ❌ 今日出場
                note = "（今日出場）"
            lines.append(f"  統一狀態：{label} {emoji}{note}")
        return lines
    except (KeyError, TypeError):
        return None


def _build_risk(base_date: str) -> list[str] | None:
    """美股/總經風險情緒：恐懼貪婪 + VIX + 殖利率曲線。"""
    lines: list[str] = []

    fg = _load("fear_greed.json")
    if fg:
        try:
            ser = fg["series"]
            cur, prev = ser[-1]["score"], ser[-2]["score"]
            label, emoji = _FG_LABEL.get(fg["latest"]["rating"], (fg["latest"]["rating"], ""))
            delta = cur - prev
            arrow = "↑" if delta > 0.05 else ("↓" if delta < -0.05 else "持平")
            suf = _date_suffix(fg.get("latest_date"), base_date)
            lines.append(f"  恐懼貪婪  {cur:.1f} {label} {emoji}（{arrow}）{suf}".rstrip())
        except (KeyError, IndexError, TypeError):
            pass

    vix = _load("vix.json")
    if vix:
        for region, name in (("us", "美股VIX"), ("tw", "台指VIX")):
            blk = vix.get(region)
            if not blk:
                continue
            try:
                lt = blk["latest"]
                label, emoji = _VIX_LABEL.get(lt["rating"], (lt["rating"], ""))
                p80 = blk["thresholds"]["p80"]
                em = f" {emoji}" if emoji else ""
                itime = lt.get("intraday_time")
                if itime and len(itime) >= 4:
                    tag = f" 盤中{itime[:2]}:{itime[2:4]}"
                else:
                    bdate = (blk.get("series") or [{}])[-1].get("date")
                    tag = _date_suffix(bdate, base_date)
                lines.append(f"  {name}  {lt['close']:.1f} {label}{em}（p80={p80:.1f}）{tag}".rstrip())
            except (KeyError, IndexError, TypeError):
                pass

    yc = _load("yield_curve.json")
    if yc:
        try:
            sp2 = yc["spread_2y"]["latest"]["spread"]
            sp3 = yc["spread_3m"]["latest"]["spread"]
            # 10Y-3M is the more recession-predictive spread; drive the
            # plain-language read off it, flag inversion from either spread.
            if sp2 < 0 or sp3 < 0:
                head = "倒掛、衰退預警 ⚠️"
            else:
                head = {
                    "flat": "未倒掛、景氣趨緩",
                    "normal": "未倒掛、景氣擴張中",
                    "steep": "未倒掛、景氣強勁",
                }.get(yc["spread_3m"]["latest"]["rating"], "未倒掛")
            suf = _date_suffix(yc.get("latest_date"), base_date)
            lines.append(f"  殖利率  {head}")
            lines.append(f"    10Y-2Y {sp2:+.2f}／10Y-3M {sp3:+.2f}{suf}")
        except (KeyError, IndexError, TypeError):
            pass

    if not lines:
        return None
    return ["🌐 風險情緒（美股/總經）"] + lines


def build_market_message() -> str:
    """Compose the market overview. Raises on missing/empty breadth data."""
    data = _load("breadth.json")
    series = (data or {}).get("series") or []
    if not series:
        raise ValueError("breadth.json series 為空")

    latest = series[-1]
    prev = series[-2] if len(series) >= 2 else None
    base_date = latest["date"]
    d = datetime.strptime(base_date, "%Y-%m-%d").date()

    # Header / freshness
    if latest.get("is_intraday"):
        iso = latest.get("intraday_time", "")
        hhmm = iso[11:16] if len(iso) >= 16 else ""
        freshness = f"盤中 {hhmm}（即時估算）" if hhmm else "盤中（即時估算）"
    else:
        freshness = f"收盤 {base_date}"
    parts = [f"[市場概況] {base_date} {_weekday_zh(d)}\n資料：{freshness}"]

    # 三尺度寬度。今日趨勢碼較前一交易日上升標 ↑、下降標 ↓（今天轉向）。
    width_lines = ["📊 市場寬度（漲/中/跌）"]
    for key, name in _SCOPES:
        trend = latest.get(f"{key}_trend", 0)
        emoji = _TREND_LABEL.get(trend, ("?", "❔"))[1]
        pv = prev or {}
        up_c = _pct_cell("漲", latest.get(f"{key}_up"), pv.get(f"{key}_up") if prev else None)
        neu_c = _pct_cell("中", latest.get(f"{key}_neu"), pv.get(f"{key}_neu") if prev else None)
        dn_c = _pct_cell("跌", latest.get(f"{key}_dn"), pv.get(f"{key}_dn") if prev else None)
        turn = ""
        if prev is not None:
            prev_t = prev.get(f"{key}_trend", 0)
            turn = "　⬆️" if trend > prev_t else ("　⬇️" if trend < prev_t else "")
        width_lines.append(f"  {name}  {emoji}　{up_c} {neu_c} {dn_c}{turn}")
    parts.append("\n".join(width_lines))

    # 大台期貨統一狀態
    futures = _build_futures(base_date)
    if futures:
        parts.append("\n".join(futures))

    # 台股籌碼 / 風險情緒
    chips = _build_chips(base_date)
    if chips:
        parts.append("\n".join(chips))
    risk = _build_risk(base_date)
    if risk:
        parts.append("\n".join(risk))

    return "\n\n".join(parts)


@restricted
async def cmd_market(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        msg = build_market_message()
    except Exception as exc:
        logger.exception("/market build failed: %s", exc)
        await update.message.reply_text(f"市場概況讀取失敗：{exc}")
        return
    await update.message.reply_text(msg)


def register(application) -> None:
    application.add_handler(CommandHandler("market", cmd_market))
