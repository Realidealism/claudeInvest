"""Equal-weight portfolio with explicit transaction-cost accounting.

We track *synthetic* share counts that, when multiplied by adjusted close,
give cash-equivalent value. Adjusted close already encodes dividend
reinvestment, so dividends do not need a separate cash-flow leg.

Cost model (design §13):
    buy:  pay  shares * price * (1 + handling + slippage)   = 1.002425
    sell: get  shares * price * (1 - handling - tax - slip) = 0.994575
    round-trip cost ≈ 0.79%
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date

HANDLING_FEE = 0.001425
TRANSFER_TAX = 0.003
SLIPPAGE = 0.001
BUY_COST = HANDLING_FEE + SLIPPAGE
SELL_COST = HANDLING_FEE + TRANSFER_TAX + SLIPPAGE


@dataclass
class Holding:
    ticker: str
    shares: float
    entered_at: date
    is_new_this_period: bool = False


@dataclass
class Trade:
    date: date
    ticker: str
    side: str  # 'BUY' | 'SELL'
    shares: float
    price: float
    cash_delta: float  # signed; +ve for sells, -ve for buys


@dataclass
class Portfolio:
    cash: float
    holdings: dict[str, Holding] = field(default_factory=dict)
    nav_history: dict[date, float] = field(default_factory=dict)
    trade_log: list[Trade] = field(default_factory=list)
    holdings_log: list[tuple[date, list[Holding]]] = field(default_factory=list)

    def mark_to_market(self, d: date, prices: Mapping[str, float]) -> float:
        nav = self.cash
        for t, h in self.holdings.items():
            p = prices.get(t)
            if p is None or p <= 0:
                continue
            nav += h.shares * p
        self.nav_history[d] = nav
        return nav

    def rebalance_equal_weight(
        self,
        d: date,
        target_tickers: list[str],
        prices: Mapping[str, float],
    ) -> None:
        """Sell anything not in `target_tickers`, then equal-weight the rest.

        New entries (in target but not currently held) get is_new_this_period=True
        for the reporting layer; existing holdings stay False unless re-entered.
        """
        # Compute current NAV from prices
        cur_nav = self.cash
        for t, h in self.holdings.items():
            p = prices.get(t)
            if p is None or p <= 0:
                continue
            cur_nav += h.shares * p

        target_set = set(target_tickers)

        # 1) Sell tickers not in target
        for t in list(self.holdings.keys()):
            if t in target_set:
                continue
            p = prices.get(t)
            if p is None or p <= 0:
                # No price; cannot liquidate. Hold.
                continue
            shares = self.holdings[t].shares
            proceeds = shares * p * (1.0 - SELL_COST)
            self.cash += proceeds
            self.trade_log.append(Trade(d, t, "SELL", shares, p, proceeds))
            del self.holdings[t]

        # Mark NEW status for old holdings to False (carried forward)
        for h in self.holdings.values():
            h.is_new_this_period = False

        # 2) Buy / re-balance target tickers to equal weight
        priced_targets = [t for t in target_tickers if prices.get(t, 0) > 0]
        if not priced_targets:
            self.holdings_log.append((d, []))
            return

        # Use post-sell NAV for allocation; we re-derive after the sells above
        avail_nav = self.cash
        for t in priced_targets:
            if t in self.holdings:
                avail_nav += self.holdings[t].shares * prices[t]
        per_slot = avail_nav / len(priced_targets)

        for t in priced_targets:
            p = prices[t]
            target_value = per_slot
            cur_value = self.holdings[t].shares * p if t in self.holdings else 0.0
            delta_value = target_value - cur_value
            if abs(delta_value) < 1.0:  # < 1 NTD, skip
                continue
            if delta_value > 0:
                # buy more
                cost_per_share = p * (1.0 + BUY_COST)
                shares_to_buy = delta_value / cost_per_share
                spend = shares_to_buy * cost_per_share
                self.cash -= spend
                if t in self.holdings:
                    self.holdings[t].shares += shares_to_buy
                else:
                    self.holdings[t] = Holding(
                        ticker=t,
                        shares=shares_to_buy,
                        entered_at=d,
                        is_new_this_period=True,
                    )
                self.trade_log.append(Trade(d, t, "BUY", shares_to_buy, p, -spend))
            else:
                # sell some
                shares_to_sell = -delta_value / p
                proceeds = shares_to_sell * p * (1.0 - SELL_COST)
                self.cash += proceeds
                self.holdings[t].shares -= shares_to_sell
                self.trade_log.append(Trade(d, t, "SELL", shares_to_sell, p, proceeds))

        self.holdings_log.append((d, list(self.holdings.values())))
