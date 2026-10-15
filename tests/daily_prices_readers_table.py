"""Every .py file that mentions tw.daily_prices, and how it stands on emerging-board prices.

tests/test_daily_prices_readers.py fails when a file mentioning daily_prices is
added or removed without updating this table.  The classification is static
(first round, 2026-10-14, plus the step-1 changes); passing the test means the
listed check points match the design, not that every execution path is proven.

Categories:
  1  codes come from a tw.stocks universe filtered by market
  2  codes are passed in by the caller (user input, watchlist, CLI, upstream table)
  3  whole-table or date-range scan without a code filter
  4  ETF prices only
  5  research / one-off script, or no production caller
  W  writes only
  T  test
  X  mentions the name without touching the table

Handling:
  guarded    checks db.price_availability (or hermit's verbatim copy) before reading
  excludes-U the query itself excludes UNAVAILABLE_CODES_SQL
  unchanged  production reader left as is; the note says why it is not affected
  known-gap  production reader that filters by market only, so it misses U's second
             class (a non-ESB code with X-shaped rows; 0 codes on 2026-10-15)
  cli-only   only reached from a CLI; not guarded in step 1 (ruling on wp v0.2, item 4)
  n/a        categories 5 / W / T / X
"""

READERS = {
    # path: (category, handling, note)
    "analysis/_build_dead_fish_panel.py": ("5", "n/a", "research script"),
    "analysis/_score_vol_calibrate.py": ("5", "n/a", "research script"),
    "analysis/_score_vol_neutral_sim3.py": ("5", "n/a", "research script"),
    "analysis/_score_vol_window_sweep.py": ("5", "n/a", "research script"),
    "analysis/attention_predict.py": ("5", "n/a", "predict_attention only reached from __main__"),
    "analysis/build_market_vol.py": ("1", "known-gap", "market IN ('TWSE','TPEx') universe only"),
    "analysis/chandelier.py": ("5", "n/a", "compute_for_stock has no caller"),
    "analysis/cover_squeeze.py": ("3", "unchanged", "codes come from tw.short_cover_calendar, written by TWSE/TPEx only"),
    "analysis/daily_liquidity.py": ("1", "unchanged", "market-filtered universe, and turnover IS NOT NULL drops X rows"),
    "analysis/donchian.py": ("5", "n/a", "compute_for_stock has no caller"),
    "analysis/financials/peer.py": ("5", "n/a", "not imported anywhere"),
    "analysis/financials/risk_scores.py": ("2", "cli-only", "not guarded in step 1"),
    "analysis/financials/valuation.py": ("2", "cli-only", "not guarded in step 1"),
    "analysis/market_breadth.py": ("1", "known-gap", "market != 'ESB' universe only"),
    "analysis/realtime_data.py": ("2", "guarded", "_fetch_all_daily_prices calls require_available first"),
    "backtest/data.py": ("2", "guarded", "load_stock_data calls require_available before the cache"),
    "backtest/orb_strategy.py": ("5", "n/a", "research script"),
    "backtest/signals.py": ("2", "unchanged", "run_signal_backtest has no caller"),
    "backup_monthly.py": ("3", "unchanged", "whole-table backup"),
    "charts/app.py": ("2", "guarded", "fetch_price_data raises, watchlist prices excluded and labelled"),
    "chip_model/db_access.py": ("2", "excludes-U", "load_common_universe excludes U"),
    "daily_update.py": ("3", "unchanged", ":658 only subtracts; :705 codes come from the TWSE/TPEx universe"),
    "db/price_availability.py": ("X", "n/a", "the shared definition of U itself"),
    "export/generate.py": ("3", "excludes-U", "_month_prices and export_signals exclude U; streaks drop unavailable codes"),
    "hermit_stock/scripts/batch_analyze_top20.py": ("5", "n/a", "research script"),
    "hermit_stock/scripts/compare_elite.py": ("5", "n/a", "research script"),
    "hermit_stock/scripts/compare_gate_variants.py": ("5", "n/a", "research script"),
    "hermit_stock/scripts/compare_robustness.py": ("5", "n/a", "research script"),
    "hermit_stock/scripts/robustness_validation.py": ("5", "n/a", "research script"),
    "hermit_stock/tests/test_price_availability.py": ("T", "n/a", "test, run in hermit's own environment"),
    "hermit_stock/src/hermit_stock/cli.py": ("2", "guarded", "goes through db_adapter price loaders; analyze reports unavailable"),
    "hermit_stock/src/hermit_stock/data/adapters/db_adapter.py": (
        "2", "guarded", "load_daily_prices raises, load_all_daily_prices drops U; "
                        "load_turnover_table is cli-only and not guarded"),
    "hermit_stock/src/hermit_stock/macro/loaders.py": ("3", "unchanged", "reads foreign_net, NULL on every X row"),
    "hermit_stock/src/hermit_stock/reports/screener.py": ("2", "guarded", "skips a ticker whose load_daily_prices raises"),
    "historical_update.py": ("3", "unchanged", "handled in step 2"),
    "passive_buy_00631l/data_layer.py": ("4", "n/a", "ETF prices only"),
    "scrapers/capital_reductions.py": ("1", "known-gap", "market-filtered universe only"),
    "scrapers/day_trading.py": ("W", "n/a", "writer"),
    "scrapers/institutional.py": ("W", "n/a", "writer"),
    "scrapers/margin.py": ("W", "n/a", "writer"),
    "scrapers/odd_lot.py": ("W", "n/a", "writer"),
    "scrapers/price_limits.py": ("W", "n/a", "writer"),
    "scrapers/securities_lending.py": ("W", "n/a", "writer"),
    "scrapers/tpex.py": ("W", "n/a", "writer"),
    "scrapers/tpex_after_hours.py": ("W", "n/a", "writer"),
    "scrapers/tpex_emerging.py": ("W", "n/a", "writer of the emerging-board rows; stopped in step 2"),
    "scrapers/twse.py": ("W", "n/a", "writer"),
    "scrapers/twse_after_hours.py": ("W", "n/a", "writer"),
    "scrapers/vix_cboe.py": ("X", "n/a", "CBOE URL only"),
    "scripts/backfill/backfill_sbl.py": ("5", "n/a", "one-off backfill"),
    "signal_backtest/engine.py": ("X", "n/a", "comment only"),
    "signals/etf_abnormal_exit.py": ("3", "excludes-U", "latest-close query excludes U"),
    "signals/etf_consecutive_reduction.py": ("3", "excludes-U", "latest-close query excludes U"),
    "signals/etf_multi_exit.py": ("3", "excludes-U", "latest-close query excludes U"),
    "strategies/etf_abnormal_position.py": ("3", "excludes-U", "latest-close query excludes U"),
    "strategies/etf_consecutive_accumulation.py": ("3", "excludes-U", "latest-close query excludes U"),
    "strategies/etf_multi_consensus.py": ("3", "excludes-U", "latest-close query excludes U"),
    "strategies/quarterly_dormant_etf_active.py": ("3", "excludes-U", "latest-close query excludes U"),
    "strategies/utils.py": ("2", "guarded", "month_end_prices drops U"),
    "telegram_bot/handlers/_attention_predict_intraday.py": (
        "2", "known-gap", "limited to ALERT_MARKETS only; reached via score._get_disposal_status from "
                          "/score (guarded in _build_reply), export/generate.py (codes from the snapshot, "
                          "which skips U) and morning_brief (codes from load_tw_watchlist, which drops U)"),
    "telegram_bot/handlers/_data_freshness.py": ("3", "unchanged", "only asks whether a date has rows"),
    "telegram_bot/handlers/score.py": ("2", "guarded", "/score entry check; /watch summary marks U unavailable"),
    "telegram_bot/health_check.py": ("3", "unchanged", "only asks whether a date has rows"),
    "telegram_bot/watchers/intraday_volume_alert.py": ("2", "guarded", "codes come from load_tw_watchlist, which drops U"),
    "telegram_bot/watchers/market_volume_brief.py": ("1", "unchanged", "market-filtered universe, sums turnover only (NULL on X rows)"),
    "telegram_bot/watchers/morning_brief.py": ("2", "guarded", "codes come from load_tw_watchlist, which drops U"),
    "tests/daily_prices_readers_table.py": ("T", "n/a", "this table"),
    "tests/test_close_ma.py": ("T", "n/a", "test"),
    "tests/test_daily_prices_readers.py": ("T", "n/a", "test"),
    "tests/test_health_check_backup.py": ("T", "n/a", "test"),
    "tests/test_health_check_corporate_actions.py": ("T", "n/a", "test"),
    "tests/test_price_availability.py": ("T", "n/a", "test"),
    "tests/test_price_availability_guards.py": ("T", "n/a", "test"),
    "tests/test_realtime_data.py": ("T", "n/a", "test"),
    "tests/test_wave_print.py": ("T", "n/a", "test"),
}
