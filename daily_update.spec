# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['daily_update.py'],
    pathex=[],
    binaries=[],
    datas=[('db/migrations', 'db/migrations')],
    hiddenimports=['scrapers.twse', 'scrapers.tpex', 'scrapers.twse_after_hours', 'scrapers.tpex_after_hours', 'scrapers.tpex_emerging', 'scrapers.odd_lot', 'scrapers.margin', 'scrapers.price_limits', 'scrapers.institutional', 'scrapers.index_prices', 'scrapers.revenue', 'scrapers.etf_holdings', 'scrapers.sitca', 'scrapers.securities_lending', 'scrapers.day_trading', 'scrapers.stock_alerts', 'scrapers.dividend_calendar', 'scrapers.shareholder_distribution', 'scrapers.insider_holdings', 'scrapers.insider_pledge_events', 'scrapers.treasury_stock', 'analysis.market_breadth', 'analysis.close', 'analysis.money', 'analysis.volume', 'analysis.daily_snapshot', 'analysis.tx_status', 'scrapers.ftse_taiwan', 'analysis.disposal_audit', 'analysis.score', 'analysis.daily_liquidity', 'analysis.chandelier', 'analysis.revenue_three_arrows', 'analysis.revenue_turnaround', 'analysis.revenue_streak', 'analysis.revenue_off_season', 'analysis.revenue_deceleration', 'analysis.revenue_decline', 'analysis.revenue_historic_low', 'analysis.revenue_peak_miss', 'signal_backtest.signal', 'signal_backtest.engine', 'signal_backtest.trade', 'signal_backtest.factories._conditions', 'signal_backtest.factories.unified', 'signal_backtest.factories.pick_touch', 'signal_backtest.factories.buy_sell', 'signal_backtest.factories.flee', 'signal_backtest.factories.macd', 'backtest.data', 'scan_signals', 'strategies', 'strategies.registry', 'strategies.base', 'strategies.quarterly_to_monthly_top10', 'strategies.quarterly_dormant_etf_active', 'strategies.dual_track_entry', 'strategies.multi_fund_consensus', 'strategies.consecutive_accumulation', 'strategies.dual_track_accumulation', 'strategies.consensus_formation', 'strategies.heavy_position_reduction', 'strategies.core_exit', 'strategies.utils', 'strategies.etf_multi_consensus', 'strategies.etf_consecutive_accumulation', 'strategies.etf_abnormal_position', 'signals', 'signals.etf_multi_exit', 'signals.etf_consecutive_reduction', 'signals.etf_abnormal_exit', 'export', 'export.generate', 'backtest', 'backtest.signals', 'numpy', 'telegram_bot.notify', 'telegram_bot.config', 'telegram_bot.handlers.score', 'telegram_bot.handlers._data_freshness', 'telegram_bot.handlers._attention_predict_intraday', 'analysis.attention_predict', 'intraday.estimate', 'intraday.store', 'scrapers.cnn_feargreed', 'scrapers.taifex_common', 'scrapers.taifex_futures', 'scrapers.taifex_institutional', 'scrapers.taifex_pc_ratio', 'scrapers.market_quote', 'scrapers.market_html', 'analysis.build_market_vol'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='daily_update',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
