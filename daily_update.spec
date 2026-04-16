# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['daily_update.py'],
    pathex=[],
    binaries=[],
    datas=[('db/migrations', 'db/migrations')],
    hiddenimports=['scrapers.twse', 'scrapers.tpex', 'scrapers.twse_after_hours', 'scrapers.tpex_after_hours', 'scrapers.tpex_emerging', 'scrapers.odd_lot', 'scrapers.margin', 'scrapers.price_limits', 'scrapers.institutional', 'scrapers.index_prices', 'scrapers.revenue', 'scrapers.etf_holdings', 'analysis.market_breadth', 'analysis.close', 'analysis.money', 'analysis.volume'],
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
