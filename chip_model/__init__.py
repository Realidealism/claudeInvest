"""集保大戶選股模型 (chip / TDCC major-shareholder stock-picking model).

Layers:
  db_access — read-only data access (reuses config.settings.DB_CONFIG).
  metrics   — tier-15 (1000+ 張) ratio and its week-over-week change.
  strategy  — parameterized stock-picking rule.
  backtest  — look-ahead-safe forward-return backtest vs TAIEX.

Run end-to-end: python -m chip_model run
"""
