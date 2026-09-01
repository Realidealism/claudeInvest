"""Load company profiles, the ecosystem dictionary, and ecosystem membership,
and snapshot today's official exchange classification.

The official-classification snapshot is the one part that is derived rather than
authored: tw.stocks.industry holds only today's label and is overwritten in
place by the nightly scraper, so this opens a dated row the first time a company
appears and closes it when the exchange moves the company to another category.
That turns a column with no memory into a history a backtest can read.

    python -m db.seed_company_context --check
    python -m db.seed_company_context
"""

import argparse
from datetime import date

from db.connection import get_cursor
from db.company_context_data import PROFILES, ECOSYSTEMS, ECOSYSTEM_MEMBERS

EVIDENCE = "台股產業分類 Master V3.7 設計稿標註，未附個別證據"


def _name_index(cur):
    cur.execute("""
        SELECT stock_id, name FROM tw.stocks
        WHERE is_active AND security_type = 'STOCK'
    """)
    index, dupes = {}, set()
    for r in cur.fetchall():
        if r["name"] in index:
            dupes.add(r["name"])
        index[r["name"]] = r["stock_id"]
    for d in dupes:
        del index[d]
    return index


def sync_profiles(cur):
    index = _name_index(cur)
    missing = [p[0] for p in PROFILES if p[0] not in index]
    if missing:
        raise SystemExit(f"unresolved company names: {missing}")
    for name, cycle, market, cn, us, site, ownership, note in PROFILES:
        cur.execute("""
            INSERT INTO tw.company_profile
                   (stock_id, cycle_attributes, main_market, china_revenue,
                    us_revenue, main_production_site, product_ownership,
                    cross_industry_note)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_id) DO UPDATE SET
                cycle_attributes = EXCLUDED.cycle_attributes,
                main_market = EXCLUDED.main_market,
                china_revenue = EXCLUDED.china_revenue,
                us_revenue = EXCLUDED.us_revenue,
                main_production_site = EXCLUDED.main_production_site,
                product_ownership = EXCLUDED.product_ownership,
                cross_industry_note = EXCLUDED.cross_industry_note,
                updated_at = NOW()
        """, (index[name], cycle, market, cn, us, site, ownership, note))
    return len(PROFILES)


def sync_ecosystems(cur, effective_from):
    for code, name, kind, coverage in ECOSYSTEMS:
        cur.execute("""
            INSERT INTO tw.ecosystem_master
                   (ecosystem_code, ecosystem_name, ecosystem_type, supply_chain_coverage)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ecosystem_code) DO UPDATE SET
                ecosystem_name = EXCLUDED.ecosystem_name,
                ecosystem_type = EXCLUDED.ecosystem_type,
                supply_chain_coverage = EXCLUDED.supply_chain_coverage
        """, (code, name, kind, coverage))

    written = 0
    for stock_id, eco_code, rel, strength, note in ECOSYSTEM_MEMBERS:
        cur.execute("SELECT 1 FROM tw.stocks WHERE stock_id = %s", (stock_id,))
        if cur.fetchone() is None:
            print(f"  skipped {stock_id}: not in tw.stocks")
            continue
        evidence = f"{EVIDENCE}（{note}）" if note else EVIDENCE
        cur.execute("""
            INSERT INTO tw.company_ecosystem_mapping
                   (stock_id, ecosystem_code, relationship_type, strength,
                    evidence, evidence_kind, confidence, source, effective_from)
            VALUES (%s, %s, %s, %s, %s, 'pending', 'D', %s, %s)
            ON CONFLICT (stock_id, ecosystem_code, effective_from) DO NOTHING
        """, (stock_id, eco_code, rel, strength, evidence, EVIDENCE, effective_from))
        written += 1
    return len(ECOSYSTEMS), written


def sync_official(cur, effective_from):
    """Open a dated row per company, close it when the exchange reclassifies."""
    cur.execute("""
        SELECT s.stock_id, s.industry, o.id, o.twse_official_industry
        FROM tw.stocks s
        LEFT JOIN tw.company_official_classification o
               ON o.stock_id = s.stock_id AND o.effective_to IS NULL
        WHERE s.is_active AND s.industry IS NOT NULL
    """)
    rows = [dict(r) for r in cur.fetchall()]

    opened = closed = 0
    for r in rows:
        if r["id"] is not None and r["twse_official_industry"] == r["industry"]:
            continue
        if r["id"] is not None:
            cur.execute(
                "UPDATE tw.company_official_classification SET effective_to = %s WHERE id = %s",
                (effective_from, r["id"]))
            closed += 1
        cur.execute("""
            INSERT INTO tw.company_official_classification
                   (stock_id, twse_official_industry, classification_source, effective_from)
            VALUES (%s, %s, 'tw.stocks.industry', %s)
            ON CONFLICT (stock_id, effective_from) DO NOTHING
        """, (r["stock_id"], r["industry"], effective_from))
        opened += 1
    return opened, closed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--effective-from", default=date.today().isoformat())
    args = ap.parse_args()

    if args.check:
        print(f"profiles {len(PROFILES)}, ecosystems {len(ECOSYSTEMS)}, "
              f"ecosystem members {len(ECOSYSTEM_MEMBERS)}")
        print("--check: nothing written")
        return

    with get_cursor() as cur:
        n_profiles = sync_profiles(cur)
        n_eco, n_members = sync_ecosystems(cur, args.effective_from)
        opened, closed = sync_official(cur, args.effective_from)

    print(f"profiles {n_profiles}, ecosystems {n_eco}, ecosystem members {n_members}")
    print(f"official classification: opened {opened}, closed {closed}")


if __name__ == "__main__":
    main()
