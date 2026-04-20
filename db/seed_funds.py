"""
Seed script for fund managers, funds, and ETFs.
Run: python -m db.seed_funds

Upserts — safe to re-run.
"""

from db.connection import get_cursor

# -----------------------------------------------------------------------
# Fund managers: (name, company)
# -----------------------------------------------------------------------
MANAGERS = [
    ("陳意婷", "統一"),
    ("陳釧瑤", "統一"),
    ("尤文毅", "統一"),
    ("莊承憲", "統一"),
    ("林叡廷", "統一"),
    ("呂宏宇", "復華"),
    ("陳茹婷", "野村"),
    ("謝文雄", "野村"),
    ("蕭惠中", "安聯"),
    ("周敬烈", "安聯"),
    ("黃千雲", "台新"),
    ("葉信良", "元大"),
    ("游景德", "野村"),
    ("施政廷", "安聯"),
    ("陳沅易", "群益"),
    ("陳朝政", "群益"),
]

# -----------------------------------------------------------------------
# Funds: (code, name, manager_name, fund_type, company, source, source_params)
#   manager_name is used to look up manager_id after insert.
# -----------------------------------------------------------------------
FUNDS = [
    # --- Active funds (13) ---
    ("unitec-allweather",  "統一全天候",         "陳意婷", "fund", "統一", "sitca", None),
    ("unitec-gallop",      "統一奔騰",           "陳釧瑤", "fund", "統一", "sitca", None),
    ("unitec-darkhorse",   "統一黑馬",           "尤文毅", "fund", "統一", "sitca", None),
    ("unitec-smid",        "統一中小",           "莊承憲", "fund", "統一", "sitca", None),
    ("unitec-gc-smid",     "統一大中華中小",     "林叡廷", "fund", "統一", "sitca", None),
    ("fhtrust-highgrowth", "復華高成長",         "呂宏宇", "fund", "復華", "sitca", None),
    ("fhtrust-allround",   "復華全方位",         "呂宏宇", "fund", "復華", "sitca", None),
    ("nomura-quality",     "野村優質",           "陳茹婷", "fund", "野村", "sitca", None),
    ("nomura-hightech",    "野村高科技",         "謝文雄", "fund", "野村", "sitca", None),
    ("allianz-dam",        "安聯台灣大壩",       "蕭惠中", "fund", "安聯", "sitca", None),
    ("allianz-tech",       "安聯台灣科技",       "周敬烈", "fund", "安聯", "sitca", None),
    ("taishin-mainstream", "台新主流",           "黃千雲", "fund", "台新", "sitca", None),
    ("yuanta-newmain",     "元大新主流",         "葉信良", "fund", "元大", "sitca", None),
    # --- Active ETFs (7) ---
    ("00981A", "統一台股增長",           "陳釧瑤", "etf", "統一", "ezmoney",     '{"fund_code": "49YTW"}'),
    ("00988A", "統一全球創新",           "陳意婷", "etf", "統一", "ezmoney",     '{"fund_code": "61YTW"}'),
    ("00991A", "復華台灣未來50",         "呂宏宇", "etf", "復華", "fhtrust",     '{"etf_code": "ETF23"}'),
    ("00980A", "野村臺灣智慧優選",       "游景德", "etf", "野村", "nomura",      '{"fund_no": "00980A"}'),
    ("00993A", "安聯台灣主動式",         "施政廷", "etf", "安聯", "allianz",     '{"fund_id": "E0001"}'),
    ("00982A", "群益台灣精選強棒",       "陳沅易", "etf", "群益", "capitalfund", '{"fund_id": "399"}'),
    ("00992A", "群益台灣科技創新主動",   "陳朝政", "etf", "群益", "capitalfund", '{"fund_id": "500"}'),
]


def seed():
    with get_cursor() as cur:
        # --- managers ---
        for name, company in MANAGERS:
            cur.execute("""
                INSERT INTO tw.fund_managers (name, company)
                VALUES (%s, %s)
                ON CONFLICT (name, company) DO NOTHING
            """, (name, company))

        # build name->id lookup
        cur.execute("SELECT id, name, company FROM tw.fund_managers")
        mgr_lookup = {(r["name"], r["company"]): r["id"] for r in cur.fetchall()}

        # --- funds ---
        for code, name, mgr_name, ftype, company, source, sparams in FUNDS:
            mgr_id = None
            if mgr_name:
                mgr_id = mgr_lookup.get((mgr_name, company))

            cur.execute("""
                INSERT INTO tw.funds (code, name, manager_id, fund_type, company, source, source_params)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (code) DO UPDATE SET
                    name = EXCLUDED.name,
                    manager_id = EXCLUDED.manager_id,
                    fund_type = EXCLUDED.fund_type,
                    company = EXCLUDED.company,
                    source = EXCLUDED.source,
                    source_params = EXCLUDED.source_params
            """, (code, name, mgr_id, ftype, company, source, sparams))

        cur.execute("SELECT code, name, fund_type FROM tw.funds ORDER BY fund_type, company")
        rows = cur.fetchall()
        print(f"Seeded {len(rows)} funds/ETFs:")
        for r in rows:
            print(f"  [{r['fund_type']}] {r['code']} - {r['name']}")


if __name__ == "__main__":
    seed()
