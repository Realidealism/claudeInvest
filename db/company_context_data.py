"""Company-level context: profiles, ecosystems, and who sits in them.

Three small hand-maintained tables that the industry tree does not cover.

PROFILES carries the judgement columns about a company as a whole — cyclicality,
where it sells, where it builds. tw.stocks cannot hold these: a nightly scraper
owns that table and would overwrite them.

ECOSYSTEMS is not ownership (that is db/derive_groups.py, which reads real
shareholding). An ecosystem is who a company sells to or builds on — NVIDIA,
Apple, Tesla, MIH — and nothing in our data implies membership, so every row is
an assertion that needs a source.

Seeded from 台股產業分類_Master_V3_7, which asserted these without per-row
evidence; they are loaded with evidence_kind='pending' and confidence='D' so the
gap is visible rather than assumed away.

Sync with: python -m db.seed_company_context
"""

# (name, cycle_attributes, main_market, china_revenue, us_revenue,
#  main_production_site, product_ownership, cross_industry_note)
PROFILES = [
    ('台積電', 'Growth, Capex', '全球', 'Low', 'High', '台灣', 'N/A（Foundry製造服務，非IC設計）', '另跨 A5 封裝(CoWoS)'),
    ('南亞', 'Cyclical, Commodity', '全球', 'High', 'Mid', '台灣/中國', 'N/A（原物料/製造）', '跨塑化+PCB材料+半導體材料，見曝險表'),
    ('台塑', 'Cyclical, Commodity', '全球', 'High', 'Mid', '台灣/美國', 'N/A（原物料/製造）', '塑化為主，多產品'),
    ('台達電', 'Growth, Capex', '全球', 'Mid', 'High', '台灣/中國/泰國', 'N/A（系統/製造）', '跨電源+散熱/液冷+自動化，見曝險表'),
    ('鴻海', 'Growth, Consumer', '全球', 'High', 'High', '中國/東南亞/墨西哥', 'N/A（EMS製造）', '跨EMS+AI Server+EV，見曝險表'),
    ('聯發科', 'Growth', '全球', 'High', 'Mid', '台灣(委外代工)', 'Own Chip（自有品牌晶片）', 'SoC/AI'),
    ('華新', 'Cyclical, Commodity, Policy', '台灣/中國', 'High', 'Low', '台灣/中國', 'N/A（原物料/製造）', '跨電纜+不鏽鋼+資源，見曝險表'),
]

# (ecosystem_code, name, type, supply_chain_coverage)
ECOSYSTEMS = [
    ('E01', 'NVIDIA Ecosystem', 'Customer/Platform', 'GPU、AI Server、PCB、CCL、載板、電源、散熱、光通訊、連接器'),
    ('E02', 'AMD Ecosystem', 'Customer/Platform', 'GPU/CPU、Server、封裝、載板'),
    ('E03', 'Apple Ecosystem', 'Customer', '組裝、鏡頭、面板、被動元件、機殼、PCB'),
    ('E04', 'Tesla / EV Ecosystem', 'Customer', '電池、電控、充電、車用零件'),
    ('E05', '雲端自研 ASIC 客戶群 (Google/AWS/Microsoft/Meta)', 'Customer', 'Google TPU / AWS / Microsoft / Meta ASIC 供應鏈'),
    ('E06', 'MIH 電動車聯盟', 'Alliance', '鴻海主導的開放電動車平台聯盟'),
]

# (stock_id, ecosystem_code, relationship_type, strength, note)
ECOSYSTEM_MEMBERS = [
    ('2330', 'E01', 'Customer', 5, None),
    ('1303', 'E01', 'Customer', 4, 'PCB材料'),
    ('2317', 'E01', 'Customer', 5, None),
    ('3037', 'E01', 'Customer', 3, None),
    ('2382', 'E01', 'Customer', 5, None),
    ('3231', 'E01', 'Customer', 5, None),
    ('6669', 'E01', 'Customer', 5, None),
    ('2308', 'E01', 'Customer', 5, None),
    ('2383', 'E01', 'Customer', 1, None),
    ('3036', 'E01', 'Customer', 1, '代理配銷'),
]
