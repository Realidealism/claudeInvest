"""Canonical source of truth for theme tags (市場題材標籤).

The companion to db/themes_data.py, under a deliberately weaker contract.
tw.themes says what a company makes, and every membership there can be checked
against the annual report's product mix. This file holds the themes the market
trades — AI ASIC, HBM, CoWoS, 先進製程 — which have no line item in any
filing. Admitting them into tw.themes would mean dropping the ~20% revenue
rule that makes that table worth trusting.

So each row here states how strong the claim is, and why:

    core         the company puts real resources into it and says so
    peripheral   genuine exposure, but not where the money is
    speculative  market attribution only, no confirmation from the company

evidence is required — the loader refuses a row without one. A tag whose
members cannot be explained a year later is the failure mode this file exists
to avoid. evidence_kind is one of annual_report_prose / investor_conf /
company_site / news.

Edit this file, then run:
    python -m db.seed_theme_tags --check   # validate only, no DB write
    python -m db.seed_theme_tags           # sync DB to match this file exactly

Format:
    TAGS    = [(name, category, description), ...]
    MEMBERS = {tag_path: [(股名, level, evidence_kind, evidence), ...], ...}
tag_path is "category > name". A company may carry many tags, and tags cross
supply chains on purpose: HBM reaches DRAM, 先進封裝, 載板, 設備 and 材料 at
once, which is exactly what the industry-chain table cannot express.
"""

# (name, category, description)
TAGS = [
    ('AI ASIC', 'AI', '雲端業者自研加速晶片的設計服務與量產'),
    ('HBM供應鏈', 'AI', '高頻寬記憶體相關的製造、封裝、材料與設備'),
    ('高速PCB', 'AI', 'AI 伺服器用高速高頻電路板與其材料'),
    ('矽光子與光通訊', 'AI', '光收發模組、雷射晶片與矽光子整合'),
    ('CoWoS', '先進封裝', '台積電 2.5D 中介層封裝的設備、材料與服務'),
    ('先進封裝', '先進封裝', '晶圓級封裝、異質整合與其設備材料的整體曝險'),
    ('先進製程', '製程', 'N3 以下先進節點與 EUV 相關曝險'),
]

# tag_path -> [(股名, level, evidence_kind, evidence), ...]
MEMBERS = {
    'AI > AI ASIC': [
        ('創意', 'core', 'annual_report_prose', 'ASIC及晶圓產品 75.38%，台積電轉投資的設計服務廠'),
        ('世芯-KY', 'core', 'annual_report_prose', 'ASIC及晶圓產品 98.45%，北美雲端客戶自研晶片主力'),
        ('智原', 'core', 'annual_report_prose', 'ASIC及晶圓產品 78.85% 加委託設計 12.72%'),
        ('益芯科', 'peripheral', 'annual_report_prose', 'ASIC及晶圓 73.24%，未揭露 AI 客戶佔比'),
        ('巨有科技', 'peripheral', 'annual_report_prose', '設計服務 60.23% 加 ASIC 38.54%，規模小'),
        ('意騰-KY', 'peripheral', 'annual_report_prose', 'AI智財授權暨技術服務 41.35%，屬邊緣 AI 非雲端'),
        ('力旺', 'peripheral', 'company_site', '技術權利金 67.50%；IP 隨 ASIC 專案出貨'),
    ],
    'AI > HBM供應鏈': [
        ('力積電', 'speculative', 'news', 'DRAM 代工與自研 1x nm，非 HBM 原廠'),
        ('南亞科', 'speculative', 'news', '利基型 DRAM 廠，無 HBM 量產'),
        ('華邦電', 'speculative', 'news', 'NOR 與利基 DRAM，無 HBM 量產'),
        ('環球晶', 'peripheral', 'company_site', '矽晶圓供應，堆疊層數上升帶動用量'),
        ('台勝科', 'peripheral', 'company_site', '矽晶圓供應'),
        ('精材', 'peripheral', 'annual_report_prose', '晶圓級封裝與 TSV 製程能力'),
        ('家登', 'speculative', 'news', '晶圓與光罩載具，需求隨堆疊製程上升'),
        ('京鼎', 'speculative', 'news', '設備零組件與系統組裝，客戶含堆疊製程設備商'),
    ],
    'AI > 高速PCB': [
        ('台光電', 'core', 'investor_conf', '低損耗 CCL 主力供應商，AI 伺服器板材'),
        ('台燿', 'core', 'investor_conf', '高頻高速 CCL 領導廠'),
        ('聯茂', 'core', 'investor_conf', '高速材料出貨為主'),
        ('金居', 'core', 'company_site', 'HVLP 電解銅箔，高速板必要材料'),
        ('金像電', 'core', 'investor_conf', 'AI 伺服器用高層數 PCB'),
        ('健鼎', 'peripheral', 'company_site', '伺服器與網通板'),
        ('欣興', 'core', 'investor_conf', 'ABF 載板，AI 加速器封裝用'),
        ('景碩', 'core', 'investor_conf', 'ABF 與 BT 載板'),
        ('南電', 'core', 'investor_conf', 'ABF 載板'),
        ('富喬', 'peripheral', 'company_site', '低介電玻纖布'),
        ('德宏', 'peripheral', 'company_site', '玻纖布'),
        ('建榮', 'peripheral', 'company_site', '玻纖布'),
    ],
    'AI > 矽光子與光通訊': [
        ('源傑科技', 'core', 'company_site', '光通訊雷射晶片，矽光子模組光源'),
        ('聯亞', 'core', 'company_site', '雷射二極體磊晶與晶片'),
        ('訊芯-KY', 'core', 'annual_report_prose', '高速光纖收發模組 61.55%'),
        ('聯鈞', 'peripheral', 'annual_report_prose', '光資訊及光通訊產品 17.00%，其餘為功率封測'),
        ('全新', 'peripheral', 'company_site', '化合物半導體磊晶片，供光通訊元件'),
        ('凱鈺', 'peripheral', 'company_site', '光收發模組驅動IC'),
        ('光環', 'peripheral', 'company_site', '光通訊元件與磊晶'),
    ],
    '先進封裝 > CoWoS': [
        ('台積電', 'core', 'investor_conf', 'CoWoS 產能持有者，擴產計畫為供應鏈需求來源'),
        ('弘塑', 'core', 'company_site', '濕製程設備供應先進封裝產線'),
        ('辛耘', 'core', 'company_site', '濕製程設備與再生晶圓，供應先進封裝產線'),
        ('均華', 'core', 'company_site', '黏晶與封裝製程設備'),
        ('萬潤', 'core', 'company_site', '點膠與封裝製程設備'),
        ('家登', 'peripheral', 'company_site', '載具與搬運方案'),
        ('京鼎', 'peripheral', 'company_site', '設備零組件與系統組裝'),
        ('台灣精材', 'peripheral', 'company_site', '石英與陶瓷腔體零件'),
        ('同欣電', 'speculative', 'news', '陶瓷基板與模組封裝，非 CoWoS 製程本體'),
    ],
    '先進封裝 > 先進封裝': [
        ('日月光投控', 'core', 'investor_conf', '全球 OSAT 龍頭，先進封裝產能主力'),
        ('精材', 'core', 'annual_report_prose', '晶圓級尺寸封裝 44.98% 加後護層封裝 5.18%'),
        ('台星科', 'core', 'annual_report_prose', '晶圓級封裝服務 60.06%'),
        ('瑞峰', 'core', 'annual_report_prose', '晶圓封裝收入 88.22%'),
        ('印能科技', 'core', 'company_site', '氣動與熱能製程設備，先進封裝翹曲控制'),
        ('竑騰', 'core', 'company_site', '先進封裝主製程設備 61.87%'),
        ('弘塑', 'core', 'company_site', '先進封裝濕製程設備'),
        ('辛耘', 'core', 'company_site', '先進封裝濕製程設備'),
        ('均華', 'core', 'company_site', '先進封裝製程設備與精密模具'),
        ('萬潤', 'core', 'company_site', '先進封裝點膠與封裝設備'),
        ('頎邦', 'peripheral', 'annual_report_prose', '凸塊 27.17%，為先進封裝前置製程'),
        ('南茂', 'peripheral', 'annual_report_prose', '晶圓凸塊 23.22%'),
        ('健策', 'peripheral', 'company_site', '均熱片與散熱模組，隨封裝功耗上升受惠'),
        ('志聖', 'peripheral', 'company_site', '半導體製程設備 40.00%，含封裝用烘烤與壓合'),
        ('均豪', 'peripheral', 'company_site', '半導體製程設備 78.83%，含封裝段機台'),
        ('同欣電', 'peripheral', 'annual_report_prose', '混合積體電路模組 20.10% 與陶瓷基板'),
    ],
    '製程 > 先進製程': [
        ('台積電', 'core', 'investor_conf', 'N3 以下節點量產與 EUV 使用者'),
        ('家登', 'core', 'company_site', 'EUV 光罩載具，先進節點專用'),
        ('弘塑', 'peripheral', 'company_site', '先進節點濕製程清洗設備'),
        ('辛耘', 'peripheral', 'company_site', '濕製程設備與再生晶圓'),
        ('帆宣', 'peripheral', 'company_site', '廠務系統與設備零組件，隨新廠建置受惠'),
        ('漢唐', 'peripheral', 'company_site', '無塵室與廠務工程，先進製程新廠承包'),
        ('崇越', 'peripheral', 'company_site', '半導體材料通路，代理先進製程材料'),
        ('光罩', 'speculative', 'news', '光罩製造；先進節點光罩以客戶自製為主'),
    ],
}
