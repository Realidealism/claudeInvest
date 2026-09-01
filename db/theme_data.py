"""Canonical theme membership — the narrative dimension.

Themes are what the market prices a name on, and they deliberately cut across
the industry tree: HBM reaches DRAM, packaging, substrate, equipment and
materials at once. Nothing here references tw.industry_tree.

Keyed by the T-code dictionary in db/theme_master_data.py. A finer name the
dictionary has no code for rides along as sub_theme rather than earning a new
code — CoWoS under T08, 高速PCB under T42.

Each member is:
    (company_name, exposure_score, evidence_kind, evidence)

    exposure_score 5 the company puts real resources into it
                   3 genuine exposure, but not where the money is
                   1 market attribution only, no company confirmation
    evidence       one sentence, always present. A theme whose members cannot
                   be explained a year later is the failure this file exists to
                   prevent.

Sync with: python -m db.seed_themes --check   (report only)
           python -m db.seed_themes           (write)
"""

# (theme_code, sub_theme) -> [(name, exposure_score, evidence_kind, evidence)]
MEMBERS = {
    ('T01', '先進製程'): [
        ('台積電', 5, 'investor_conf', 'N3 以下節點量產與 EUV 使用者'),
        ('家登', 5, 'company_site', 'EUV 光罩載具，先進節點專用'),
        ('崇越', 3, 'company_site', '半導體材料通路，代理先進製程材料'),
        ('帆宣', 3, 'company_site', '廠務系統與設備零組件，隨新廠建置受惠'),
        ('弘塑', 3, 'company_site', '先進節點濕製程清洗設備'),
        ('漢唐', 3, 'company_site', '無塵室與廠務工程，先進製程新廠承包'),
        ('辛耘', 3, 'company_site', '濕製程設備與再生晶圓'),
        ('光罩', 1, 'news', '光罩製造；先進節點光罩以客戶自製為主'),
    ],
    ('T02', ''): [
        ('世芯-KY', 5, 'annual_report_prose', 'ASIC及晶圓產品 98.45%，北美雲端客戶自研晶片主力'),
        ('創意', 5, 'annual_report_prose', 'ASIC及晶圓產品 75.38%，台積電轉投資的設計服務廠'),
        ('智原', 5, 'annual_report_prose', 'ASIC及晶圓產品 78.85% 加委託設計 12.72%'),
        ('力旺', 3, 'company_site', '技術權利金 67.50%；IP 隨 ASIC 專案出貨'),
        ('巨有科技', 3, 'annual_report_prose', '設計服務 60.23% 加 ASIC 38.54%，規模小'),
        ('意騰-KY', 3, 'annual_report_prose', 'AI智財授權暨技術服務 41.35%，屬邊緣 AI 非雲端'),
        ('益芯科', 3, 'annual_report_prose', 'ASIC及晶圓 73.24%，未揭露 AI 客戶佔比'),
    ],
    ('T07', ''): [
        ('台積電', 5, 'news', 'HBM4 base die 邏輯基底晶片代工，並負責與 GPU 的 CoWoS 整合'),
        ('京元電子', 3, 'news', 'IC測試 58.78%；HBM 相關晶圓與成品測試'),
        ('台勝科', 3, 'company_site', '矽晶圓供應'),
        ('日月光投控', 3, 'news', '封裝與測試龍頭，承接 HBM 堆疊後段相關工序'),
        ('環球晶', 3, 'company_site', '矽晶圓供應，堆疊層數上升帶動用量'),
        ('穎崴', 3, 'news', '測試座供應，高頻寬記憶體測試介面'),
        ('精材', 3, 'annual_report_prose', '晶圓級封裝與 TSV 製程能力，TSV 是堆疊的必要製程'),
        ('精測', 3, 'news', '探針卡供應，記憶體與邏輯測試介面'),
        ('京鼎', 1, 'news', '設備零組件與系統組裝，客戶含堆疊製程設備商'),
        ('力積電', 1, 'news', 'DRAM 代工與自研 1x nm；非 HBM 原廠亦未接 HBM 代工'),
        ('南亞科', 1, 'news', '利基型 DRAM 廠，無 HBM 量產'),
        ('家登', 1, 'news', '晶圓與光罩載具，需求隨堆疊製程上升'),
        ('華邦電', 1, 'news', 'NOR 與利基 DRAM，無 HBM 量產'),
    ],
    ('T08', ''): [
        ('印能科技', 5, 'company_site', '氣動與熱能製程設備，先進封裝翹曲控制'),
        ('台星科', 5, 'annual_report_prose', '晶圓級封裝服務 60.06%'),
        ('均華', 5, 'company_site', '先進封裝製程設備與精密模具'),
        ('弘塑', 5, 'company_site', '先進封裝濕製程設備'),
        ('日月光投控', 5, 'investor_conf', '全球 OSAT 龍頭，先進封裝產能主力'),
        ('瑞峰', 5, 'annual_report_prose', '晶圓封裝收入 88.22%'),
        ('竑騰', 5, 'company_site', '先進封裝主製程設備 61.87%'),
        ('精材', 5, 'annual_report_prose', '晶圓級尺寸封裝 44.98% 加後護層封裝 5.18%'),
        ('萬潤', 5, 'company_site', '先進封裝點膠與封裝設備'),
        ('辛耘', 5, 'company_site', '先進封裝濕製程設備'),
        ('健策', 3, 'company_site', '均熱片與散熱模組，隨封裝功耗上升受惠'),
        ('南茂', 3, 'annual_report_prose', '晶圓凸塊 23.22%'),
        ('同欣電', 3, 'annual_report_prose', '混合積體電路模組 20.10% 與陶瓷基板'),
        ('均豪', 3, 'company_site', '半導體製程設備 78.83%，含封裝段機台'),
        ('志聖', 3, 'company_site', '半導體製程設備 40.00%，含封裝用烘烤與壓合'),
        ('頎邦', 3, 'annual_report_prose', '凸塊 27.17%，為先進封裝前置製程'),
    ],
    ('T08', 'CoWoS'): [
        ('台積電', 5, 'investor_conf', 'CoWoS 產能持有者，擴產計畫為供應鏈需求來源'),
        ('均華', 5, 'company_site', '黏晶與封裝製程設備'),
        ('弘塑', 5, 'company_site', '濕製程設備供應先進封裝產線'),
        ('萬潤', 5, 'company_site', '點膠與封裝製程設備'),
        ('辛耘', 5, 'company_site', '濕製程設備與再生晶圓，供應先進封裝產線'),
        ('京鼎', 3, 'company_site', '設備零組件與系統組裝'),
        ('台灣精材', 3, 'company_site', '石英與陶瓷腔體零件'),
        ('家登', 3, 'company_site', '載具與搬運方案'),
        ('同欣電', 1, 'news', '陶瓷基板與模組封裝，非 CoWoS 製程本體'),
    ],
    ('T12', '矽光子'): [
        ('源傑科技', 5, 'company_site', '光通訊雷射晶片，矽光子模組光源'),
        ('聯亞', 5, 'company_site', '雷射二極體磊晶與晶片'),
    ],
    ('T16', '光通訊'): [
        ('訊芯-KY', 5, 'annual_report_prose', '高速光纖收發模組 61.55%'),
        ('光環', 3, 'company_site', '光通訊元件與磊晶'),
        ('全新', 3, 'company_site', '化合物半導體磊晶片，供光通訊元件'),
        ('凱鈺', 3, 'company_site', '光收發模組驅動IC'),
        ('聯鈞', 3, 'annual_report_prose', '光資訊及光通訊產品 17.00%，其餘為功率封測'),
    ],
    ('T42', '高速PCB'): [
        ('南電', 5, 'investor_conf', 'ABF 載板'),
        ('台光電', 5, 'investor_conf', '低損耗 CCL 主力供應商，AI 伺服器板材'),
        ('台燿', 5, 'investor_conf', '高頻高速 CCL 領導廠'),
        ('景碩', 5, 'investor_conf', 'ABF 與 BT 載板'),
        ('欣興', 5, 'investor_conf', 'ABF 載板，AI 加速器封裝用'),
        ('聯茂', 5, 'investor_conf', '高速材料出貨為主'),
        ('金像電', 5, 'investor_conf', 'AI 伺服器用高層數 PCB'),
        ('金居', 5, 'company_site', 'HVLP 電解銅箔，高速板必要材料'),
        ('健鼎', 3, 'company_site', '伺服器與網通板'),
        ('富喬', 3, 'company_site', '低介電玻纖布'),
        ('建榮', 3, 'company_site', '玻纖布'),
        ('德宏', 3, 'company_site', '玻纖布'),
    ],
}
