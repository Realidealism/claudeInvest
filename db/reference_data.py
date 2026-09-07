"""Static reference tables for the classification: supply-chain positions,
cycle attributes, region rules, the exchange-category bridge, and R001-R021.

Transcribed once from 台股產業分類_Master_V3_7 and kept here rather than read
from that spreadsheet's export directory, which is not in version control — a
loader that reaches outside the repo works on the machine that happens to hold
the source and nowhere else.

These are dictionaries, not judgements: they change only when the taxonomy
itself does. Rules we authored ourselves live in db/seed_skeleton.LOCAL_RULES,
kept separate so R001-R021 stay recognisably the imported draft.
"""

# (position_name, seq, description)
SUPPLY_CHAIN_POSITIONS = [
    ('原物料', '①', '矽晶圓、玻纖、銅箔、鋼胚、石化原料'),
    ('材料', '②', 'CCL、光阻、特用氣體、封裝材料'),
    ('材料加工', '③', '靶材、金屬加工、化學品加工'),
    ('設備', '④', '半導體/PCB/機械設備'),
    ('設備零組件', '⑤', '真空幫浦、閥件、腔體、精密零件'),
    ('製造', '⑥', 'Foundry、IDM、面板廠、PCB廠（台積電）'),
    ('IC Design', '⑦', 'CPU/GPU、ASIC、PMIC、MCU、RF'),
    ('IP', '⑧', 'Silicon IP、Design Service'),
    ('封裝', '⑨', 'OSAT、先進封裝、載板'),
    ('測試', '⑩', 'IC Test、Probe Card、Test Interface、FA（旺矽）'),
    ('模組', '⑪', '記憶體模組、光收發模組、電源模組'),
    ('系統', '⑫', '伺服器、網通設備、整車、系統整合'),
    ('ODM / OEM', '⑬', '代工組裝（廣達、鴻海、緯創）'),
    ('通路', '⑭', '電子通路、代理（文曄、大聯大）'),
    ('終端', '⑮', '品牌、終端產品'),
    ('服務', '⑯', '雲端、SaaS、SI、維運'),
]

# (layer, coverage)
SUPPLY_CHAIN_LAYERS = [
    ('上游', '原物料、材料、材料加工、設備、IC Design、IP、製造'),
    ('中游', '封裝、測試、模組、零組件'),
    ('下游', '系統、ODM/OEM、通路、終端、服務'),
]

# (attribute, representative_industries)
CYCLE_ATTRIBUTES = [
    ('Growth 成長', 'AI ASIC、GPU、CPO、AI Server'),
    ('Cyclical 循環', 'DRAM、NAND、面板、鋼鐵、航運'),
    ('Defensive 防禦', '食品、電信、部分醫療、公用事業'),
    ('Capex 資本支出', '半導體設備、廠務、PCB設備、重電'),
    ('Consumer 消費', 'PC、手機、零售、家電'),
    ('Rate Sensitive 利率敏感', '金融、營建、不動產、保險'),
    ('Commodity 原物料', '石化、鋼鐵、銅、紙漿'),
    ('Policy Driven 政策', '綠能、電網、國防、電動車'),
]

# (section, item, value)
REGION_EXPOSURE_RULES = [
    ('Revenue Exposure（營收地，High/Mid/Low）', '中國', 'High / Mid / Low'),
    ('Revenue Exposure（營收地，High/Mid/Low）', '美國', 'High / Mid / Low'),
    ('Revenue Exposure（營收地，High/Mid/Low）', '日本', 'High / Mid / Low'),
    ('Revenue Exposure（營收地，High/Mid/Low）', '歐洲', 'High / Mid / Low'),
    ('Revenue Exposure（營收地，High/Mid/Low）', '東南亞', 'High / Mid / Low'),
    ('Revenue Exposure（營收地，High/Mid/Low）', '台灣', 'High / Mid / Low'),
    ('Revenue Exposure（營收地，High/Mid/Low）', '其他', 'High / Mid / Low'),
    ('Production Exposure（生產地，High/Mid/Low）', '中國', 'High / Mid / Low'),
    ('Production Exposure（生產地，High/Mid/Low）', '台灣', 'High / Mid / Low'),
    ('Production Exposure（生產地，High/Mid/Low）', '東南亞', 'High / Mid / Low'),
    ('Production Exposure（生產地，High/Mid/Low）', '美國', 'High / Mid / Low'),
    ('Production Exposure（生產地，High/Mid/Low）', '日本', 'High / Mid / Low'),
]

# (company, manufacturing_mode, product)
MANUFACTURING_MODES = [
    ('台積電', 'Foundry', 'Logic'),
    ('聯電', 'Foundry', 'Logic / Specialty'),
    ('世界先進', 'Foundry', 'Analog / Power'),
    ('華邦電', 'IDM', 'NOR / DRAM'),
    ('旺宏', 'IDM', 'NOR / NAND'),
    ('南亞科', 'IDM', 'DRAM'),
    ('采鈺', 'Foundry / 特殊製程', 'CIS'),
]

# (twse_official_category, master_l1, master_l2_range, mapping_type)
TWSE_INDUSTRY_MAPPING = [
    ('半導體業', 'A 半導體', 'A1～A7', '一對一'),
    ('電腦及週邊設備業', 'C AI伺服器 / 電腦', 'C1～C15', '一對一'),
    ('光電業', 'E 光電 / 顯示 / LED', 'E1～E6', '一對一'),
    ('通信網路業', 'D 網通 / 光通訊', 'D1～D6', '一對一'),
    ('電子零組件業', 'B PCB / IC載板 · F 電子零組件', 'B1～B6 / F1～F7', '一對多'),
    ('電子通路業', 'V 電子通路 / EMS', 'V1～V6', '一對一'),
    ('其他電子業', 'V 電子通路 / EMS', 'V7 (EMS/模組)', '一對多'),
    ('資訊服務業', 'U 資訊服務 / 軟體 / 雲端', 'U1～U7', '一對一'),
    ('數位雲端', 'U 資訊服務 / 軟體 / 雲端', 'U3～U7', '一對一'),
    ('電機機械業', 'G 電機 / 自動化 / 機器人', 'G1～G6', '一對一'),
    ('電器電纜', 'I 能源 / 電力 / 儲能 · L 鋼鐵 / 金屬', 'I6 / L', '跨產業'),
    ('汽車工業', 'H 汽車 / EV', 'H1～H6', '一對一'),
    ('化學工業', 'J 化學 / 石化', 'J1～J9', '一對一'),
    ('生技醫療業', 'N 生技 / 醫療', 'N1～N8', '一對一'),
    ('玻璃陶瓷', 'P 建材 / 玻璃 / 造紙', 'P2 (玻璃)', '一對一'),
    ('造紙工業', 'P 建材 / 玻璃 / 造紙', 'P3 (造紙)', '一對一'),
    ('水泥工業', 'P 建材 / 玻璃 / 造紙', 'P1 (水泥)', '一對一'),
    ('鋼鐵工業', 'L 鋼鐵 / 金屬', 'L1～L9', '一對一'),
    ('塑膠工業', 'J 化學 / 石化 · K 塑膠 / 橡膠', 'J1 / K1～K2', '一對多'),
    ('橡膠工業', 'K 塑膠 / 橡膠', 'K5～K6', '一對一'),
    ('紡織纖維', 'M 紡織 / 成衣', 'M1～M6', '一對一'),
    ('食品工業', 'O 食品 / 農業', 'O1～O5', '一對一'),
    ('建材營造', 'Q 營造 / 不動產 · P 建材', 'Q1～Q3 / P4', '跨產業'),
    ('航運業', 'R 航運 / 物流', 'R1～R8', '一對一'),
    ('觀光餐旅', 'Y 觀光 / 餐旅 / 娛樂', 'Y1～Y5', '一對一'),
    ('金融保險', 'W 金融 / 保險 / 證券', 'W1～W11', '一對一'),
    ('貿易百貨', 'X 零售 / 消費通路', 'X1～X4', '一對多'),
    ('油電燃氣', 'Z 公用事業 / 環保', 'Z1～Z3', '一對一'),
    ('綠能環保', 'I 能源 / 電力 / 儲能 · Z 環保', 'I1～I7 / Z4～Z6', '跨產業'),
    ('運動休閒', 'Y 觀光 / 餐旅 / 娛樂', 'Y5；運動用品製造無對應節點', '部分無節點'),
    ('居家生活', 'X 零售 / 消費通路 · P 建材', 'X1～X4 / P4；居家用品製造無對應節點', '部分無節點'),
]

# (rule_id, rule, description) — R001-R021 as handed to us
V37_RULES = [
    ('R001', '一家公司可有多產業曝險', '不強制單一產業'),
    ('R002', 'Official Industry 僅作 Reference', '不決定 Master 分類'),
    ('R003', 'Theme 不可作為 Primary Industry', 'AI、EV、HBM、SiC/GaN 皆屬題材'),
    ('R004', '同一營收/產品曝險應有唯一主要 Industry 節點', '跨產業業務可存在於多個節點，但不得對同一業務重複計算（例：D5光通訊設備／D6光通訊元件可並存；A2真空/精密零組件與F電子零組件可並存）'),
    ('R005', '新產品先進入 Theme', '尚未形成獨立產業時（如 Chiplet）'),
    ('R006', '產業曝險可用營收/獲利/策略權重', '須明確標示權重來源'),
    ('R007', 'Supply Chain 獨立於 Industry', '一家公司可有多個供應鏈位置'),
    ('R008', 'Cycle 可多選', 'Growth + Capex 等可並存'),
    ('R009', '地區曝險與產業獨立', 'Revenue 與 Production 分開標示'),
    ('R010', '熱門名詞不各自獨立成 Theme', 'AI Agent / LLM / GenAI / ML 歸入 T35 AI Software'),
    ('R011', '電源類 Mapping', 'Power IC→A4 PMIC；PSU/Server Power→C5；UPS→C13(機房)或 I7；一般電源供應器→I7 電力電子；重電→I6'),
    ('R012', 'Power × Compound 可交叉', 'SiC/GaN 同屬 Power Semiconductor 與 Compound Semiconductor；一家可同掛兩個 A3 子產業曝險＋T25 Theme'),
    ('R013', 'Group ≠ Theme', 'Group 描述企業關係(母子/投資/關聯)；Theme 描述市場主題；分表存放'),
    ('R014', 'Group ≠ Ecosystem', 'Group＝正式集團/控制關係；Ecosystem＝公司與特定客戶、供應商、平台、聯盟或技術體系形成的關聯網絡（不限客戶/供應商，如 NVIDIA CUDA 平台型生態系）'),
    ('R015', 'A3 兩維度', 'Foundry/IDM＝製造模式(商業模式)；Memory/Power/CIS/Compound＝產品類型；個股另標『製造模式』'),
    ('R016', '跨分類依產品/供應鏈角色，不依公司名稱', 'Micron→A3(DRAM/NAND)+C8(Server Memory)；群聯→A4(Controller)+F7(SSD/Module)'),
    ('R017', 'HBM＝Theme', 'HBM 為 Theme T07；產業節點為 A3 Memory、A5 HBM Packaging、A6 HBM Test'),
    ('R018', 'Exposure Type ＋ Confidence', 'Type：Direct/Indirect/Theme；Confidence：A 財報明確 / B 官方產品 / C 供應鏈推估 / D 市場題材'),
    ('R019', 'Core Industry 應自動推導，不手動維護', 'Core Industry 由『產業曝險表』(角色=Core) 自動推導；TWSE 官方分類獨立維護於『官方對照』，兩者不合併為單一 Truth Source'),
    ('R020', 'Weight 可為 NULL，不強制換算成 %', 'Revenue/Profit/Strategic Exposure 分開評估；未揭露時 Weight=NULL，改以 Confidence(A~D) 標示精確度，不用臆測百分比'),
    ('R021', '時間維度：Effective_From/To + Known_Date', '產業曝險、集團生態系、題材對應、官方分類等隨時間變動的資料，需加註 Effective_From/Effective_To（生效期間）與 Known_Date（資料可得/公布日），避免回測 look-ahead bias'),
]

