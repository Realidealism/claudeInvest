"""Canonical industry tree — the classification skeleton the DB mirrors.

Three levels: L1 大產業 (A-Z), L2 子產業 (A1, A2...), L3 細分類 (A1-01...).
An L2 with no children is a leaf in its own right; 21 of the 26 branches stop
there, and only 半導體/PCB/汽車EV/電機/航太 are cut to the third level.

Node codes are the primary key and must stay stable: tw.company_industry_exposure
references them, so renaming a node is safe but renumbering one moves its members.

Leaf names are bilingual — 電源管理 IC (PMIC) — because the name is what reaches
the frontend's industry column, and that column is searched by plain substring
(RevenueScreensPage). An English-only name is unreachable by a Chinese query and
vice versa. The English half is the term people actually type, not a
translation, which is why 軟板 keeps (FPC) and ASIC 設計服務 keeps neither.

Names stay to one term; enumerations belong in main_products. The draft put them
in the name — 'Test Socket / Load Board / Test Interface' — which reads as a
definition and overflows any table it lands in.

Transcribed once from 台股產業分類_Master_V3_7, whose export directory is not in
version control; this file is the tree now, and is hand-maintained.

Sync with `python -m db.seed_skeleton`, or with `python -m db.seed_industry
--with-tree` when the same round also moves exposures — splitting a leaf that
has members needs both edits in one transaction, because the leaf guard refuses
children while the parent still carries them.
"""

# (node_code, parent_code, level, name, main_products)
NODES = [
    ('A', None, 1, '半導體', None),
    ('A1', 'A', 2, '半導體材料', None),
    ('A1-01', 'A1', 3, '矽晶圓', '12吋/8吋/6吋晶圓'),
    ('A1-02', 'A1', 3, '再生晶圓', '晶圓再生、翻新、Reclaim Wafer'),
    ('A1-03', 'A1', 3, 'Epitaxy', 'Silicon Epitaxy、磊晶（可與 SiC/GaN/GaAs 磊晶製程共用）'),
    ('A1-04', 'A1', 3, '化合物半導體材料', 'SiC、GaN、GaAs、InP、化合物磊晶/晶圓'),
    ('A1-05', 'A1', 3, '光阻/微影材料', 'EUV、ArF、KrF、i-line、電子束、顯影液、BARC'),
    ('A1-06', 'A1', 3, '蝕刻/清洗化學品', '酸、鹼、溶劑、蝕刻液、清洗液、特殊化學品'),
    ('A1-07', 'A1', 3, '特用氣體', '氮/氬/氫/氦、NF3、SiH4、NH3、WF6、電子級氣體'),
    ('A1-08', 'A1', 3, 'CMP 材料', 'CMP Slurry、CMP Pad、Conditioner、Diamond Disc'),
    ('A1-09', 'A1', 3, '靶材', '銅、鋁、鈦、鎢、鈷、鉭、稀有金屬'),
    ('A1-10', 'A1', 3, '光罩 / Photomask', 'Mask Blank、Photomask、EUV Mask、Mask Repair'),
    ('A1-11', 'A1', 3, '石英/陶瓷', '石英、陶瓷、SiC零件、AlN、高純度材料'),
    ('A1-12', 'A1', 3, '封裝材料', '封裝膠、Underfill、Die Attach、Silver Paste、Mold Compound、銅球/柱'),
    ('A1-13', 'A1', 3, '導線架 (Lead Frame)', '導線架、IC沖壓件、Lead Frame plating'),
    # Ceramic substrate sits beside 石英/陶瓷 rather than under B: its makers are
    # ceramic component houses, and B is organised around laminate boards.
    ('A1-14', 'A1', 3, '陶瓷基板', '氧化鋁/氮化鋁陶瓷基板、DBC、AMB、陶瓷電路板'),
    # A2 is cut on the process stage the equipment SERVES, and only then, inside
    # a stage, on function. That was already the tree's own rule and nobody had
    # written it down: A2-08 晶圓檢測/量測 and A2-13 測試/檢測設備 are the same
    # machine function split by stage, and no one argues they should be one node.
    # A2-05 清洗設備 was left unscoped, so it read as a machine type beside
    # A2-12's process stage -- and a wet cleaner serving packaging satisfied both.
    # 弘塑 69.86%, 博磊 65.07% and 辛耘 41.90% each lost their largest equipment
    # line to that overlap, because R020② can only report "cannot be assigned
    # uniquely"; it cannot say whether the filing was coarse or the tree asked a
    # question the filing never answers (R031).
    ('A2', 'A', 2, '半導體設備',
     '半導體設備。**跨階段的界線是設備服務哪一段製程**，階段之內再依功能／製程單元細分：'
     'A2-01～A2-09 只涵蓋**前段晶圓製造**設備，A2-12／A2-13 涵蓋**後段封裝／測試**設備。'
     '★設備的技術型態本身不得跨越這條界線——服務封裝製程的清洗機屬 A2-12 而非 A2-05，'
     '一如晶圓檢測(A2-08)與後段測試(A2-13)本來就是同一種機台功能被階段切開。'
     '★A2-10 設備零組件、A2-11／A2-14 設備服務屬不同維度（賣的是零件與服務不是設備），'
     '不納入上述設備類節點的互斥判斷(R031)。'),
    # 2026-09-07: dropped 「PCB/載板 LDI 直接成像曝光機」. It contradicted this
    # node's own 【前段晶圓製造】 tag and A2's L2 rule that the stage served is the
    # boundary, and it collided head-on with B5-01. PCB exposure machines are B5-01.
    ('A2-01', 'A2', 3, '曝光 / 光刻設備', '【前段晶圓製造】Coater、Developer、Track、Mask 相關設備'),
    ('A2-02', 'A2', 3, '蝕刻設備', '【前段晶圓製造】Dry Etch、Plasma Etch、Wet Etch'),
    ('A2-03', 'A2', 3, '薄膜沉積', '【前段晶圓製造】CVD、PVD、ALD、Epitaxy、PECVD'),
    ('A2-04', 'A2', 3, 'CMP 設備', '【前段晶圓製造】化學機械研磨／平坦化設備 (Chemical Mechanical Polishing)、研磨頭、修整器與其整合機台'),
    ('A2-05', 'A2', 3, '清洗設備', '【前段晶圓製造】Wet Cleaning、Single Wafer Cleaning、Batch Cleaning、Wet Process'),
    ('A2-06', 'A2', 3, '熱處理', '【前段晶圓製造】Furnace、RTP、Annealing'),
    ('A2-07', 'A2', 3, '離子佈植', '【前段晶圓製造】離子佈植機 (Ion Implanter)、高／中／低電流佈植機、佈植後快速熱退火整合機台'),
    ('A2-08', 'A2', 3, '晶圓檢測 / 量測', '【前段晶圓製造】AOI、Wafer Inspection、Defect Inspection、Metrology、CD-SEM、Overlay、Film Thickness'),
    ('A2-09', 'A2', 3, '晶圓搬運 / 自動化', '【前段晶圓製造】EFEM、Robot、FOUP、Stocker、AMHS'),
    ('A2-10', 'A2', 3, '真空 / 精密零組件', '真空幫浦、閥件、腔體、Showerhead、Heater、ESC、Focus Ring'),
    ('A2-11', 'A2', 3, '零件清洗 / 再生',
     '半導體設備零組件之清洗、再生與表面處理**服務**：腔體與零件之濕式／電漿清洗、'
     '塗層剝除與再噴塗 (Al2O3／Y2O3／YF3)、精密件研磨拋光與尺寸再生。'
     '★與 A2-10 的分界是**製造 vs 服務**：A2-10 製造零組件本身，本節點對既有零件提供延壽服務。'
     '★與 A2-14 的分界是**對象**：A2-14 服務的是機台（裝機、移機、維修），'
     '本節點服務的是拆下來的零件。'),
    ('A2-12', 'A2', 3, '封裝設備',
     '【後段封裝】**凡主要服務於半導體後段封裝製程之設備**，包括但不限於：'
     '固晶 (Die Bonder)、打線 (Wire Bonder)、模封 (Molding)、切割 (Singulation／Dicing)、'
     '取放 (Pick & Place)、熱壓鍵合 (TCB／Hybrid Bonder)、植球 (Ball Placement)、'
     '點膠 (Dispensing)、翹曲控制、暫時貼合／剝離 (TBDB)、雷射開槽，'
     '以及**封裝／基板用的濕製程與電漿清洗設備**。'
     '★2026-09-04 由六項機台列舉改成階段／功能邊界(R025：定義不完整→修定義，不新增節點)。'
     '舊定義窄於節點名稱本身，使點膠、翹曲控制、封裝用濕製程等後段設備沒有具名的落點，'
     '只能靠「供先進封裝使用」這種下游應用語掛進來，而那是 R031 禁止的。'),
    # Back-end test and inspection equipment belongs with the other equipment,
    # and A2 already holds the back-end 封裝設備; A6 next door is testing as a
    # service, which is a different business from selling the machine.
    ('A2-13', 'A2', 3, '測試 / 檢測設備', '【後段測試】Test Handler、ATE、AOI/AVI 檢測、雷射加工、老化設備'),
    # Servicing the equipment is not making it. A2-01..A2-13 are all machines a
    # company builds and sells; 漢測's 42.87% is engineers on a fab floor
    # installing, moving and repairing someone else's. A7-01 next door is the
    # building's systems, which is a different trade again.
    ('A2-14', 'A2', 3, '半導體設備工程服務 (Equipment Field Service)',
     '設備裝機、移機、改機、驗機、維修與保養等對設備本身的現場工程服務（不製造設備；廠務系統見 A7-01）'),
    ('A3', 'A', 2, '晶圓製造', None),
    ('A3-01', 'A3', 3, 'Foundry (純晶圓代工)',
     '受託製造客戶設計之積體電路，不自有產品線：邏輯製程、特殊製程 '
     '(BCD／高壓／嵌入式記憶體／MEMS)、成熟與先進節點之晶圓代工。'
     '★與 A3-02 IDM 的分界是**產品歸誰**：本節點造的是客戶的晶片，'
     'IDM 造的是自己的；同時有兩者時依 R023 判主從。'
     '★特定產品專屬的自有 fab 製造另有節點，不因它們也是晶圓製造而併入本節點：'
     'DRAM 見 A3-03、NOR／NAND Flash 見 A3-07、功率半導體見 A3-04、'
     '影像感測見 A3-05、化合物半導體見 A3-06。'),
    ('A3-02', 'A3', 3, 'IDM', '整合元件製造（自有 fab）'),
    ('A3-03', 'A3', 3, 'DRAM 製造',
     '以自有晶圓廠製造動態隨機存取記憶體：標準型 DRAM (DDR3／4／5)、'
     '低功耗 LPDDR、利基型與客製化 DRAM、繪圖與伺服器用 DRAM、DRAM 晶圓代工。'
     '★不含：NOR／NAND Flash 製造（A3-07）、記憶體 IC 設計而無自有 fab 者（A4-21）、'
     '記憶體模組封裝與模組銷售（見 A5／V 分支）。'),
    ('A3-04', 'A3', 3, '功率半導體製造 (Power)', 'MOSFET、IGBT、整流器、二極體、TVS、齊納二極體、SiC、GaN（自有 fab；無自有 fab 的設計公司見 A4-22）'),
    ('A3-05', 'A3', 3, '影像感測製程 (CIS)', 'CMOS Image Sensor、BSI、CIS製程'),
    ('A3-06', 'A3', 3, '化合物半導體製造 (GaN/SiC)',
     '以化合物半導體為基材之晶圓製造與元件生產：砷化鎵 (GaAs)、氮化鎵 (GaN)、'
     '碳化矽 (SiC)、磷化銦 (InP) 之磊晶與晶圓製程，以及**以上述化合物基材製造**之'
     '射頻與微波元件、功率元件、光通訊晶粒。'
     '★矽基的功率元件不在本節點：有自有 fab 者見 A3-04，無自有 fab 之設計公司見 A4-22。'
     '★與 A3-01 的分界是**基材**：矽基晶圓製造屬 A3-01，化合物基材屬本節點。'
     '★2026-09-07 移除定義末尾括號內的三個公司名——那是 mapping 結果寫進 Definition 欄。'
     '★與 A1-03 Epitaxy 的邊界未寫定：A1-03 收的是磊晶片這個材料本身，'
     '本節點收的是以化合物基材製造元件的晶圓製程，兩者在同一家公司的同一條營收線上'
     '仍會同時成立（3707 漢磊即此例），屬 taxonomy backlog，'
     '依 R031 在該邊界完成前不得以任一方推翻另一方的既有成員。'),
    # DRAM and NAND run on separate cycles, so they are separate nodes; a maker
    # of both (華邦電) carries an exposure to each rather than one blended row.
    ('A3-07', 'A3', 3, 'NOR / NAND Flash 製造',
     '以自有晶圓廠製造非揮發性記憶體之積體電路：NOR Flash、NAND Flash，'
     '以及 EEPROM、ROM 等利基型非揮發性記憶體。'
     '★與 A3-03 的分界是**記憶體型態**：揮發性的 DRAM 屬 A3-03，非揮發性屬本節點。'
     '★與 A4-21 記憶體 IC 設計的分界是**有無自有晶圓製造**；'
     '模組端見 F7-01。★2026-09-07 移除定義末尾括號內的公司名。'),
    # A4 是依**產品功能類別**互斥切分的。兩個節點不在這個軸上，因此不參與互斥判斷
    # (R031)：A4-05 ASIC 設計服務與 A4-06 矽智財，賣的是設計服務與 IP 授權而不是
    # 某一類晶片——一顆為客戶做的 ASIC 可以是任何功能。
    ('A4', 'A', 2, 'IC Design',
     'IC 設計（不自有晶圓廠，或以委外製造為主）。子節點依**產品的功能類別**互斥切分：'
     '功能相近時以「驅動什麼負載／處理什麼訊號」為界，不以終端應用為界(R031)。'
     '★A4-05 ASIC 設計服務與 A4-06 矽智財 (Silicon IP) 的軸不同——賣的是設計服務與 IP 授權，'
     '不是某一類晶片，故不納入功能類別的互斥判斷。'),
    ('A4-01', 'A4', 3, 'CPU', '通用處理器'),
    ('A4-02', 'A4', 3, 'GPU', '繪圖 / 運算 GPU'),
    ('A4-03', 'A4', 3, 'NPU / AI Accelerator', 'AI 加速器'),
    ('A4-04', 'A4', 3, 'AI ASIC', 'AI 專用晶片'),
    ('A4-05', 'A4', 3, 'ASIC 設計服務',
     '受客戶委託設計特定應用積體電路並提供量產交付：委託設計 (NRE)、'
     '前端 RTL 至後端實體設計、IP 整合與驗證、光罩與量產晶圓之 turnkey 交付。'
     '★本節點是**商業模式**不是晶片功能類別：交付的 ASIC 可以是任何功能，'
     '故不參與 A4 功能類別的互斥判斷(R031)。'
     '★與 A4-06 矽智財的分界是**交付物**：本節點交付晶片（含 turnkey 晶圓），'
     'A4-06 交付可再授權的設計 IP。'
     '★以自有規格設計並自售之 AI 專用晶片屬 A4-04，不屬本節點。'),
    ('A4-06', 'A4', 3, '矽智財 (Silicon IP)',
     '以**授權**方式提供可整合進客戶晶片之電路設計資產：處理器核心 IP、'
     '介面與高速傳輸 IP (SerDes／PHY)、記憶體編譯器與嵌入式非揮發性記憶體 IP、'
     '安全與加密 IP、AI 加速核心 IP，以及其導入所需之技術服務；'
     '收入來自授權金、權利金與技術服務費。'
     '★與 A4-01 CPU 等各 IC 設計節點的分界是**交付物**：那些節點交付可出貨的晶片，'
     '本節點交付設計資產本身——一段處理器核心 IP 屬本節點，一顆處理器晶片屬 A4-01。'
     '★與 A4-05 ASIC 設計服務的分界是**成果歸誰**：A4-05 受託為客戶完成整顆晶片並交付該晶片，'
     '本節點授權可重複使用的既有 IP。'
     '★2026-09-07 移除定義末尾括號內的三個公司名。'),
    ('A4-07', 'A4', 3, '電源管理 IC (PMIC)', '電源管理、DC/DC、LDO、Charger、Power IC'),
    ('A4-08', 'A4', 3, '微控制器 IC (MCU)',
     '泛用/嵌入式微控制器、8/16/32-bit MCU、車用與工控 MCU。伺服器基板管理控制器見 A4-26'),
    ('A4-09', 'A4', 3, '多媒體 SoC', '多媒體/影音 SoC、數位影像控制晶片、消費性 SoC'),
    ('A4-10', 'A4', 3, '網通 IC', 'Ethernet、Switch、Router、Wi-Fi、PHY、Network Processor'),
    ('A4-11', 'A4', 3, '輸出入介面 IC (I/O)', 'PCIe、USB、Thunderbolt、橋接 IC'),
    ('A4-12', 'A4', 3, '顯示器驅動 IC (DDI)',
     '驅動顯示面板像素的積體電路：源極／閘極驅動 IC (Source／Gate Driver)、'
     'TFT-LCD 與 AMOLED 顯示驅動、觸控與顯示整合驅動 (TDDI)、時序控制器 (TCON)。'
     '★與 A4-18 的分界是**負載**：驅動面板像素者屬本節點，驅動 LED 光源'
     '（背光、照明、LED 顯示屏）者屬 A4-18。'
     '★列印頭致動器之驅動屬 A4-27；面板用電源管理 IC 屬 A4-07。'),
    ('A4-13', 'A4', 3, '射頻 IC (RF)', 'RFIC、PA、LNA、Switch、Filter、Tuner'),
    ('A4-14', 'A4', 3, '音訊 IC (Audio)',
     '專責音訊訊號鏈的積體電路：音訊編解碼器 (Audio Codec)、音訊 DAC／ADC、'
     'D 類功率放大器 (Class-D Amplifier)、耳機驅動、麥克風前端與 MEMS 麥克風晶片、'
     '語音／音訊 DSP、藍牙音訊 SoC。'
     '★與 A4-09 的分界是**處理的訊號**：只處理音訊者屬本節點，'
     '同時處理視訊的多媒體 SoC 屬 A4-09。'),
    ('A4-15', 'A4', 3, '感測 / HMI IC (Sensor)', 'Touch、Fingerprint、MEMS、Pressure、Temperature、人機介面控制 IC'),
    ('A4-16', 'A4', 3, '影像 IC (CIS/ISP)',
     '影像擷取與處理的積體電路：CMOS 影像感測器 (CIS)、影像訊號處理器 (ISP)、'
     '影像前端與感測器介面、影像編碼與視訊處理晶片。'
     '★與 A4-15 的分界是**是否成像**：以取得影像為目的之感測屬本節點；'
     '觸控、指紋、慣性、壓力、溫濕度等非成像感測屬 A4-15。'
     '★整合影音編解碼與應用處理的 SoC 屬 A4-09。'),
    ('A4-17', 'A4', 3, '馬達驅動 IC (Motor Driver)',
     '驅動與控制電動機的積體電路：H 橋驅動、無刷／步進／有刷馬達驅動與控制、'
     '馬達用閘極驅動器、風扇驅動 IC、馬達控制專用之驅動級。'
     '★與 A4-07 的分界是**負載**：本節點的輸出端是馬達繞組，'
     'A4-07 電源管理 IC 的輸出端是為系統供電的電壓軌。'),
    ('A4-18', 'A4', 3, 'LED 驅動 IC',
     '驅動發光二極體之定電流／定電壓控制積體電路：背光驅動、照明驅動、'
     'LED 顯示屏驅動、Mini／Micro LED 驅動、LED 矩陣掃描驅動。'
     '★與 A4-12 的分界是**負載**：驅動 LED 光源者屬本節點，'
     '驅動顯示面板像素者屬 A4-12。'
     '★與 A4-07 的分界是**目的**：本節點以定電流驅動發光元件為目的，非為系統供電。'),
    ('A4-19', 'A4', 3, '記憶體控制 IC (Ctrl)',
     '控制記憶體讀寫、錯誤更正與磨損平衡之積體電路：NAND Flash 控制器、'
     'SSD 控制器、DRAM 控制器、記憶卡與隨身碟控制器、UFS／eMMC 控制器。'
     '★與 A4-21 記憶體 IC 設計的分界是**晶片做什麼**：A4-21 設計儲存資料的記憶體本體，'
     '本節點設計驅動它的控制器。'
     '★與 F7-01 記憶體模組的分界是**交付層次**：本節點交付控制晶片，F7-01 交付裝上顆粒的模組。'
     '★2026-09-07 移除定義末尾括號內的公司名。'),
    ('A4-20', 'A4', 3, 'Security IC / Secure Element', 'Secure Element、eSE、TPM、加密IC、安全MCU'),
    # A4-19 is the controller, not the memory die: 晶豪科/鈺創/愛普 design the
    # memory itself and have no fab, so neither A3-03 nor A4-19 fits them.
    ('A4-21', 'A4', 3, '記憶體 IC 設計', 'DRAM/SRAM/Flash 晶片設計、利基型記憶體、記憶體 IP'),
    # A3-04 is the fab side of power devices; these are design houses.
    ('A4-22', 'A4', 3, '功率分立元件設計', 'MOSFET、IGBT、二極體、整流器設計（無自有 fab）'),
    ('A4-23', 'A4', 3, '光通訊 IC', '光收發控制、雷射驅動、TIA、CDR、光模組控制 IC'),
    ('A4-24', 'A4', 3, 'MPU / 嵌入式處理器', '通用微處理器、嵌入式應用處理器'),
    # A4-24 is the embedded application processor; a phone platform SoC is that
    # plus the baseband modem, and the modem is the part the whole business runs
    # on. Filing it under 網通 IC put 聯發科 beside Ethernet PHY vendors.
    ('A4-25', 'A4', 3, '行動通訊 SoC / 應用處理器 (Mobile SoC)',
     '手機/平板平台 SoC、整合基頻數據機之應用處理器、5G Modem'),
    # Technically a controller, commercially not an MCU: a BMC is sold to server
    # ODMs as out-of-band management silicon, on the server cycle and against a
    # different competitive set. Merging it into A4-08 would put 信驊 in the pool
    # of general-purpose MCU vendors (R025).
    ('A4-26', 'A4', 3, '伺服器管理控制晶片 (BMC)',
     'BMC 基板管理控制器、KVM over IP、平台信任根 (PRoT)、伺服器頻外管理與平台安全晶片'),
    # A driver IC, but not for a display: a printhead driver switches piezo or
    # thermal actuators that eject ink, sold to printer OEMs as a consumable-tied
    # custom part. A4-12 covers panels, A4-17 motors, A4-07 rails — none of their
    # definitions reaches it, which is the R025 hard condition rather than a
    # preference. Added for 普誠, whose display-driver team moved onto it whole.
    ('A4-27', 'A4', 3, '印表機頭驅動 IC (Printhead Driver)',
     '噴墨/熱感/雷射印表機頭驅動晶片、壓電與熱氣泡噴頭驅動 IC。'
     '以列印頭致動器之驅動為產品本質；面板顯示驅動見 A4-12'),
    ('A5', 'A', 2, '封裝與先進封裝',
     '半導體封裝。A5-01～A5-02 依**封裝技術類別**互斥切分，分界為製程在晶圓上還是在晶粒上。'
     '★A5-05 封裝測試代工 (OSAT) 與 A5-06 晶圓中段製程代工的軸不同——它們是'
     '**受託代工的商業模式**，一家 OSAT 同時提供傳統封裝與先進封裝，'
     '故不納入技術類別的互斥判斷(R031)，一如 V7 EMS 之於 V1～V6、A2-10／A2-14 之於 A2 設備類。'
     '★★**未決：兩個維度對「受託提供先進封裝」這條線並不正交**（2026-09-07 記錄，taxonomy backlog）。'
     'V7 EMS 之於 V1～V6 是真正的不同維度——「代理某類產品」與「替你製造」是兩種不同的營收；'
     '但「提供晶圓級封裝服務」這一句同時完整落在 A5-02（技術）與 A5-05（商業模式），是同一條收入的兩種命名。'
     '現況即不一致：3265 台星科「晶圓級封裝服務 60.06%」與 3374 精材「晶圓級尺寸封裝 50.16%」掛 A5-02，'
     '而 6239 力成「晶圓級封裝服務 5.24%」掛 A5-05，三者的業務性質相同。'
     '在此裁定前，不得以任一節點的定義去推翻另一節點的既有成員；'
     '真正正交的情形是**自有晶圓製造者做自己的先進封裝**（只有 A5-02，沒有 A5-05）。'
     '★★**兩個維度不衝突時，同一條營收線仍只能記一次**(R004)：'
     '一家 OSAT 的封裝服務收入已由 A5-05 完整承載，不得再以「它做的是先進封裝」為由'
     '把同一條線另計於 A5-02；反之亦然。兩軸同時出現，必須是兩條不同的營收線。'
     '★★2026-09-07 移除兩格(R031 sibling 單軸條款)：'
     '**A5-03 RDL／TSV／Interposer** 是完成先進封裝所用的製程技術與結構元素，不是封裝類別——'
     'CoWoS 即由中介層與 TSV 構成、Fan-Out 即由 RDL 構成，A5-02 的定義本來就涵蓋它們；'
     '且掃全庫 2025 營收品項，RDL／TSV／Interposer／中介層／重佈線**零命中**。'
     '**A5-04 HBM Packaging** 的原定義是「（HBM 本身為 Theme T07）」——那是指向 Theme 的指標'
     '不是產品邊界，與同日移出的 A6-07 HBM Test 完全同型：HBM 是終端產品與技術主題，'
     '其封裝實作落在 A5-02（CoWoS／TSV／2.5D），Theme T07 已承載該語意。'),
    ('A5-01', 'A5', 3, '傳統封裝',
     '以打線或覆晶方式將已切割晶粒裝入基板或導線架之封裝：'
     'BGA、QFN、QFP、SOP／SOIC、DIP、TO 系列功率封裝、SiP 模組封裝。'
     '★與 A5-02 的分界是**製程在晶圓上還是在晶粒上**：本節點先切割再封裝，'
     'A5-02 在整片晶圓上完成重佈線與封裝結構後才切割。'
     '★2026-09-07 移除原列舉中的 WLCSP：晶圓級晶片尺寸封裝是在晶圓上完成的，'
     '屬 A5-02；原定義把它列在本格，與 A5-02 的成員 3374 精材（晶圓級尺寸封裝 + '
     '晶圓級後護層封裝 50.16%）直接矛盾——反向測試不通過的是定義，不是歸屬。'),
    ('A5-02', 'A5', 3, '先進封裝',
     '在晶圓層級完成重佈線與封裝結構之封裝：'
     'CoWoS、SoIC、Fan-Out (InFO／FOWLP／FOPLP)、Hybrid Bonding、2.5D／3D 堆疊、'
     '晶圓級晶片尺寸封裝 (WLCSP)、晶圓級後護層封裝。'
     '★**其製程技術與結構元素涵蓋於本節點，不另立節點**：'
     '重佈線層 (RDL)、矽穿孔 (TSV)、矽中介層 (Interposer)、晶圓凸塊 (Bumping)、'
     '暫時貼合與剝離。它們是完成上述封裝所用的技術與構造，不是與傳統封裝並列的封裝類別——'
     'CoWoS 本身即由中介層與 TSV 構成，Fan-Out 本身即由 RDL 構成。'
     '★2026-09-07 由原列舉擴寫，同時吸收被移除的 A5-03 RDL／TSV／Interposer(R025)。'),
    # The field used to read 日月光、矽品、力成、京元、南茂、超豐、頎邦 -- seven
    # company names where a boundary belongs, and 16 members were judged against
    # the list of companies already in it. Same defect V1 carried; [definition]
    # now finds this kind rather than only the empty ones.
    ('A5-05', 'A5', 3, '封裝測試代工 (OSAT)',
     '受託提供半導體封裝與／或測試服務者 (Outsourced Semiconductor Assembly and Test)：'
     '不自有 IC 設計、亦不以自有晶圓製造為主，收入來自客戶委託的封裝、測試或兩者之代工。'
     '★本節點是**商業模式**不是封裝技術：同一家 OSAT 可同時做 A5-01 傳統封裝與 '
     'A5-02 先進封裝，彼此不互斥(R031)。'
     '★只做測試不做封裝者見 A6 測試分支；只做晶圓薄化／背面金屬等中段製程者見 A5-06。'),
    # Mid-end: the filing's own word. Thinning and backside metal happen on a
    # customer's finished wafer, so this is neither A3 (which makes the device)
    # nor A5-05 (which packages it) — but its demand comes from Fan-Out, bumping
    # and 3D, which is why it hangs under packaging rather than under 晶圓製造.
    ('A5-06', 'A5', 3, '晶圓中段製程代工 (Wafer Mid-end Process)',
     '晶圓薄化、晶背研磨、晶圓正/背面金屬製程、載板貼合；受託加工客戶晶圓，不含封裝與測試'),
    ('A6', 'A', 2, '測試 / 可靠度 / FA',
     '半導體測試與可靠度之**受託服務**，以及測試所需之**接觸介面產品**。'
     '本組宣告兩個維度，各自內部互斥，彼此不比較（同 A2 對設備／零組件／服務的處理）：'
     '**(一) 服務軸**——A6-01 量產測試判定良否、A6-05 壽命與環境應力驗證、A6-06 失效事後追因，'
     '三者依**目的與時點**互斥；'
     '**(二) 測試介面產品軸**——A6-02 晶圓級接觸元件、A6-03 封裝後測試座與載板，依**測試階段**互斥。'
     '★★**不得再以應用或測試場景切出第三個軸**（2026-09-07 裁定，R031 sibling 單軸條款）：'
     'Burn-in 與 HBM 都是測試條件或終端技術，不是產品或服務類別。'
     '原 A6-07 HBM Test 橫跨 A6-01（HBM 測試服務）與 A6-03（HBM 測試座），'
     '其定義本身即寫著「見 Theme T07」，已移出本樹改由該 Theme 表達。'
     '★**A6-04 Burn-in 目前處於 existence test 未決狀態**，刻意保留無定義：'
     '掃全庫 2025 營收品項（預燒／老化／Burn／可靠度／信賴性／驗證）無任何一條支撐它獨立成格，'
     'burn-in 只以**修飾語**出現在產品上——3581 博磊「積體電路**預燒用**測試連接座」、'
     '6683 雍智「IC**老化**測試載板」——兩句的核心名詞都是連接座與載板，burn-in 是用途。'
     '待決的問題不是「怎麼幫它寫定義」，而是「Burn-in 是否構成足以與 A6-01／A6-03／A6-05／A6-06 '
     '互斥的獨立 value proposition」；在該問題有答案之前不建立定義，'
     '**且依 R031 不得以其空白狀態作為排除 A6-03 或其他格的依據**。'
     '若最終認定不成立，應退出 Industry 層級而非硬造一格。'),
    ('A6-01', 'A6', 3, 'IC 測試服務 (IC Test)', 'Wafer Sort、Final Test、系統級測試'),
    ('A6-02', 'A6', 3, '探針卡 (Probe Card)',
     '半導體晶圓級測試之接觸元件：探針 (Probe)、探針卡 (Probe Card)、探針介面 (Probe Interface)、'
     '垂直式與懸臂式探針卡、MEMS 探針卡。'
     '★與 A6-03 的分界是**測試階段**：本節點接觸的是尚未切割的晶圓，A6-03 接觸的是封裝後的元件。'
     '★2026-09-07 移除原定義末尾括號內的兩個公司名——那是 mapping 結果寫進了 Definition 欄，'
     '會形成「公司掛這格 → 定義寫公司名 → 定義再證明公司該在這格」的循環；'
     '且其中一家實際掛在 A6-03（測試載板 98.04%），定義引用的還是一個錯的 mapping。'
     '**改寫時不得把移除的名字再抄進說明**，否則欄位裡仍有公司名，'
     '這一點是 [definition_entity_reference] 上線當天實測出來的。'),
    ('A6-03', 'A6', 3, '測試座 (Test Socket)', 'Test Socket、Load Board、Test Interface'),
    ('A6-04', 'A6', 3, 'Burn-in', None),
    ('A6-05', 'A6', 3, '可靠度驗證服務 (Reliability)',
     '以**受託測試**方式驗證半導體、電子元件、材料或產品於壽命、環境、機械、電性等條件下之'
     '可靠度、耐久性與失效風險：高溫工作壽命 (HTOL)、高加速應力 (HAST)、溫濕度與溫度循環、'
     '機械應力與落摔、電性應力與壽命推估、車規與工規之可靠度驗證。'
     '★與 A6-01 的分界是**目的**：A6-01 是量產線上判定個別晶片良否，本節點是以樣本驗證族群的壽命與耐受性。'
     '★與 A6-06 的分界是**時點**：本節點在失效發生前施加應力以預測風險，A6-06 在失效發生後追究原因。'
     '★2026-09-07 由無定義補齊：本節點是**服務**不是設備，這一點由發行人自述決定而非推測——'
     '3587 閎康「主要服務項目係提供⋯可靠度測試(RA)、故障分析(FA)及材料分析(MA)服務」，'
     '3289 宜特「服務涵蓋⋯故障分析、材料分析、可靠度驗證、訊號測試」。'
     '★證據粒度限制：上述兩家的年報比重表只給一條合併的服務收入線，'
     '無法拆出可靠度部分，故其權重依 R020② 永久為 NULL——'
     '這不是「不知道公司有沒有這門生意」，而是「知道有、但不知道它占多少」。'),
    ('A6-06', 'A6', 3, '失效分析 (FA)',
     '半導體與電子元件之失效分析與材料分析實驗室服務：故障點定位 (EMMI／OBIRCH)、'
     '樣品製備 (FIB／切片)、電子顯微 (SEM／TEM)、成分與材料分析、'
     '訊號完整性與電性驗證分析。'
     '★與 A6-01 的分界是**目的**：A6-01 是量產測試判定良否，本節點是找出失效的原因；'
     '與 A6-05 Reliability 的分界是**時點**：可靠度是壽命與環境應力驗證，失效分析是事後追因。'),
    ('A7', 'A', 2, '半導體廠務', None),
    ('A7-01', 'A7', 3, '廠務系統', '無塵室、HVAC、Gas、Chemical、Water、Wastewater、Power、Pollution Control、Facility Automation'),
    ('B', None, 1, 'PCB / IC載板', None),
    ('B1', 'B', 2, 'PCB材料', None),
    ('B1-01', 'B1', 3, '一般 CCL', 'FR-4、CEM-3、一般消費/工業用銅箔基板'),
    ('B1-02', 'B1', 3, '玻纖布', '玻璃紗、玻纖布、Low Dk、Low CTE'),
    ('B1-03', 'B1', 3, '樹脂', 'Epoxy、PPO、PPE、Cyanate Ester、BT Resin'),
    ('B1-04', 'B1', 3, '軟性銅箔基板 (FCCL/PI)', 'PI Film、FCCL、Coverlay'),
    ('B1-05', 'B1', 3, '銅箔', 'ED Copper、RA Copper、HVLP、VLP'),
    # AI board material and consumer board material are the same product class
    # on paper and different businesses in practice — different customers,
    # different pricing, opposite points in the cycle.
    ('B1-06', 'B1', 3, '高速高頻 CCL', 'Low Dk、Low Df、Ultra Low Loss、AI/HPC 用板材'),
    # PI resin is the raw material; B1-04 PI is the film made from it.
    ('B1-07', 'B1', 3, '聚亞醯胺樹脂', 'PI Resin、PI Varnish'),
    # Prepreg is neither of its neighbours: B1-02 is the woven cloth, B1-03 the
    # resin, B1-01 the laminate after copper is bonded on. A B-stage sheet is a
    # product sold in its own right, and widening B1-01 to reach it would file
    # high-frequency prepreg under 一般 CCL (R025).
    ('B1-08', 'B1', 3, '膠片 (Prepreg)',
     '半固化片、黏合片、Low CTE Non-flow PP；浸樹脂而未貼銅箔之 B-stage 材料'
     '（貼銅後見 B1-01／B1-06，玻纖布本體見 B1-02）'),
    ('B2', 'B', 2, 'PCB製造',
     '自製整片電路板之製造。**本組的分類軸是板材結構**，且只有這一個軸：'
     'B2-01 整片皆硬、B2-02 整片皆軟、B2-03 同一片兼含硬區與軟區，三者互斥且窮盡。'
     '★2026-09-07 裁定(R031 sibling 單軸條款)：原本並列的 B2-04 高速高頻、B2-05 HDI／SLP、'
     'B2-06 AI Server PCB 分屬電性／材料、製程／密度、終端應用三個不同的軸，'
     '彼此也不互斥——同一片 PCB 可以同時是 HDI、高速高頻且用於 AI Server——'
     '那是三個**屬性**不是三個**類別**，已移出本樹，改由 Theme／attribute 表達'
     '（T42 High Frequency / Low Loss、T03 AI Server；HDI／SLP 尚無對應 theme，屬缺口）。'
     '同時把 B2-01 定義中的「HDI、Server/AI Server PCB、車用PCB」一併移除——'
     '那些是屬性與應用，不是結構。'
     '★B2-07 PCB 製程代工屬**商業模式**軸，依 R031 不納入上述結構軸的互斥判斷'
     '（同 V1–V6 與 V7 的既有處理）；它究竟應為 Industry leaf 還是 business-model attribute，'
     '待 A／B 分支整體設計一併裁定。'),
    ('B2-01', 'B2', 3, '硬板 / 多層板',
     '整片皆為硬質基材之電路板：單面板、雙面板、多層板、高層數板。'
     '★與 B2-02／B2-03 的分界是**結構**：整片皆軟屬 B2-02，同一片兼含硬區與軟區屬 B2-03。'
     '★2026-09-07 由「一般硬板、多層板、HDI、高層數板、Server/AI Server PCB、車用PCB」收斂而來：'
     'HDI 是製程密度、Server/AI Server 是終端應用、車用是終端市場，三者皆非板材結構，'
     '寫在本格定義裡使本格同時吞掉 B2-05 與 B2-06，28 家成員的歸屬因此無法驗證(R031)。'
     '**高頻微波基板、HDI 板、AI Server 板只要整片皆硬，結構上就屬本節點**，'
     '其材料等級、製程密度與終端應用由 Theme／attribute 表達，不在本軸上再切一格。'),
    ('B2-02', 'B2', 3, '軟板 (FPC)', 'FPC、RFPCB、Camera/Display/Battery FPC'),
    ('B2-03', 'B2', 3, '軟硬結合',
     '軟硬結合板 (Rigid-Flex PCB)：硬質基材與軟性基材壓合為一體之電路板，'
     '含多層軟硬結合板與動態彎折應用。'
     '★與 B2-01／B2-02 的分界是**結構**：整片皆硬屬 B2-01、整片皆軟屬 B2-02，'
     '同一片同時含硬區與軟區者屬本節點。'),
    # Selling a step of board making is not selling the board: 聯致 does lamination
    # and 凱崴 drilling and electrical test, for board houses, and both sat on
    # consumables and EMS nodes whose definitions exclude a process service.
    ('B2-07', 'B2', 3, 'PCB 製程代工 (Toll Processing)',
     '壓合、鑽孔、雷射鑽孔、成型、電性測試等對他人板材之受託加工；'
     '自製整板見 B2-01～B2-03，設備見 B5，耗材與藥水見 B6。'
     '★本節點是**商業模式**軸（賣的是加工服務不是板子），'
     '依 R031 不納入 B2-01～03 結構軸的互斥判斷'),
    # Branch-level definition adjudication pending (2026-09-03). B3-01 'ABF 載板 /
    # AI/HPC 高階載板' and B3-05 'HPC / AI Substrate / 高階ABF、大尺寸載板' are near
    # duplicates; B3-03 and B3-04 carry no definition at all and 0/1 members. A
    # sixth node for 恆勁's material-unspecified substrate was proposed and
    # refused: adding to a branch with two undefined nodes and one duplicate is
    # the fragmentation R025 warns about. Settle B3-01..B3-05 first.
    # Waiting: 6920 恆勁 82% (filing never says ABF, yet sits on B3-01).
    ('B3', 'B', 2, 'IC載板', None),
    ('B3-01', 'B3', 3, 'ABF 載板', 'AI/HPC 高階載板（週期異於 BT）'),
    ('B3-02', 'B3', 3, 'BT 載板', '記憶體 / 手機載板'),
    ('B3-03', 'B3', 3, 'FC-BGA Substrate', None),
    # Definition deferred (2026-09-04). B3-01 ABF 載板 and B3-05 HPC/AI Substrate
    # are near-duplicates and B3-03 is undefined, so this node's exclusivity
    # cannot be verified against its siblings. R031 says settle the branch
    # boundary before writing a sibling definition -- writing this one alone
    # would be the mapping-before-definition order in a different disguise.
    ('B3-04', 'B3', 3, 'FC-CSP Substrate', None),
    ('B3-05', 'B3', 3, 'HPC / AI Substrate', '高階 ABF、大尺寸載板'),
    ('B4', 'B', 2, 'COF', None),
    ('B4-01', 'B4', 3, '捲帶封裝', 'COF、COP、COG'),
    ('B5', 'B', 2, 'PCB設備',
     'PCB／IC 載板之製造，以及其後之電路板組裝 (PCBA) 段所用之設備。'
     '★2026-09-07 由「製造用設備」擴為含 PCBA 組裝段(R025)：B5-02 收的 ICT／SPI／AXI '
     '是組裝板而非裸板的檢測，全樹別無 SMT 設備節點，母節點若只寫製造段，'
     '子節點就會超出母節點範圍。SMT 貼片與迴銲等組裝**製程**設備依此併入 B5-01，目前無成員。'
     '與 A2 半導體設備、E7 顯示設備的分界同為'
     '**設備服務哪一段製程**；階段之內再依**設備在產線上做的事**分節點：'
     'B5-01 改變板材本身，B5-02 判定板材良否，B5-03 只搬運板材與串接製程。'
     '★設備的技術型態本身不得跨越這條界線——同一台 AOI 服務 PCB 製程屬 B5-02，'
     '服務晶圓製程屬 A2-08，服務後段封測屬 A2-13，服務面板製程屬 E7-02。'
     '★受託加工他人板材者賣的是服務不是設備，見 B2-07；耗材與藥水見 B6。'),
    ('B5-01', 'B5', 3, '製程設備',
     '【PCB／IC 載板製造】凡主要服務於印刷電路板、IC 載板及其基材 (CCL) 製造製程之設備：'
     '鑽孔、雷射鑽孔、曝光與直接成像 (DI／LDI)、顯影、蝕刻、電鍍、壓合、塗佈、'
     '烘烤與乾燥、電漿清洗與表面處理、成型；以及電路板組裝 (SMT) 段之貼裝與迴銲設備。'
     '★與 A2-12 的分界是**廠**不是機台：載板**製造廠**產線上的電漿清洗與濕製程屬本節點，'
     '封裝廠內對已完成載板／晶圓所做的封裝端濕製程與電漿清洗屬 A2-12。'
     '★2026-09-07 由機台列舉改成階段／功能邊界，並移出 AOI／AVI(改 B5-02)。'
     '舊定義同時收製程與檢測兩種功能，使 24 家公司無論做鑽孔機還是做 AOI 都落在同一格，'
     '而 A2 對同一組功能早已切成 A2-01～07／A2-08、A2-13／A2-09 三群(R025：定義不完整→修定義)。'),
    ('B5-02', 'B5', 3, '檢測設備',
     '【PCB／IC 載板製造】凡主要服務於印刷電路板、IC 載板及組裝電路板 (PCBA) 之檢測與量測設備：'
     '線路檢查 (AOI)、外觀檢查 (AVI／AFVI)、孔位與孔壁檢查、線寬線距與尺寸量測、'
     '錫膏檢測 (SPI)、X-Ray 檢測 (AXI)、電性測試機 (ICT／飛針)。'
     '★測試治具與探針板屬設備之工裝，隨其所服務的檢測機掛本節點，'
     '與 B6 的分界是 B6 收的是進到板子裡的耗材（鑽針、藥水、墊板），治具不進板子。'
     '★與 B5-01 的分界是設備做的事：本節點判定板材良否而不改變板材。'
     '★服務晶圓製程者見 A2-08，服務後段封測者見 A2-13，服務面板製程者見 E7-02。'),
    ('B5-03', 'B5', 3, '自動化 / 搬運設備',
     '【PCB／IC 載板製造】凡主要服務於印刷電路板、IC 載板產線之自動化、搬運與整線整合設備：'
     '收放板機、Loader／Unloader、輸送與翻板、儲板與智慧物流 (AMHS)、'
     '產線串接、整廠自動化規劃與整合。'
     '★本節點既不改變板材(B5-01)亦不判定良否(B5-02)，只移動板材與串接製程。'
     '★服務晶圓廠者見 A2-09，服務面板產線者見 E7-03。'),
    ('B6', 'B', 2, 'PCB耗材', None),
    ('B6-01', 'B6', 3, 'PCB 耗材', '鑽針、銑刀、磷銅球、墊板'),
    ('B6-02', 'B6', 3, 'PCB 製程藥水', '電鍍藥水、蝕刻液、清洗液、表面處理藥水'),
    ('C', None, 1, 'AI伺服器 / 電腦', None),
    ('C1', 'C', 2, 'AI Server', None),
    ('C1-01', 'C1', 3, '伺服器整機', 'GPU Server、AI Accelerator Server、Rack Server、Storage Server、Edge AI'),
    ('C2', 'C', 2, 'Server ODM/OEM', None),
    ('C2-01', 'C2', 3, '代工廠',
     '受託設計與製造伺服器及資料中心機櫃之代工：依雲端服務業者或品牌客戶之規格'
     '完成主機板整合、系統組裝、機櫃整合與出貨測試，收入來自代工製造而非自有品牌銷售。'
     '★2026-09-07 改寫：原定義通篇只有八家伺服器組裝廠的名字，'
     '那是 mapping 結果寫在 Definition 欄，使本節點無法用來判斷任何其他公司。'
     '★與 C1-01 伺服器整機的邊界尚未寫定——C1-01 的定義是機種列舉，'
     '未說明自有品牌與受託代工的歸屬——屬 taxonomy backlog；'
     '在該邊界完成前本節點不得掛成員(R031 mapping gate)。'),
    ('C3', 'C', 2, 'Server Motherboard', None),
    ('C3-01', 'C3', 3, '伺服器主機板', None),
    ('C4', 'C', 2, 'Server Chassis', None),
    ('C4-01', 'C4', 3, '機殼', None),
    ('C5', 'C', 2, 'Server Power', None),
    ('C5-01', 'C5', 3, '伺服器電源', 'PSU、CRPS、BBU、Power Shelf、AI Data Center Power'),
    ('C6', 'C', 2, 'Server Cooling', None),
    ('C6-01', 'C6', 3, '散熱', 'Air Cooling、Liquid Cooling、CDU、Cold Plate、Pump、Fan、TIM'),
    ('C7', 'C', 2, 'Server Connector', None),
    ('C7-01', 'C7', 3, '連接器', '高速連接器、Backplane、Cable、Socket'),
    ('C8', 'C', 2, 'Server Memory', None),
    ('C8-01', 'C8', 3, '伺服器記憶體', 'DDR5、RDIMM、MRDIMM、CXL Memory'),
    ('C9', 'C', 2, 'Storage', None),
    ('C9-01', 'C9', 3, 'System / Enterprise Storage', 'HDD、SSD、Enterprise SSD、Storage System（Controller→A4、NAND→A3；模組/元件端見 F7）'),
    ('C10', 'C', 2, 'PC', None),
    ('C10-01', 'C10', 3, '個人電腦', 'Desktop、Notebook、Mini PC、AIO'),
    ('C11', 'C', 2, 'Peripheral', None),
    ('C11-01', 'C11', 3, '週邊', 'Keyboard、Mouse、Webcam、Printer、Scanner'),
    ('C12', 'C', 2, 'Rack / Rack Integration', None),
    ('C12-01', 'C12', 3, '機櫃整合', None),
    ('C13', 'C', 2, 'UPS', None),
    ('C13-01', 'C13', 3, '不斷電系統', None),
    ('C14', 'C', 2, 'Power Distribution', None),
    ('C14-01', 'C14', 3, '電源分配', 'PDU、Busbar、匯流排'),
    ('C15', 'C', 2, 'Data Center Infrastructure', None),
    ('C15-01', 'C15', 3, '機房基礎設施', '機電、消防、監控、機房建置'),
    ('D', None, 1, '網通 / 光通訊', None),
    ('D1', 'D', 2, 'Ethernet', None),
    ('D1-01', 'D1', 3, '乙太網路 (Ethernet)', 'Switch、Router、NIC、PHY、Ethernet Controller'),
    ('D2', 'D', 2, 'Wi-Fi', None),
    ('D2-01', 'D2', 3, '無線網路 (Wi-Fi)', 'Wi-Fi 6/7/8'),
    ('D3', 'D', 2, '5G', None),
    ('D3-01', 'D3', 3, '行動通訊設備 (5G)', 'Base Station、Small Cell、RF、Antenna'),
    ('D4', 'D', 2, 'Broadband', None),
    ('D4-01', 'D4', 3, '寬頻 (Broadband)', 'GPON、EPON、PON、Cable modem'),
    ('D5', 'D', 2, '光通訊設備', None),
    ('D5-01', 'D5', 3, '光通訊設備 (Optical Equipment)', 'Optical Network、Optical Switch、Optical Transport、Network Equipment'),
    ('D6', 'D', 2, '光通訊元件', None),
    ('D6-01', 'D6', 3, '光通訊元件 (Optical Component)', 'Optical Transceiver、Optical Engine、EML、DML、VCSEL、TIA、Driver、DSP、Silicon Photonics、Fiber、Passive（CPO 為跨產業 Theme，見 T12）'),
    ('E', None, 1, '光電 / 顯示 / LED', None),
    ('E1', 'E', 2, 'LCD', None),
    ('E1-01', 'E1', 3, '液晶面板', '面板、Cell、Module'),
    ('E2', 'E', 2, 'OLED', None),
    ('E2-01', 'E2', 3, 'OLED', 'OLED Panel、AMOLED、LTPO、OLED材料'),
    # 2026-09-07 ruling on E: technology generation stops being E's first-level
    # sibling axis, and the two finest generation cells go with it.
    #
    # E4 Mini LED had no members, and 「Mini LED」 appears in zero of the 2025
    # revenue lines in the database. The cell was finer than any filing.
    #
    # E3 Micro LED had one member. 「Micro LED」 appears in exactly one 2025
    # revenue line and that one is inspection equipment, not a display. The node
    # is gone; the member's evidence is not -- see the E3 block in
    # db/industry_data.py. A removed node is not a refuted exposure.
    #
    # Both technologies keep existing where a technology that cuts across
    # products belongs: T28 Micro LED in theme_master.
    #
    # Left standing on purpose because they are unruled: E's own parent scope
    # (display industry, or the wider optical/photonics domain), and with it E5's
    # rewrite, E6 光學元件 and E7 顯示設備. No definition is written in this pass.
    ('E5', 'E', 2, 'LED', None),
    ('E5-01', 'E5', 3, '發光二極體', 'LED Chip、Package、Display、Backlight、Driver'),
    ('E6', 'E', 2, '光學元件', None),
    ('E6-01', 'E6', 3, '光學',
     'Lens、Prism、Optical Film、Filter；微型光學元件（矽基微透鏡陣列、超穎表面 Metasurface、光波導、晶圓級光學）'),
    # 2026-09-07: E held only the panels, LEDs and optics themselves -- searching
    # 面板／顯示／LCD／Panel／蒸鍍／貼合／塗佈／鍍膜 across the tree returned no
    # equipment node at all, which is why 3535 晶彩科 93.09% and 3455 由田 22.47%
    # had nowhere to hang. Split the same way A2 and B5 are.
    ('E7', 'E', 2, '顯示設備',
     '面板／顯示模組製造用設備。與 A2 半導體設備、B5 PCB 設備的分界同為'
     '**設備服務哪一段製程**：本分支收服務於 TFT-LCD、OLED、觸控面板、光罩'
     '與顯示模組製造製程之設備；階段之內再依**設備在產線上做的事**分節點。'
     '★設備的技術型態本身不得跨越這條界線——同一台 AOI 服務面板製程屬 E7-02，'
     '服務晶圓製程屬 A2-08，服務 PCB 屬 B5-02。'
     '★面板／顯示器本體見 E1 LCD、E2 OLED、E5 LED，光學元件見 E6。'),
    ('E7-01', 'E7', 3, '製程設備',
     '【面板／顯示製造】凡主要服務於 TFT-LCD、OLED、觸控面板與顯示模組製造製程之設備：'
     '鍍膜與薄膜沉積、曝光與顯影、蝕刻、濕製程與清洗、配向與封合、貼合與組裝、'
     '切割與裂片、退火與烘烤。★本節點改變面板／基板本身，與 E7-02 的分界即在此。'),
    ('E7-02', 'E7', 3, '檢測設備',
     '【面板／顯示製造】凡主要服務於面板、觸控與顯示模組製造之檢測與量測設備：'
     'Array／Cell／Module 各段自動光學檢測 (AOI)、素玻璃與基板檢測、'
     '**面板用**光罩之檢測（半導體光罩見 A2-01／A2-08）、'
     '突起物與缺陷檢測、線寬與膜厚量測、點燈與電性檢測。'
     '★本節點判定面板良否而不改變它。'
     '★服務晶圓製程者見 A2-08，服務 PCB 者見 B5-02。'),
    ('E7-03', 'E7', 3, '自動化 / 搬運設備',
     '【面板／顯示製造】凡主要服務於面板／顯示產線之自動化、搬運與整線整合設備：'
     '玻璃基板搬運與儲存 (Stocker／AMHS)、機械手臂上下料、'
     '產線串接、整廠自動化規劃與整合。'
     '★服務晶圓廠者見 A2-09，服務 PCB 產線者見 B5-03。'),
    ('F', None, 1, '電子零組件', None),
    ('F1', 'F', 2, '被動元件', None),
    ('F1-01', 'F1', 3, '被動', '電阻、MLCC、電感、電容、磁性元件、EMI/EMC'),
    ('F2', 'F', 2, '連接器', None),
    ('F2-01', 'F2', 3, '連接器', 'Board-to-Board、Wire-to-Board、FPC、RF、High Speed、Automotive、Server'),
    ('F3', 'F', 2, '線材', None),
    ('F3-01', 'F3', 3, '線纜', 'Cable、Coaxial、Fiber、High Speed Cable'),
    ('F4', 'F', 2, '機構件', None),
    ('F4-01', 'F4', 3, '機構', '金屬沖壓、CNC、壓鑄、塑膠射出、組裝'),
    ('F5', 'F', 2, 'PCB零件', None),
    ('F5-01', 'F5', 3, '板上零件', 'Switch、Relay、Fuse、Sensor'),
    ('F6', 'F', 2, '電池相關零件', None),
    ('F6-01', 'F6', 3, '電池', 'BMS、Battery connector、Protection IC、Battery pack'),
    ('F7', 'F', 2, '記憶體模組 / SSD', None),
    # A wireless module is not a memory module and not Wi-Fi equipment: 鈺寶 sells
    # its own 2.4/5GHz audio protocol as a module to brand customers, and the only
    # neighbours were F7-01 (DRAM/SSD), D2-01 (Wi-Fi kit) and C11-01 (finished
    # peripherals). Kept as an L2 leaf rather than an L2+L3 pair.
    ('F8', 'F', 2, '無線模組',
     '無線音訊傳輸模組、RF／私有協定收發模組、藍牙與 2.4G 模組；'
     '以模組為出貨形式而晶片為自有設計者，晶片端另計 A4（見 R024 一）'),
    ('F7-01', 'F7', 3, '記憶體模組 (Module)',
     '將記憶體與快閃記憶體顆粒組裝為可出貨模組之製造與銷售：'
     'DRAM 模組 (UDIMM／SODIMM／RDIMM)、固態硬碟 (SSD)、記憶卡、隨身碟、'
     '嵌入式儲存模組。'
     '★與 A4-21／A4-19 的分界是**交付層次**：那兩格交付晶片，本節點交付裝上顆粒的模組。'
     '★與 A3-03／A3-07 的分界是**有無自有晶圓製造**：本節點採購顆粒而不自製。'
     '★系統端見 C9 System/Enterprise Storage。'
     '★2026-09-07 移除定義中括號內的三個公司名。'),
    ('G', None, 1, '電機 / 自動化 / 機器人', None),
    ('G1', 'G', 2, '工業自動化', None),
    ('G1-01', 'G1', 3, '自動化',
     'PLC、IPC、Motion Control、Servo、Robot、Vision；運動控制零組件（步進馬達、伺服馬達、驅動器）— 車用動力馬達見 H5-01'),
    ('G2', 'G', 2, '工業電腦', None),
    ('G3', 'G', 2, '機器人', None),
    ('G3-01', 'G3', 3, 'Industrial / Collaborative Robot', '工業機器人、協作機器人 (Cobot)'),
    ('G3-02', 'G3', 3, 'Mobile Robot', 'AMR、AGV'),
    ('G3-03', 'G3', 3, 'Service / Humanoid Robot', 'Service Robot、Humanoid'),
    ('G4', 'G', 2, '精密機械', None),
    ('G4-01', 'G4', 3, '精密機械零組件與工具 (Precision Components & Tooling)',
     'CNC、Tooling、Linear Guide、Ball Screw、Bearing、Gear；研磨與切削工具（砂輪、磨料、鑽石/cBN 工具、切削刀具）'),
    # A2-10's 閥件 are vacuum parts for semiconductor tools; G4-01 is transmission
    # and tooling; G6-01 is whole machines. An industrial valve for a refinery is
    # none of those, and 捷流's 62.36% sat on the fab utilities node.
    ('G4-02', 'G4', 3, '工業閥門與流體控制元件',
     '蝶閥、球閥、閘閥、控制閥、致動器與流體控制元件（石化、鋼鐵、造船、水處理、電廠）；'
     '半導體設備用真空閥件見 A2-10'),
    # Definition deferred (2026-09-04): G4 精密機械, G5 工具機 and G6 機械設備 all
    # plausibly hold a machine tool and none of the three is defined. Deferred for
    # the reason E's cells were -- the branch boundary comes first (R031).
    ('G5', 'G', 2, '工具機', None),
    ('G6', 'G', 2, '機械設備', None),
    ('G6-01', 'G6', 3, '專用機械', '食品機械、塑膠機械、紡織機械、包裝機械、其他專用機械（半導體設備→A2、PCB設備→B5）'),
    ('H', None, 1, '汽車 / EV', None),
    ('H1', 'H', 2, '傳統汽車', None),
    ('H2', 'H', 2, 'EV', None),
    # main_products stays NULL on purpose. The old value read
    # 「（亦為 Theme；投資題材用途保留）」 -- a note about the node, not a boundary,
    # sitting in the boundary column. H is frozen pending the product-vs-
    # application architecture question (R031), so there is no definition to
    # write yet, and NULL says that honestly where the note did not.
    ('H2-01', 'H2', 3, '電動車', None),
    ('H3', 'H', 2, '車用電子', None),
    ('H3-01', 'H3', 3, 'ADAS', 'Radar、Camera、LiDAR、感測融合'),
    ('H3-02', 'H3', 3, '車用資訊娛樂 (Infotainment)', None),
    ('H3-03', 'H3', 3, 'Telematics', None),
    ('H3-04', 'H3', 3, 'ECU', None),
    ('H3-05', 'H3', 3, 'Cockpit', '數位座艙'),
    ('H3-06', 'H3', 3, '車用網路', '車載乙太、CAN、LIN、FlexRay'),
    ('H4', 'H', 2, '車用半導體', None),
    # Mapping gate, 2026-09-03: H4-01 has a definition but is NOT open for
    # exposures yet, and neither is the rest of H. The whole branch carries zero
    # members and half of it carries no definition, so nothing has been lost by
    # leaving it shut — what is undecided is architectural, not per-company:
    # H4-01 names an end market, A4-* name products, and a chip designer with an
    # automotive line would qualify for both, which R004 forbids for one revenue
    # line. R031 states the entry condition; the axis question (does Industry
    # stay product/value-chain only, with 車用 becoming a separate application
    # exposure?) is open and decides whether H4-01 ever opens at all.
    ('H4-01', 'H4', 3, '車用IC', 'Power、MCU、Sensor、CIS、SiC、GaN'),
    ('H5', 'H', 2, 'EV Powertrain', None),
    ('H5-01', 'H5', 3, 'Motor', None),
    ('H5-02', 'H5', 3, 'Inverter', None),
    ('H5-03', 'H5', 3, 'OBC', None),
    ('H5-04', 'H5', 3, 'DC/DC', None),
    ('H5-05', 'H5', 3, 'BMS', None),
    ('H5-06', 'H5', 3, 'Battery', None),
    ('H5-07', 'H5', 3, 'Thermal Management', '電池/馬達熱管理、Cooling Plate、Thermal Interface Material、冷卻液路'),
    ('H6', 'H', 2, '車用零組件', None),
    ('H6-01', 'H6', 3, '零組件', '車燈、車身、座椅、內裝、傳動、煞車、輪胎'),
    ('I', None, 1, '能源 / 電力 / 儲能', None),
    ('I1', 'I', 2, '太陽能', None),
    ('I1-01', 'I1', 3, '太陽能', 'Silicon wafer、Cell、Module、Inverter'),
    ('I2', 'I', 2, '風電', None),
    ('I2-01', 'I2', 3, '風力', '陸上、離岸'),
    ('I3', 'I', 2, '儲能', None),
    ('I3-01', 'I3', 3, '儲能系統', 'Battery Cell、BMS、PCS、EMS、Container、System Integration'),
    ('I4', 'I', 2, '充電', None),
    ('I4-01', 'I4', 3, '充電', '充電樁、充電營運'),
    ('I5', 'I', 2, '電網', None),
    ('I5-01', 'I5', 3, '電網', '輸配電、智慧電網'),
    ('I6', 'I', 2, '重電 / 電力設備', None),
    ('I6-01', 'I6', 3, '重電', 'Transformer、Switchgear、配電盤、電線電纜、發電設備'),
    ('I7', 'I', 2, '電力電子', None),
    ('I7-01', 'I7', 3, '電力電子', 'Inverter、PCS、電源模組、UPS'),
    ('I8', 'I', 2, '電力管理 / 電能品質', None),
    ('I8-01', 'I8', 3, '電力設備', 'UPS、PDU、Power Quality、Energy Management（電力設備本身；資料中心應用見 C13/C14）'),
    ('J', None, 1, '化學 / 石化',
     '化學品之製造。**本分支的主軸是產品在化學價值鏈上的位置與其價值來源**：'
     '愈上游者以規模與成本為價值來源（J1 上游石化、J2 芳香烴、J3 合成橡膠原料、J9 化纖原料、J10 無機化學品），'
     '愈下游者以特定功能、配方或性能為價值來源（J4 特用化學、J5 樹脂、J6 塗料與油墨、J7 染料與顏料、'
     'J11 接著劑、J15 碳黑）。'
     '★三格明示不在該主軸上，依 R031 不納入其互斥判斷：'
     'J13 煤化工以**原料來源**（煤而非石油）分格；'
     'J14 家用化學品與 J16 電子成像耗材以**終端用途**分格，收的是已配製完成、可直接使用的成品。'
     '★J8 電子化學與 J4 特用化學的邊界目前僅由節點名稱區分，尚未寫成可比較的定義，'
     '屬 taxonomy backlog（記於 J4 定義內），依 R031 不得以任一方的空白狀態排除另一方。'),
    ('J1', 'J', 2, '上游石化',
     '石化產業之上游原料與基礎石化產品：以石油、天然氣或其裂解產物為起點所生產之'
     '基礎單體、中間體與大宗聚合物原料，子節點再依產品／製程邊界細分。'
     '★**與 J4 特用化學的分界是價值來源**：本節點的產品以**大宗、基礎、可替代**的性質交易，'
     '價值來自規模、成本與純度規格；J4 的產品價值來自特定功能、配方、性能或終端用途。'
     '同一種化學品若被做成具功能訴求的配方品，即離開本節點。'
     '★本節點收的是**原料本身**，不是以它加工出的製品：塑膠加工品見 K、化纖成品見 M、'
     '樹脂本體見 J5、芳香烴系列見 J2。'),
    ('J1-01', 'J1', 3, '石化+塑化原料',
     '基礎石化單體與中間體，以及由其聚合而成的大宗塑膠原料：'
     '乙烯、丙烯、丁二烯、苯乙烯單體 (SM)、氯乙烯單體 (VCM)、丙烯腈 (AN)、乙二醇 (EG) 等單體與中間體；'
     'PE (LDPE／HDPE／LLDPE)、PP、PVC、PS／EPS／GPS／IPS、ABS、EVA 等大宗聚合物原料；'
     '以及石油系蠟品等大宗煉製產品。'
     '★2026-09-07 由「乙烯、丙烯、丁二烯、PE/PP/PVC/ABS/EVA（塑化原料）」改寫：'
     '原定義是**產品列舉**而非邊界，且列舉涵蓋不住自己的成員——'
     '1742 台蠟「精煉白蠟及微晶蠟 64.56%」、1710 東聯「乙二醇 66.00%」、'
     '1314 中石化「丙烯腈 40.14%」三條都不在原列舉之內。'
     '改以「基礎單體／中間體／大宗聚合物」的類別陳述，列舉降為例示。'),
    ('J2', 'J', 2, '芳香烴', 'PTA、PET、苯、甲苯'),
    ('J3', 'J', 2, '合成橡膠原料', 'SBR、BR、SBS'),
    ('J4', 'J', 2, '特用化學',
     '以**特定功能、性能或應用需求為主要價值來源**之特用化學品，'
     '子節點再依產品功能／化學品類型細分。'
     '★**與 J1 上游石化的分界**：J1 的產品以大宗基礎原料的身分交易，價值來自規模與成本；'
     '本節點的產品以它「能做到什麼」交易——阻燃、抗氧化、光安定、界面活性、觸媒、導電——'
     '即使化學結構同源，只要價值主張是功能而非 feedstock，即屬本節點。'
     '★與 J5 樹脂／J6 塗料與油墨／J11 接著劑的分界：那三格是已具名的產品類別，'
     '本節點不吸收可明確歸入它們者。'
     '★★**taxonomy backlog（2026-09-07 記錄，非分類依據）**：J8 電子化學（電子級化學品）'
     '與本節點子項的半導體與電子用化學品在文字上重疊，兩者的邊界尚未寫成可比較的定義。'
     '在該邊界完成前，依 R031 不得以任一方的定義空白或無成員作為排除另一方的依據；'
     '現有成員維持原判，待邊界寫定後做反向 overlap review。'),
    ('J4-01', 'J4', 3, '特化',
     '以功能訴求交易之特用化學品，依其功能類別例示如下：'
     '**高分子添加劑**（阻燃劑、抗氧化劑、光安定劑與紫外光吸收劑、光引發劑、交聯劑、成核劑）、'
     '**觸媒與電池材料**（氧化觸媒、動力電池正極與電解質材料）、'
     '**界面活性劑**（脂肪醇聚氧乙烯醚 AEO／其硫酸鹽 AES）、'
     '**環氧乙烷衍生物**（乙醇胺、乙二醇醚、聚乙二醇）、'
     '**造紙化學品**（乾強劑、上膠劑、顯色劑）、半導體與光電製程用化學品。'
     '以特用化學品之**製造**為業務核心；清潔劑等家用成品見 J14。'
     '★2026-09-07 補上高分子添加劑與觸媒／電池材料兩類：'
     '原列舉未含它們，而 3430 奇鈦科（紫外光吸收劑、抗氧化劑、光引發劑 97.70%）、'
     '1721 三晃（無鹵阻燃劑、抗氧化劑、交聯劑、成核劑 57.19%）、'
     '4739 康普（氧化觸媒與動力電池材料 77.23%）三家的主力產品全在列舉之外，'
     '列舉涵蓋不住成員即為定義不完整(R025)，故修定義而非改歸屬。'),
    ('J5', 'J', 2, '樹脂',
     'Epoxy、PU、Acrylic、Silicone、石油樹脂（C5/C9、氫化石油樹脂）。'
     '以樹脂本體之製造為業務核心；配方成品見 J6 塗料與油墨、J11 接著劑'),
    ('J6', 'J', 2, '塗料與油墨 (Coating & Ink)', '建築/工業/防火塗料、溶劑性與 UV 油墨'),
    # Renamed from 染料: both members sell pigments (永光 色料化學品, 大恭
    # 調製應用顏料+合成有機顏料) and 染料 alone is narrower than what the node
    # holds. R025 says an imprecise name is fixed by renaming, not by a new node.
    ('J7', 'J', 2, '染料與顏料',
     '著色劑之製造：有機顏料、無機顏料、分散染料、活性染料、色料與色漿之調製。'
     '★與 J6 的分界是**交付層次**：本節點做的是著色劑本身，'
     'J6 做的是已配製成的塗料與油墨。'
     '★碳黑見 J15；電子成像用色料（碳粉）見 J16。'),
    ('J8', 'J', 2, '電子化學', '電子級化學品'),
    ('J9', 'J', 2, '化纖原料', '聚酯粒、尼龍粒'),
    ('J10', 'J', 2, '無機化學品 (Inorganic)', '純鹼、氧化鋅、亞硫酸鹽、無機鹽類'),
    ('J11', 'J', 2, '接著劑 (Adhesive)', '熱熔膠、白膠、工業接著劑、封裝膠'),
    ('J12', 'J', 2, '農用化學品 (Agrochemical)', '農藥、植物保護、肥料'),
    ('J13', 'J', 2, '煤化工 (Coal Chemical)', '煤焦油、焦炭、精碳材料'),
    ('J14', 'J', 2, '家用化學品 (Household)', '清潔劑、洗衣劑、芳香除濕、家用消耗品'),
    ('J15', 'J', 2, '碳黑 (Carbon Black)', '橡膠補強碳黑、色料碳黑、導電碳黑'),
    # Toner is a dry imaging powder sold as a printer consumable: J6 is coatings
    # and liquid inks, and J15 carbon black is the raw material that goes into
    # toner rather than the toner itself.
    ('J16', 'J', 2, '電子成像耗材 (Toner & Imaging Consumables)',
     '碳粉、碳粉匣、列印耗材、電子成像用色料（碳黑原料見 J15）'),
    # K 的子節點分屬三層而不是同一層，同層才互斥。兩個歷史節點跨了層：K3 工程塑膠
    # 是原料的一個等級（與 K1 同軸）、K4 塑膠射出是一種成形方式（與 K2 同軸）。
    # 兩者目前都 0 成員，但 K2 已有成員用到射出（1323 永裕的「射出廠製品」），
    # 所以重疊在資料裡是活的，依 R031 要先解邊界再 mapping。
    ('K', None, 1, '塑膠 / 橡膠',
     '塑膠與橡膠。子節點分兩層，**同層才互斥**：原料層（K1 塑膠原料，其下 K1-01 加工用原料）、'
     '加工與製品層（K2 塑膠加工、K5 橡膠製品、K6 輪胎）。'
     '★★**2026-09-07 裁定：移除 K3 工程塑膠與 K4 塑膠射出**（R031 sibling 單軸條款）。'
     '此前的版本已診斷出「K3 是原料的一個**等級**（與 K1 同軸）、K4 是加工的一種**成形方式**（與 K2 同軸）」'
     '並對兩者上了 mapping gate，缺的只是那一刀；R031 於同日增訂單軸條款後，'
     '正確處置明確為重整分類軸而非替空格補寫定義。'
     '★證據面同向且更強——兩格皆無專屬領地：'
     '掃全庫 2025 營收品項，命中「工程塑膠／尼龍／PC／POM／PBT／PPS」的七條線裡，'
     '尼龍粒（1455 集盛、1447 力鵬）屬 J9 化纖原料、尼龍與 PC 複合材料（6151 晉倫）屬 K1-01 加工用原料、'
     '尼龍包款（8442 威宏-KY）是成品，**沒有一條需要 K3**；'
     '命中「射出／成型」的十九條線裡，射出成型**機**（6603 富強鑫、4544 春日、2070 精湛）屬 G6-01 專用機械、'
     '電子與光學用射出**件**（6235 華孚、6867 坦德、3685 元創、4545 銘鈺等）屬 F4-01 機構'
     '——該節點定義逐字列有「塑膠射出」——'
     '容器與瓶罐類射出製品屬 K2（1323 永裕的 weight_item 即含「射出廠製品」），'
     '**同樣沒有一條需要 K4**。K4 不只與 K2 同軸重疊，還與 F4-01 三方重疊。'),
    ('K1', 'K', 2, '塑膠原料',
     '供二次加工使用之塑膠原料：以聚合物粒料為基礎所調配、改質或再生而成的加工端投料，'
     '子節點再依原料型態細分。'
     '★**與 J1-01 石化+塑化原料的分界是所在的價值鏈段**：J1-01 是石化廠聚合出來的大宗聚合物本體'
     '（PE／PP／PVC／PS／ABS／EVA），本節點是把那些粒料再調配、著色、補強或回收再造之後，'
     '交給加工廠成形用的原料。同一顆 PP 粒在石化廠出料時屬 J1-01，'
     '被做成玻纖補強複合料或色母後屬本節點。'
     '★**與 K2 的分界是交付形式**：本節點交付的是尚未成形的料，K2 交付的是已成形的製品。'
     '★材料等級（工程塑膠、通用塑膠、生物可分解）是原料的**屬性**不是同層類別，'
     '不在本軸上另切節點——原 K3 工程塑膠即因此於 2026-09-07 移除，見 K 母節點。'),
    ('K1-01', 'K1', 3, '加工用原料', '複合料、色母、再生料（非上游塑化原料）'),
    ('K2', 'K', 2, '塑膠加工',
     '以塑膠原料二次加工成形之製品：薄膜與片材、塑膠布與合成皮、管材與板材、'
     '型材與塑膠建材、容器與瓶罐、軟管、膠帶與包材、發泡製品。'
     '★**涵蓋所有成形方式**（壓延、押出、吹膜、射出、貼合、發泡）——'
     '成形方式不是產品類別，故不因射出成形而改掛他格'
     '（原 K4 塑膠射出已於 2026-09-07 移除，見 K 母節點）。'
     '★與 F4-01 機構的分界是**產品類別**不是成形方式：兩者都用射出，'
     '本節點做的是塑膠製品本身（容器、片材、管材、皮布），'
     'F4-01 做的是裝進他人產品裡的機構零組件。'
     '★不含：塑膠原料之調配與粒料（K1）、橡膠製品（K5）、輪胎（K6）。'),
    ('K5', 'K', 2, '橡膠製品', '天然橡膠、合成橡膠、工業橡膠'),
    ('K6', 'K', 2, '輪胎', '汽車胎、卡車胎、工程胎、機車胎'),
    ('L', None, 1, '鋼鐵 / 金屬', None),
    ('L1', 'L', 2, '鋼鐵製造', '鋼胚、熱軋、冷軋、鍍鋅、電磁鋼、不鏽鋼、特殊鋼'),
    ('L2', 'L', 2, '鋼鐵加工', '螺絲、扣件、鋼管、鍛造'),
    ('L3', 'L', 2, '金屬加工', 'CNC、沖壓、壓鑄'),
    ('L4', 'L', 2, '銅（非鐵）', '銅箔、銅材、電解銅、銅加工'),
    ('L5', 'L', 2, '鋁（非鐵）', '鋁錠、鋁擠型、鋁板、鋁合金'),
    ('L6', 'L', 2, '鎳',
     '鎳及鎳合金之冶煉、加工與銷售：電解鎳、鎳錠與鎳粉、鎳合金與鎳基超合金、'
     '鎳帶與鎳片、電鍍與電池用鎳材。'
     '★與 L9 稀有金屬、L10 貴金屬同軸（依金屬別切分），彼此以金屬種類互斥。'),
    ('L7', 'L', 2, '鋅', None),
    ('L8', 'L', 2, '鎢', None),
    ('L9', 'L', 2, '稀有金屬',
     '鈦、鉭、鈮、鉬、鋯、銦、鎵、鍺、稀土等工業稀有金屬之冶煉、鍛造與加工。不含金銀鉑族貴金屬——貴金屬價格隨金銀行情與回收循環，與稀有金屬的工業需求是兩個市場'),
    # L9 was defined this round to exclude precious metals, so there is no
    # widening of it that covers them — which is what makes this a new node
    # under R025 rather than a definition fix. Bullion processing tracks the
    # gold and silver price and the recycling loop; rare metals track
    # industrial and strategic demand.
    ('L10', 'L', 2, '貴金屬材料 (Precious Metals)',
     '金、銀、鉑族金屬之熔煉、加工、回收與銷售。以貴金屬本身之材料加工、回收及銷售為業務核心；不因其最終應用於電子、半導體或其他產業而改歸該下游產業（半導體用金靶材屬 A1-09）'),
    ('M', None, 1, '紡織 / 成衣', None),
    ('M1', 'M', 2, '上游/化纖', '聚酯、尼龍、Rayon、彈性纖維'),
    ('M2', 'M', 2, '紡紗', '棉紗、混紡'),
    ('M3', 'M', 2, '織布', '平織、針織、機能布'),
    ('M4', 'M', 2, '染整', None),
    ('M5', 'M', 2, '成衣', '運動服、戶外服、機能服'),
    ('M6', 'M', 2, '機能性材料', '防水、運動、戶外'),
    ('M7', 'M', 2, '不織布 (Nonwoven)', '紡黏、熔噴、複合不織布；衛材、醫材、工業用'),
    ('N', None, 1, '生技 / 醫療', None),
    ('N1', 'N', 2, '製藥', '原料藥、學名藥、特殊學名藥、新藥'),
    ('N2', 'N', 2, '生技', '抗體、蛋白質、細胞治療、基因治療'),
    ('N3', 'N', 2, '醫療器材', '手術器械、植入物、影像設備、牙科'),
    ('N4', 'N', 2, '診斷', 'IVD、PCR、快篩、檢測'),
    ('N5', 'N', 2, 'CDMO', None),
    ('N6', 'N', 2, 'CRO', None),
    ('N7', 'N', 2, '保健食品', None),
    ('N8', 'N', 2, '醫療服務', None),
    ('O', None, 1, '食品 / 農業', None),
    ('O1', 'O', 2, '食品製造', '食用油、麵粉、肉品、冷凍食品、乳品'),
    ('O2', 'O', 2, '飲料', None),
    # Definition Pending (2026-09-03): named correctly but carries no Product
    # Definition, and companies are waiting on it. Do NOT write one to fit the
    # company in front of you -- that is how a taxonomy ends up shaped by the
    # cases it happened to meet. Definition first, then map the whole table,
    # then re-run every Core whose candidate set the definition changed.
    # Waiting: 8068 全達 4.92% 酒品買賣.
    ('O3', 'O', 2, '食品通路', None),
    ('O4', 'O', 2, '農業', '種子/種苗、肥料、農藥、農機、農業科技'),
    ('O5', 'O', 2, '水產', None),
    ('P', None, 1, '建材 / 玻璃 / 造紙', None),
    ('P1', 'P', 2, '水泥', '水泥、熟料、預拌混凝土'),
    ('P2', 'P', 2, '玻璃', '平板/建築/汽車/光電/特殊玻璃'),
    ('P3', 'P', 2, '造紙', '紙漿、工業紙、包裝紙、紙箱、特殊紙'),
    ('P4', 'P', 2, '建材', '磁磚、石材、衛浴、廚具、隔音、防火、綠建材'),
    ('Q', None, 1, '營造 / 不動產', None),
    ('Q1', 'Q', 2, '營造', '公共工程、交通建設、廠房、科技廠、水利、土木'),
    ('Q2', 'Q', 2, '建設', '住宅、商辦、都更、危老'),
    ('Q3', 'Q', 2, '不動產', '工業地產、REIT、都市更新、資產股'),
    ('R', None, 1, '航運 / 物流', None),
    ('R1', 'R', 2, '貨櫃航運', '遠洋、近洋'),
    ('R2', 'R', 2, '散裝航運', 'Capesize、Panamax、Supramax'),
    ('R3', 'R', 2, '油輪', '原油、成品油、LNG'),
    ('R4', 'R', 2, '航空', '客運、貨運（航空公司唯一歸此）'),
    ('R5', 'R', 2, '港口', None),
    ('R6', 'R', 2, '造船 / 船舶', '造船、船舶零件、船舶維修'),
    ('R7', 'R', 2, '物流', '海運/空運物流、冷鏈、倉儲'),
    ('R8', 'R', 2, '鐵路 / 陸運', None),
    ('S', None, 1, '航太 / 國防', None),
    ('S1', 'S', 2, '航太', '飛機零件、航太材料、引擎零件'),
    ('S2', 'S', 2, '無人機', 'Drone、Drone IC、Flight Controller、Camera'),
    ('S3', 'S', 2, '國防', None),
    ('S3-01', 'S3', 3, 'Defense Electronics', '軍用電子、航電、通訊、光電'),
    ('S3-02', 'S3', 3, 'Radar / EW', '雷達、電子戰'),
    ('S3-03', 'S3', 3, 'Weapons System', '飛彈、彈藥、火砲、武器系統'),
    ('S4', 'S', 2, '衛星', '衛星、衛星通訊、地面設備、低軌衛星'),
    ('T', None, 1, '電信', None),
    ('T1', 'T', 2, '電信商', '行動通信、固網、寬頻、IDC、Cable'),
    ('T2', 'T', 2, '通訊服務', None),
    ('U', None, 1, '資訊服務 / 軟體 / 雲端', None),
    ('U1', 'U', 2, '軟體', 'ERP、CRM、SaaS、Security、Enterprise/HR Software'),
    ('U2', 'U', 2, 'SI', 'System Integration、IT Service'),
    ('U3', 'U', 2, 'Cloud', 'Cloud Service、Cloud Infrastructure/Integration'),
    ('U4', 'U', 2, 'Data Center', 'IDC、Colocation'),
    ('U5', 'U', 2, 'Cybersecurity', 'Network Security、Endpoint、Identity、SOC'),
    ('U6', 'U', 2, 'AI Software', 'LLM、AI Agent、Computer Vision、AI SaaS、AI Infrastructure'),
    ('U7', 'U', 2, 'Digital Platform', None),
    # V1-V6 are cut on ONE axis: the product category of what is distributed.
    # 3090 日電貿 is the proof and it was sitting in the data the whole time --
    # one company, two lines, two nodes: its 被動元件 81.35% is V2's Core and its
    # 半導體 7.87% is a V1 Major. A second axis (broad-line vs specialist) would
    # break exactly that: 日電貿 is a specialist, so it would go to V6, while its
    # passives line already belongs to V2, and the 81.35% would have two homes.
    # V7 EMS is a business-model axis, not a product one, so it is outside the
    # V1-V6 mutual-exclusivity test rather than a peer of them (R031).
    ('V', None, 1, '電子通路 / EMS',
     '通路／代理／EMS 類業務。V1–V6 依**所經銷產品的類別**建立互斥的同層節點（產品類別軸）；'
     'V7 EMS 是製造服務的商業模式，屬不同維度，不納入 V1–V6 的產品類別互斥判斷。'),
    # Rebuilt 2026-09-03. The field used to read 文曄、大聯大、至上 -- three company
    # names, a mapping result written into the definition column, which is worse
    # than empty because it reads as a definition and 32 members were judged
    # against it. The node is NOT split: a review of all 32 found R025's hard
    # condition met by none, and 港建*/華立/崇越/利機 each report ONE line spanning
    # components and equipment, so splitting would put the classification at a
    # finer grain than the evidence (R004).
    #
    # Named 半導體通路（含設備與材料）rather than 半導體／電子零組件通路: the
    # broadening that needed to show in the name is the equipment and materials,
    # and 電子零組件 is the superset V6 is the residual of -- claiming it here
    # would put the same words on two sibling nodes.
    ('V1', 'V', 2, '半導體通路（含設備與材料）',
     '半導體之代理／通路銷售業務線：IC（類比、邏輯、特定應用、微控制器）、記憶體、'
     '分立元件與功率元件，以及半導體製程設備與材料之代理銷售。'
     '該業務線收入來自佣金或轉售價差，本身不含自有製造。'
     '★不含被動元件（V2）。依 R022，本節點只收公司的代理／通路業務線；'
     '同一公司若另有自有製造線，各自獨立判定，不因此降級。'
     '★不含：資訊產品／系統通路（V3）、工控（V4）、網通（V5）、檢測與工程服務、'
     '非電子產品之貿易。'
     '★設備與材料收在本節點而非另立，理由是證據粒度：港建*「電子零件、設備及材料 92.46%」、'
     '華立「半導體產業 31.27%」、崇越「半導體相關產品 84.60%」、利機「半導體產品 67.00%」'
     '都是公司對外揭露的不可再拆營收線，拆開會使分類粒度高於證據粒度（R004）。'),
    ('V2', 'V', 2, '被動元件通路',
     '被動元件之代理／通路銷售：電容（陶瓷、固態、電解、鉭質）、電阻、電感、變壓器、'
     '濾波器、石英與振盪元件、保護元件。'
     '★不含二極體、電晶體、MOSFET 等分立元件——那些是主動元件，屬 V1。'),
    # Definition written 2026-09-03; mapping still owed. Waiting on a re-judge:
    # 2347 聯強 62.8% 3C, 5386 青雲 12.68% 板卡與伺服器.
    ('V3', 'V', 2, '電腦通路',
     '資訊系統與消費電子**成品**之代理／通路銷售：個人電腦、伺服器、儲存設備、'
     '周邊裝置、板卡、手機與消費性電子產品。'
     '★與 V1／V2 的分界是成品 vs 零組件：本節點賣的是可獨立使用的整機或成品，'
     '不是裝進別人產品裡的元件。'),
    ('V4', 'V', 2, '工控通路',
     '工業自動化與控制產品之代理／通路銷售：PLC、變頻器、伺服驅動與工業馬達、'
     '工業感測器與量測儀器、人機介面、工業電腦、氣動與液壓元件。'
     '★與 V1 的分界同 V3：本節點賣的是可獨立部署的控制裝置與模組，不是構成它們的半導體。'),
    ('V5', 'V', 2, '網通通路',
     '網路與通訊設備之代理／通路銷售：交換器、路由器、無線基地台、'
     '光纖與傳輸設備、網通模組與終端裝置。'
     '★與 V1 的分界同 V3／V4：裝置與設備，不是構成它們的半導體。'),
    # Renamed from 電子零件代理 2026-09-03. 電子零件 is the SUPERSET of V1's
    # discretes and V2's passives, so a node by that name cannot be a peer of
    # them -- it was a residual bucket wearing a category's name. Definition
    # written 2026-09-03; mapping still owed. Waiting on a re-judge:
    # 2459 敦吉, 3444 利機 31%, 8096 擎亞 15.54%, 6924 榮惠 87.89%.
    ('V6', 'V', 2, '其他電子零組件通路',
     '僅收納**產品類別無法歸入 V1～V5 任一者**之電子零組件代理／通路業務線。'
     '★Residual node，依 R031 有兩條前提：(一) 其成立以 V1～V5 已具明確、互斥且足以覆蓋'
     '其預定範圍的 Definition 為前提；(二) **不得吸收已有 sibling node 可明確歸屬的產品**'
     '——某產品若理論上可歸入 V1～V5 而現行 Definition 不足以判定，應先修訂該 Definition，'
     '不得直接放進本節點。'
     '★可能落入的產品類別（例示非窮舉，且每一筆仍須先逐一問過 V1～V5）：'
     '連接器與線材、機構與被覆件、電池與電源模組、光電元件與顯示模組、印刷電路板。'),
    ('V7', 'V', 2, '電子製造服務 (EMS)',
     '為客戶提供電子產品之**受託製造、組裝、測試、加工或整體製造服務**者，'
     '包括依客戶規格進行 PCB 組裝 (PCBA)、電子產品組裝、模組代工與相關製造流程整合之 EMS 服務。'
     '★**本節點以受託製造服務為分類依據，不因所代工產品之終端類別而分入 V1–V6 的產品代理節點。**'
     '兩者問的是不同的問題：V1–V6 問「你賣／代理什麼產品」，本節點問「你替客戶做什麼製造服務」。'
     '★本節點成立的理由不是「它是一種商業模式」，而是**受託製造本身即為一項獨立、可辨識、'
     '可重複分類的經濟活動**——它是公司提供給客戶的 value proposition，不是某個產品的修飾語。'
     '這一點決定了它能否成為 Industry leaf；同理，若某個「服務」只是產品的用途或條件描述'
     '（如 Burn-in、HDI），就不具備這個資格(R031)。'
     '★★**成員資格的證據門檻（2026-09-07 訂）：受託製造必須是公司自己揭露的一條營收線或'
     '自述的主要業務，不得僅憑所營業務句中出現「組裝」「加工」等字樣認定。**'
     '兩個對照：2316 楠梓電年報比重表單列「印刷電路板組裝 22.92%」，成立；'
     '6269 台郡所營業務句寫「⋯之設計、開發、製造及銷售，以及組裝打件模組等業務」'
     '但比重表只有軟式印刷電路板與射頻積體電路兩條、無任何組裝服務收入，'
     '且該組裝是把元件打在自己的 FPC 上（前向整合），不是替第三方製造，不成立。'
     '★不含**採購與供應鏈服務**：委由他廠生產再銷售者，賣的是產品不是製造服務，'
     '依所售產品類別歸 V1–V6（6924 榮惠-KY創即屬此類，已移至 V6）。'),
    # Numbered V8 rather than slotting in beside V6: V7 is already EMS and
    # V7-01 carries seven members, and a node_code that has been used is not
    # renumbered for the sake of reading order.
    #
    # Created 2026-09-04 after the V1-V8 boundary matrix, not for the companies
    # that revealed the gap (R025/R031). Two rules make it exclusive of its
    # siblings: component is not material -- a panel MODULE is V6 and panel
    # MATERIAL is here -- and a material belongs to the process it serves, not
    # to the technology of the end device, so O-LED emitting material for
    # display manufacture is V8 even though an LED is a semiconductor. That is
    # the same test A2 uses to separate front-end from back-end equipment.
    ('V8', 'V', 2, '非半導體電子材料通路',
     '以代理、經銷、通路或供應鏈服務方式提供**非半導體產業**之電子材料、製程材料、耗材'
     '及相關材料產品：顯示器、光電、PCB／載板、電子製造等領域之材料與耗材。'
     '★不含**半導體製程材料**（屬 V1，與本節點同為材料通路，差別在服務的產業）。'
     '★不含以**電子零組件本體**為代理標的者（屬 V6）——Component ≠ Material，'
     '面板**模組**是 V6，面板**材料**才是本節點。'
     '★材料的歸屬看它服務的製程，不看終端元件的技術：LED 是半導體元件，'
     '但供顯示模組製造的 O-LED 發光材料仍屬本節點；供 IC／LED **封裝製程**的材料與工具屬 V1。'),
    ('V7-01', 'V7', 3, '電子製造服務 (EMS)', 'EMS、ODM、OEM、Module、System Integration、Box Build'),
    ('W', None, 1, '金融 / 保險 / 證券', None),
    ('W1', 'W', 2, '銀行', '商業銀行、國內/外商銀行、數位銀行'),
    ('W2', 'W', 2, '金融服務', None),
    ('W2-01', 'W2', 3, '消費金融', '房貸、信貸、信用卡、財富管理'),
    ('W3', 'W', 2, '特色金融', '中小企業金融、企業金融、跨境金融'),
    ('W4', 'W', 2, '金控', None),
    ('W5', 'W', 2, '壽險', '壽險、年金、投資型保單、外幣保單'),
    ('W6', 'W', 2, '產險', '車險、火險、工程險、責任險'),
    ('W7', 'W', 2, '再保', None),
    ('W8', 'W', 2, '證券商', '經紀、自營、承銷'),
    ('W9', 'W', 2, '投信 / 資產管理', 'ETF、主動式ETF、基金、資產管理'),
    ('W10', 'W', 2, '租賃', None),
    ('W11', 'W', 2, '信用卡 / 支付', None),
    ('X', None, 1, '零售 / 消費通路',
     '面向終端消費者之商品零售與零售通路活動：以店面、賣場或網路平台將商品售予'
     '最終消費者，收入來自商品銷售價差、專櫃與聯營抽成、平台服務費。'
     '★★2026-09-07 本支經 parent identity／sibling axis／商品垂直三題裁定後重整，'
     '裁定內容如下四條。'
     '★**一、本支是零售，不是消費**。消費活動中的餐飲、旅館、旅遊已由 Y 承接，'
     '不得以「消費」這個大傘把它們併回本支；節點名同日由「消費 / 零售」改寫，'
     '因為原名本身就在誘導那個併法。'
     '★**二、製造端不屬本支**。自製消費性商品的公司是製造商，其 value-chain role '
     '在製造側，不因為賣的是消費品而成為零售商。全樹目前沒有消費品製造節點，'
     '這是已知的 taxonomy gap（R025）——掃 2025 年報，未分類母體中主要營收線落在'
     '鞋類、自行車、家具、球具與健身器材、寵物用品、隱形眼鏡且占比 >= 20% 的公司'
     '共 34 家、36 條線，其中 12 條 >= 90%。**但缺節點不是把公司塞進本支的理由**，'
     '也還不是開節點的理由：那 34 家目前全部未經逐家裁定，'
     '在對它們的 evidence 做過 population 檢查之前，任何共通邊界都只是先驗知識。'
     '★**三、sibling 分類軸＝零售業態，且僅為 PROPOSAL 而非 approved**：'
     '百貨、超商、電商、賣場，依店型與交易場所切分，同層互斥。'
     '標為提案的理由是證據還撐不住——全庫 2025 年報中零售業者的營收切分方式共有'
     '門市別、業態別、事業別、商品垂直、經營模式、收入性質等七種，彼此不同軸；'
     '其中直接以業態命名營收線的只有一家一條（「量販業」），'
     '而最能定義超商、百貨、賣場三種業態的三家指標公司在庫內營收表皆為 0 筆，'
     '只有敘述。**沒有 driver 不等於沒有市場；但沒有 driver，'
     '就不能先把 taxonomy 當成已證實。**'
     '★**四、因此本支全部子節點上 mapping gate**：在業態軸經實際 population evidence '
     '驗證、且各子節點寫出 Product Definition 之前，不得掛任何公司(R031)。'
     '★**商品垂直不是本支的 sibling**。家電、家具、美妝、運動用品、食品零售五格'
     '（原 X5～X8、X10）與業態軸不同軸——同一家超商同時賣食品與美妝，'
     '同一間百貨同時賣家電與運動用品——已於 2026-09-07 移除。'
     '零售的商品類別應以 attribute 或 Theme 表達，但 tw.theme_master 現有 11 個 '
     'theme_group 全屬科技、能源與總經，**沒有消費／零售 group**，'
     '故本輪是直接移除而非移入 Theme，theme group 的建立屬 backlog。'
     '處置與 B2-04／B2-05／B2-06、A5-03／A5-04、K3／K4 一致：'
     '屬性、應用與方法被誤置為 sibling 時，正解是移除，不是替空格補寫定義。'
     '★原 X9 居家 / 生活用品同日移除：其定義列的家用品、文具、玩具、樂器、珠寶、皮革'
     '同樣是商品類別而非業態，且其三家成員全部是製造商，沒有一家是零售商——'
     '本支唯一有成員的一格，成員卻全部不符 parent 定義，這正是重整的起點。'
     '★與 O 食品 / 農業的分界是價值鏈段：O 是食品的生產與加工，本支是把商品賣給'
     '終端消費者。與 V 電子通路的分界是客戶：V 的客戶是廠商，本支的客戶是終端消費者。'),
    ('X1', 'X', 2, '百貨', None),
    ('X2', 'X', 2, '超商', None),
    ('X3', 'X', 2, '電商', None),
    # Definition Pending (2026-09-03): named correctly but carries no Product
    # Definition, and companies are waiting on it. Do NOT write one to fit the
    # company in front of you -- that is how a taxonomy ends up shaped by the
    # cases it happened to meet. Definition first, then map the whole table,
    # then re-run every Core whose candidate set the definition changed.
    # All four of X1-X4 sit under X's mapping gate: the retail-format axis is a
    # proposal, not evidence, so a definition written now would be prior
    # knowledge dressed as a boundary. main_products stays NULL deliberately --
    # a note about a node's status is not a boundary and does not belong in the
    # boundary column (the H2-01 lesson).
    # Waiting on the gate: 3555 博士旺 56.91% 電玩通路, 1712 興農 23% 農友超市.
    ('X4', 'X', 2, '賣場 / 通路', None),
    # X5 家電 / X6 家具 / X7 美妝 / X8 運動用品 / X9 居家·生活用品 /
    # X10 食品零售 removed 2026-09-07: product verticals, not retail formats.
    # See X. X9's three members were all manufacturers; their disposition is
    # recorded in db/industry_data.py where the X9 member block used to be.
    ('Y', None, 1, '觀光 / 餐旅 / 娛樂', None),
    ('Y1', 'Y', 2, '飯店', None),
    ('Y2', 'Y', 2, '旅行社', None),
    ('Y3', 'Y', 2, '遊樂園', None),
    ('Y4', 'Y', 2, '餐飲',
     '餐飲服務：餐廳與連鎖餐飲之經營、外帶與外送、團膳與餐飲供應、餐飲品牌與加盟。'
     '★飯店附設之餐飲併入 Y1 飯店，不在本節點分列。'),
    # Definition Pending (2026-09-03): named correctly but carries no Product
    # Definition, and companies are waiting on it. Do NOT write one to fit the
    # company in front of you -- that is how a taxonomy ends up shaped by the
    # cases it happened to meet. Definition first, then map the whole table,
    # then re-run every Core whose candidate set the definition changed.
    # Waiting: 1312 國喬 6.64% 電視頻道與影視內容(緯來). A new sibling was
    # proposed instead; refused, because an undefined empty node next door is a
    # definition question, not a missing node.
    ('Y5', 'Y', 2, '娛樂 / 休閒', None),
    ('Z', None, 1, '公用事業 / 環保', None),
    ('Z1', 'Z', 2, '電力公用事業', '台電概念、發電/輸電/配電公用事業'),
    ('Z2', 'Z', 2, '天然氣', 'LNG、天然氣發電、瓦斯'),
    ('Z3', 'Z', 2, '石油', '原油、煉油、油品'),
    ('Z4', 'Z', 2, '水務', '污水、工業用水、純水'),
    ('Z5', 'Z', 2, '廢棄物', '廢棄物處理、回收、資源循環'),
    ('Z6', 'Z', 2, '環保工程', '空污、廢氣、脫硫、脫硝、環境工程'),
]
