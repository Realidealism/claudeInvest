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
    ('A2-10', 'A2', 3, '真空 / 精密零組件',
     '凡主要交付予半導體設備製造商或半導體設備使用者，作為半導體設備本體、其製程套件或其功能所需之精密零組件、結構件與模組者，包括但不限於：真空幫浦、閥件、腔體（Chamber）、Showerhead、Heater、ESC、Focus Ring，以及設備用之精密機構件與結構件、運動與定位元件、氣體與流體管路件，及由前述零件構成的模組。★【適用設備範圍】本節點之零組件、結構件與模組，可供應半導體前段晶圓製造設備、後段封裝設備及後段測試設備使用。判定重點為可辨識之交付物確屬設備本體、製程套件或設備功能所需之零組件、結構件或模組，且有證據支持其與合格半導體設備之關聯；不得僅因產品用於半導體產業、出現在設備供應鏈，或具有精密加工性質，即推定符合本節點。★界線是交付的是零組件、結構件或模組，而非半導體設備整機；整機屬 A2-01～A2-09（前段）或 A2-12／A2-13（後段）。★【交付對象】交付對象包括半導體設備製造商及半導體設備使用者。前段設備使用者包括晶圓廠；後段設備使用者包括 OSAT。交付對象資格僅解除客戶類型之限制，不代表其採購之產品必然屬設備零組件；仍須依實際交付物判定，不得將模具、治具、測試介面產品、耗材、維修或其他服務，僅因交付予 OSAT 或用於後段設備，即自動歸入本節點。★【與 A2-11、A2-14 的分界】A2-11 為對拆下來的零件提供之清洗、再生與表面處理服務；A2-14 為對機台本身之裝機、移機、維修與保養等現場工程服務。若同一營收線混合製造與服務，依 R020 ②-MX 處理，不得將整條混合營收線自動視為 A2-10。★與 A2-09 的分界是功能：晶圓搬運與廠內自動化（FOUP、Stocker、EFEM）屬 A2-09；專門用於該功能之零組件亦依 A2-09 歸屬，不因其同時具有精密機構件性質而轉入 A2-10。★與 A7-01 廠務系統的分界是服務對象與系統層級：本節點的零組件、結構件或模組係裝設於半導體設備本體或其製程套件；廠房級的氣體、化學品、純水、排氣等供應與處理系統及其零組件屬 A7-01。★【與 A6 測試介面產品之分界】探針、探針卡、探針介面、Test Socket、Load Board 及其他依 A6 定義之測試接觸介面產品，依 A6-02／A6-03 等相應節點歸屬；不得僅因其安裝於或搭配後段測試設備，即轉入 A2-10。後段測試設備本體及其非測試接觸介面之合格零組件，則分別依整機或零組件之定義判定。★本節點限半導體設備側：面板／顯示製程設備之零組件不屬本節點；其歸屬是 A2 與 E7 的邊界問題，未經該邊界裁定不得以本定義涵蓋。'),
    ('A2-11', 'A2', 3, '零件清洗 / 再生',
     '半導體設備零組件之清洗、再生與表面處理**服務**：腔體與零件之濕式／電漿清洗、'
     '塗層剝除與再噴塗 (Al2O3／Y2O3／YF3)、精密件研磨拋光與尺寸再生。'
     '★與 A2-10 的分界是**製造 vs 服務**：A2-10 製造零組件本身，本節點對既有零件提供延壽服務。'
     '★與 A2-14 的分界是**對象**：A2-14 服務的是機台（裝機、移機、維修），'
     '本節點服務的是拆下來的零件。'),
    ('A2-12', 'A2', 3, '封裝設備',
     '【後段封裝】**凡主要服務於半導體後段封裝製程之設備**，包括但不限於：固晶 (Die Bonder)、打線 (Wire Bonder)、模封 (Molding)、切割 (Singulation／Dicing)、取放 (Pick & Place)、熱壓鍵合 (TCB／Hybrid Bonder)、植球 (Ball Placement)、點膠 (Dispensing)、翹曲控制、暫時貼合／剝離 (TBDB)、雷射開槽，以及**封裝／基板用的濕製程與電漿清洗設備**。★2026-09-04 由六項機台列舉改成階段／功能邊界(R025：定義不完整→修定義，不新增節點)。舊定義窄於節點名稱本身，使點膠、翹曲控制、封裝用濕製程等後段設備沒有具名的落點，只能靠「供先進封裝使用」這種下游應用語掛進來，而那是 R031 禁止的。★封裝設備之整機依本節點判定；其零組件、結構件與模組，若符合 A2-10 定義，依 A2-10 判定。A2-10 不因零組件所屬整機歸入本節點，而自動排除該零組件。'),
    # Back-end test and inspection equipment belongs with the other equipment,
    # and A2 already holds the back-end 封裝設備; A6 next door is testing as a
    # service, which is a different business from selling the machine.
    ('A2-13', 'A2', 3, '測試 / 檢測設備', '【後段測試】Test Handler、ATE、AOI/AVI 檢測、雷射加工、老化設備。★測試設備之整機依本節點判定；其零組件、結構件與模組，若符合 A2-10 定義，依 A2-10 判定。A2-10 不因零組件所屬整機歸入本節點，而自動排除該零組件。'),
    # Servicing the equipment is not making it. A2-01..A2-13 are all machines a
    # company builds and sells; 漢測's 42.87% is engineers on a fab floor
    # installing, moving and repairing someone else's. A7-01 next door is the
    # building's systems, which is a different trade again.
    ('A2-14', 'A2', 3, '半導體設備工程服務 (Equipment Field Service)',
     '設備裝機、移機、改機、驗機、維修與保養等對設備本身的現場工程服務（不製造設備；廠務系統見 A7-01）'),
    # Refurbishing a whole machine is neither making one nor servicing one in
    # place. A2-11 works on parts that came off a tool and charges for the
    # processing; A2-14 sends engineers to a fab and charges for their time;
    # here the company buys a used tool, restores it and sells the tool. Three
    # different value propositions, so the third gets its own node rather than
    # being pressed into either neighbour (R025). Population is one company
    # today; the definition is written for the activity, not for that company.
    ('A2-15', 'A2', 3, '半導體設備再生 / 翻修 (Refurbished Equipment)',
     '對既有半導體製程設備進行取得、整修、翻新、升級或性能恢復後，'
     '再以翻修設備形式交付或銷售之設備再生業務。'
     '★不包含單純零件清洗再生（見 A2-11，服務對象是拆下來的零件）、'
     '亦不包含設備現場維修、裝機、移機等工程服務（見 A2-14，賣的是工時不是設備）。'
     '★與 A2-01～A2-13 的分界是**設備從何而來**：該群製造新機並銷售，本節點取得既有機台翻修後再售。'),
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
    ('A3-05', 'A3', 3, '半導體光學晶圓製程 (CIS / Wafer-level Optics)',
     '以半導體晶圓製程製造光學結構之製造與晶圓代工：CMOS 影像感測器 (CIS)、'
     '背照式 (BSI) 與其微透鏡及彩色濾光片等 CIS 製程；以及晶圓級光學 '
     '(Wafer-level Optics)——微透鏡陣列、超穎表面 (Metasurface) 濾波元件、'
     '矽光子用微光學、光波導。'
     '★2026-09-07 依 R025 放寬：原定義只寫 CIS，於是同一座晶圓廠以同一種製程'
     '產出的晶圓級光學元件無處可掛，被錯掛到顯示分支的光學元件節點。'
     'R025 的順序是先放寬既有定義、確認仍容不下才新增節點，本案止於前者。'
     '★與 A3-01 的分界是**產出是否為光學結構**；與 E 顯示分支的分界是'
     '**製程屬半導體晶圓製造**——終端應用（手機相機、AR 眼鏡、光通訊）是 application，'
     '不是歸屬依據。'),
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
    ('A5', 'A', 2, '封裝與測試代工',
     '受託為客戶提供半導體封裝、測試或晶圓中段製程之代工服務。'
     '★**本分支沿單一軸切分：商業模式（受託提供什麼服務）**——'
     'A5-05 封裝／測試代工、A5-06 晶圓中段製程代工，兩者以服務範圍互斥。'
     '★★**2026-09-11 移除 A5-01 傳統封裝與 A5-02 先進封裝（技術軸），'
     '本分支自此為單軸**。理由是 R031 兩軸共存判準的第一個裁定案例：'
     '原本 A5-01／A5-02 依**封裝技術**切分、A5-05／A5-06 依**商業模式**切分，'
     '而「晶圓級封裝服務」這一條營收線**同時完整落入 A5-02 與 A5-05 的 Definition**。'
     'R031 對混軸共存的寬容有一個前提——沒有任何一條營收線能同時滿足兩格——'
     '該前提在此不成立，故共存不再中性，必須裁決。'
     '★裁決依母體：A5-05 有 16 家成員共享同一 identity（受託為客戶封裝／測試）；'
     'A5-02 的三家成員（7873 瑞峰、3265 台星科、3374 精材）**全部也是這個 identity**，'
     '而技術軸專屬的情形——自有晶圓製造者做自己的先進封裝——**實際觀測母體為 0**；'
     'A5-01 則自始沒有成員。'
     '★★★**技術軸不只是與商業模式重疊，它會截斷量測**：'
     '3265 台星科掛 A5-02 時記 60.06%，因為只有「封裝」符合先進封裝的技術定義，'
     '而同樣是受託服務的「測試服務 38.98%」被排除；移入 A5-05 後為 99.04%。'
     '3374 精材同理由 50.16% 更正為 98.02%（最大的一條測試服務 47.86% 原本被排除）。'
     '這是「軸選錯會讓 exposure 量測失真」的直接證據，不只是分類整潔問題。'
     '★**本次並未主張「先進封裝不是產業」**——證明的是更窄的一件事：'
     '在目前觀測母體中，先進封裝未形成獨立於 OSAT 商業模式的 economic territory，'
     '故降為 attribute／theme。若日後出現該正向母體，依 R025 重新建立 leaf。'
     '★處置與 A5-03／A5-04、B2-04／B2-05／B2-06、K3／K4 一致：移除格子、保留理由。'),
    ('A5-05', 'A5', 3, '封裝測試代工 (OSAT)',
     '受託提供半導體封裝與／或測試服務者 (Outsourced Semiconductor Assembly and Test)：'
     '不自有 IC 設計、亦不以自有晶圓製造為主，收入來自客戶委託的封裝、測試或兩者之代工。'
     '★本節點是**商業模式**：收入來自客戶委託，與該服務用什麼封裝技術完成無關——'
     '打線、覆晶、晶圓級封裝 (WLCSP／Fan-Out／CoWoS) 皆在本節點之內。'
     '★★**2026-09-11 起技術不再是 sibling**：原本並列的 A5-01 傳統封裝與 '
     'A5-02 先進封裝已移除，理由見 A5 分支定義（R031 兩軸共存判準）。'
     '封裝技術自此為 attribute／theme，不參與 Industry leaf 的互斥判斷。'
     '★**eligible revenue 是受託服務的每一條線，不只封裝那一條**：'
     '同一家 OSAT 的測試服務收入同樣由本節點承載——'
     '3265 台星科 60.06%+38.98%=99.04%、3374 精材 44.98%+5.18%+47.86%=98.02%，'
     '兩者原本掛在技術格時都只記了封裝那一條。'
     '★只做測試不做封裝者見 A6 測試分支；只做晶圓薄化／背面金屬等中段製程者見 A5-06。'),
    # Mid-end: the filing's own word. Thinning and backside metal happen on a
    # customer's finished wafer, so this is neither A3 (which makes the device)
    # nor A5-05 (which packages it) — but its demand comes from Fan-Out, bumping
    # and 3D, which is why it hangs under packaging rather than under 晶圓製造.
    ('A5-06', 'A5', 3, '晶圓中段製程代工 (Wafer Mid-end Process)',
     '晶圓薄化、晶背研磨、晶圓正/背面金屬製程、載板貼合；受託加工客戶晶圓，不含封裝與測試。'
     '★與 A5-05 無營收線重疊（2026-09-11 查證，R004 成立）：本節點是對客戶晶圓的加工，'
     'A5-05 是把晶粒封裝成成品，兩者不會由同一條營收線同時滿足。'
     '★★**涵蓋不足，待補（2026-09-11）**：唯一成員 8028 昇陽半導體自述'
     '「本公司主要從事半導體晶圓製程服務…包含提供**晶圓再生**、晶圓薄化製程服務」，'
     '而**晶圓再生不在本節點的列舉之內**。互斥與完整是兩個獨立的檢查：'
     '本節點與 A5-05 互斥成立，不代表它已涵蓋自己該涵蓋的領域。'
     '在補寫定義前不得據此收新成員(R031 末段 Mapping Gate)。'),
    ('A6', 'A', 2, '測試 / 可靠度 / FA',
     '半導體測試與可靠度之**受託服務**，以及測試所需之**接觸介面產品**。本組宣告兩個維度，各自內部互斥，彼此不比較（同 A2 對設備／零組件／服務的處理）：**(一) 服務軸**——A6-01 量產測試判定良否、A6-05 壽命與環境應力驗證、A6-06 失效事後追因，三者依**目的與時點**互斥；**(二) 測試介面產品軸**——A6-02 晶圓級接觸元件、A6-03 封裝後測試座與載板，依**測試階段**互斥。★★**不得再以應用或測試場景切出第三個軸**（2026-09-07 裁定，R031 sibling 單軸條款）：Burn-in 與 HBM 都是測試條件或終端技術，不是產品或服務類別。原 A6-07 HBM Test 橫跨 A6-01（HBM 測試服務）與 A6-03（HBM 測試座），其定義本身即寫著「見 Theme T07」，已移出本樹改由該 Theme 表達。★**A6-04 Burn-in 目前處於 existence test 未決狀態**，刻意保留無定義：掃全庫 2025 營收品項（預燒／老化／Burn／可靠度／信賴性／驗證）無任何一條支撐它獨立成格，burn-in 只以**修飾語**出現在產品上——3581 博磊「積體電路**預燒用**測試連接座」、6683 雍智「IC**老化**測試載板」——兩句的核心名詞都是連接座與載板，burn-in 是用途。待決的問題不是「怎麼幫它寫定義」，而是「Burn-in 是否構成足以與 A6-01／A6-03／A6-05／A6-06 互斥的獨立 value proposition」；在該問題有答案之前不建立定義，**且依 R031 不得以其空白狀態作為排除 A6-03 或其他格的依據**。若最終認定不成立，應退出 Industry 層級而非硬造一格。★與 A2-10 之分界：依 A6-02／A6-03 定義之測試接觸介面產品，仍歸 A6 相應節點；不得僅因產品安裝於、搭配或用於半導體後段測試設備，即轉入 A2-10。後段測試設備整機及非測試接觸介面之設備零組件，則分別依 A2-13 或 A2-10 等相應定義判定。A2-10 的交付對象資格不改變本組之測試介面產品邊界。'),
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
     '★證據粒度限制：上述兩家的年報比重表只給一條合併的服務收入線，無法拆出可靠度部分，'
     '故其權重依 R020 ②-MX 為 NULL，'
     'weight_status 依該線其他組成能否具名判定（TOO_COARSE 或 GRANULARITY_INSUFFICIENT）——這不是「不知道公司有沒有這門生意」，'
     '而是「知道有、但不知道它占多少」。'),
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
     '★與 A2-12 的分界是**被加工物與製程階段**，不是客戶或廠別：被加工物為裸板、'
     'IC 載板本體或其基材 (CCL)，且處於**板材製造階段**者屬本節點；被加工物為晶圓、晶粒，'
     '或已完成載板上之封裝結構，且處於**半導體後段封裝階段**者屬 A2-12。'
     '同一型機台（如電漿清洗、濕製程）因此可能分屬兩格，依其所加工的對象與階段判定。'
     '若同一條營收線同時含兩者而年報未拆分，依 R020 ②-MX 處理，'
     '不得以客戶身分或廠別擇一全額歸屬。'
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
    ('C4-01', 'C4', 3, '機殼',
     '伺服器、儲存與網通機房設備用機殼／機箱（Rackmount Chassis、Storage Chassis／JBOD 機箱、Tower Server Chassis）及其機構配件之製造。個人電腦、筆電與消費性電子之機殼與機構件屬 F4-01。★伺服器／儲存機殼與 PC 等機殼的分界，依本節點前段的範圍與排除判定（不以 F4-01 的製程列舉另作判斷；營收線名稱提到機殼，不等於已完成這項邊界判定）；同一營收線混合兩者而年報未拆分者，依 R020 ②-MX 處理，不推測比例拆列。'),
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
    ('C10-01', 'C10', 3, '個人電腦', '以自有品牌設計、行銷與銷售之個人電腦整機，包括 Desktop、Notebook、Mini PC、AIO；生產可自製或委外代工。★不屬本節點：C16-01 所列終端（POS 終端、電子支付終端、電子收銀機、自助服務終端、彩券投注機），不因採一體機（AIO）外形而歸入本節點；年報未揭露為上述終端用途的自有品牌 AIO，仍可依本節點判斷。年報證據符合 G2 既有定義的工業電腦、嵌入式電腦、強固型與寬溫電腦、工業級人機介面電腦（Panel PC），依 G2 判斷，不因外形為筆電、Mini PC 或 AIO 而歸入本節點；具強固、寬溫或嵌入式特徵但年報證據不足以確認符合 G2 者，不自動歸入 G2，依既有規則判斷或維持未分類。受託為品牌客戶設計或製造個人電腦（ODM／OEM）者，依受託製造服務歸 V7-01，並須符合 V7 成員資格的既有證據門檻；代理他廠品牌電腦者依 V3 規則判斷。★年報未能辨認自有品牌銷售與受託製造角色者，不得僅因產品為個人電腦整機而掛入本節點。'),
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
    # C16 added 2026-10-15 (classification draft pilot): POS and payment terminals had no home.
    ('C16', 'C', 2, 'POS / Payment Terminal', None),
    ('C16-01', 'C16', 3, '銷售點與支付終端', 'POS 終端機、電子金融交易（支付）終端機、電子收銀機、自助點餐／自助服務終端（Kiosk）、彩券投注機之設計與製造；以及年報明確揭露為上述終端專用週邊、且與公司自售之上述終端搭配銷售之產品（如錢箱、顧客顯示器、POS 專用鍵盤、收據印表機等）。條碼掃描器僅於年報明確揭露為上述終端之專用週邊時適用本節點。★不屬本節點：未兼售上述終端之週邊專業廠的週邊產品（依既有節點定義判斷，無對應節點者維持未分類）；通用電腦週邊與一般用途掃描器（屬 C11-01）；顯示面板與一般顯示器（依 E 支既有定義處理）；供應上述終端或其專用週邊之零組件與模組（如面板、觸控模組、印表機頭），不因應用於上述終端而歸入，依各自價值鏈角色判斷；於營業比重表單獨列示之安裝、保固或維修服務線（依既有節點定義另行判斷，資訊系統整合與 IT 服務見 U2，無對應節點者維持未分類）；POS 軟體與支付處理服務；工業控制用運算平台（屬 G2）；博弈機台（見 TAXONOMY_GAPS G10）。'),
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
    ('D6-01', 'D6', 3, '光通訊元件 (Optical Component)', 'Optical Transceiver、Optical Engine、EML、DML、VCSEL、TIA、Driver、DSP、Silicon Photonics、Fiber（限已整合於光元件或連接器之光纖組件，如光纖陣列、尾纖等；未接頭光纖及光纜之歸屬尚未裁定）、Passive（CPO 為跨產業 Theme，見 T12）。★Passive 指光被動元件，包括光纖連接器（光纖連結器、接頭）、光纖耦合器、分光器、光隔離器、波長分波多工器（WDM）等；應以年報將產品明確揭露為光纖連接器或光被動元件，或有足夠產品內容可確認其屬性為準，不得僅憑「Fiber」或「光通訊」等字樣判定。★光纖跳接線如年報將其揭露為光纖連接器或光被動元件產品線的一部分，或年報明確揭露產品為具連接器端接的光纖跳接線，可歸入本節點。★未接頭光纖、光纜及預端接光纜（含 MPO trunk 等已端接之光纜組件）之歸屬尚未裁定，不因名稱含「Fiber」「光纖」或已端接連接器而歸入本節點。電氣連接器屬 F2-01。'),
    ('E', None, 1, '顯示產業',
     '顯示器及其專用供應鏈之產業活動。'
     '★本支的 L2 分類軸是**價值鏈角色**——產品、模組、設備各為一格，'
     '與 A 半導體同型（材料、設備、製造、設計、封測並列於 L2）。'
     '技術世代（TFT-LCD、OLED、Micro LED、Mini LED）**不是**本支的軸，'
     '降為產品節點內的技術細分或 Theme（2026-09-07 裁定）。'
     '★不屬本支者逐項寫明去處：光通訊元件見 D6-01、光通訊設備見 D5-01；'
     '顯示器驅動 IC 見 A4-12；觸控控制 IC 見 A4-15；'
     '半導體光學晶圓製程（晶圓級光學、超穎表面、光波導）見 A3-05。'
     '★成像光學（相機鏡頭、光學鏡片、光學鍍膜元件）全樹尚無節點，屬 R025 taxonomy gap；'
     '**不得因為全樹只有本支收得下就掛進來**——「只有這裡收得下」不是歸屬理由。'
     '★觸控的邊界規則：本支只收帶**顯示限定語**的觸控（顯示器觸控模組、觸控顯示模組、'
     '與面板貼合之觸控面板）；不與顯示器貼合的通用輸入元件（薄膜觸控開關、鍵盤開關）屬 F。'
     '「觸控」二字本身不構成進入本支的理由。'
     '★**通用 LED 元件不屬本支**（2026-09-08 裁定，E5 已移除）。'
     '先前以為問題是申報粒度不足；讀完年報後證據反了過來——'
     '一家封裝廠 94.16% 的「LED」線，其年報「重要用途」欄同時列出電子看板、'
     'LCD 背光源、交通號誌、電腦滑鼠、家用電器、光電開關位置感測與紅外線接收器，'
     '而同一份年報有能力把「照明 0.48%」與「LCD 及其他 5.36%」單獨列出。'
     '**不可拆的是產品族本身，不是報表**：同一顆元件賣進多種終端，'
     'LED 元件的經濟活動天生跨應用。'
     '★因此顯示只是 LED 的客戶之一，如同光通訊元件歸 D、成像光學留為 gap：'
     '**元件族不因顯示是它的客戶之一就屬於顯示產業**。'
     '★分界因此很乾淨：通用 LED 元件不屬本支；'
     '**顯示專用**的背光模組、LED 背光模組與光源條屬 E2-02，'
     '因為那是為顯示器光學路徑而製的子系統而非通用元件。'
     '★LED 封裝／光電發射元件、不可見光 (IR／UV) 元件、照明模組三者'
     '全樹皆無節點，屬 R025 taxonomy gap；車燈模組見 H 汽車。'
     '**不得為了讓這批公司有地方掛而把它們留在顯示分支。**'),
    ('E1', 'E', 2, '顯示產品',
     '以自有面板製程產出之顯示面板與顯示器本體。'
     '★與 E2 顯示模組的分界是**有無自有面板製程**：自製面板（Array／Cell）屬本節點；'
     '採購面板或 Open Cell 後貼合、組裝、整合者屬 E2。'
     '★技術世代（TFT-LCD、STN、OLED、Micro LED）在本節點內為技術細分，不另立節點。'),
    ('E1-01', 'E1', 3, '顯示面板',
     '以自有面板製程製造之顯示面板與顯示器：Array、Cell 與 Module 各段自製，'
     '涵蓋 TFT-LCD、STN、OLED、Micro LED 等各技術世代之面板本體。'
     '★技術世代在本節點內為技術細分而非分格依據（2026-09-07 裁定）。'
     '★採購面板進行貼合與整合者見 E2-01；背光模組見 E2-02；面板製造用設備見 E7。'
     '★2026-09-07 由「E1 LCD／E1-01 液晶面板」改寫而來；改寫前本碼從未掛過任何成員。'
     '★2026-09-07 mapping gate 解除：34 家候選已逐家讀年報業務章節裁定。'
     '判準是三選一——自有面板製程屬本節點、採購面板後整合屬 E2-01、'
     '僅為相關零組件回其對應的 value-chain 節點；讀不出來者留在未分類，不硬塞。'
     '★這道 gate 是必要的：「TFT 84.13%」「液晶顯示器模組 97.67%」這類品項字面'
     '無法分辨面板廠與模組廠——前者的年報寫「液晶顯示**觸控模組**設計、製造業務」，'
     '後者寫「搭配 TFT 面板廠…以自有模組設計、組裝產線的優勢」。'
     '★已知缺格：顯示器／電視**整機組裝**在本支無節點。'
     '本輪有公司以整機品牌與 ODM 為主業（顯示器 52.30%、數位電視機 12.77%），'
     '既非自有面板製程亦非模組貼合；依 R025 不以單一公司開格，列為 taxonomy gap。'),
    ('E2', 'E', 2, '顯示模組',
     '採購面板、觸控感測器或光源元件後，貼合、組裝或整合為顯示相關模組之製造。'
     '★本組的分類軸是**模組類別**：一為顯示與觸控之貼合整合模組，'
     '一為背光模組及其專用光學零組件，兩者互斥。'
     '★與 E1 的分界是**有無自有面板製程**。'
     '★只收帶顯示限定語的觸控；不與顯示器貼合的通用輸入元件屬 F5-01。'),
    ('E2-01', 'E2', 3, '顯示與觸控模組',
     '採購面板或 Open Cell 後之貼合、組裝與整合模組：觸控感測器 (Film Sensor)、'
     '觸控面板與觸控模組、液晶顯示模組 (LCM)、觸控顯示模組、'
     '光學貼合 (Optical Bonding)、蓋板玻璃貼合，'
     '以及以上述模組整合而成之特殊應用顯示器（醫療、工控、車載、船舶）。'
     '★觸控須帶**顯示限定**：服務顯示器貼合與整合之觸控面板／模組屬本節點；'
     '不與顯示器貼合的通用輸入元件（筆記型電腦鍵盤薄膜觸控開關等）屬 F5-01。'
     '★自有面板製程者見 E1-01；背光模組見 E2-02；觸控控制 IC 見 A4-15。'
     '★2026-09-07 由「E2 OLED／E2-01 OLED」改寫而來；改寫前本碼從未掛過任何成員。'),
    ('E2-02', 'E2', 3, '背光模組與顯示光學零組件',
     '顯示器光學與背光子系統所需之專用膜材、光學零組件與模組：背光模組、'
     'LED 背光模組與光源條、導光板、擴散板、增光片（稜鏡片）、反射片、遮光反射材料、'
     '偏光板與其保護膜材 (TAC)，以及面板與背光用之各式光學膜。'
     '★本節點的邊界是**用途專屬性**，不是價值鏈深度：只要該膜材、光學件或模組'
     '是為顯示器的光學路徑與背光子系統而製，不論它落在「材料／膜材／光學件／子系統」'
     '的哪一層，都屬本節點。'
     '★與 E2-01 的分界是**產出進入哪裡**：本節點的產出進入顯示器的光學路徑'
     '（背光、導光、增亮、偏光），E2-01 的產出是貼合整合後的顯示或觸控模組本體。'
     '★**不再另立「顯示專用材料」leaf**（2026-09-08 裁定）。2026-09-07 首次評估時'
     '三家候選分屬顯示光學零組件、精密塗佈膠膜與投影銀幕三種不同經濟活動，因異質性否決；'
     '2026-09-08 以年報敘述重掃後母體擴為偏光板、TAC 膜、增光膜、導光板與擴散板、'
     '背光膜材共五家，形狀一致得多，仍維持不拆——**同一條供應鏈不等於同一個 Industry leaf**，'
     '而這五家橫跨材料→膜材→光學件→背光子系統四層，尚未證明其中任何一刀'
     '能形成與 E2-01 互斥且可重複套用的邊界。「導光板究竟是材料還是零組件」'
     '就是那一刀目前答不出來的地方。母體規模在兩次評估中都不是依據。'
     '★通用光學膜材與成像光學不屬本節點。'
     '★**通用 LED 元件亦不屬本節點，且全樹已無 LED 節點**（E5 於 2026-09-08 移除）：'
     '本節點收的是**顯示專用**的背光模組、LED 背光模組與光源條——'
     '它們是為顯示器光學路徑而製的子系統；一顆可賣進照明、車用、感測與背光的'
     'LED 元件不是，那批公司屬 R025 taxonomy gap。'),
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
    # E5 LED left E on 2026-09-08, and the reason is the opposite of the one
    # expected. The working theory was that Taiwanese filings say only 「LED」
    # and the applications could not be told apart. Reading them showed the
    # applications are genuinely mixed inside one product: a packager's 94.16%
    # LED line names electronic signage, LCD backlight, traffic signals, mice,
    # home appliances, photo-interrupters and infrared receivers as its uses --
    # in a filing that separately breaks out 照明 at 0.48% and LCD 及其他 at
    # 5.36%. The company can split what is splittable; what it did not split is
    # one component sold into many markets.
    #
    # So display is one of LED's customers, not its industry, and E is the
    # display industry. Same shape as optical communication going to D and
    # imaging optics staying a gap: a component family does not join a branch
    # because that branch buys from it.
    #
    # Nothing about the members is retracted. The LED population -- packaging
    # and emitter makers, invisible-light (IR/UV) makers, lighting-module makers
    # -- has no node anywhere in the tree and is an R025 gap, recorded in
    # db/industry_data.py where its evidence is. Automotive lamp modules go to
    # H; display-specific backlight modules and LED light bars are E2-02, which
    # is where they already were.
    # E6 光學元件 dissolved 2026-09-07. Its definition claimed Lens / Prism /
    # Filter and held no lens maker at all, while fifteen imaging-optics
    # companies sat outside the tree unclassified -- the B2-01/HDI shape with
    # the ground taken from outside rather than from a sibling. Its two members
    # turned out to belong to two different worlds: display optical film, and
    # wafer-level optics produced by a semiconductor foundry. Neither is an
    # optical-component industry, so there was nothing left to define.
    # Imaging optics stays an R025 taxonomy gap; wafer-level optics went to
    # A3-05 by widening it rather than by adding a node.
    #
    # core_reassessment carries a foreign key onto industry_tree, so a deleted
    # node takes its queue history with it. E6-01's one resolved item is kept
    # here instead, because the record is the point of the queue:
    #   2026-09-04 全表重掃：候選 4 家次；4 家候選中 2 家為 AOI 檢測機台製造商
    #   （非光學元件）、1 家為 R020② 混合線、1 家為載板廠，無應補建者。
    #   ★方法：以節點定義與名稱的品項詞，比對 447 家已分類公司 FY2025 營收
    #   品項名稱，逐筆對定義判定。★盲點：(1) 無法辨識商業模式，通路型節點的
    #   候選多為製造商；(2) 純英文定義不會命中中文營收線。本次掃描不宣稱窮盡。
    # Read now, that rescan is itself evidence for the dissolution: a node named
    # 光學元件 whose whole candidate set was inspection machines, a mixed line and
    # a substrate maker was never sorting optical components.
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
     '★面板／顯示器本體見 E1 顯示產品，貼合與整合模組見 E2 顯示模組。'
     '★population gate（2026-09-07 設，2026-09-08 檢視）：本節點的存在依據是'
     '**兩家以上具可辨識經濟規模的顯示設備業務**，不是「顯示設備」這個名目。'
     '角色本身不是判準——設備商以 Major 形式兼營顯示設備，只要規模可辨識就計入；'
     '不計入的是佔比零星的順帶業務。（2026-09-07 初稿誤寫成「以 Minor／Major 形式'
     '兼營者不構成存在依據」，比裁定本身嚴格，已更正。）'
     '★2026-09-08 檢視：條件成立且已超過門檻。三家具可辨識規模——'
     '一家 93.09% 以面板檢測設備為本業；一家 LCD 檢測系統 22.47%；'
     '一家面板設備 20.87%，其年報「顯示器產業設備」欄列出九項產品：'
     'Array ADI/AEI AOI、Color Filter Repair、Laser Repair、檢測與量測專用設備、'
     'LCM 整廠設備、R2R 關鍵設備、AMOLED 設備、ACF/FPC Bonder、OLB Bonder。'
     '第三家尚未掛入本支：其最大營收線是半導體機台及零件 43.93%，Core 須先另案裁定，'
     '本輪不與 E7 存在性混判。另有三家佔比 5.35%～10.20% 的兼營，不作為存在依據。'
     '★邊界仍以**設備服務哪一段製程**為準：面板級封裝 (FOPLP) 檢測與量測設備'
     '服務的是半導體先進封裝，不因名稱含「面板」二字而屬本支。'
     '★存在條件不再成立時本節點應重新檢討。'),
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
    ('F1', 'F', 2, '被動元件',
     '被動元件。子節點依**元件的電氣功能**切分（產品類別軸）：F1-01 為阻抗、儲能與濾波類（電阻、電容、電感、磁性元件、EMI/EMC），F1-02 為以石英壓電效應產生或穩定頻率的頻率元件。'),
    ('F1-01', 'F1', 3, '被動', '電阻、MLCC、電感、電容、磁性元件、EMI/EMC'),
    # F1-02 added 2026-10-15 (classification draft pilot): quartz frequency parts had no
    # leaf of their own; only V2 named them, and V2 is the distribution side.
    ('F1-02', 'F1', 3, '頻率元件',
     '石英晶體（Crystal Unit／石英晶體諧振器）、石英振盪器（XO、TCXO、OCXO、VCXO 等）之製造。不含石英原料、人工水晶與半導體製程用石英材料／零件（屬 A1-11）；石英元件之代理銷售屬 V2（R022）。'),
    ('F2', 'F', 2, '連接器', None),
    ('F2-01', 'F2', 3, '連接器', 'Board-to-Board、Wire-to-Board、FPC、RF、High Speed、Automotive、Server。★本節點限電氣連接器。光纖連接器屬 D6-01 光被動元件，不納入本節點。'),
    ('F3', 'F', 2, '線材', None),
    ('F3-01', 'F3', 3, '線纜', 'Cable、Coaxial、High Speed Cable。★光纖跳接線依 D6-01 判斷，不符 D6-01 判準者亦不掛入本節點；未接頭光纖、光纜及預端接光纜之歸屬尚未裁定，裁定前不掛入本節點。'),
    ('F4', 'F', 2, '機構件',
     '機構件。子節點依**產品類別**切分（產品類別軸）：F4-02 為樞紐／轉軸與顯示器底座等提供開合、旋轉或支撐調整功能的機構組件，不論以沖壓、MIM 或 CNC 製造皆屬 F4-02；其餘以製程界定的一般機構件屬 F4-01。'),
    ('F4-01', 'F4', 3, '機構', '金屬沖壓、CNC、壓鑄、塑膠射出、組裝'),
    # F4-02 added 2026-10-15 (classification draft pilot): hinges had no leaf of their own.
    ('F4-02', 'F4', 3, '樞紐／轉軸',
     '筆記型電腦、平板、顯示器、AIO、折疊式手機與穿戴裝置用樞紐（Hinge）、轉軸及顯示器底座（Stand）之製造。伺服器滑軌與抽屜式鋼珠導軌不屬本節點。'),
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
    ('G', None, 1, '工業自動化與機械',
     '工業製造體系中，交付自動化、控制、運動與機械設備之產業活動。'
     '★本支的 L2 分類軸是**在工業製造價值鏈上交付什麼角色**：'
     '零組件（傳動、流體控制、工具）／整機（工具機、專用機械、機器人）／'
     '控制運算（工業電腦與機器控制器）。'
     '★**「系統整合」曾列為本軸的第四個角色，2026-09-08 撤回**。'
     '撤回的理由不是母體太小，而是 population audit 證明它沒有獨立 territory：'
     '樹上已存在一個宣告過且互相交叉引用的 partition——自動化與搬運設備'
     '**依其服務的製程**歸屬（晶圓廠見 A2-09、PCB／載板見 B5-03、面板見 E7-03）——'
     '11 家自動化設備公司中 7 家早已依此分流。'
     '而「系統整合」一詞的可觀測母體（營收線 ≥15%）由 IT 資訊服務與廠務工程佔滿：'
     '25 家中 8 家已 Core 在 A7-01 廠務系統，14 家是 IT 系統整合（屬 U 分支），'
     '**工業自動化系統整合一家都沒有**。'
     '★這與 G3 併格是同一條判準：**一個描述詞可以描述很多不同 Industry，'
     '但描述詞本身不是 Industry partition**。'
     '★**經濟活動存在 ≠ Industry node 應存在**。跨製程的自動化整合活動確實存在'
     '（4585 達明自動化整合方案 15.40%、6125 廣運自動化設備 80.89%），'
     '但兩家彼此不像，不構成 AA 那種「高度一致且已被全樹排除既有 homes」的 population。'
     '該候選角色列 taxonomy gap G11，見 db/TAXONOMY_GAPS.md。'
     '★**本節點的 scope 與 leaf 的存在是兩層，不可混**：parent scope 說哪些經濟活動'
     '允許進入本支，L2 leaf 說哪些活動已形成可獨立分類的 Industry territory。'
     '一家自動化整合公司落在本支的 scope 內，不因此就必須有一個 leaf 收它。'
     '★**入場條件是服務工業製造流程**，不是產品名稱裡有「自動化」三個字。'
     '辦公室自動化商品、金融自動化服務機器、實驗室核酸萃取自動化儀器都自稱自動化，'
     '而其經濟活動分別在辦公事務、金融服務與生技醫療。'
     '這與 AA 的「供應成像光路而非使用成像光學」是同一條 R016 判準：'
     '技術詞同時是應用詞時，看它服務誰。'
     '★2026-09-08 更名。原名「電機／自動化／機器人」承諾了「電機」，而電機與重電在 I：'
     'I6-01 重電的定義即 Transformer／Switchgear／配電盤／電線電纜／發電設備，'
     'I5-01 電網收輸配電，兩格皆已定義且當時零成員。'
     '變壓器、配電盤、高低壓配電之公司一律歸 I，**本支不為它們開任何節點**'
     '（R025：既有節點容得下就不新增）。這是兩個 L1 之間的名稱級重疊，'
     '而 [definition_boundary_overlap] 只比對 sibling 的定義，看不到它。'
     '★**本支收的不限於狹義的加工機械本體**：直接服務工業製造流程之製程設備與'
     '控制元件同屬本支——製程管線上的控制閥服務的是生產流程，即使它不裝在機器本體內。'
     '（2026-09-08 G4-02 裁定時確立，一併適用於泵、流量與壓力控制、製程管線設備等'
     '日後會問同一個問題的產品。）'
     '★其他不屬本支者：伺服器與其機殼見 C；無塵室與機電空調統包工程見 A7-01；'
     '半導體設備見 A2、PCB 設備見 B5、面板／顯示設備見 E7。'
     '★2026-09-08 G1 工業自動化與產線整合連同 G1-01 自動化一併刪除，理由如上。'
     'G1-01 原宣告 PLC、Motion Control、Servo、Vision 與運動控制零組件，'
     '而這五項在營收線 ≥15% 的可觀測母體分別是 0、0、1、3、1 家，'
     '其中 Vision 那 3 家全部已在 B5-02／E7-02，'
     'Servo 那 1 家是 7750 新代（G2 那家公司的零組件線），'
     '運動控制零組件那 1 家是 2483 百容（做導線架、繼電器、連接器的零組件廠）。'
     '**宣告一個角色不等於它有母體。**'
     '★**四天前的獨立重掃已得出同一結論**，本裁定不是單一次掃描的產物：'
     'core_reassessment #13（G1-01，raised 2026-09-03，resolved 2026-09-04）記載'
     '「全表重掃：候選 15 家次；15 家候選全因**「自動化」一詞誤中**，'
     '經查皆已掛 A2-09 晶圓廠自動化或 B5-01 PCB 製程設備；本節點是運動控制零組件與'
     '工業自動化產品，無應補建者。」該列隨節點刪除而消失，故全文抄錄於此——'
     '**刪掉一個節點會連它的佇列歷史一起帶走**（2026-09-07 刪 E6-01 時學到的）。'),
    ('G2', 'G', 2, '工業電腦與控制運算',
     '直接提供服務工業製造流程之控制、運算或人機控制硬體：工業電腦 (IPC)、'
     '嵌入式工業運算平台與板卡、強固型與寬溫電腦、工業級人機介面電腦，'
     '以及專用機器控制器（工具機 CNC 控制器、產業專機控制器、機器人控制器）。'
     '★**判準是公司交付控制／運算硬體本身**，而不是使用該硬體去完成自動化、'
     '物流、零售、樓宇或其他終端應用。'
     '★**下列字眼出現在營收品項或產品名稱中，都不足以進入本節點**：'
     '工業電腦、Edge Computing、Controller、HMI、automation、smart manufacturing。'
     '本節點的入場靠的是交付物，不是詞彙。'
     '★**Edge computing 不另立 leaf**：2026-09-08 的 P1→P3 掃描中，'
     '觀測範圍內沒有任何一家公司只做邊緣運算電腦。'
     '6166 凌華、6570 維田、6579 研揚、3479 安勤的邊緣運算產品與它們的'
     '嵌入式電腦、工業主機板並列在'
     '同一份產品線裡，是**產品形態與部署位置**，不是另一個經濟角色。'
     '這與 AA 把「是否產生影像」排除在成立條件之外是同一條判準：'
     '產品形態與部署環境不構成 Industry partition。'
     '★**IPC 與機器控制器同屬一個角色**，不拆。字面證據來自 7750 新代的年報'
     '所營業務第一項：「PC-based CNC 控制器 (PC-based Computer Numerical Controller)」'
     '——它交付的就是一台控制機器的電腦。4587 寶元數控同向'
     '（「各種精密設備之控制器之研究開發與製造銷售」，自有品牌 LNC）。'
     '★**正面 boundary fixture：7750 新代**（控制器產品 48.45 ／ 伺服產品 46.50 ／'
     '智慧製造產品 3.14）。控制器那條屬本節點，伺服驅動器與馬達那條屬驅動零組件角色'
     '（驅動元件角色，G4 的軸宣告了「驅動」但全樹無節點，見 db/TAXONOMY_GAPS.md G12）。'
     '一家公司兩條線分屬兩格，不因它自稱自動化就整家歸一格。'
     '★**機器人控制器屬本節點，機器人本體屬 G3**：新代與寶元數控都製造機械手臂'
     '控制器而都不交付機器人本體。與 G3-01「交付機器人本體」的判準互為反面。'
     '★**與 G5 的分界**：本節點交付控制與運算硬體，G5 交付具完整加工功能的工具機整機。'
     '控制器不因「專門給工具機使用」而歸 G5——否則 G2 會被 G5 吞掉。'
     '★**與系統整合活動的分界是交付物**：本節點交付運算與控制硬體，'
     '整合商交付的是設備與產線本身。7870 聯剛即此例：其 64.68% 的「數位智能解決方案」'
     '是從顧問諮詢、場域勘查、系統架構設計、施工到教育訓練的智慧工廠系統整合，'
     '不屬本節點。★該整合活動於 2026-09-08 起**全樹無節點**——原 G1 已刪除，'
     '見 G 的說明與 taxonomy gap G11。所以這條分界的作用是把整合活動擋在本節點外，'
     '不是把它交給某一格。'
     '★以下為 2026-09-08 建立本定義時實測過的 negative test，'
     '每一條都有具名公司，不是預防性條款：'
     '**①控制器晶片見 A4**——MCU 見 A4-08、人機介面控制 IC 見 A4-15、'
     '記憶體控制 IC 見 A4-19、BMC 見 A4-26、Ethernet Controller 見 D1-01、'
     '飛控見 S2。3268 海德威（多媒體控制晶片 54.84%，自述為 IC 晶片韌體設計廠商）屬此類。'
     '**②工控產品代理見 V4 工控通路**(R022)——8374 羅昇年報原文「目前**代理**之商品'
     '大致如下」，逐類列出工業傳動、自動化控制、能源管理之代理品項。'
     '6474 華豫寧的 MCU 線同理，其所營業務首項即「IC 韌體設計與應用服務、IC 代理及買賣」。'
     '**③工業級儲存與記憶體模組見 F7-01**——5289 宜鼎工業用嵌入式儲存 37.17%＋'
     '工業用 DRAM 模組 55.24%＝92%；「工業用」是**等級形容詞不是產業**。'
     '7870 聯剛的「邊緣運算儲存系統 35.32%」實體是自有品牌 ARAID 磁碟陣列與 RAID 備份設備，'
     '同屬儲存產品——年報原文「應用於工業電腦內與邊緣運算儲存等環境」，'
     '該詞描述的是它賣進去的環境，不是它交付的東西。'
     '**④手持式資料擷取終端不屬本支**——6160 欣技「掌上型工業電腦」69.42%，'
     '產品為 CipherLab OS／Android 掌上型電腦、CCD 與雷射條碼掃描器、行動 POS，'
     '服務零售與物流現場；**該公司年報自行引用的官方分類是「資料輸出入週邊設備業」**。'
     '產品名稱有「工業電腦」四個字不構成入場。'
     '**⑤樓宇、安防與節能自動化不屬本支**——2390 云辰（智能安防節能自動化系統 48.17%'
     '＋智能控制器 30.56%＋智慧建築 10.23%）服務的不是工業製造流程，'
     '撞在 G 的入場條件上。'
     '**⑥人機介面 (HID) 周邊不屬本支**——3349 寶德「人機介面 85.63%」與'
     '「遊戲控制器 5.74%」並列，該詞在此指鍵鼠與遊戲周邊，不是工業 HMI。'
     '**⑦線材與連接線組不屬本支**——6134 萬旭「儲能工控 43%」，'
     '所營業務首項為「連接線組立及其相關零組件之裝配及銷售」，'
     '該營收品項名的是**它的線材賣進去的市場**，不是它的產品。'
     '**⑧博弈機台不屬本支**——6441 廣錠營收線「工業電腦 98.66%」，'
     '而同一份年報的商品項目為「工業電腦(博弈產業)之研發／生產／銷售」，'
     '主要業務是博弈機台設計開發與組裝銷售；4916 事欣科「博奕及工業電腦 35%」'
     '亦列博奕板卡、準系統、玩家追蹤系統與機台框體。'
     '該角色目前全樹無 home，列 taxonomy gap G10，見 db/TAXONOMY_GAPS.md。'
     '★**⑧是本支最重要的一條**：廣錠若只讀營收品項就會以 98.66% 的「工業電腦」'
     '進入本節點。R016「同名產品不構成同一 Industry」在這裡不是理論——'
     '本定義的整份 negative test 清單存在的理由，就是擋掉'
     '「industrial computer → G2」這種 lexical shortcut。'
     '**⑨半導體設備內之控制系統依 A2 設備角色判**；**⑩工具機整機見 G5**。'
     '★2026-09-08 裁定維持單一 role，不拆 G2-01 IPC／G2-02 機器控制器／G2-03 Edge。'
     '拆的必要條件是兩個角色出現互相重疊或各自獨立的 population，目前皆未出現。'
     '★本節點無子節點，曝險直接掛在 L2——與 V1、AA1、K2 等 111 個 L2 leaf 同型，'
     '不另造一個與母同名的 G2-01。'
     '★2026-09-08 mapping 未做：本輪只裁 ontology 與 Definition。'
     '已通過 P2 的 ① 群 candidate 為 2395 研華、2397 友通、3479 安勤、6579 研揚、'
     '6166 凌華、6570 維田、6161 捷波、7562 博來，② 群為 7750 新代、4587 寶元數控；'
     '2399 映泰（工業電腦 11%，主體為消費性主機板）與 5484 慧友（工業電腦 26.16%，'
     '主體為影像監控）為 mixed，role 待判。'),
    ('G3', 'G', 2, '機器人',
     '以機械本體形式交付之機器人產品的設計、製造與銷售。'
     '★2026-09-08 本組由三格併為一格。原三格為 '
     'G3-01 Industrial／Collaborative、G3-02 Mobile、G3-03 Service／Humanoid，'
     '它們用的是**三種不同的描述詞**——應用場域與人機關係／移動型態／場域加外形——'
     '而這些描述可以同時為真：AMR 同時是 mobile 與 industrial，'
     'humanoid 同時是 mobile 與 service。'
     '保留三格等於允許同一個 economic activity 同時屬於多個 sibling。'
     '★這是 B2、E、G4 修過的同一種 ontology defect，'
     '但**操作方向相反**：G4 是同一軸下兩個功能被錯併故拆，'
     'G3 是三格根本不同軸且互相重疊故合。'
     '上位規則同一條：**L2 siblings 必須共享同一分類軸，'
     '且在該軸上可形成互斥 territory**。'
     '★決定性證據是 6215 和椿一條 11.10% 的營收線，'
     '年報自述同時涵蓋 AMR、工業機器人與服務型機器人——'
     '同一條經濟活動橫跨三格，直接證明三格不成 partition。'
     '見 tests/fixtures/cases/6215_hechun_one_line_crosses_three_leaves.md。'
     '★移動型態、協作型態、外形與應用場域自本日起為**產品屬性**，'
     '可作 Theme 或 descriptive property，不作本樹的 L2／L3 Industry partition。'),
    ('G3-01', 'G3', 3, '機器人',
     '具可程式化、自主或半自主運動能力，並以機械本體形式直接交付之機器人產品的'
     '設計、製造與銷售。'
     '★判準是**公司交付的是機器人本體**，而不是使用機器人完成自動化服務、'
     '系統整合或特定終端應用。定義刻意不寫「自動化機器人解決方案」——'
     '那個寫法會把產線整合活動整片吸進來。'
     '★**產線與自動化系統整合不屬本節點**：把機器人整進客戶產線者交付的是產線。'
     '4585 達明同一份年報同時示範了兩側：機器手臂及相關零組件 83.87% 屬本節點，'
     '自動化整合方案 15.40% 不屬本節點。一家公司可以兩邊都有，但那是兩條線不是一條。'
     '★2026-09-08 起該整合活動**全樹無節點**（原 G1 已刪除，見 G 與 taxonomy gap G11），'
     '所以達明那條 15.40% 目前無處可掛，屬 gap 而不是漏 map。'
     '★**半導體製程設備中的晶圓移載／搬運機器人及其整合系統不屬本節點**，'
     '依其半導體設備價值鏈角色歸 A2-09（EFEM、Robot、FOUP、Stocker、AMHS）。'
     '2049 上銀的機器人線同時含關節式與史卡拉機械手臂、晶圓移載系統與晶圓機器手臂——'
     '產品都叫 robot，經濟角色不同（R016）。'
     '★**醫療復健機器人不屬本節點**，歸 N3 醫療器材。'
     '理由不是「robot 不能是 G3」，而是該產品的主要經濟角色是醫療器材。'
     '上銀的下肢復健機器人即此例。'
     '★**代理或外購他人機器人轉售不建立本節點曝險**(R022)；'
     '幫客戶整合他人機器人者依交付物歸適當角色；'
     '若其交付物是產線整合本身，該角色目前全樹無節點（taxonomy gap G11）。'
     '6215 和椿（整合 AMR／工業／服務型機器人並代理銷售商用配送、清掃、'
     '調酒與廣告機器人）與 4584 君帆（UR 機械手臂系統整合）皆屬此類。'
     '★2026-09-08 mapping 未做：本輪只裁 ontology 與 Definition。'
     '已觀測到的唯一本體製造 candidate 是 4585 達明，evidence 已備但**尚未 map**。'),
    ('G4', 'G', 2, '工業機械零組件與傳動',
     '工業機械之零組件與傳動元件。'
     '★本組的分類軸是**零組件類別**：依零件在機械中承擔的功能切分'
     '（傳動與線性運動／驅動／流體控制／工具），彼此互斥。'
     '★流體控制於 2026-09-08 經五道 gate 確認屬本軸（見 G4-02）。'
     '★但**軸已宣告不代表該軸下每一格都已確認**——這是 Definition governance 的通則，'
     '不因本例已結案而失效：新增 child 仍須各自通過存在性測試。'
     '★**零組件的 Industry 歸屬以其產品功能與機械價值鏈角色判定，不以終端市場判定**'
     '（2026-09-08 裁定，適用本組全部 child）：本組零組件可服務工業製造以外之終端市場，'
     '**不要求其全部終端應用均屬工業製造流程**。G 的 parent scope 描述本支的主要 territory，'
     '但零組件的 identity 跨終端市場，兩者不衝突。'
     '★這與 G5 的「機床 identity 優先於 application」是同一條原則延伸到零組件層。'
     '若不這樣裁，會得到「上銀線性滑軌屬 G4-01，而 8083 瑞穎因車庫門與手推車軸承、'
     '4571 鈞興因電動工具與園林工具齒輪箱而被踢出 G」的結果，'
     '等於強迫本組依**客戶應用**切 revenue line，反而破壞已建立的功能軸。'
     '★因此**同一顆軸承賣進自動倉儲、車庫門或手推車仍是軸承**，其 supply-chain role 未變，'
     '不得依終端市場拆分 weight。'
     '★2026-09-08 更名：原名「精密機械」讀起來像整機或設備類型，'
     '而其 children 實際是零組件與工具、以及工業閥門——node label 與 child ontology 衝突。'
     '★本節點的 L3 細分（傳動與線性運動／馬達與驅動／其他工業機械零組件）為 candidate，'
     '尚未建立；建立前既有 G4-01 維持原狀。'
     '★G4-02 於 2026-09-08 通過五道 gate 後批准並改名，見該節點。'
     '其決勝點是第四關：同一個 Definition 能收進兩家製程閥門廠，'
     '同時把三類長得很像的活動排除出去。'),
    ('G4-01', 'G4', 3, '傳動與線性運動',
     '傳遞、轉換、導引或定位機械運動之零組件：線性滑軌、滾珠螺桿、線性模組、'
     '軸承、齒輪與齒輪箱、減速機、聯軸器及其他傳動元件。'
     '★功能判準是**機械運動**：零件承擔的是運動的傳遞、轉換、導引或定位。'
     '與 G4-02 的分界在**被作用的對象**——那裡控制的是工業製程管線中的製程流體；'
     '與 G4-03 的分界在**做的事**——那裡以切削或磨耗去除材料。'
     '★2026-09-08 自本欄移除 CNC 與 Tooling，並改名。原名「精密機械零組件與工具」'
     '同時裝了 G4 軸上的兩個功能。CNC 指的是工具機整機，屬 G5；'
     'Tooling 與研磨切削工具屬新分出的 G4-03。'
     '★**液壓／氣壓動力元件不屬本節點**：以流體壓力作為主要能量傳遞媒介之'
     '油缸、氣缸及其壓力／方向／流量控制閥，其主要經濟活動是流體動力元件，'
     '與本節點以機械件傳遞運動不是同一個角色。判準是**經濟活動與價值鏈角色**，'
     '不是物理上算不算運動。該角色目前全樹無節點，列為 candidate，'
     '見 db/TAXONOMY_GAPS.md；它同時被 G4-02 的定義排除，兩格都不收。'
     '★★**未決衝突（2026-09-08 自報）**：本欄原寫「車用專屬之動力傳動零件見 H」，'
     '那是**依終端市場**把零件轉出，與同日寫進 G4 的裁定'
     '「零組件的 Industry 歸屬以產品功能與機械價值鏈角色判定，**不以終端市場判定**」'
     '直接相反。兩句話同時存在於本節點與其母節點，**必須擇一**。'
     '★本輪不逕行擇一，因為擇任一邊都等於預先裁定 H 分支的合法性：'
     '刪掉指標＝主張車用傳動零件屬 G4-01；保留指標＝承認 Industry 樹允許'
     '產品軸與終端市場軸並存。後者正是 `industry_tree_data.py` 中 H4-01 的'
     'mapping gate 註解已經記著的未決問題（「H4-01 names an end market, A4-* name'
     'products… the question is open and decides whether H4-01 ever opens at all」）。'
     '★**在 H／S 兩支完成 branch audit 之前，本指標停用**：'
     '車用專屬之動力傳動零件的歸屬列為待判，不得依本欄逕行轉出至 H，'
     '亦不得逕行收入本節點。H 分支目前 22 個節點全部零成員，S 分支 8 個節點亦然，'
     '依 R031 的「空節點不是排除的證據」，它們的空狀態同樣不能反過來當成'
     '「所以歸 G4-01」的理由。'),
    # A2-10's 閥件 are vacuum parts for semiconductor tools; G4-01 is transmission
    # and tooling; G6-01 is whole machines. An industrial valve for a refinery is
    # none of those, and 捷流's 62.36% sat on the fab utilities node.
    ('G4-03', 'G4', 3, '切削與研磨工具',
     '以切削或磨耗去除材料之工具與磨料：砂輪、磨料與磨具、鑽石與 cBN 工具、'
     '切削刀具與刀片、研磨與拋光耗材。'
     '★功能判準是**去除材料的方式**而不是產品名稱：只要其經濟功能是以切削或'
     '磨耗去除工件材料即屬本節點，新型態的工具無須改寫定義即可套用。'
     '★**工具機整機見 G5**：工具機做的事也是切削，但它交付的是整機而不是工具。'
     '2026-09-08 的 population 掃描中，兩家「切削中心機」廠即因此不屬本節點。'
     '★**PCB 製程用鑽針與其耗材見 B6**：同一次掃描中兩家鑽針廠服務的是 PCB 製程。'
     '★2026-09-08 由 G4-01 分出。原 G4-01 同時裝了 G4 軸上的兩個功能（傳動與工具），'
     '而 G4 的軸是「零組件在工業機械中承擔的功能」——兩者本就是不同功能，'
     '**合併才是違反，拆開不是為了湊一格**。'
     '★**本節點的存在論證不是「n=1 所以成立」**：先有已成立的軸，'
     '再發現該軸上一個獨立功能被錯誤合併，population audit 最後才提供 mapping evidence。'
     '母體規模是 evidence strength，不是 ontology 依據。'
     '★**工具夾持件屬本節點**（2026-10-07 增訂）：鑽夾頭、刀桿、刀把、筒夾'
     '等把切削工具固定於主軸或工具機上之夾持件，是切削工具系統 (tooling) 的一部'
     '分——它們本身不去除材料，但其經濟功能是使切削工具得以作業，在 G4 的功能軸上與刀'
     '具同屬「工具」而不屬傳動（G4-01）。2026-09-08 自 G4-01 分出本'
     '節點時已記載「Tooling 與研磨切削工具屬新分出的 G4-03」，本條把該記載寫'
     '進定義。成員依據：4575 銓寶「精密鑽夾頭、刀桿及主軸配件,主要應用在綜合加工機、'
     '銑床、鑽床與電動手工具等設備上」。★工件夾治具與測試治具不因名稱含「夾」「治具」而屬'
     '本節點：半導體測試座與探針卡見 A6，PCB 測試治具見 B5-02。'),
    ('G4-02', 'G4', 3, '工業製程閥門與流體控制元件',
     '調節工業製程管線中流體之流通、停止、流量、方向與壓力之閥件與流體控制元件：'
     '蝶閥、球閥、閘閥、控制閥、V 型閥及其致動器；'
     '應用於石化、鋼鐵、造船、能源與電廠、水處理、造紙、製藥與半導體廠之製程管線。'
     '★判準是**流體在哪裡、為什麼被控制**：所控制者須是工業製程管線中的製程流體。'
     '以下三類長得很像而不屬本節點，各附 2026-09-08 建立本定義時實測過的判別案例：'
     '★**液壓／氣壓傳動元件不屬本節點**——油缸、氣缸等執行元件，'
     '以及其壓力控制閥、方向控制閥、流量控制閥與電磁閥。'
     '那裡的流體是**傳遞力量的介質**，不是被控制的製程流體，屬流體動力 (fluid power) 角色。'
     '★**裝置用微型泵閥不屬本節點**——血壓計、咖啡機、汽車座椅氣動腰托、智能馬桶、'
     '淨水器內的微型泵與微型閥，是終端裝置內的元件，不在工業製程管線上。'
     '★**一般標準閥門不屬本節點**——家用瓦斯管線、樓宇水處理、消防管道用閥。'
     '「一般標準／工業用／嚴苛工況」這個三分法取自本節點成員年報所引的產業慣例，'
     '不是本樹自創的切法。'
     '★半導體**設備**用真空閥件見 A2-10：那裡的閥是設備零組件，不是製程管線元件。'
     '★**外購代理之閥門不建立製造曝險**(R022)：本節點收自製或自行提供之製程閥門；'
     '成員若另有外購閥轉售之營收線，該線不計入本格比重。'),
    # Definition deferred (2026-09-04): G4 精密機械, G5 工具機 and G6 機械設備 all
    # plausibly hold a machine tool and none of the three is defined. Deferred for
    # the reason E's cells were -- the branch boundary comes first (R031).
    ('G5', 'G', 2, '工具機',
     '提供具獨立工具機功能之加工機械：以切削、研磨、塑性成形等機械加工方式，'
     '直接對工件進行加工、成形或精密加工之整機。'
     '綜合加工中心機、CNC 車床、銑床、磨床、搪床、放電加工機，'
     '以及金屬塑性成形機（成型機、打頭機、輾牙機、鍛壓成形機）。'
     '★**判準是設備本身是否具有工具機之經濟與產品 identity**，'
     '而不是它最終加工出什麼產品、賣給哪個產業，或專用程度有多高。'
     '★**被加工的材料不構成本節點的分類軸**（2026-09-08 增訂）：'
     '木工、金屬及其他材料之加工機床，只要設備本身具工具機之經濟與產品 identity，'
     '均屬本節點。上開列舉與下文引用的 C291「金屬加工用機械設備製造業」'
     '是本節點目前母體的實況描述，**不是入場條件**——首句的「以切削、研磨、'
     '塑性成形等機械加工方式直接對工件加工」本來就未限定材料，'
     '本條把這件事寫明，避免讀者由列舉反推出材料限制。'
     '★regression case 是 **1528 恩德**：其證據獨立於本條——品項自陳'
     '「各式電腦數控產業機械**(適用金屬及非金屬)**」，產業概況第一節即'
     '「工具機應用於木工產業」，而該公司在本條寫成之前就已在本節點，'
     '可見本節點在此條寫明之前實質上已是材料中立。'
     '1539 巨庭（自動鉋木機 38.21%＋落地形圓鋸機 34.95%＋落地形手動鉋木機 6.74%）'
     '是**促成本條被寫明**的 mapping，其歸屬的實質依據是首句的切削整機判準'
     '與 2026-09-08 的 G6 audit，不是本條——**本條與巨庭那一列不得互相引用為唯一依據**。'
     '★**「專用於單一產品」不把工具機推進 G6**。若採「是否由被服務的產品定義」'
     '作為本節點的軸，會得到「一般 CNC 車床屬 G5、專門加工某一零件的 CNC 屬 G6」'
     '這種不穩定的結果——Industry identity 不應被 customer／application specificity 破壞。'
     '2026-09-08 的決定性反例是 4544 春日（成型機 77.39%＋輾牙機 5.79%＋打頭機 5.51%）：'
     '其產品為螺絲成型機 (Boltformer)、螺帽成型機 (Nutformer)、零件成型機 (Parts former)，'
     '只服務扣件生產，年報自述「扣件成型機為生產扣件必備之**工具機**」。'
     '它是**專用於扣件生產的工具機**，不是專用機械。'
     '見 tests/fixtures/cases/4544_chunzu_specificity_does_not_move_a_machine_tool.md。'
     '★**precedence：機床 identity 優先於 application**。'
     '本身是工具機者（CNC 車床、加工中心、磨床、金屬成形機）一律屬本節點，'
     '客戶是汽車、航太、扣件或模具都不改變；'
     '本身是特定產業製程設備者（PCB 成型／鑽孔設備、半導體 wafer processing、'
     '顯示製程設備）屬 B5／A2／E7，**不因它也是加工機而進本節點**。'
     '此判準逐條營收線適用：4526 東台自有一條「印刷電路板加工機」營收線，'
     '屬 B5-01，不計入本格比重；'
     '1528 恩德的 PCB 鑽孔成型檢測機併於「機械」一線，'
     '依 R020 ②-MX 處理，亦不計入本格比重。'
     '★**與 G6 的分界不是通用 vs 專用，而是設備 identity**：'
     '非工具機之專用製程設備屬 G6——6603 富強鑫塑膠射出成型機 94.61%、'
     '4575 銓寶 PET 吹瓶機 85.16% 皆屬之。'
     '★**這個分界的理由不是材料**（2026-09-08 隨材料中立條款一併改寫）：'
     '射出成型**沒有進入機器的既有工件**——它把熔融原料注入模具直接成形出零件，'
     '不是對工件施以切削、研磨或塑性成形，故不在首句範圍內。'
     '富強鑫進 G6 不是因為它只做某一種產品，也不是因為它做的是塑膠。'
     '★★**未決邊界：4575 銓寶的 PET 吹瓶機（2026-09-08 自報）**——'
     '吹瓶是把既有的預成形體 (preform) 加熱吹脹成形，**確實有工件**，'
     '上開「無既有工件」的理由對它不成立；原本擋住它的是「不是金屬加工機床」'
     '這個材料理由，而材料已於同日宣告不是本節點的分類軸。'
     '**本輪不改變它的歸屬**（G6，2026-09-08 裁定；該公司目前尚未 map）。'
     '在本項邊界被裁定之前，不得以本節點的定義收入吹瓶／押出／中空成型類設備，'
     '也不得以材料中立為由把它們拉進來——這條記的是一個已知的未決衝突，'
     '不是可以任一方向引用的授權。'
     '★**「由被服務的製程／產品定義」只是輔助判準，不是本節點的軸**：'
     '先判設備本身的 machine identity；只有在該設備不屬既有工具機 identity 時，'
     '才看其主要製程、材料或產品用途是否構成 G6 的專用設備。'
     '★**零組件不屬本節點**：主軸、線性運動與傳動元件見 G4-01'
     '（4561 健椿精密主軸 100%，其主軸裝在本節點成員的機器裡）；'
     '刀具、砂輪與磨料見 G4-03（1560 中砂做砂輪，4513 福裕做磨床，'
     '一個是工具、一個是持有工具的整機）。'
     '★**使用加工設備生產零件者不屬本節點**：4528 江興鍛「汽車零件鍛壓製造、加工 96.94%」'
     '的產品是前輪傳動主件、懸吊系統鍛件、齒輪、離合器與差速器——'
     '它**用**鍛壓做汽車零件，不製造鍛壓機，歸 H。'
     '★本節點的 identity 有樹外佐證：母體自我描述一致'
     '（1530 亞崴與 4563 百德年報用字面相同的一句「係屬產業機械中之金屬切削機械製造業」，'
     '4513 福裕、4526 東台、6606 建德、1540 喬福的登記營業項目皆為「各種工作母機之製造」），'
     '且有自己的公會與出口統計（TAMI／TMBA），'
     '4561 健椿年報並引用行政院主計總處《行業標準分類》29 中類之'
     '金屬加工用機械設備製造業 (C291) 與其他專用機械設備製造業 (C292)。'
     '**這是外部既有的產業切法，不是本樹自創。**'
     '★**但該外部切法只覆蓋金屬子集**（2026-09-08 自報，隨材料中立條款一併記）：'
     'TAMI／TMBA 的工具機出口統計口徑是「金屬切削工具機＋金屬成型工具機」，'
     'C291 亦寫明金屬，**木工機床不在其內**。因此 1539 巨庭進入本節點靠的是'
     '首句的內部判準（鉋床與圓鋸以切削去除工件材料之整機），不是這段樹外佐證。'
     '本段的效力範圍僅及於金屬加工機床那一部分，不得被引用來支持或反對材料中立條款。'
     '★2026-09-08：G4／G5／G6 自 2026-09-04 掛著的「誰收工具機」邊界爭議，'
     '因 L2 改採價值鏈角色軸而解消——G4 是零組件、G5 與 G6 都是整機，'
     '兩者之間再依是否為工具機分界。★Product Definition 尚未完整寫定。'),
    ('G6', 'G', 2, '專用機械與製造設備',
     '服務特定製程、材料或製造工藝之專用機械與製造設備整機，'
     '且其設備 identity 不屬 G5 工具機。'
     '★2026-09-08 更名：原名「機械設備」是 residual 語意（「其他機械都放這」），'
     '改為描述它實際交付的角色。'
     '★**與 G5 的分界不是通用 vs 專用**：專用程度不是本節點的入場條件。'
     '4544 春日的扣件成型機只服務扣件生產，仍屬 G5，因為它的設備 identity 是工具機。'
     '進入本節點的是**設備 identity 本身不是工具機**者：'
     '6603 富強鑫塑膠射出成型機、4575 銓寶 PET 吹瓶機——射出與吹瓶是'
     '另一種製造製程設備 identity。★**理由不是材料**：G5 於 2026-09-08 宣告'
     '被加工的材料不構成其分類軸，本節點的分界隨之改寫——射出成型沒有進入機器的'
     '既有工件，故不在 G5 首句「對工件進行加工、成形」的範圍內。'
     '**銓寶的吹瓶機是這條理由不能覆蓋的例外**（有 preform 這個既有工件），'
     '該未決衝突記於 G5，本輪不改變其歸屬。'
     '★**判斷順序**：先判設備是否具工具機 identity（是則 G5），'
     '再看其主要製程、材料或產品用途是否構成本節點的專用設備。'
     '★已由特定製程分支定義者不屬本節點：半導體設備見 A2、PCB 設備見 B5、'
     '面板／顯示設備見 E7、廠務系統見 A7-01。'
     '★**2026-09-08 本節點為 candidate role，未封版**：population audit 確認它有'
     'territory 但尚不足以寫定 Definition。正面母體目前 3 家——6603 富強鑫塑膠射出'
     '成型機 94.61%、4575 銓寶 PET 吹瓶機 85.16%、4744 皇將製藥與保健食品包裝'
     '單機設備 40.77%＋整線設備 36.32%（貼標、數粒、液體與粉體充填、理瓶、旋蓋、'
     '軟膠囊產線）。三家分屬塑膠成型與製藥包裝兩個製程族。'
     '★**不得以「不是 G5 所以 G6」建立本節點**。這是本節點的主要失效模式，'
     '2026-09-08 有四個實測案例：1539 巨庭（木工機，雖極專用仍屬 G5，'
     '其登記營業項目首項即「鑽床、車床、銑床、自動鉋床」）與 4562 穎漢'
     '（金屬彎管機、切管機、管端成型機）都是先通過 G5 才沒有進來；'
     '而 3580 友威科（真空濺鍍／蒸鍍／電漿蝕刻設備，橫跨 IC 載板、半導體、OLED、'
     '觸控、太陽能、汽車）與 2070 精湛（光學影像篩選機、渦電流篩選機、藥錠異檢機，'
     '橫跨緊固件、粉末冶金、LED、被動元件、藥錠）**都不進本節點**——'
     '它們彼此也不是同一種 functional role（一個鍍膜、一個檢測），'
     '一起收進來就會重新製造「其他非工具機設備」這個 residual。兩家各留 gap，'
     '見 db/TAXONOMY_GAPS.md G13／G14。'
     '★**入場必須靠 positive identity**：「專用」與「通用」在兩個方向上都不是判準——'
     '巨庭極專用而屬 G5，精湛橫跨五個產業而仍不是工具機。'
     '★**本節點的下一個 existence test**：能否找到第三個以上、彼此不靠產品名稱相似、'
     '而是能由同一句「非工具機之明確製程／製造設備 identity」定義同時收進的獨立 population。'
     '在那之前 G6-01 這個 leaf 不封版。'
     '★precedence 除 A2／B5／E7／A7 之外**還須包含 G5 優先於本節點**，'
     '這是本輪由巨庭與穎漢兩家實測出來的。'
     '★★**2026-10-07 封版，分為三個葉節點**：上開 existence t'
     'est 經使用者裁定通過——在塑橡膠成型（6603 富強鑫、4575 銓寶）與包裝（'
     '4744 皇將）之外，辨識出食品機械（1580 新麥，商用烘焙設備，自述「本公司係屬'
     '食品機械產業之中游製造廠商」）與第二家包裝機械（8374 羅昇之子公司怡進，捆包設備'
     '），每一家都以其製程指名自己的機械，沒有一家靠「不是工具機」入場。本節點自此依**製'
     '程族**分為 G6-01 塑橡膠成型機械、G6-02 包裝機械、G6-03 食品機械'
     '；上文「candidate role，未封版」與「G6-01 這個 leaf 不封版'
     '」為 2026-09-08 的歷史記載。★新製程族須有自己的觀測母體與 positi'
     've identity 才得增設葉節點；原 G6-01 列舉之紡織機械無觀測成員，未'
     '設。★辨識這些母體的掃描只比對營收線名稱，未命中不等於不存在。'
     '★2026-10-07 同日增設 G6-04 製藥機械（4744 皇將之子公司 Ch'
     'angsung，軟膠囊生產設備）。'),
    ('G6-01', 'G6', 3, '塑橡膠成型機械',
     '以整機形式交付、把塑膠或橡膠原料成形為製品之成型機械之設計、製造與銷售：塑膠射出成型'
     '機、吹瓶機。★入場靠 positive identity：年報須以成型製程指名其機械'
     '，不得以「不是工具機」入場（見 G6）。★成員依據：6603 富強鑫（塑膠射出成型機'
     '）、4575 銓寶（PET 吹瓶機）。★4575 銓寶的吹瓶機與 G5 的未決邊界仍'
     '記於 G5，本節點不改變該記載。★不屬本節點：使用成型機生產塑膠製品者（見 K2 塑'
     '膠加工）；模具；工具機（G5，優先）。★2026-10-07 由原「專用機械」改定並'
     '更名：原列舉之包裝機械、食品機械分別移至 G6-02、G6-03，紡織機械無觀測成員'
     '、不設葉節點。★沿革：原「其他專用機械」結尾條款已於 2026-09-08 移除並停'
     '用（1315 達新之裝訂機、護貝機曾據以掛入，同日退出），不因本次改定而恢復。'),
    ('G6-02', 'G6', 3, '包裝機械',
     '以整機或整線形式交付、對產品施行充填、貼標、數粒、封蓋、捆包、裹膜等包裝製程之機械之'
     '設計、製造與銷售。★入場靠 positive identity：年報須以包裝製程指名'
     '其機械（見 G6）。★成員依據：4744 皇將（製藥與保健食品包裝單機設備與整線：貼'
     '標、數粒、液體與粉體充填、理瓶、旋蓋）；8374 羅昇之子公司怡進（捆包設備，財報轉'
     '投資資訊載其主要營業為「捆包設備生產製造及銷售」）——後者在羅昇營業比重表上的對應為'
     '判讀，見該列 evidence。★不屬本節點：包裝材料之製造（紙器見 P3、塑膠包材'
     '見 K2）；以包裝機械提供充填或包裝代工者；包裝機械之代理買賣（R022）。★202'
     '6-10-07 自原 G6-01「專用機械」的列舉分出。'),
    ('G6-03', 'G6', 3, '食品機械',
     '以整機形式交付、對食品原料施行攪拌、分割、整型、發酵、烘烤等調製與加工製程之機械之設'
     '計、製造與銷售；含商用烘焙設備。★入場靠 positive identity：年報須'
     '以食品製程指名其機械（見 G6）。★成員依據：1580 新麥——「本公司主要經營商業'
     '用烘焙設備及廚房設備」，自述「本公司係屬食品機械產業之中游製造廠商」。★**商用烘焙'
     '設備屬本節點**（2026-10-07 裁定）：機器的用途是生產麵包與蛋糕，其客戶為'
     '「麵包店、批發廠、超市、便利商店、飯店及咖啡店」而非食品工廠，不改變它是食品製程機械'
     '。★不屬本節點：冰箱、展示櫃、餐飲設備與器具等非製程設備——同一公司的這類營收線不計'
     '入本節點；使用食品機械生產食品者；家用機器。★2026-10-07 自原 G6-01'
     '「專用機械」的列舉分出。'),
    ('G6-04', 'G6', 3, '製藥機械',
     '以整機或整線形式交付、對藥品與保健食品施行劑型製造製程（軟膠囊之明膠烹煮、混料、成型'
     '充填封合、乾燥）之製藥機械之設計、製造與銷售。★入場靠 positive ident'
     'ity：年報須以製藥製程指名其機械（見 G6）。★成員依據：4744 皇將之子公司 '
     'Changsung Softgel System——年報載其「專注於軟膠生產設備研發'
     '生製造」，財報轉投資資訊載其主要營業為「製藥機械製造」。★與 G6-02 的分界是製'
     '程：本節點的機械製造藥品劑型本身，G6-02 的機械對已成形之產品施行充填裝瓶、貼標'
     '、封蓋等包裝；'
     '同一營收線同時包含兩者而年報未拆分時，依 R020 ②-MX 處理。★'
     '不屬本節點：使用製藥機械生產藥品或保健食品者；製藥包裝設備（G6-02）。★2026'
     '-10-07 增設：觀測母體為 1 家，與 G6-03 同為單一成員之製程族。'),
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
    ('I6-01', 'I6', 3, '重電',
     '發電、輸電、變電與配電所用之電力設備及電力導體之設計、製造與銷售：電力與配電變壓器、'
     '電抗器、比壓器與比流器；高低壓配電盤、馬達控制中心、分電箱；氣體絕緣開關設備 (GI'
     'S)、斷路器、分段開關、熔絲鏈開關等開關設備與輸配電線路器材；避雷器與高低壓電容器；'
     '發電機組；電力電纜與電線。★判準是公司交付在電力系統中承擔變壓、啟斷與保護、配電或輸'
     '送電力功能之設備或導體本身；電力系統指自發電端經輸配電線路至用戶受電與場內配電之範圍'
     '，不含用電設備內部的電源。★電子變壓器與磁性元件不屬本節點：裝在電源供應器、通信設備'
     '或其他電子產品內部之變壓器、電感器與線圈是被動元件（見 F1-01），不因品名為變壓'
     '器而入本節點——6292 迅德（變壓器 94.00%，年報載為工業電源、通信設備、L'
     'ED 燈具等應用之電感器與變壓器）、6109 亞元（各式數據機用變壓器 47.02%'
     '）即此類。★電力電纜與電線沿原列舉留在本節點，以在電力系統中輸送或分配電力者為限；品'
     '名含電線電纜者（如電線電纜附屬器材）仍須確認實際交付物，不因字樣而入本節點。是否另立'
     '節點未裁。★工程承攬不屬本節點：變電所與昇壓站統包、輸配電線路、機電、軌道與太陽光電'
     '等工程，交付的是工程而非設備，不因施作對象是電力系統而入本節點。'
     '隨自製設備出貨之安裝，符合 R020 ②-MX 附隨服務例外（MX-INC）條件者，'
     '不使該設備線成為混合線；年報獨立列報之工程營收線依其實際交付物另行判定，'
     '不得自行估算其中自製設備之比例而計入本節點。'
     '不屬本節點不表示全樹無節點。★電力電子設備不屬本節點：不斷電系統'
     '與電能品質、電源保護設備見 I8-01；整流器、充電機、直流供電設備與變流器依 I7'
     '-01、I8-01、I1-01 各該節點判，太陽能變流器之歸屬仍未裁。★代理或外購他'
     '廠電力設備轉售者見 V9 (R022)。★發電設備之製造屬本節點（如發電機組）；風力'
     '發電機組見 I2-01、太陽能發電設備見 I1-01。★與 I5-01 的分界：本節'
     '點依公司交付之電力設備或電力導體本身，以及其在電力系統中承擔的功能判定；不得僅因產品'
     '或服務應用於輸配電、智慧電網，就將非本節點所定義之系統、工程或服務交付物納入本節點。'
     '節點歸屬依交付物的功能與價值鏈角色判定，不以下游應用場景單獨決定。★下列三項歸屬未裁'
     '，裁定前不得據本節點掛該等營收線；未裁不表示全樹無節點：(一) 電動機（馬達）——原'
     '列舉未含，G4 之軸宣告驅動而無節點（taxonomy gap G12）；(二) 匯'
     '流排與銅排——C14-01 列舉含 Busbar 與匯流排而無可執行定義；(三) 漆'
     '包線，以及電子設備用之訊號線材、線束與連接線組——F3-01 線纜無可執行定義。★範'
     '圍依據（用以支持本節點涵蓋之設備與功能範圍；非封閉清單，亦非所列公司之成員裁定）：1'
     '514 亞力年報「重電機電力輸配電及相關產業產品主要以變壓器、配電盤、監控系統、高壓'
     'GIS、比壓器、比流器及高低壓電容器為主」（印刷頁 p63）；1503 士電年報載重'
     '電類產品「專注於輸配電關鍵設備之研發製造」（印刷頁 p91）；1519 華城年報稱其'
     '變壓器及配電盤為「各電力系統從電力公司發電輸電配電到產業建/擴廠及各公共建設皆需要的'
     '設備」（印刷頁 p43）。★2026-10-11 由品項列舉改寫為定義。'),
    ('I7', 'I', 2, '電力電子', None),
    ('I7-01', 'I7', 3, '電力電子',
     'Inverter、PCS、電源模組、一般電源供應器（AC-DC／DC-DC 電源供應器與轉換器、外接式電源／適配器、基板電源／Open Frame）。★伺服器／資料中心電源依 C5-01 範圍判斷，不屬本節點(R011)；同一營收線混合一般與伺服器電源而年報未拆分者，依 R020 ②-MX 處理，不得依推測比例拆列。（UPS 於 2026-10-07 移出，見 I8-01）'),
    ('I8', 'I', 2, '電力管理 / 電能品質', None),
    ('I8-01', 'I8', 3, '不斷電與電能品質設備',
     '不斷電系統 (UPS) 與電能品質設備之設計、製造與銷售：在線式、離線式、在線互動式'
     '不斷電系統，主動式電力濾波器，電源保護與電源分配管理設備。★判準是公司交付保護負載、'
     '維持供電品質或分配電力之電源設備本身。★成員依據：3043 科風（「負責研發、製造與'
     '行銷不斷電系統產品」）、6409 旭隼（在線式與離線式 UPS）、3628 盈正（自'
     '行研發製造之單相與三相 UPS、主動式電力濾波器）。★與 I7-01 的分界：I7-'
     '01 收電力轉換之元件與模組（逆變器、PCS、電源模組），本節點收成套之電力保護設備'
     '；2026-10-07 起 I7-01 的列舉不再含 UPS。★太陽能逆變器不屬本節'
     '點——I1-01 與 I7-01 的列舉皆含 Inverter，其歸屬未裁。★代理或'
     '外購他廠不斷電系統轉售者不屬本節點(R022)。★不斷電系統之製造者不因產品用於資料'
     '中心而歸 C13-01；C13-01 現無定義，不得據以掛公司。★2026-10-0'
     '7 由品項列舉改寫為定義。★插接式電源保護與配電設備屬本節點（2026-10-08 '
     '增訂）：公司交付之設備若直接介於供電來源與負載之間，並以突波抑制、雜訊濾除、電源分配'
     '或配電監控等功能提供電源保護、供電品質管理或電力分配——包括突波保護插座、電源濾波／'
     '淨化器、機櫃電源分配器 (PDU) 等——不因其為消費性或資訊週邊產品而排除；判準仍'
     '是公司交付該電源設備本身。★裝在別人產品裡的電路保護元件（壓敏電阻、氣體放電管、TV'
     'S、PPTC 等）不屬本節點。'),
    ('J', None, 1, '化學 / 石化',
     '化學品之製造。**本分支的主軸是產品在化學價值鏈上的位置與其價值來源**：'
     '愈上游者以規模與成本為價值來源（J1 上游石化、J2 芳香烴、J3 合成橡膠原料、J9 化纖原料、J10 無機化學品），'
     '愈下游者以特定功能、配方或性能為價值來源（J4 特用化學、J5 樹脂、J6 塗料與油墨、J7 染料與顏料、'
     'J11 接著劑、J15 碳黑）。'
     '★三格明示不在該主軸上，依 R031 不納入其互斥判斷：'
     'J13 煤化工以**原料來源**（煤而非石油）分格；'
     'J14 家用化學品與 J16 電子成像耗材以**終端用途**分格，收的是已配製完成、可直接使用的成品。'
     '★★**2026-09-11 移除 J8 電子化學**：本分支原記「J8 與 J4 的邊界尚未寫成可比較的定義」，'
     '把它當成一個真實存在、只是定義不夠好的 leaf——**那個記述本身是錯的**。'
     'J8 唯一成員 1722 台肥沒有任何營收列，其 membership 建立在兩句話上：產業概況章對'
     '電子級化學品這個**產業**的通則描述，以及「透過整合上下游供應鏈與**提升至**電子級'
     '產品來擴大銷售市場」這句**策略意圖**。該公司業務章「電子化學」零命中，其上下游'
     '關聯圖是把硝酸、磺胺酸、發煙硫酸賣給包含電子業在內的九個客戶產業。'
     '**沒有可觀測的 revenue-bearing 活動，就不是 R031 的軸問題，是 R025 的母體問題**——'
     'R031 的判準要求一條營收線，而這裡一條也沒有，判準從未取得啟動條件。'
     '領域無損：J4-01 的定義已明文包含「半導體與光電製程用化學品」。'),
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
     '★★**2026-09-11 更正這條 backlog 的類型**：原記「J8 電子化學與本節點子項的'
     '半導體與電子用化學品在文字上重疊，兩者的邊界尚未寫成可比較的定義」——'
     '那個描述會讓後來的審查者以為 J8 是一個真實存在、只是定義不夠好的 leaf。'
     '實際情形是 **J8 從未建立可觀測母體**：唯一成員 1722 台肥零營收列，'
     'membership 源自產業概況與策略意圖，不足以建立 Industry leaf，已於同日移除(R025)。'
     '★因此本節點與 J8 之間**沒有發生過 R031 的 overlap**——判準需要一條營收線，'
     '而 J8 一條也沒有。半導體與光電製程用化學品的領域由本節點的 J4-01 明文承載。'),
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
     '命中「射出／成型」的十九條線裡，成型**機**屬機械而不屬本支——6603 富強鑫的射出成型機屬 '
     'G6-01 塑橡膠成型機械、4544 春日的扣件成型機屬 G5 工具機、2070 精湛的篩選機列 gap G14'
     '（2026-10-07 更正：原句將三家並列於 G6-01 專用機械，後二者已於 2026-09-08 另判）、'
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
    ('L5', 'L', 2, '鋁（非鐵）', '鋁錠、鋁擠型、鋁板、鋁合金、電容用鋁箔（電蝕鋁箔、化成鋁箔、陰極箔）'),
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
    ('N3', 'N', 2, '醫療器材',
     '醫療器材之製造與銷售：手術器械、植入物、體外醫療與影像設備、牙科器材，'
     '以及視力矯正與眼科用醫材（隱形眼鏡、人工水晶體）。'
     '★2026-09-08 依 R025 放寬：原定義列舉四類而未含視力矯正器材，'
     '但拋棄式隱形眼鏡製造以醫療器材製造業登記、產品受醫材法規管理，'
     '是成熟且可獨立辨識的產業產品；放寬既有定義優先於新增節點。'
     '★與 AA1 成像光學元件的分界是**鏡片在誰的光路上**：矯正人眼視力者屬本節點，'
     '成像光路上的鏡頭與鏡片屬 AA1。兩者都叫「鏡片」，這正是 R016 所說'
     '產品名稱不得單獨決定歸屬的情形。'),
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
     '工業自動化機電零組件與控制產品之代理／通路銷售：PLC、變頻器、伺服驅動與工業馬達、'
     '工業感測器與量測儀器、人機介面、工業電腦、氣動與液壓元件，以及傳動與線性運動元件'
     '（滾珠螺桿、線性滑軌與線性模組、減速機、聯軸器、軸承）。'
     '★與 V1 的分界同 V3：本節點賣的是可獨立部署的控制裝置與模組，不是構成它們的半導體。'
     '★傳動與線性運動元件於 2026-10-07 列入：代理商把它們與控制產品賣給同一批設備製造商，'
     '是同一門生意——8374 羅昇年報自述「本公司從事自動化機電零組件經銷代理,其產品依特性'
     '大致可分為工業傳動類及自動化控制類」，6215 和椿的自動化零件一條線亦載'
     '「包含高精度傳動、驅動、感測與控制元件」。列入當時讀過的三家代理商中，兩家同線兼賣'
     '傳動與控制、一家（2359 所羅門）只賣控制側，沒有只代理機械傳動件者，故不另立節點；'
     '全表僅以營收線名稱與業務敘述用詞掃描，未命中不等於不存在。'
     '★自製者不屬本節點：傳動元件之製造見 G4-01，依 R022 逐線判定。'
     '★不含整機之買賣（工具機、包裝機械、專用機械），亦不含非自動化用途之一般五金與機械零件貿易。'),
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
    ('V9', 'V', 2, '電力設備通路',
     '電力設備之代理／通路銷售業務線：不斷電系統、發電機組、匯流排與配電設備等供電與電力保'
     '護設備之代理、經銷，及隨同之安裝、專案工程與維修。該業務線賣的是他廠製造的設備，本身'
     '不含自有製造(R022)。★與 V4 的分界：V4 賣工業自動化之控制與傳動產品，本'
     '節點賣供電與電力保護設備。★自製不斷電系統與電能品質設備見 I8-01；同一公司兩者'
     '皆有時各自獨立判定。★成員依據：2359 所羅門機電事業群（Cummins 發電機、'
     'Piller 與 Eaton 不斷電設備等，年報產製過程表載「代理業務，無產製過程」'
     '）；3628 盈正專案工程（「代理大容量不斷電系統以專案工程方式銷售及服務」）；83'
     '74 羅昇能源管理類產品之一部（子公司 BlueWalker「能源管理產品之買賣及服'
     '務」）。★不屬本節點：以機電或電力工程承攬為主體而設備買賣僅為其一部者（7792 安'
     '葆機電工程 86.22% 即此類，未安置）；能源技術服務（節能工程）。★2026-1'
     '0-07 增設：V1–V6 依所經銷產品類別切分，電力設備不屬其中任一類。上層節點名'
     '稱為「電子通路 / EMS」，本節點所收並非電子零組件，該名稱是否調整未裁。'),
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
    # AA, because A-Z are all taken. Opened 2026-09-08 from the G3 gap after a
    # sweep read all twenty-two candidate filings: nine turned out to be one
    # coherent activity with no home anywhere in the tree, and the other
    # thirteen resolved to D, A3, N, F, H or to roles of their own. That is the
    # shape R025 asks for -- a population that survived a full search for an
    # existing home -- and not the shape it forbids, which is one company
    # needing somewhere to go.
    ('AA', None, 1, '成像與精密光學',
     '以光學成像、取像、投影或精密光學元件與系統之設計、製造與整合為主要經濟活動者：'
     '光學鏡頭與鏡片、成像光路元件、光學模組與光機系統，及其專用光學材料。'
     '★本支的 L2 分類軸是**價值鏈角色**（材料／元件／模組／系統），與 A、E 同型。'
     '★parent scope 刻意窄於「光學」二字，因為泛稱會與四個既有歸屬直接衝突，'
     '逐項排除如下：光通訊用薄膜濾光片與主被動元件見 D6-01、設備見 D5-01；'
     '半導體晶圓級光學製程與鍍膜（晶圓級濾波片、微透鏡、超穎表面、光波導）見 A3-05；'
     '顯示器專用光學（背光模組、導光板、擴散板、增光片、偏光板）見 E2-02；'
     '視力矯正用隱形眼鏡見 N3 醫療器材。'
     '★判準是**光路的用途歸誰**，不是產品名稱含不含「光學」：'
     '同樣是薄膜濾光片，服務光通訊模組者屬 D，服務晶圓級封裝者屬 A3-05，'
     '服務成像鏡頭者屬本支——這一條是 R016 的直接應用。'
     '★**不以「是否產生影像」作為入場條件**：唯有公司直接提供成像光路中的'
     '光學元件、模組或系統，其經濟活動才進入本支的候選集合。'
     '相機、投影機、光達、AR／VR 裝置、機器視覺、車用視覺與智慧型手機都用到成像光學，'
     '但**使用者不是供應者**——做車用感測系統者其經濟活動在 H，'
     '不因產品裡有鏡頭而進入本支；做數位相機整機者交付的是相機而不是光學件'
     '（本支現有成員之一即有此線，該線未計入）。'
     '反過來，相機模組與光學引擎交付的正是光路本身，故屬本支的候選 role。'
     '★2026-09-08 建立時只批准一個 leaf。材料、模組、系統三個角色'
     '已各自看到公司，但每個都只有一例，依 R025 列為 candidate 而非節點，'
     '記於 db/TAXONOMY_GAPS.md。**parent 成立不使其 child 自動成立。**'),
    ('AA1', 'AA', 2, '成像光學元件',
     '成像與取像用光學元件之設計與製造：光學鏡頭、光學鏡片（塑膠、玻璃、模造玻璃、'
     '非球面）、鏡頭組、稜鏡與成像用濾光片，以及其專用光學模具。'
     '應用涵蓋手機、筆電與平板視訊、車載與監控、投影、工業與醫療檢測、'
     '生物辨識、AR／VR 與空拍機取像。'
     '★與 D6-01 的分界是**光路服務誰**：本節點的元件在成像光路上，'
     '光通訊薄膜濾光片（E-PON／G-PON／CWDM／DWDM）在傳輸光路上，屬 D6-01。'
     '★與 A3-05 的分界是**製程**：以半導體晶圓製程產出的光學結構屬 A3-05，'
     '本節點是光學冷加工、模造、研磨、鍍膜與組裝。'
     '★與 E2-02 的分界是**服務對象**：為顯示器光學路徑與背光子系統而製者屬 E2-02。'
     '★**不含**：光學玻璃毛胚等光學材料、相機模組組裝、光學引擎與光機系統——'
     '三者各為不同 value-chain role，2026-09-08 皆列 candidate 未建節點；'
     '在其節點成立前，此類公司留在未分類而非掛入本節點(R031)。'),
]
