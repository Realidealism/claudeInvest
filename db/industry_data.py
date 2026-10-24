"""Canonical industry-chain membership — one entry per company per node.

Written with company NAMES, not stock ids: an active listed name resolves to
exactly one company, while a mistyped id resolves silently to the wrong one.
That was the defect that forced the 2026-07 rebuild, and the loader refuses to
write anything if a single name fails to resolve.

Each member is:
    (company_name, role, weight_item, evidence_kind, evidence, exposure_type)

    role          Core / Major / Minor / Emerging. Exactly one Core per company
                  across all nodes; the database enforces it.
    weight_item   the annual-report product line this exposure is read off, by
                  name. db.fill_weight looks its share up in tw.product_revenue.
                  None means no share is claimed — never a guess.
    evidence      one sentence, always present; the loader and a DB CHECK both
                  refuse an empty one.
    R022 governs distributors: what an agent resells is not what it makes. A
    distributor's product lines may name equipment, materials, substrates or ICs
    without earning it a Core on the matching manufacturing node — at most an
    Indirect exposure. Without that rule the industry totals count agents as
    manufacturers (港建, 利機, 陞泰, 蔚華科 all read that way from their filings
    alone). Check the official category and look for commission revenue.

    exposure_type Direct, or Indirect when this revenue is already counted
                  elsewhere and must not be counted twice (R004) — either on
                  another node of the same company, or by a consolidated
                  subsidiary that is itself a member of this node.

Sync with: python -m db.seed_industry --check   (report only)
           python -m db.seed_industry           (write)
"""

# node_code -> [(name, role, weight_item, evidence_kind, evidence, exposure_type)]
MEMBERS = {
    "L10": [
        ('鑫科', 'Major', '貴金屬材料', 'annual_report_table', '年報營業比重表：貴金屬材料 55.97%，年報自述為「純金料及銀料之加工及銷售」，另有設備零組件貴金屬洗淨回收循環', 'Direct'),
    ],
    "A5-06": [
        ('昇陽半導體', 'Major', None, 'annual_report_prose', '年報自述業務為「晶圓再生、晶圓薄化製程服務」兩項並列，並以整段說明晶圓薄化為「介於前段和後段之間」的中段製程；營收僅「半導體晶圓代工服務 100.00%」一條粗線橫跨兩節點故不計比重', 'Direct', 'Too_Coarse'),
    ],
    "U6": [
        ('倍利科', 'Minor', 'AI智慧醫療系統', 'annual_report_table', '年報營業比重表：AI智慧醫療系統 0.68%，年報自述為「人工智慧(AI)電腦輔助偵測軟體」，屬醫材軟體(SaMD)非硬體，故不掛 N3', 'Direct'),
    ],
    "I1-01": [
        ('有成精密', 'Major', '太陽能', 'annual_report_table', '年報營業比重表：太陽能 45.92%，年報自述自有品牌 WINAICO 模組製造「建立自有生產能量,擺脫純代工模式」', 'Direct'),
    ],
    "L6": [
        ('鑫科', 'Major', None, 'annual_report_table', '年報「鈦鎳產品 22.42%」含鈦鎳合金鍛造、無縫管及板材，鎳(本節點)與鈦(L9)合併揭露無法拆分，故不計比重', 'Direct', 'Too_Coarse'),
    ],
    "L9": [
        ('鑫科', 'Major', None, 'annual_report_table', '年報「鈦鎳產品 22.42%」含鈦鎳合金鍛造、無縫管及板材，鈦(本節點)與鎳(L6)合併揭露無法拆分，故不計比重', 'Direct', 'Too_Coarse'),
    ],
    # "E6-01" dissolved with the node on 2026-09-07. Its two members went to the
    # two different places they actually belonged: the display optical film to
    # E2-02, and the wafer-level optics into the widened A3-05, where it stopped
    # being a separate exposure because it is the same activity as that Core.
    "N3": [
        ('精華', 'Core', '拋棄式鏡片', 'annual_report_table', '年報營業比重表：拋棄式鏡片 99.87%。所營業務之主要內容首項為「醫療器材製造」；年報自述「專注於拋棄式隱形眼鏡鏡片(Disposable Soft Contact Lens)的生產及銷售，拋棄式軟性隱形眼鏡採注模式(Cast-Molding)生產」，產品含近視、遠視、散光、老花、像差控制、角膜變色與彩妝鏡片。★2026-09-08 由 G3 成像光學掃描分流至此：它因品項含「鏡片」二字被光學關鍵字命中，但它的鏡片在人眼的光路上，不在成像光路上(R016)', 'Direct'),
        ('望隼', 'Core', '隱形眼鏡', 'annual_report_table', '年報營業比重表：隱形眼鏡 99.65%。所營業務首列「醫療器材製造業」；年報自述「主要從事軟式拋棄型隱形眼鏡之研發、製造及銷售，所有產品均為本公司自行研發，並⋯承接國內外隱形眼鏡委外代工業務」。★本公司不在營收品項候選集合中，是敘述層掃描找到的第二家隱形眼鏡廠——隱形眼鏡因此不是單一公司而是一個母體', 'Direct'),
    ],
    "AA1": [
        ('大立光', 'Core', '光學元件', 'annual_report_table', '年報營業比重表：光學元件 99.48%。年報產品項目欄只有一項「光學鏡頭」，用途為手機鏡頭、空拍機鏡頭、手機 3D 結構光鏡頭、平板電腦、體感遊戲機、筆電鏡頭、智慧型電視鏡頭、IP 相機鏡頭、汽車鏡頭、運動相機鏡頭。★該線名為「光學元件」而內容全是成像鏡頭——本節點取名「成像光學元件」而非業界慣用的「光學元件」，正是為了讓這個判定可以重複(R016)', 'Direct'),
        ('玉晶光', 'Core', '鏡頭製品', 'annual_report_table', '年報營業比重表：鏡頭製品 75.00%。商品項目為光學鏡頭（傳統相機、數位相機、手機、手提電腦）與光學鏡片（光學滑鼠、指紋辨識器、觀景窗）。★另有照明製品（手電筒、路燈、隧道燈之 LED 光學鏡片）與模具收於「其他 25.00%」，年報未拆故不另計(R020②)', 'Direct'),
        ('佳凌', 'Core', '各類光學應用鏡頭', 'annual_report_table', '年報營業比重表：各類光學應用鏡頭 98.30%。年報自述「本公司係屬光學產業中之**光學元件業**⋯由前端各類光學鏡片、鏡頭設計與製造及機體組裝與外殼設計，具備獨立完整光學機體設計及生產能力」，並列出非球面模造玻璃與球面研磨玻璃生產能力；應用為車用、視訊、監控、投影機、消費性電子與光通訊鏡頭', 'Direct'),
        ('新鉅科', 'Core', '鏡頭製品', 'annual_report_table', '年報營業比重表：鏡頭製品 100.00%。商品項目為筆電與平板鏡頭組、手機攝像鏡頭（含潛望式）、屏下指紋辨識鏡頭、人臉辨識鏡頭（準直、IR）、AR／VR／MR 鏡頭。年報自述「係屬於光學元件產業」', 'Direct'),
        ('久禾光', 'Core', '光學鏡頭', 'annual_report_table', '年報營業比重表：光學鏡頭 98.99%。用途欄為筆電與桌機視訊模組、醫療用光學、機器人視覺與智能家居；另一產品為「各式光學鏡頭之塑膠鏡片」', 'Direct'),
        ('保勝光學', 'Core', '光學鏡頭 + 光學鏡片', 'annual_report_table', '年報營業比重表兩條合計 95.09%：光學鏡頭 61.09、光學鏡片 34.00(R020④)。用途欄為高階投影機、工業檢測設備、天文望遠鏡、運動光學、航太、生醫及半導體檢測設備。★用途多為非消費性，但產品本體仍是成像光路上的鏡頭與鏡片，故屬本節點；服務半導體檢測的是**設備商的鏡頭**，不是半導體製程本身', 'Direct'),
        ('中揚光', 'Core', '鏡頭及鏡片', 'annual_report_table', '年報營業比重表：鏡頭及鏡片 55.33%（手機鏡頭及鏡片）。Core 理由：⑤本格 55.33% 大於模具 28.21%；③該模具線的內容是「手機鏡片模具、光學製品模具（生產手機鏡片、鏡室、車載鏡片使用）」與模仁，是本公司自身光學鏡片製造的上游能力而非獨立業務——年報自述「本集團係以模具開發設計為主，著重於製造開發高精度光學模具及塑膠光學鏡片」，兩者是同一件事的兩端', 'Direct'),
        ('亞光', 'Core', '光學元件產品', 'annual_report_table', '年報營業比重表：光學元件產品 48.45%，為最大營收線。年報產業概況「本公司主要之業務內容為各種光學元件之製造，主要應用在精密光學之鏡片、鏡頭為主，包括手機相機鏡頭、數位相機鏡頭、瞄準器、雷射測距儀、影像感測元件、投影機鏡頭」。Core 理由：⑤48.45% 為最大線；①業務內容自述即「各種光學元件之製造」。★數位相機產品 19.67% 是**整機**、影像感測元件 14.03%、光電零組件 12.48% 三線未計入本格，各自歸屬待判', 'Direct'),
        ('先進光', 'Core', '光學產品', 'annual_report_table', '年報營業比重表：光學產品 56.00%。所營業務之主要內容首列「數位相機之鏡頭模組、手機用數位相機之鏡頭、多功能事務機之鏡頭、其他光學鏡頭模組及鏡片」；商品項目為光學鏡頭（NB Camera、平板、光學式多點觸控螢幕、手機、DV、DSC、PC Camera、多功能事務機及條碼閱讀機）與光學鏡片。★電子零組件產品 44.00%（含指紋辨識模組）未計入本格', 'Direct'),
        ('澤米', 'Major', '精密光學元件製造', 'annual_report_table', '年報營業比重表：精密光學元件製造 43.73%。年報產品及服務項目載明其內容為「可交換鏡頭相機之光學低通濾波器(OLPF)」「LOE 元件」「玻璃晶圓載板/光學玻璃/光學石英/震盪石英/使用之基板加工及供應」三項合併揭露：OLPF 在成像光路上屬本節點，LOE 屬 AR 波導、基板加工屬材料加工，年報未拆分，故不計比重。★2026-09-08 裁定 exposure 可識別而 weight 不可可靠拆分；2026-10-07 裁定成列', 'Direct', 'Too_Coarse'),
    ],
    "E1-01": [
        ('友達', 'Core', None, 'annual_report_prose', '年報營運概況「業務範圍」載明「友達集團是全球光電解決方案領導廠商，憑藉深厚的**面板研發、製造**經驗為基礎，致力發展前瞻顯示技術；同時以顯示技術為核心，結合 AIoT 致力成為各場域的解決方案商」；產業概況之上中下游關聯圖把「液晶顯示面板、液晶顯示模組」列為中游，即本公司所在位置。判為自有面板製程：年報自述面板製造為其基礎能力。★不計比重(R020②)：tw.product_revenue_status 記 FY2025 status=no_section——年報 PDF 已取得但營業比重章節定位失敗，庫內無任何 2025 營收線。本列因此為全庫最大面板廠之一而無 weight，是 discovery 層的缺口不是證據的缺口。★年報另載「2025 年度智慧移動與垂直場域營業額佔整體合併營業額已來到 43%」——該數字是**場域別**營收分類而非面板營收比重，**不得回填為本格 weight**；那 43% 的 value-chain role 未判，屬未歸屬★2026-09-09 weight_status=Not_Disclosed：業務範圍章完整，唯一的「比重」是市場敘述「觸控螢幕筆電比重也持續增加」。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('群創', 'Core', '顯示面板及相關多元跨域應用產品', 'annual_report_table', '年報營業比重表單一線 100.00%。年報業務範圍首句「本公司合併營收主要來源為 TFT-LCD 平面顯示器」，商品項目為「TFT-LCD 液晶面板、觸控模組及電視整機代工」。判為自有面板製程：年報自述面板尺寸「完整涵蓋⋯是全方位液晶顯示器提供者」，並載明將 3.5 代廠改建為扇出型面板級封裝(FOPLP)產線。★該 100% 線同時含觸控模組與電視整機代工，年報未拆故不另計(R020②)——整機組裝在本支無節點，見 E1-01 定義所記之 taxonomy gap', 'Direct'),
        ('彩晶', 'Core', 'TFT-LCD', 'annual_report_table', '年報營業比重表：TFT-LCD 98.68%（年報記 99%）。年報業務範圍首句「薄膜電晶體液晶顯示器(TFT-LCD)及觸控面板之研發、設計、**製造**、銷售及維修」，商品項目為車載、工控、穿戴、手機、平板、筆電各應用之 TFT-LCD 面板，含 TDDI In-cell 異形全面屏。判為自有面板製程：業務範圍明列製造', 'Direct'),
        ('智晶', 'Core', 'OLED(1.5”以上) + OLED(1.5”以下)', 'annual_report_table', '年報營業比重表兩條合計 91.47%：OLED(1.5 吋以上) 51.53、OLED(1.5 吋以下) 39.93(R020④)。判為自有面板製程：公司章程所營業務第 3 項為「研究、設計、開發、**製造**及銷售⋯被動式有機發光顯示器(PMOLED)」，年報自述主要商品為 PMOLED 顯示模組之製造與銷售、尺寸 0.19 吋至 8.9 吋。★年報另載「增加薄膜電晶體液晶顯示器(TFT-LCD)與準系統模組化產品」，該部分為外購面板之模組，併於「其他 8.53%」揭露、未達 R026 門檻故不另掛 E2-01', 'Direct'),
        ('華凌', 'Major', 'OLED顯示器面板及模組', 'annual_report_table', '年報營業比重表：OLED顯示器面板及模組 25.66%。判為自有面板製程：年報商品項目第 3 項為「有機電激發光顯示器(OLED)**面板**生產及銷售」，所營業務首列「有機電激發光(OLED)顯示器模組」。★本線名稱即「面板及模組」，年報未拆兩者故整條計入本格(R020②)；本公司另有 STN 與 TFT 模組線掛 E2-01 Core', 'Direct'),
    ],
    "E2-01": [
        ('TPK-KY', 'Core', '觸控模組', 'annual_report_table', '年報營業比重表：觸控模組 85.38%。年報業務範圍自述「主要經營之業務為觸控模組、觸控模組顯示器及導電玻璃等相關產品之研發、生產及銷售」，商品項目為各尺寸觸控模組解決方案與精密光機電模組貼合服務', 'Direct'),
        ('熒茂', 'Core', '觸控面板', 'annual_report_table', '年報營業比重表：觸控面板 100.00%。商品項目為觸控面板模組，含 Optical Bonding、光學膠模組貼合、Open Cell 觸控模組與 EPD 貼合模組，終端為軍用筆電、軍工控、醫療工業電腦與 POS', 'Direct'),
        ('富晶通', 'Core', '觸控面板', 'annual_report_table', '年報營業比重表：觸控面板 99.84%（本庫記 100.00%）。商品項目為 1.8 吋至 65 吋電阻式與電容式觸控面板，並提供觸控面板與 LCM 貼合及垂直整合解決方案', 'Direct'),
        ('萬達光電', 'Core', '觸控面板', 'annual_report_table', '年報營業比重表：觸控面板 65.77%。商品項目為各尺寸電阻式、投射電容式與全平面觸控面板，含軍事、醫療特殊應用客製', 'Direct'),
        ('創為精密', 'Core', '電阻式觸控面板 + 投射式電容觸控面板', 'annual_report_table', '年報營業比重表兩條合計 64.93%：電阻式觸控面板 50.54、投射式電容觸控面板 14.39(R020④)。年報業務範圍自述「觸控面板之製造及銷售、控制器及驅動程式之銷售」，商品項目含光學貼合製程與全系列加值型 TDS (Touchscreen Display Solutions)、電子紙光學貼合', 'Direct'),
        ('GIS-KY', 'Core', '平板電腦觸控顯示模組 + 筆電觸控顯示模組', 'annual_report_table', '年報營業比重表兩條合計 80.00%：平板電腦觸控顯示模組 61.00、筆電觸控顯示模組 19.00(R020④)。年報業務範圍自述「主要從事各類型觸控與顯示模組之研發、生產及銷售」，商品項目含觸控感測器 (Film Sensor)、觸控模組、顯示器模組（整合 TFT-LCD、驅動 IC、偏光板、背光模組之 LCD 模組代工）與觸控顯示器模組。Core 理由：①自述即「觸控與顯示模組」；⑤本格兩條合計 80.00% 遠大於指紋辨識模組 15.00%。★指紋辨識模組 15.00% 未計入本格，模組端全樹尚無節點(A4-15 收的是 IC 設計)', 'Direct'),
        ('眾福科', 'Core', '觸控式液晶顯示器模組 + 液晶顯示器模組 + 醫療及工業用顯示器', 'annual_report_table', '年報營業比重表三條合計 90.12%：觸控式液晶顯示器模組 64.19、醫療及工業用顯示器 17.81、液晶顯示器模組 8.12(R020④)。年報業務範圍自述「從事觸控式液晶顯示器模組及液晶顯示器模組之設計製造及銷售,產品主要應用領域為船舶顯示器、工控顯示器、特殊車輛顯示器及醫療顯示器」，商品項目首項為「高規格觸控式液晶顯示模組(TDM/LCM)」', 'Direct'),
        ('勁豐', 'Core', '醫療用觸控顯示器 + 特殊應用顯示模組', 'annual_report_table', '年報營業比重表兩條合計 63.29%：醫療用觸控顯示器 53.50、特殊應用顯示模組 9.79(R020④)。Core 理由：⑤本格 63.29% 大於嵌入式控制系統 28.16%；①年報自述「主要應用以人機介面系統為主,範圍包括嵌入式控制系統、醫療用觸控顯示器及特殊應用顯示模組」。★嵌入式控制系統 28.16% 未計入本格且歸屬待判——G2 工業電腦尚無 Product Definition', 'Direct'),
        ('云光', 'Major', 'LCD模組', 'annual_report_table', '年報營業比重表：LCD模組 30.19%', 'Direct'),
        ('晶采', 'Core', 'TFT + TN.STN.FSTN', 'annual_report_table', '年報營業比重表兩條合計 98.45%：TFT 84.13、TN.STN.FSTN 14.32(R020④)。判為外購面板之模組整合：年報所營業務首項為「液晶顯示**觸控模組**設計、製造業務」，商品項目為「液晶顯示器模組之設計、製造及應用。包括大、中、小、TN、STN、FSTN、TFT，電容TP模組」。★「TFT 84.13%」這條品項字面看似面板，實為 TFT 模組——正是 E1-01 需要 mapping gate 的原因', 'Direct'),
        ('久正', 'Core', '液晶顯示器模組', 'annual_report_table', '年報營業比重表：液晶顯示器模組 97.67%。判為外購面板之模組整合：產業概況載明「久正**搭配 TFT 面板廠**，憑藉設計及製造能力，提供客戶客製化模組設計」，並自述「以**自有模組**設計、組裝產線的優勢」。★保留矛盾點：商品項目第 2 項寫「液晶顯示器**面板**之設計、製造及應用」，與上述「搭配 TFT 面板廠」相互矛盾，可能指 TN/STN 側之自有 cell 製程；本裁定依 97.67% 模組線與產業概況，若日後證實 TN/STN 面板自製，本列須重判', 'Direct'),
        ('光聯', 'Core', '液晶顯示器模組(LCM)', 'annual_report_table', '年報營業比重表：液晶顯示器模組(LCM) 96.79%，另有液晶顯示器(LCD) 1.56% 未達門檻。判為外購面板之模組整合：計畫開發之新產品為「內建觸控功能的顯示器面板模組」「高精度對位的光學貼合」，產業概況以「中小型的 LCM 業者」自況、並記 LCD 面板半成品價格為其成本★2026-09-15 M5：weight_item 對齊年報品項名稱「液晶顯示器模組(LCM)」，權重由印出值 96.79% 解析（原為金額推算 96.791）', 'Direct'),
        ('凌巨', 'Core', '液晶顯示模組', 'annual_report_table', '年報營業比重表：液晶顯示模組 87.71%（★庫內記 43.86%，見下）。判為外購面板之模組整合：年報自述「致力於成為全方位的中小尺寸平面顯示器專業供應商⋯主要產品為 TFT-LCD 液晶顯示模組」。另有液晶顯示面板 4.60% 未達 R026 門檻故不另掛 E1-01。★資料缺陷：tw.product_revenue_share 把年報的合計列「營業淨額 8,595,510」當成一條產品線存入，分母因此加倍，每一條比重都被折半（模組 87.71→43.86、面板 4.60→2.30）。weight_item 仍指向庫內品項名，數值以庫內為準以免與 fill_weight 不一致', 'Direct'),
        ('達運', 'Core', '液晶顯示器及模組代工', 'annual_report_table', '年報營業比重表：液晶顯示器及模組代工 76.57%。判為外購面板之模組整合：年報自述「整合上下游從光學元件、精密加工部品及模具、全尺吋背光模組至電視機產品服務」，面板端則寫「結合**集團公司**在面板產業研發團隊的全力支援與技術合作交流」——面板不在本公司。Core 理由：⑤本格 76.57% 遠大於光電科技產品及週邊零組件 23.43%（該線掛 E2-02 Major）；①年報自述的整合鏈以「至電視機產品服務」收尾，模組與整機端是其終點而背光零組件是其中一段', 'Direct'),
        ('晶達', 'Core', '高亮度液晶面板模組 + 高亮度液晶顯示器', 'annual_report_table', '年報營業比重表兩條合計 75.98%：高亮度液晶面板模組 42.29、高亮度液晶顯示器 33.69(R020④)。判為外購面板之模組整合：年報產品用途欄載明其為「陽光下可視、寬溫、大的亮度區間、抗反射光學貼膜、寬輸入電壓、防水防塵防震」之高亮度模組與顯示器，應用於數位看板、車輛船舶、POS、KIOSK、ATM。★工業用主機板 13.61% 未計入本格，G2 工業電腦尚無 Product Definition', 'Direct'),
        ('鈺緯', 'Core', '醫療用顯示器 + 工業用顯示器', 'annual_report_table', '年報營業比重表兩條合計 70.38%：醫療用顯示器 64.37、工業用顯示器 6.01(R020④)。判為外購面板之模組整合：年報產業概況全篇論醫療級顯示器之解析度、亮度、延遲率與 DICOM 規格要求，計畫開發為手術房遠距傳輸顯示器、數位病理專用顯示器、下一代顯示技術(OLED、miniLED)醫療顯示器，無任何面板製程敘述。★醫療用組件 23.44% 未計入本格，內容待判', 'Direct'),
        ('華凌', 'Core', 'STN顯示器模組 + TFT顯示器模組', 'annual_report_table', '年報營業比重表兩條合計 71.16%：STN顯示器模組 41.77、TFT顯示器模組 29.39(R020④)。判為外購面板之模組整合：商品項目第 1、2 項為「STN／TFT 液晶顯示器模組標準品及客制品研發設計生產及銷售」。Core 理由：⑤本格 71.16% 大於自產 OLED 面板線 25.66%（該線掛 E1-01 Major）', 'Direct'),
        ('全台', 'Core', None, 'annual_report_table', '年報營業比重段落全文為「主要為液晶顯示器、電容式觸控面板及其模組，以及嵌入式觸控顯示器，佔營業比例達 100%」，四類併為單一 100% 不可拆故不計比重(R020②)。判為外購面板之模組整合：商品項目為「液晶顯示器、電容式觸控面板及嵌入式觸控顯示器之設計、製造及應用」，新商品含「TFT 模組+Sensor+Cover Lens 之全貼合產品」，產業概況自述所屬產業為「中小尺寸液晶顯示器與電容式觸控面板及其整合模組產業」。★本列來自 2026-09-07 的 residual 掃描：本公司 tw.product_revenue_status 為 no_section，庫內無任何 2025 營收線，因此不曾出現在以營收品項關鍵字建立的 34 家候選集合中', 'Direct', 'Too_Coarse'),
        ('微端', 'Core', '液晶顯示模組', 'annual_report_table', '年報營業比重表：液晶顯示模組 45.52%，為最大營收線。判為外購面板之模組整合：年報自述「液晶顯示模組(LCM)為光電產品中之顯示元件，本公司所生產之液晶顯示模組含 STN LCM 及 TFT LCM，主要應用於醫療、通訊、辦公室自動化和工業控制」。★數位影音多媒體產品 27.25%（專業音效器材）與軍警用設備 5.77% 未計入本格，另有歸屬待判', 'Direct'),
    ],
    "E2-02": [
        ('瑞儀', 'Core', '背光板-模組及零組件', 'annual_report_table', '年報營業比重表：背光板-模組及零組件 99.54%', 'Direct'),
        ('先益', 'Core', 'LED背光模組', 'annual_report_table', '年報營業比重表：LED背光模組 87.60%', 'Direct'),
        ('云光', 'Core', '背光模組', 'annual_report_table', '年報營業比重表：背光模組 63.52%。Core 理由：⑤本格 63.52% 大於 LCD模組 30.19%，後者掛 E2-01 Major', 'Direct'),
        ('穎台科技', 'Core', '光學膜 + 導光板 + 擴散板 + 照明板', 'annual_report_table', '年報營業比重表四條合計 81.58%：光學膜 49.05、導光板 22.18、擴散板 9.55、照明板 0.80(R020④)。年報商品項目載明「擴散板主要應用在高亮度液晶顯示器直下式背光模組」「導光板應用於側光型背光模組,是影響光效率的重要元件」；光學膜為聚合物押製之多層光學薄膜，應用於 TV、MNT、NB、手機與車用顯示。★車用顯示 8.92% 未計入本格', 'Direct'),
        ('山太士', 'Major', '光學材料', 'annual_report_table', '年報營業比重表：光學材料 30.34%，年報「公司目前之產品項目」第一項即「LCD 背光模組材料包括擴散片、反射片、增光片及遮光反射材料」。★2026-09-07 由 E6-01 Core 改為本節點 Major：E6 解散，且本公司最大營收線是半導體材料 67.80%，Core 依 R023⑤ 移至 A1-12', 'Direct'),
        ('友輝', 'Core', None, 'annual_report_prose', '年報營業比重表僅分內外銷（增光膜 內銷 60.71% + 外銷 39.29% = 100%），非產品別拆分故不計比重(R020②)。年報商品項目：「本公司目前主要生產之產品為增光膜及保護膜，增光膜主要應用於液晶顯示器之背光模組中⋯每個液晶顯示器中都需有一到二片的增光膜-稜鏡片，故增光膜是背光模組中非常重要關鍵材料」。★residual 掃描所得：status=no_section，庫內無 2025 營收線★2026-09-09 weight_status=Not_Disclosed：年報**有**比重表，但軸是內銷 60.71／外銷 39.29——地區別不是產品別。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('誠美材', 'Core', None, 'annual_report_prose', '年報營業比重表：偏光板 99.59%（庫內無營收線，數值取自年報正文，不計比重）。業務範圍首句「本公司所營業務主要以液晶顯示器上游光學膜類材料與元件為出發點，並專注在重要光學元件「偏光板」為領域」；產業概況載明「偏光板是液晶面板上游零組件的重要材料，每片液晶面板需要使用上下共 2 片偏光板」。產品涵蓋 TV／IT／中小尺寸超薄型／OLED 用圓偏光板與車用高耐久偏光板。★residual 掃描所得：status=no_section★2026-09-09 weight_status=Not_Extracted：年報 @265「偏光板…8,607,565 99.59 其他 化學品等 35,625 0.41」。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
        ('達輝光電', 'Core', None, 'annual_report_prose', '年報所營業務列「研究、設計、製造及銷售：1. TAC 膜 2. LCD 用光學補償膜」，產銷量值表僅有 TAC 膜一項。年報自述「TAC(Tri-Acetyl Cellulose)膜材為偏光板之核心保護膜材料。每片偏光板通常需使用 2 至 4 片 TAC 膜」，兩條產線分別生產 TAC 保護膜與代工 cellulose base 光學補償膜。庫內無 2025 營收線故不計比重(R020②)。★residual 掃描所得：status=no_section。★本列與友輝、誠美材同屬「面板用光學膜材」，見本節點定義所記之材料／零組件內部分界 backlog——該 backlog 的母體因本輪擴大★2026-09-09 weight_status=Not_Disclosed：年報以 TAC 膜的產銷量值表代替比重表，單一產品無從算出比重。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('達運', 'Major', '光電科技產品及週邊零組件', 'annual_report_table', '年報營業比重表：光電科技產品及週邊零組件 23.43%。年報自述本公司「生產包含液晶電視、個人顯示器、筆記型電腦、車用顯示器等之專業**背光模組**產品」，計畫開發之新技術首列 MNT 高效率導光板(LGP)、車用曲面導光板、車用 Local Dimming Mini-LED 背光模組', 'Direct'),
        ('臺龍', 'Core', 'LED光源條 + 背光模組', 'annual_report_table', '年報營業比重表：LED 光源條 55%、背光模組 45%，兩條合計 100.00%(R020④)。年報商品項目載明「公司主要產品為背光模組、LCM模組、觸控整合LCM 模組、LED Light bar及電子主機板等產品自製與加工,係應用於液晶電視、筆記型電腦、小筆電、平板電腦、液晶監視器、工業控制及車載等產品之客製化產品」；本節點定義具名「LED 背光模組與光源條」。★2026-09-08 裁定 Display Backlight／LED light bar 歸本節點；2026-10-07 補寫。商品項目另列 LCM 模組與觸控整合 LCM 模組（屬 E2-01）、電子主機板，營業比重表未單獨列示，未另掛', 'Direct'),
    ],
    "Z1": [
        ('單井', 'Major', '光電太陽能類', 'annual_report_table', '年報營業比重表：光電太陽能類 32.00%，年報自述「維運屋頂型太陽能光伏發電系統,售電給供電局」，屬售電而非太陽能製造', 'Direct'),
        ('強茂', 'Minor', '太陽能', 'annual_report_table', '年報營運部門銷售表：太陽能 1.53%，該部門所營業務為「太陽能電站電力之銷售」，屬售電而非太陽能製造', 'Direct'),
    ],
    "F2-01": [
        ('正淩', 'Core', '連接器', 'annual_report_table', '比重表「連接器」62%，年報自述「本公司專精於PICMG 及SFF標準規範之高頻及光通訊連接器以及各式客製化連接器」，F2-01 明列 High Speed、Automotive 等連接器。', 'Direct'),
        ('信音', 'Core', '電子連接器', 'annual_report_table', '年報營業比重表「電子連接器 3,836,937 98.39」，年報自述研發製造銷售「各種電子連接產品是作為各類電源、影音訊號、電子訊號傳遞的」連接裝置。', 'Direct'),
        ('宏致', 'Core', '連接器', 'annual_report_table', '年報營業比重表：連接器 61.62%，年報自述「主要產品為連接器、極細同軸線連接線組、」等。Core 理由：營業比重最高(61.62%)且為比重表與所營業務首列。', 'Direct'),
        ('凡甲', 'Core', '連接器', 'annual_report_table', '年報營業比重表：連接器 100.00%，年報自述業務為「電子連接器、連接線及其零組件之研發、生產、銷售」。', 'Direct'),
        ('百容', 'Major', '電子零組件', 'annual_report_table', '年報商品項目逐線載「(2)電子零組件:(a)程式開關(b)端子台(c)連接器」，其中連接器與端子台屬本節點（端子台為導線與電路板或導線之間的電氣連接件，屬定義所列 Wire-to-Board；2026-10-23 裁定，不依年報把它與開關、繼電器並列為「機構元件」的分組）；同線程式開關屬 F5-01，年報未拆分，依 R020 ②-MX，完整混合（MX-FULL），不計比重。', 'Direct', 'Too_Coarse'),
        ('中探針', 'Major', None, 'annual_report_prose', '年報所營業務首列「POGO PIN 連接器(spring loaded connector)製造加工買賣」與「大電流端子與大電流連接器製造加工買賣」，年報載明 POGO PIN 已應用至 TWS、智慧手機、智慧手錶、筆記型電腦等，大電流連接器已應用至電動汽機車與充電槍/樁；併於探針 69.76% 揭露無法拆分故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "F5-01": [
        ('新巨', 'Core', '微動開關', 'annual_report_table', '年報營業比重表微動開關 53.12%，年報列「微動開關、交換式電源供應器、其他等。」，微動開關屬 F5-01 Switch，使用者先前已裁定成立。Core 理由：營業比重最高（53.12%）且為年報產品首列。', 'Direct'),
        ('國巨*', 'Minor', '感測器', 'annual_report_table', '年報營業比重表：感測器 9.00%；商品項目列「(5)感測器」，為溫度與工業感測器，屬本節點 Sensor。', 'Direct'),
        ('松川精密', 'Core', '新能源用繼電器 + 通用繼電器 + 車用繼電器', 'annual_report_table', '年報營業比重表三條合計 100%：新能源用繼電器 57.03、通用繼電器 22.12、車用繼電器 20.85(R020④)；年報業務範圍自述「本公司主要從事繼電器之研發、製造及銷售」，同一產品類別依 R004 只建一列。', 'Direct'),
        ('百容', 'Major', '繼電器 + 電子零組件', 'annual_report_table', '年報營業比重表繼電器 13% 一線（商品項目「(3)繼電器」）屬本節點 Relay；另「(2)電子零組件:(a)程式開關(b)端子台(c)連接器」一線中的程式開關屬本節點 Switch（端子台與連接器屬 F2-01），該線依 R020 ②-MX，完整混合（MX-FULL）；依 R020④ 本節點不得給部分和，整列不計比重；role 依 R026 預設 Major。', 'Direct', 'Too_Coarse'),
        ('科嘉-KY', 'Core', '薄膜觸控開關', 'annual_report_table', '年報營業比重表：薄膜觸控開關 69.08%，為最大營收線。年報所營業務自述「本公司及其子公司主要從事筆記型電腦鍵盤薄膜觸控開關(以下簡稱「MTS」)之製造及銷售等業務」——鍵盤用薄膜開關，屬本節點的 Switch。★2026-09-07 顯示觸控母體逐家裁定時判出：本公司被「觸控」二字帶進 E 分支候選，但產品是鍵盤輸入零件而非顯示或觸控模組。★2026-10-07 補寫：裁定當日未寫入本檔。另有銘板及其他 30.92% 未另掛', 'Direct'),
    ],
    "J16": [
        ('永光', 'Minor', '碳 粉 產 品', 'annual_report_table', '年報營業比重表：碳粉產品 12.90%，致股東報告書載明「碳粉事業」，提供碳粉與卡匣整合服務', 'Direct'),
    ],
    "G4-03": [
        ('恩德', 'Major', '機械', 'annual_report_table', '年報商品項目「機械」線載「曲面磨床及相關刀具、零配件」，主要產品用途載為「提供上述機械各項功能使用時所需之平刀及型刀」，屬本節點定義之切削刀具（非 PCB 鑽針，不屬 B6-01）；自製依據為短期計劃載「本公司預計將刀具生產移往」「大陸,由子公司生產,原刀具廠廠房則做為精密機械及電子機械零組件加工」（原文跨行）(R022)，所營業務另有「各種機械零件及工具之買賣業務」，年報未拆分刀具中自製與買賣的部分，本列不推定全部為自製。同線另含 G5、B5-01、B5-02 產品與全樹無節點之零配件，年報未拆分，依 R020 ②-MX，含未定組成的混合（MX-OPEN），不計比重；role 依 R026 預設 Major。', 'Direct', 'Granularity_Insufficient'),
        ('中砂', 'Major', '砂輪及刀具製品', 'annual_report_table', '年報營業比重表：砂輪及刀具製品 23.42%，主要磨料為氧化鋁及碳化矽，屬工業研磨市場。★2026-09-08 由 G4-01 移入：G4-01 原本同時裝了傳動與工具兩個功能，本輪拆為 G4-01 傳動與線性運動與 G4-03 切削與研磨工具，本列屬後者。role 與 weight 均不變(R026)；Core 仍在 A1-02，不因本次移動而變', 'Direct'),
        ('銓寶', 'Minor', '精密元件(鑽夾頭、刀桿)', 'annual_report_table', '年報營業比重表：精密元件(鑽夾頭、刀桿) 13%。年報載「本集團之精密鑽夾頭、刀桿及主軸配件,主要應用在綜合加工機、銑床、鑽床與電動手工具等設備上」，並稱自創業初期即「投入精密鑽夾頭應用領域的研發製造」——把切削工具固定於主軸與工具機上的工具夾持件，屬本節點（2026-10-07 增訂之工具夾持條款）。敘述標題另列主軸配件，營業比重表該線僅標鑽夾頭、刀桿。★2026-10-07 掛入', 'Direct'),
    ],
    "A4-26": [
        ('信驊', 'Core', '多媒體積體電路', 'annual_report_table', '年報營業比重表：多媒體積體電路 96.32%，內容為伺服器基板管理控制晶片(BMC)、遠端 KVM 控制器與平台信任根(PRoT)。原掛 A4-10 網通 IC 係關鍵字誤中機櫃設計敘述中的「網通」二字', 'Direct'),
        ('新唐', 'Major', None, 'annual_report_prose', '年報商品項目 7)伺服器遠端管理控制晶片；產業概況載明雲端運算產品「包括:伺服器專用之遠端管理控制器(Baseboard Management Controller)」', 'Direct', 'Too_Coarse'),
    ],
    "A4-24": [
        ('威盛', 'Core', None, 'annual_report_prose', '年報自述「威盛長期深耕 x86 處理器與嵌入式運算平台技術」；該業務併於「中央處理器、智慧車載、邊緣 AI、工業及智慧建築解決方案等業務,約占集團總營收 56%」一線揭露，橫跨晶片與系統方案無法拆分故不計比重(R020②)。Core 理由：年報自述集團「逐步建構涵蓋『x86 運算平台、AI 邊緣智能系統、高速連接技術與先進 IC 設計服務』之完整技術生態系」，x86 運算平台列於四者之首，且其餘三者(邊緣智能系統、威鋒之高速連接、威宏之設計服務)皆建立在該平台之上，價值創造的源頭在 x86 與嵌入式處理器(R023①②)。★不採營收排序：56% 那條線本身已宣告不可拆，與威宏的 28% 不具可比性。★未決：56% 線的主體是 Mobile360 車載系統、Firewatch 365、4D+ 數位孿生等系統級方案而非晶片，樹上對應的 G2 工業電腦/H3-03 Telematics 皆無 Product Definition 且零成員，補格後須重跑本公司裁定', 'Direct', 'Too_Coarse'),
    ],
    # "E5-01" went with the node on 2026-09-08 when E5 LED left the display
    # branch. 8111 立碁's evidence stands and is not withdrawn:
    #   年報營業比重表 LED模組產品 55.55%；商品項目為室內外 LED 照明模組、
    #   傳統與超高亮發光二極體燈。
    # That is a lighting company. It was on A3-04 功率半導體製造 before, by a
    # keyword hitting 「傳統發光二極體燈」, and it went to E5-01 next because
    # E5 was the only cell with LED in its name -- twice placed by a word rather
    # than by what it makes. It is now taxonomy-gap pending: 照明模組 has no node
    # anywhere in the tree, and 「LED」 is not a reason to keep it under a display
    # parent.
    #
    # The wider gap this opens, recorded here because this is where the evidence
    # lives. Observed candidates only -- the narrative sweep found five more that
    # revenue-item search cannot see, so the true population is larger:
    #   LED 封裝／發射元件：2393 億光 LED 94.16（用途欄橫跨背光、號誌、滑鼠、
    #     家電、感測、紅外）、3031 佰鴻 89.00（可見光 Lamp/Display/SMD/PLCC/
    #     Light Bar/照明燈具 + 不可見光 Infrared/Sensor Assembly）、
    #     3437 榮創 SMD LED 97.07（自述涵蓋背光、照明、感測穿戴、車用）、
    #     6226 光鼎 100.00（年報自我定位為「光電產業之光電元件」）、
    #     6164 華興 96.36、3609 三一東林 29.49、8064 東捷 21.07；
    #     敘述層另有 3591 艾笛森、3717 聯嘉投控、6168 宏齊、6654 天正國際、6278 台表科
    #   不可見光 (IR／UV)：6559 研晶 不可見光LED 90.72
    #   照明模組：8111 立碁 55.55
    # 這三類全樹皆無節點。★不得以單一公司或單一群體反推新建節點——
    # 每一類都要先做 population audit 並證明其邊界可互斥、可重複套用(R025)。
    #
    # Not in this gap, because they already have somewhere true to go:
    #   車燈模組 2248 華勝 80.98、2254 巨鎧 90.40、3346 麗清 64.44 → H 汽車
    #   顯示專用背光 3531 先益 87.60、6246 臺龍 → E2-02，其中先益已在位
    # "E3" went with the node on 2026-09-07. 6854 錼創科技-KY創's evidence is not
    # withdrawn -- re-reading the filing made it stronger than it was:
    #   業務範圍首句「本公司主要從事微發光二極體(MicroLED)顯示器之研發、製造及銷售」
    #   商品項目 (1) COC (chip on carrier)：6" Red／Green／Blue MicroLED COC
    #            (4) PixeLED Display／PixeLED Matrix／μ-PixeLED MicroLED 顯示器
    #   計畫開發之新商品首項：車用透明顯示器 8~12 吋
    # ①④⑤ all say MicroLED display maker, and the older A3-06 化合物半導體製造
    # attribution was a misreading of the annual report's 第三代半導體 industry
    # overview rather than of its own products.
    #
    # What it has no longer is a node. E's parent scope is unruled, so no display
    # product node exists to hold it, and an exposure cannot be recorded against
    # a node that does not exist (company_industry_exposure.node_code and
    # core_reassessment.trigger_node both carry foreign keys onto industry_tree).
    # It goes to taxonomy-gap pending -- the 9950 萬國通 disposition -- and comes
    # back when E's scope is ruled. It MUST NOT be routed to E5 by elimination:
    # 「Micro LED 這格沒了所以回 LED」 is the inference the X round ruled out.
    #
    # Data note found in the same re-read: the 2025 table holds COC 36.74% +
    # 其他 73.26% = 110.00%. The filing's own figures are 496,674 and 855,144 on
    # a total of 1,351,818, so 其他 is 63.26% and the annual report printed one
    # digit wrong -- its 2024 column sums to 100 correctly. Recorded rather than
    # silently corrected: tw.product_revenue_share still holds what the source
    # says, and the row carries weight_status Too_Coarse either way (其他 is unnamed).
    "C6-01": [
        ('邁科', 'Core', '散熱模組', 'annual_report_table', '年報營業比重表：散熱模組 99.95%；商品項目自述「本公司主要產品為散熱模組。」，內容含風扇、散熱鰭片、熱導管、均溫板與水冷頭，對應 C6-01 的 Air Cooling／Liquid Cooling／Cold Plate／Fan／TIM。', 'Direct'),
        ('華旭先進', 'Core', '散熱管材', 'annual_report_table', '年報營業比重表：散熱管材 73.03%（前一年度為 0），年報自述「主要為液冷散熱關鍵元件,應用於伺服器散熱系統」。Core 理由：73.03% 遠大於晶圓 22.27%，且產能與研發已移向液冷散熱(R023①③)', 'Direct'),
        ('聚鼎', 'Major', '散熱基板', 'annual_report_table', '年報營業比重表：散熱基板 35.55%，另有散熱模組與散熱材料', 'Direct'),
    ],
    "F1-01": [
        ('禾伸堂', 'Core', '被動元件', 'annual_report_table', '比重表「被動元件」42%，年報產業段標示為「被動元件(自製產品)」，並自述「深耕積層陶瓷電容(MLCC)之研發與生產」，F1-01 明列 MLCC。Core 理由：營收占比最高（42%）且為年報唯一標明自製的產品線。', 'Direct'),
        ('三集瑞-KY', 'Core', '一體成型式功率電感之PC產品(註1) + 一體成型式功率電感之其他產品(註2) + 組裝式功率電感之其他產品(註3) + 組裝式功率電感之主機板', 'annual_report_table', '年報營業比重表四條合計 100%，皆為功率電感（一體成型與組裝式），年報自述「營收組成完全來自電感產品」，F1-01 定義明列電感。', 'Direct'),
        ('鈺邦', 'Core', '捲繞型導電高分子固態電容器 + 晶片型導電高分子固態電容器', 'annual_report_table', '年報營業比重表捲繞型 68.05% 加晶片型 31.95% 合計 100%，年報自述「本公司主要以研發及銷售固態電容為主」。', 'Direct'),
        ('光頡', 'Core', '精密電阻 + 一般電阻 + 高頻電感', 'annual_report_table', '年報營業比重表精密電阻 53.36%、一般電阻 37.40%、高頻電感 5.64% 合計 96.40%，所營業務列「薄膜精密電阻(Thin Film Precision Resistor)」與「高頻電感,功率電感 (RF Inductors, Power Inductor)」，皆為自製被動元件。', 'Direct'),
        ('大毅', 'Core', '晶片電阻', 'annual_report_table', '年報營業比重表「晶片電阻 4,819,703 88.86%」，年報自述「本集團主要產品厚膜晶片電阻(Thick Film Chip Resistor)及厚膜晶片排阻」。', 'Direct'),
        ('立隆電', 'Core', '鋁電解電容器', 'annual_report_table', '年報營業比重表「鋁電解電容器 6,630,107 63.2% 7,541,861 66.8%」，年報自述「鋁質電解電容器、鋁箔及電子材料之製造與買賣業務」，電容器為自製主力產品。', 'Direct'),
        ('迅德', 'Core', '變壓器', 'annual_report_table', '年報營業比重表變壓器 94%，年報自述產品「磁性元件中的兩大項目變壓器及電感器」，F1-01 定義含電感與磁性元件，公司自製加工而非通路。Core 理由：營業比重最高（94%）且年報自述主業為變壓器及電感器。', 'Direct'),
        ('百徽', 'Core', '標準電子元件 + 客製化磁性元件 + 變壓器', 'annual_report_table', '年報營業比重表：標準電子元件 54.00%、客製化磁性元件 44.55%、變壓器 0.01%，合計 98.56%；商品項目列「電磁波防制與靜電/突波保護元件之生產與銷售」與「電源相關磁性繞線元件之生產與銷售」。', 'Direct'),
        ('凱美', 'Core', '電阻 + 電解電容器', 'annual_report_table', '年報所營業務含「鋁質電解電容器之製造、加工、裝配與買賣」與「電阻器之製造加工與買賣」，兩條線合計 89.62% 皆為自製被動元件。', 'Direct'),
        ('金山電', 'Core', '液態電解電容器 + 固態電解電容器', 'annual_report_table', '年報所營業務為「各種電子器材電容器之製造加工買賣及進出口業務」，液態 75.42%＋固態 24.58% 為同一電容器自製產品。', 'Direct'),
        ('天二科技', 'Core', '厚膜電阻 + 金屬板電阻 + 薄膜電阻', 'annual_report_table', '年報自述「本公司為專業晶片電阻製造商」，三條營收線 39.03%＋34.55%＋25.97% 同為自製晶片電阻。', 'Direct'),
        ('華容', 'Core', '塑膠薄膜電容器', 'annual_report_table', '年報營業比重表：塑膠薄膜電容器 85.31%，年報自述「本公司為專業塑膠薄膜電容器製造廠」。', 'Direct'),
        ('國巨*', 'Core', '磁性元件 + 鉭質電容 + 積層陶瓷電容 + 晶片電阻', 'annual_report_table', '年報營業比重表：磁性元件 27、鉭質電容 22、積層陶瓷電容 19、晶片電阻 14，合計 82%（R020④）；所營業務為「主要從事於被動元件與電子零組件,及其生產設備之研發、生產及銷售」。', 'Direct'),
        ('千如', 'Core', '電感器', 'annual_report_table', '年報營業比重表：電感器 97.48%；所營業務含「晶片線圈、變壓器及其磁鐵心」，屬電感與磁性元件。', 'Direct'),
        ('聚鼎', 'Core', '保護元件', 'annual_report_table', '年報營業比重表：保護元件 64.45%（PPTC/OVP/CLM），年報自述「PPTC 與 OVP 元件係分類於電子零組件業中之電阻器產業」。原掛 A1-14 陶瓷基板，係關鍵字誤中——年報該句講的是 CLM 模組的投入材料，不是聚鼎的產品。Core 理由：①年報所經營業務順序為「(1)**保護元件**：包括過電流保護(OCP)及過電壓保護(OVP)…(2)散熱基板、散熱模組及散熱材料」，保護元件列首；商品項目首句「本公司**專注於保護元件**與散熱材料之研究、開發、製造及銷售」；④計畫開發首句為「**PPTC 產品平台**」；⑤本格 64.45% 大於 C6-01 散熱 35.55%。三項同向', 'Direct'),
        ('台聚', 'Minor', '電子材料', 'annual_report_table', '年報營業比重表：電子材料 6.00%；所營業務「(27)錳鋅及鎳鋅軟性鐵氧磁粉、磁鐵芯、碳化矽粉,以及其他相關電感零組件之製造及銷售業務」，產品項目列「(16)錳鋅軟性鐵氧磁鐵粉」「(18)錳鋅軟性鐵氧磁鐵芯」，計畫開發之新產品第 44～54 項全為鐵芯開發。本節點定義即含磁性元件', 'Direct'),
    ],
    # X9 居家 / 生活用品 was removed from the tree on 2026-09-07 together with the
    # other product verticals (see X in db/industry_tree_data.py), and its three
    # memberships are retracted with it. None of the three was ever a retailer,
    # which is why this was the node that exposed the branch:
    #
    #   9950 萬國通  行李箱 94.17% (annual_report_table)  -- was Core, and X9 was
    #     its only exposure. A luggage maker is a manufacturer; its value-chain
    #     role is on the production side, and the tree has no consumer-goods
    #     manufacturing node. Per the 2026-09-07 ruling this is an R025 taxonomy
    #     gap with mapping pending, NOT a company that cannot be classified: the
    #     revenue evidence stays in tw.product_revenue_share, and the company
    #     joins the 34 others (36 lines >= 20%: footwear, bicycles, furniture,
    #     golf and fitness, pet supplies) that already sit outside the classified
    #     universe for exactly this reason. One company and one line do not
    #     justify a new branch -- that is the mistake B2 and A5 made in reverse.
    #
    #   6509 聚和  辦公文具用品 22.00% -- was Major; Core stays on J4-01. Whether
    #     the self-branded stationery line is manufacturing, brand or channel is
    #     unadjudicated, and it must not be re-homed by default just because a
    #     node was removed underneath it.
    #
    #   1315 達新  鋼鐵製品製造加工 3.28% (main product 傢俱用品) -- was Minor;
    #     Core stays on K2. Same treatment: the furniture line needs its own
    #     value-chain judgement, not an automatic move.
    #
    # 6509 and 1315 therefore lose 22.00pp and 3.28pp of assigned weight, which
    # will show up in [coverage]. That is the honest state, not a regression.
    "M7": [
        ('恆大', 'Core', '紡結與複合不織布 + 加工品 + 熔噴不織布 + 材料加工', 'annual_report_table', '年報營業比重表：紡結與複合不織布 87.82% 加加工品 9.51% 加熔噴不織布 0.50% 加材料加工 0.02%，本節點定義即為「紡黏、熔噴、複合不織布」，合計 97.85%(R020④)。Core 理由：全部營收皆在本節點(R023⑤)', 'Direct'),
    ],
    "K6": [
        ('南港', 'Core', '輪胎-汽車', 'annual_report_table', '年報營業比重表：汽車輪胎 98.99%', 'Direct'),
        ('泰豐', 'Core', '輪胎', 'annual_report_table', '年報營業比重表：輪胎 91.00%', 'Direct'),
        ('正新', 'Core', '自行車外胎 + 內胎 + 機車外胎 + 輻射層汽車外胎 + 其他輪胎', 'annual_report_table', '年報營業比重表：五條輪胎線合計 98.86%', 'Direct'),
        ('華豐', 'Core', '機車胎 + 自行車胎 + 農工業用胎 + 卡汽車胎 + 輻射層外胎', 'annual_report_table', '年報營業比重表：機車胎 55.06% 加自行車胎 22.23% 加農工業用胎 20.75% 加卡汽車胎 1.40% 加輻射層外胎 0.39%，本節點定義即含汽車胎、卡車胎、工程胎、機車胎，合計 99.83%(R020④)。Core 理由：年報自述「公司銷售產品項目以外胎為主,約占營收比重 99%」，全部營收皆在本節點(R023①⑤)', 'Direct'),
        ('建大', 'Core', None, 'annual_report_table', '年報僅列「各種橡塑膠製品之製造加工及批發與零售 83%」，未拆產品線；建大為輪胎製造廠', 'Direct', 'Too_Coarse'),
    ],
    "K5": [
        ('鑫永銓', 'Core', '橡膠產品', 'annual_report_table', '年報營業比重表：橡膠產品 98.00%，主力為輸送帶', 'Direct'),
        ('厚生', 'Core', '橡膠膠皮 + 綠塑膠皮 + 塑膠膠皮', 'annual_report_table', '年報營業比重表：橡膠膠皮 37% 加綠塑膠皮 10% 加塑膠膠皮 10%；另有倉儲 23% 與營建 18%', 'Direct'),
    ],
    "K2": [
        ('三芳', 'Core', '環保合成皮革 + PU合成皮革 + 薄膜', 'annual_report_table', '年報營業比重表：PU合成皮革 40.70% 加環保合成皮革 49.00% 加薄膜 5.10%，合計 94.80%(R020④)。★三個數字於 2026-09-07 更正：09-04 重新解析年報後庫內品項已改，但本句仍留著舊值 44.27／39.79／9.30', 'Direct'),
        ('富林-KY', 'Core', '乳膠皮 + PU/TPU + 柔軟皮 + 膠布', 'annual_report_table', '年報營業比重表：四條合成皮與膠布線合計 99.39%', 'Direct'),
        ('信立', 'Core', '乾式PU合成皮', 'annual_report_table', '年報營業比重表：乾式 PU 合成皮 52.00%', 'Direct'),
        ('達新', 'Core', '塑膠製品製造加工', 'annual_report_table', '年報營業比重表：塑膠製品製造加工 67.06%。Core 理由：①年報「目前之商品(服務)項目」順序為 **雨衣**、成衣、塑膠什項加工、傢俱用品、衣櫥、裝訂機、護貝機、PP瓦楞板——雨衣（PVC 塑膠製品）列首而成衣次之；所營業務亦以 (2)塑膠皮布板管材 (3)塑膠膜袋 (4)塑膠日用品 三條並列於成衣之後但同為塑膠加工；⑤本格 67.06% 大於 M5 成衣 22.32%', 'Direct'),
        ('再生-KY', 'Core', '箱包片材 + 普通片材 + 高彈性發泡 + 地墊', 'annual_report_table', '年報營業比重表：四條塑膠片材線合計 84.76%', 'Direct'),
        ('勝昱', 'Core', '鋁箔麥拉帶 + 塑膠膜 + 積層膜', 'annual_report_table', '年報營業比重表：鋁箔麥拉帶 59% 加塑膠膜 11% 加積層膜 9%', 'Direct'),
        ('世坤', 'Core', '一般塑膠布 + 軟質加壓塑膠布 + 貼合塑膠布 + 半硬質半硬質塑膠布', 'annual_report_table', '年報營業比重表：一般塑膠布 71.52%、軟質加壓塑膠布 9.33%、貼合塑膠布 5.43%、半硬質塑膠布 13.72%（其他 0.00%），四者同為 PVC 塑膠布加工製品，合計 100.00%。★前版評語稱「產品為聚氨酯發泡材」，年報全文無「聚氨酯」一詞，該敘述已刪；前版比重只取兩列（72%＋14%）是因解析漏列，非判定排除', 'Direct'),
        ('炎洲', 'Core', '膠帶 + 包材 + 薄膜', 'annual_report_table', '年報營業比重表：膠帶 64% 加包材 14% 加薄膜 7%，合計 85.00%(R020④)。年報「(3)公司目前之產品 (A)BOPP薄膜、PVC軟質薄膜、PE薄膜」，並自述「炎洲從 PE 製膜到上膠以完全自製且一貫化作業模式確保產品的品質」——薄膜與膠帶垂直整合、同屬本節點。Core 理由：塑膠加工三線占營收 85%(R023⑤)', 'Direct'),
        ('永裕', 'Core', '製瓶廠製品 + 射出廠製品 + 軟管廠製品 + 其他製品', 'annual_report_table', '年報營業比重表：四條塑膠容器線合計 99.60%', 'Direct'),
        ('大洋', 'Major', '塑膠布 + 塑膠建材 + 合成皮', 'annual_report_table', '年報營業比重表：塑膠布 23.52% 加塑膠建材 22.71% 加合成皮 5.13%', 'Direct'),
        ('華夏', 'Major', 'PVC建材產品:塑膠管、管件、塑膠門板、防蝕片 + 塑膠布:軟質膠布、硬質膠布、半硬質膠布 + 塑膠皮、塑膠軟皮', 'annual_report_table', '年報主要產品比重表列 PVC 建材產品(塑膠管、管件、塑膠門板、防蝕片) 7%、塑膠布(軟質/硬質/半硬質膠布) 8%、塑膠皮及塑膠軟皮 8%，三者同為自製 PVC 加工製品，合計 23%(R020④)', 'Direct'),
    ],
    "K1-01": [
        ('大洋', 'Core', '塑膠原料', 'annual_report_table', '年報營業比重表：塑膠原料 48.64%，為加工用複合料而非上游塑化原料。Core 理由：這是 2.7pp 的近距離判定，且①內部不一致，理由必須寫明。①證據改引（2026-09-07）：先前引的「1.主要營業內容」其實是**章程所營事業登記清單**（同段續列住宅及大樓開發租售、百貨公司業、超級市場業），不是業務自述。真正的自述句在下一段：「2.營業比重：**本公司主要從事塑膠原料及其製品之製造及銷售**，有關各項業務營業比重如下」——原料列於製品之前；但同一年報「目前商品」的事業部排列為 加工事業部 → 合皮事業部 → 建材事業部 → **原料事業部**，原料列末，①內部仍不一致。⑤ K2 塑膠加工 51.36% 略大於本格 48.64%。採自述句（對業務性質的直接陳述）而非事業部編排順序，故 Core 維持 K1-01。★若日後年報拆分或自述改變，本裁定須重跑', 'Direct'),
    ],
    "J15": [
        ('國際中橡', 'Core', '碳黑', 'annual_report_table', '年報營業比重表：碳黑 98.35%', 'Direct'),
    ],
    "J3": [
        ('台橡', 'Core', '合成橡膠', 'annual_report_table', '年報營業比重表：合成橡膠 98.18%，產品為 SBR/BR 等合成橡膠原料', 'Direct'),
        ('南帝', 'Core', 'SBR合成橡膠乳液(乳膠) + NBR合成橡膠(耐油膠)', 'annual_report_table', '年報營業比重表：SBR 乳膠 73% 加 NBR 耐油膠 22%', 'Direct'),
        ('申豐', 'Core', '醫材及其他用途之合成乳膠原料', 'annual_report_table', '年報營業比重表：合成乳膠原料 51.00%', 'Direct'),
    ],
    "J14": [
        ('花仙子', 'Core', '驅塵潔淨類 + 芳香消臭類 + 除濕類', 'annual_report_table', '年報營業比重表：驅塵潔淨 55% 加芳香消臭 28% 加除濕 17%', 'Direct'),
        ('毛寶', 'Core', '家 庭 清 潔 系 列 + 洗 衣 劑 系 列', 'annual_report_table', '年報營業比重表：家庭清潔 54% 加洗衣劑 42%', 'Direct'),
        ('興農', 'Major', None, 'annual_report_table', '年報營業比重表：家用品.團膳 16%。該線橫跨本節點(家用化學品)與 Y4 餐飲(團體膳食供應)，年報未拆分故兩格皆不計比重(R020②)。★原以整條 16% 掛本節點，等於把餐飲營收算成日化營收', 'Direct', 'Too_Coarse'),
    ],
    "J13": [
        ('中碳', 'Core', '煤焦油及化學品系列產品 + 焦炭系列產品 + 精碳材料系列產品', 'annual_report_table', '年報營業比重表：煤焦油及化學品 60% 加焦炭 7% 加精碳材料 7%；另有輕油及油品 26%', 'Direct'),
    ],
    "J12": [
        ('興農', 'Core', '植 物 保 護', 'annual_report_table', '年報營業比重表：植物保護 56.00%；另有超市百貨 23% 屬零售，未收錄。Core 理由：①年報「業務主要內容」原文即「**農藥**、肥料、微生物農藥、微生物肥料、化學品等」，農藥列首；商品項目順序亦為 農藥、肥料 → 微生物農藥肥料 → 農業資材 → 農藥原體；④計畫開發首項為「新劑型農藥」；⑤本格 56.00% 為最大營收線。四項同向', 'Direct'),
        ('國慶科技', 'Major', '植物保護用藥', 'annual_report_table', '年報營業比重表：植物保護用藥 27.00%', 'Direct'),
        ('康普', 'Minor', '肥料', 'annual_report_table', '年報營業比重表：肥料 7.00%', 'Direct'),
        ('台肥', 'Core', None, 'annual_report_prose', '年報無營業比重表；台灣肥料公司為國內最大肥料製造商，主業為化學肥料與複合肥料。Core 理由：①年報業務範圍首句即「台肥公司以三項成熟事業「**肥料**、化工、不動產」及新興事業「乾淨能源、深層海水、投資」為發展主軸」，肥料列於三項成熟事業之首；候選四格（本格、J10 無機化學品、J8 電子化學、Q3）之權重皆為 NULL（年報比重表未拆），⑤不可用，故裁定完全依①。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報業務範圍首句「台肥公司以三項成熟事業『肥料、化工、不動產』⋯為發展主軸」，肥料事業列首——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Disclosed：業務內容章完整，營業比重／營業比率／營收比重／主要產品四詞零命中（唯一的「比率」是研發費用占營業額）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('東鹼', 'Core', None, 'annual_report_prose', '年報經營項目「A. 肥料(硫酸鉀)及其附屬產品之生產及銷售,包括硫酸鉀、鹽酸、液體氯化鈣、小蘇打」；年報自述「兩大主力產品線 - A.硫酸鉀 B.純碱」，並以「A. 硫酸鉀是一種無機鹽,化學式為K2SO4,為高硫、高鉀及低氯之化合物…是優質高效單質鉀肥」開啟整段產業概況。本節點定義含肥料。併於「製造-化工類產品 63.76%」一列且與 J10 共用故不計比重(R020②)。Core 理由：硫酸鉀為製造線頭號產品、公司自述兩大主力之首，且年報把它列為肥料業務(R023①②)；純鹼雖為另一主力但屬貿易線非自製', 'Direct', 'Too_Coarse'),
        ('和益', 'Minor', None, 'annual_report_prose', '年報註 3「其他部門之產品主要包括產製銷售農藥產品及糖業貿易等業務」；商品項目「(5)其他:主要為銷售農藥產品及糖業貿易等業務」。年報以「產製銷售」形容農藥故為 Direct，但同一條「農藥及其他 6.67%」線另含糖業貿易(R022 通路性質)，無法拆分故不計比重(R020②)。role 理由：和益的主要經營活動為烷化部門，單一部門即為年報壓倒性最大的一條營收線；農藥被併入年報最小的「農藥及其他」一欄，該欄另含糖業貿易且公司未給拆分依據，農藥既非烷化本業的延伸也未被單獨列示，相對於主要經營活動屬附帶之經濟地位，故列 Minor。該欄合計 6.67% 僅作佐證', 'Direct', 'Too_Coarse'),
    ],
    "J11": [
        ('德淵', 'Core', '熱熔膠 + 其他膠', 'annual_report_table', '年報營業比重表：熱熔膠 74% 加其他膠 16%', 'Direct'),
        ('南寶', 'Core', '接著劑', 'annual_report_table', '年報營業比重表：接著劑 70.00%。Core 理由：①年報商品項目僅兩類且順序為 (1)**接著劑**—主係應用於運動鞋材及服飾 (2)塗料；④計畫開發表列首項為接著劑類「NIKE Mini Foam 3.0水性處理劑」；⑤本格 69.62% 遠大於 J6 塗料與油墨 7.64%。三項同向', 'Direct'),
        ('誠泰科技', 'Core', '熱熔膠接著劑', 'annual_report_table', '年報營業比重表：熱熔膠接著劑 92.00%', 'Direct'),
        ('日勝化', 'Minor', None, 'annual_report_prose', '年報「3. 公司目前之商品項目:…M. 食品包裝複合膜接著劑 N. 反應型PU熱熔膠」，屬現行商品非計畫開發；最近開發成功之技術另列「H. 應用於系統廚具平貼的溼氣固化熱熔膠」。本節點定義首項即熱熔膠。該等產品與建材塗料、UV 單體/寡聚物、TPU 膠粒同被併進「其他產品 20%」，無法拆分故不計比重(R020②)。role 理由：日勝化的主要經營活動為 PU 合成樹脂與 PE 多元醇樹脂，兩者是年報單獨列示的產品線；熱熔膠與接著劑未被單獨列示，而是與建材塗料、UV 單體／寡聚物、TPU 膠粒同被併進「其他產品」一欄，相對於樹脂本業屬附帶之經濟地位，故列 Minor。該欄合計 20% 僅作佐證', 'Direct', 'Too_Coarse'),
    ],
    "J10": [
        ('中華化', 'Core', '基礎化學品', 'annual_report_table', '年報營業比重表：基礎化學品 45.76%。年報商品項目表在「基礎化學品」類別下逐字列出「濃硫酸、發煙硫酸、各類稀酸／硫酸鋁、聚氯化鋁／液鹼、鹽酸及各項**無機化學品**等製造與買賣銷售」，全部落在本節點定義的無機鹽類與無機化學品。★2026-09-07 新增：此線先前無節點認領，我在 2026-09-07 的缺口清單裡曾診斷為「卡在 J1 上游石化無定義」，**那個診斷是錯的**——它不是石油裂解系的上游石化，而是無機酸鹼鹽，J10 早就存在且定義相符，缺的不是定義而是沒有人去讀年報的商品項目表。Core 理由（2026-09-07 因本格新增使候選成為二取一而依 R023 順序重跑）：①年報比重表與商品項目表皆以**基礎化學品**列首；②價值創造位置同向——電子化學品那條線的商品項目自述為「電子級溶劑產品**分裝／混合**」與「濕式電子化學品 **OEM/ODM**」，是受託分裝與代工，而基礎化學品是自有製程的酸鹼鹽生產。①②同指本節點，Core 由 A1-06 移入。⑤同向作為佐證：45.76% 大於電子化學品 29.42%。★年報第三條「特用化學品 24.81%」仍未指派：其商品項目為染顏料中間體、電子產業應用樹酯單體、特殊功能樹酯／塑膠原料、甲苯磺化系列、磷酸鹽類及界面活性劑，橫跨 J7／J5／J4-01／J10 至少四格，年報未給拆分依據(R020②)', 'Direct'),
        ('東鹼', 'Major', None, 'annual_report_table', '年報備註「化工類產品中,製造為硫酸鉀、鹽酸、液體氯化鈣及小蘇打等」，其中鹽酸、液體氯化鈣、小蘇打屬本節點定義之無機化學品，硫酸鉀屬 J12 肥料，同一條「製造-化工類產品 63.76%」線橫跨兩節點故不計比重(R020②)。★原 evidence 兩句皆與年報相反：純鹼是**貿易**線的產品不是製造線的，貿易線也不是「自有產品銷售」而是「純碱、小蘇打、無水硫酸鈉、氯化鉀及晒鹽等基本化學原料之進口、買賣銷售及轉口貿易業務」。★另一條「貿易-化工類產品 7.69%」依 R022 屬通路性質、應為 Indirect，但本節點已有本公司的製造型 Direct 一列，無法在同一節點再記一列，故僅於此載明', 'Direct', 'Too_Coarse'),
        ('國化', 'Core', '氧化鋅 + 吊白塊 + 重亞硫酸鈉 + 保險粉', 'annual_report_table', '年報營業比重表：四項無機化學品合計 100%', 'Direct'),
        ('中石化', 'Minor', None, 'annual_report_prose', '年報「其他尚有電、氰酸、硫酸銨、工業硫酸、精製硫酸等副產品」；產品項目「目前之產品項目有丙烯腈、己內醯胺、尼龍粒、鄰苯基苯酚、硫酸銨、硫酸(含精製硫酸)、氰酸、電、工塑產品等項目」。硫酸銨與各級硫酸、氰酸皆為本節點定義之無機鹽類，但同一條「其他部門 12.28%」線另含電力與工塑產品，無法拆分故不計比重(R020②)。。role 理由：中石化的主要經營活動為己內醯胺與丙烯腈兩大石化產品部門，兩者為年報最大的兩條營收線；本節點涵蓋的硫酸銨、各級硫酸與氰酸，年報自述為「其他尚有電、氰酸、硫酸銨、工業硫酸、精製硫酸等**副產品**」——公司自身即將其定位為主產品製程的副產物而非獨立經營的業務線，相對於主要經營活動屬附帶之經濟地位，故列 Minor。其他部門合計 12.28% 僅作佐證', 'Direct', 'Too_Coarse'),
        ('台肥', 'Major', None, 'annual_report_prose', '年報「2. 化工事業」為公司自列三大成熟事業之一；「目前本公司經營的產品如硝酸、磺胺酸、發煙硫酸等,因產品本身或其副產品具有運輸與儲存限制而在台灣本地生產」——三者皆為本節點定義之無機化學品且明寫自產。★液氨、工業尿素與美耐明依年報自述「以較具市場競爭力的進口貨源取代」自產，不計入本格。年報無營業比重表故不計比重(R020⑤：NULL 不代表 Indirect)', 'Direct', 'Too_Coarse'),
        ('昶昕', 'Major', '回收再生產品', 'annual_report_table', '年報營業比重表：回收再生產品(含銅鹽類及其他金屬) 82.00%；商品項目表列「綠色再生資源材料供應／工業用金屬化合物：工業用硫酸銅、氧化銅、碳酸銅、氫氧化錫」，年報並載明「再利用金屬與其化合物的供應項目,包括硫酸銅、碳酸銅以及氧化銅、氫氧化錫等,每月產銷約達 2,800 公噸」。硫酸銅、碳酸銅、氧化銅屬本節點定義之無機鹽類與金屬氧化物。★依 R016 以產品歸屬而非以活動歸屬：回收是取得原料的方式，賣出的是無機金屬化合物，故不掛 Z5 廢棄物', 'Direct'),
    ],
    "J9": [
        ('中石化', 'Core', '己內醯胺及其附屬產品', 'annual_report_table', '年報「2.主要產品及其營業比重」銷售比重表：己內醯胺及其附屬產品 46.81%（含尼龍粒），為四列中最大；丙烯腈及其附屬產品 40.14% 另計於 J1-01。Core 理由：⑤營收比重 46.81% > 40.14%，且①公司自述「本公司為台灣唯一的 CPL 生產廠家，境內市占率約 55%」，兩者同向。★前版存的 50.157%／49.445% 是錯的：那組數字來自年報另一張以**公噸**為單位的產銷量表（丙烯腈 196,161、己內醯胺尼龍粒 198,983、鄰苯基苯酚 1,580），三者合計僅實際營收 19,217,032 仟元的 2.1%，且排除了「其他部門 12.28%」後重新正規化——年報正文自述 AN 年產能 23 萬噸、CPL 年產能 20 萬噸即為佐證。Core 判在 J9 的結論不變，數字與論據已更換', 'Direct'),
        ('材料*-KY', 'Core', '二醋酸纖維絲束 + 二醋酸纖維素', 'annual_report_table', '年報營業比重表：二醋酸纖維絲束 86% 加二醋酸纖維素 14%', 'Direct'),
        ('國喬', 'Minor', None, 'annual_report_prose', '年報產品清單「國喬集團目前之產品(服務):…五、尼龍(PA)」；計畫開發之新商品「三、高性能尼龍66(PA66)及多功能複合材:秉持「技術護城河」策略,擴展尼龍工業絲市場並開發高階應用」。合併比重表註明「其他包括汽電、氫氣、包材、尼龍、版權、轉播收入及餐飲服務等銷售收入,因所占營業比重皆在5%以下,故彙總列示」，尼龍混在「其他 14.29%」內無法拆分故不計比重(R020②)。★個體財報的尼龍 8.19% 分母是個體營收，依 R020① 不得填入以合併營收為分母的欄位。role 理由：國喬的主要經營活動為 ABS／HIPS／PP 塑膠與 SM／丙烯石化兩大部門，兩者為年報僅有的兩條主要營收線；尼龍未被單獨列示，而是與汽電、氫氣、包材、版權、餐飲等併入「其他」一欄，年報明載該欄各項「所占營業比重皆在 5% 以下，故彙總列示」——公司自身即以規模過小為由不予單列，相對於主要經營活動屬附帶之經濟地位，故列 Minor。該欄合計 14.29% 僅作佐證', 'Direct', 'Too_Coarse'),
        ('台化', 'Minor', None, 'annual_report_table', '年報營業比重表：纖維部 2.3%，主要產品欄為「耐隆粒、耐隆原絲、加工絲」；耐隆粒屬本節點定義(尼龍粒)，原絲與加工絲屬 M1，同一條線橫跨兩節點故不計比重(R020②)。role 理由：台化的主要經營活動為化工與塑膠（化工一、二、三部與塑膠部為年報最大的四個部門），纖維部是年報七個部門中倒數第二小者，而本節點又僅是該部門三項產品中的一項；相對於主要經營活動屬附帶、非主要之經濟地位，故列 Minor。纖維部 2.3% 僅作佐證', 'Direct', 'Too_Coarse'),
    ],
    "J7": [
        ('永光', 'Core', '色 料 化 學 品', 'annual_report_table', '年報營業比重表：色料化學品 36.40%，致股東報告書載明「色料事業:114年營收新台幣27.6億元,佔比36%」，客戶為服飾皮件運動品牌商。Core 理由：①致股東報告書以事業別依序說明經營概況，第一項即「**一、色料事業**：114年營收新台幣27.6億元，佔比36%」，色料列於各事業之首；⑤本格 36.40% 為年報比重表中最大的可辨識線，大於特用化學品 24.00%（該線內含彩色與半導體光阻，年報不細分，故 A1-05 實際小於 24%）與碳粉 12.90%。★年報「肆、營運概況」章節未被解析器擷取到（抓到的是目錄頁與致股東報告書），故①改引致股東報告書', 'Direct'),
        ('大恭', 'Core', '調製應用顏料 + 合成有機顏料', 'annual_report_table', '年報營業比重表：調製應用顏料 67% 加合成有機顏料 30%', 'Direct'),
    ],
    "J6": [
        ('永記', 'Core', '塗料類', 'annual_report_table', '年報營業比重表：塗料類 92.00%', 'Direct'),
        ('國碳科', 'Core', '防火塗料', 'annual_report_table', '年報營業比重表（114 年度）：防火塗料 48.88%；另有防火材料 23.39%、防火工程 12.07% 與阻燃劑 9.77%。Core 理由（2026-09-07 因新增 J4-01 阻燃劑使候選成為二取一而依 R023 順序重跑）：①核心產品能力定案——年報比重表中防火塗料 48.88%、防火材料 23.39%、防火工程 12.07% 三條合計八成四皆為**已配製完成的防火成品與其施工**，公司的能力與交付形式建立在配方與塗裝，而非販售添加劑本體；阻燃劑是同一配方能力向上游延伸出的單一原料品項。⑤同向作為佐證：48.88% 約為 9.77% 的五倍★2026-09-15 M5：更正——原數字（防火塗料 52.20%、防火材料 20.39%、防火工程 6.34%、阻燃劑 13.06%）為 113 年度欄，改為 114 年度欄；Core 方向不變', 'Direct'),
        ('泓瀚', 'Core', '溶劑性墨水 + UV墨水', 'annual_report_table', '年報營業比重表：溶劑性墨水 65% 加 UV墨水 27%', 'Direct'),
        ('南寶', 'Minor', '塗料', 'annual_report_table', '年報營業比重表：塗料 8.00%；接著劑為其本業，見 J11', 'Direct'),
    ],
    "J5": [
        ('日勝化', 'Core', 'PU合成樹脂 + PE多元醇樹脂', 'annual_report_table', '年報營業比重表：PU合成樹脂 77% 加 PE多元醇樹脂 3%（資料由年報業務範圍表格補得，營業比重章節解析不到）。Core 理由：①年報商品項目順序為 A.人造合成皮用 **PU樹脂** B.紡織上膠貼合用 PU樹脂 C.二榔皮用 PU樹脂 D.聚酯多元醇 E.架橋劑，前四項皆為樹脂本體；④計畫開發首項「不黃變三聚體架橋劑開發」同屬樹脂化學；⑤本格 80.00% 而 J11 接著劑無比重', 'Direct'),
        ('大立', 'Core', '合成樹脂', 'annual_report_table', '年報營業比重表：合成樹脂 95.00%', 'Direct'),
        ('永捷', 'Core', 'PU樹脂', 'annual_report_table', '年報營業比重表：PU樹脂 25.00%；其乾溼式PU合成皮 34% 與榔皮 28% 為同一 PU 材料的下游成品（R024）', 'Direct'),
        ('長興', 'Core', '合成樹脂', 'annual_report_table', '年報營業比重表：合成樹脂 48%；產業概況將該段拆為「A.塗料樹脂」「B.不飽和聚酯樹脂」，產品項目列 (1)塗料樹脂 (2)聚酯樹脂 (3)環氧樹脂。原掛 B1-03 樹脂，但該節點位於 B1（PCB／載板材料）分支、定義為 CCL 用的 Epoxy/PPO/PPE/Cyanate Ester/BT Resin，不含塗料樹脂與不飽和聚酯樹脂(R025 反面警訊)。Core 理由：48% 為最大營收線，且為公司登記所營業務之首項「合成樹脂及塑膠製造業」(R023①⑤)', 'Direct'),
        ('國慶科技', 'Major', '熱可塑性聚胺脂膠粒 + 樹脂', 'annual_report_table', '年報營業比重表：熱可塑性聚胺脂膠粒 11.24% 加樹脂 4.07%，合計 15.31%(R020④)；所營業務「(4)熱可塑彈性體、塑膠粒。(5)合成樹脂、表面處理劑、接著劑、硬化劑、促進劑」；產業概況「(2)高分子產品 包含熱可塑性塑膠粒(TPU)及熱可塑性彈性體(TPV)」。TPU 即聚胺酯，本節點定義第二項即 PU', 'Direct'),
        ('和益', 'Minor', '樹脂部門(註2)', 'annual_report_table', '年報註 2「樹脂部門主要產製C9石油樹脂及氫化C9石油樹脂等產品」；商品項目「(4)C9石油樹脂/氫化石油樹脂:為黏著劑與熱融接著劑上游原料,提供增黏及潤濕效果」。本節點定義於 2026-09-03 依 R025 擴充納入石油樹脂（原定義僅列 Epoxy／PU／Acrylic／Silicone）；和益為上游樹脂製造商而非接著劑成品商，故不掛 J11', 'Direct'),
        ('國精化', 'Core', 'UV光固化材料 + 不飽和聚酯樹脂 + 塗料樹脂', 'annual_report_table', '年報營業比重表（合併欄）三條樹脂本體合計 79.00%：UV光固化材料 49.00、不飽和聚酯樹脂 18.00、塗料樹脂 12.00(R020④)。★UV光固化材料屬本節點而非 J6 塗料與油墨：年報商品項目第一條寫明「a.UV 光固化**單體與寡聚物**」，是樹脂本體不是配方成品，而本節點定義即「以樹脂本體之製造為業務核心；配方成品見 J6」。年報業務主要內容亦為「各種工業用**合成樹脂**,及其他有關化學材料之製造、加工、銷售」。★2026-09-04 由 [core_pending] 全表重掃補建：本公司先前只有 A1-06 一格 18%，79% 的樹脂業務完全未建檔。Core 理由：⑤本格 79.00% 遠大於 A1-06 電子化學材料 18.00%；①公司自述以合成樹脂製造列首', 'Direct'),
    ],
    "J4-01": [
        ('立凱-KY', 'Core', '磷酸鐵鋰電池正極材料', 'annual_report_table', '年報營業比重表：磷酸鐵鋰電池正極材料 99.43%；年報自述主要營收來自「磷酸鐵鋰電池正極材料之生產、研發及銷售業務」，屬 J4-01 定義的電池材料，比照康普動力電池材料。', 'Direct'),
        ('國碳科', 'Minor', '阻燃劑', 'annual_report_table', '年報營業比重表（114 年度）：阻燃劑 9.77%。★2026-09-07 由 J4-01 定義補入「高分子添加劑」類別後的全表重掃新增：阻燃劑以功能訴求交易，屬本節點；與其 Core 所在的 J6 塗料與油墨分界清楚——防火塗料 48.88% 是已配製成的塗料成品，阻燃劑是加進他人配方裡的添加劑本體★2026-09-15 M5：更正——原 13.06% 為 113 年度欄', 'Direct'),
        ('國慶科技', 'Core', '特用化學品', 'annual_report_table', '★2026-09-07 公司名由「三晃」變更為「國慶科技」（tw.stocks 當日刷新）；MEMBERS 以公司名索引，故三筆同步改名，stock_id 1721 未變。年報營業比重表：特用化學品 57.00%。Core 理由：④與⑤同向而①指向另一邊，取二比一。①年報所營業務首條為「植物保護用藥原料藥」（屬 J12），但商品項目表在**特用化學品**類別下列出無鹵阻燃劑、抗氧化劑、交聯劑、電子化學品、成核劑，④計畫開發欄第一項為「DOPO-AESt(2Me)」（磷系阻燃劑），⑤本格 57.19% 亦遠大於 J12 農用化學品 26.70% 與 J5 樹脂 15.31%', 'Direct'),
        ('奇鈦科', 'Core', '紫外光吸收劑及光安定劑 + 抗氧化劑 + 光引發劑', 'annual_report_table', '年報營業比重表：紫外光吸收劑及光安定劑 61% 加抗氧化劑 23% 加光引發劑 15%，同屬高分子添加劑', 'Direct'),
        ('美琪瑪', 'Core', None, 'annual_report_table', '年報列電池及先進材料原料 59% 與氧化觸媒 51%，兩者相加 110% 顯示比例欄有誤，金額欄則為 1,242,272 與 1,288,390（約 49%/51%）；比例不可信故不計比重★2026-09-09 weight_status=Disclosure_Invalid：公司**有**揭露比例，但兩條線 59% 與 51% 相加為 110%，比例欄本身錯誤（金額欄反推約 49/51）。與 Not_Disclosed 分開記，是因為後續動作不同：這裡要等公司更正或改用金額欄推算，不是等公司開始揭露。', 'Direct', 'Disclosure_Invalid'),
        ('康普', 'Core', '動力電池材料 + 特用化學材料', 'annual_report_table', '年報營業比重表：動力電池材料 64% 加特用化學材料 14%。Core 理由：①年報商品項目「依產業應用可分為五大類」順序為 (1)**氧化觸媒** (2)動力電池材料 (3)肥料，觸媒與電子材料列於肥料之前；④計畫開發首項「觸媒回收技術」；⑤本格 77.23% 遠大於 J12 農用化學品 6.64%。三項同向', 'Direct'),
        ('聚和', 'Core', '精密化學品 + 特用化學品', 'annual_report_table', '年報營業比重表：精密化學品 46% 加特用化學品 30%，合計 76.00%(R020④)。★特用化學品實為造紙化學品（中空樹脂、乾強劑、表面上膠劑、顯色劑），本節點定義於 2026-09-03 依 R025 擴充納入造紙化學品，故無須改掛他格。Core 理由（因本節點定義擴充而重跑 R023）：兩條化學品線合計 76% 遠大於辦公文具 22%，且公司自述以精密化學品之研發製造為主業(R023②⑤)', 'Direct'),
        ('東聯', 'Major', '特化事業', 'annual_report_table', '年報營業比重表：特化事業 27.00%；商品項目「(3) 乙醇胺、碳酸乙烯酯、乙二醇丁醚、脂肪醇聚氧乙烯醚、聚乙二醇、聚乙二醇單甲醚…等特用化學品之製造與買賣」；產業概況「本公司乙二醇丁醚(EB)廠設計產能3萬噸/年…並為台灣唯一生產廠商」「推出超淨高純的乙醇胺產品,供應台灣與亞洲半導體製程清洗劑客戶」。本節點定義於 2026-09-03 依 R025 擴充納入環氧乙烷衍生物與界面活性劑', 'Direct'),
        ('和桐', 'Major', None, 'annual_report_prose', '年報產品項目「化學品」含脂肪醇聚氧乙烯醚(AEO)及脂肪醇聚氧乙烯醚硫酸(AES)；年報自繪第二條產業鏈「上游為棕櫚仁油/椰子油,經萃取提煉生產出脂肪醇,再進一步加工生產脂肪醇聚氧乙烯醚及脂肪醇聚氧乙烯醚硫酸」。本節點定義於 2026-09-03 依 R025 擴充納入界面活性劑；★原提議為此另建「界面活性劑原料」新節點，但同一批擴充已使本節點涵蓋該產品（東聯的脂肪醇聚氧乙烯醚即同一化學），依 R025 硬條件不成立故不新建。併於「化學品 94.23%」一線與 J2 共用故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "J2": [
        ('和益', 'Core', '烷化部門(註1)', 'annual_report_table', '年報營業比重表：烷化部門 80.88%。Core 理由（2026-09-03 因 J5 定義擴充使 J5 成為新候選而重跑 R023）：烷化部門為最大營收線且遠大於樹脂部門 12.44%，公司自述之核心製程為烷化(R023②⑤)；樹脂與農藥兩線皆屬下游延伸，資源未自烷化移出，反否決條件不成立', 'Direct'),
        ('和桐', 'Core', None, 'annual_report_prose', '年報產品項目「化學品:包含正烷屬烴、直鏈烷基苯、直鏈烷基苯磺酸、脂肪醇聚氧乙烯醚及脂肪醇聚氧乙烯醚硫酸」，其中直鏈烷基苯與直鏈烷基苯磺酸屬本節點；「化學品 94.23%」一線橫跨本節點與 J4-01 故不計比重(R020②)。Core 理由（2026-09-03 因 J4-01 定義擴充使界面活性劑成為新候選而重跑 R023）：年報自述主力為直鏈烷基苯(LAB)，為全球主要 LAB 供應商之一，且年報產業關聯圖以 LAB 鏈為第一條、脂肪醇鏈為第二條(R023①②)', 'Direct', 'Too_Coarse'),
        ('台苯', 'Minor', '對二乙苯', 'annual_report_table', '年報營業比重表：對二乙苯 2.6%；產品項目「(1)苯乙烯單體 (2)對二乙苯 (3)甲苯 (4)氫氣 (5)蒸汽 (6)乙苯」。本節點定義含苯、甲苯', 'Direct'),
        ('台化', 'Core', '化工一部 + 化工三部', 'annual_report_table', '年報營業比重表：化工一部 38.5%（主要產品：苯、甲苯、鄰二甲苯、對二甲苯、間二甲苯）加化工三部 11.9%（純對苯二甲酸、純間苯二甲酸），兩部門逐項皆落在本節點定義(PTA、苯、甲苯)，合計 50.40%(R020④)。Core 理由：本節點為最大營收合計(50.40% vs 塑膠部 20.5%)，且芳香烴與 PTA 是公司自述之化工主業(R023⑤)', 'Direct'),
    ],
    "J1-01": [
        ('南亞', 'Minor', '乙二醇', 'annual_report_table', '年報營業比重表：乙二醇 9.51%。★2026-09-07 由 J1-01 定義改寫後的全表重掃新增：乙二醇原不在該格的產品列舉內，改以「基礎單體／中間體／大宗聚合物」的類別陳述後即明確涵蓋。南亞的乙二醇屬其塑膠原料事業，年報業務範圍列於「塑膠原料產品：乙二醇、可塑劑、丙二酚、酜酸酐、異辛醇等」', 'Direct'),
        ('台塑', 'Core', '塑膠事業部 + 聚烯事業部 + 聚丙烯事業部 + 化學品事業部', 'annual_report_table', '年報營業比重表：塑膠事業部 35.24% 加聚烯事業部 20.21% 加聚丙烯事業部 15.62% 加化學品事業部 9.71%，合計 80.78%(R020④)。★原合計含台麗朗事業部 16.85%，但該部門主要產品欄為「丙烯酸酯、碳纖維、正丁醇、高吸水性樹脂」，碳纖維被年報列在「(4)人造纖維產品」而非塑化原料，高吸水性樹脂亦不在本節點定義內，該線橫跨節點且無法拆分，依 R020② 剔出合計。Core 理由：四個事業部占營收 80.78% 且皆為本節點定義之塑化原料(R023⑤)', 'Direct'),
        ('台聚', 'Core', '塑膠原料', 'annual_report_table', '年報營業比重表：塑膠原料 91.00%，主力為聚乙烯 PE。Core 理由：①年報產品項目順序為 (1)低密度聚乙烯(LDPE) (2)乙烯醋酸乙烯酯樹脂(EVA) (3)高密度聚乙烯(HDPE) (4)線型低密度聚乙烯(LLDPE) (5)環狀嵌段共聚物(CBC)，五項皆為塑化原料；④計畫開發首項「光學級新應用材料環狀嵌段共聚物開發」同屬原料；⑤本格 91.00% 遠大於 F1-01 電子材料 6.00%。三項同向', 'Direct'),
        ('華夏', 'Core', 'PVC粉、塑膠粒及化學品 + 氯乙烯單體', 'annual_report_table', '年報營業比重表：PVC 粉塑膠粒及化學品 66% 加氯乙烯單體 11%。Core 理由：①年報所營業務順序為 (1)**塑膠及其所需原料之製造** (2)塑膠加工品及其所需化學品之製造，原料列於加工之前；比重表順序亦為 氯乙烯單體 → PVC粉塑膠粒及化學品 → PVC建材 → 塑膠布 → 塑膠皮；⑤本格 77.00%（PVC粉塑膠粒 66 加氯乙烯單體 11，R020④）大於 K2 塑膠加工 23.00%', 'Direct'),
        ('亞聚', 'Core', '乙烯醋酸乙烯酯樹脂', 'annual_report_table', '年報營業比重表：乙烯醋酸乙烯酯樹脂 EVA 63.00%', 'Direct'),
        ('台達化', 'Core', '發泡級聚苯乙烯(EPS + 丙烯腈-丁二烯-苯乙烯共聚合樹脂(ABS + 通用級聚苯乙烯(GPS + 耐衝擊聚苯乙烯(IPS', 'annual_report_table', '年報營業比重表：發泡級聚苯乙烯(EPS) 44.97%、丙烯腈-丁二烯-苯乙烯共聚合樹脂(ABS) 28.20%、通用級聚苯乙烯(GPS) 22.94%、耐衝擊聚苯乙烯(IPS) 0.04%，合計 96.15%；四者皆為本節點定義具名的塑化原料（PS 系與 ABS）。★前版只取 EPS＋GPS 是因 ABS 與 IPS 兩列未進 tw.product_revenue，非判定排除。Core 理由：①年報所營業務順序為 (1)聚苯乙烯(GPS)及發泡聚苯乙烯(EPS)樹脂之製銷 (2)ABS 之製銷 (3)SAN 之製銷，玻璃棉未列前三；主要產品表順序亦為 1.EPS 2.ABS 3.GPS 4.玻璃棉 5.IPS；④計畫開發首項「TAIECOR PIR ABS ESG 環保材料」；⑤本格 96.15% 遠大於 P4 建材（玻璃棉）3.85%', 'Direct'),
        ('台苯', 'Core', '苯乙烯單體', 'annual_report_table', '年報營業比重表：苯乙烯單體 SM 93.30%。Core 理由：①年報所營業務首條為「**苯乙烯單體**及中間產品、副產品之生產與銷售」；產品項目順序為 (1)苯乙烯單體 (2)對二乙苯 (3)甲苯 (4)氫氣 (5)蒸汽 (6)乙苯，苯乙烯單體列首而甲苯（J2 芳香烴）列第三；⑤本格 93.30% 遠大於 J2 的 2.60%', 'Direct'),
        ('國喬', 'Core', '塑膠銷貨收入(ABS、HIPS、PP) + 石化銷貨收入(SM、Propylene)', 'annual_report_table', '年報營業比重表：塑膠 39.56% 加石化 39.51%，產品為 ABS/HIPS/PP 與 SM/丙烯。Core 理由：①年報「國喬集團目前之產品(服務)」順序為 一、產銷**苯乙烯單體(SM)**及其衍生物 二、產銷 **ABS** 三、氫氣 四、蒸汽與電力 五、**尼龍(PA)**——尼龍（J9 化纖原料）列於末位；④計畫開發首項為「高階 ABS 及 SAN 關鍵素材」；⑤本格 79.07% 而 J9 無比重。三項同向', 'Direct'),
        ('台化', 'Major', '塑膠部', 'annual_report_table', '年報營業比重表：塑膠部 20.5%，主要產品欄為「ABS、PS、PP、PC」，逐項落在本節點定義。★原 evidence 稱「部門名稱不指明產品,無法拆到節點」，但 FY2025 年報那張表有「主要產品」欄，該前提已不成立，2026-09-03 更正。★化工二部 21.8%(苯乙烯、合成酚、丙酮)橫跨本節點與 J2，依 R020② 兩格皆不認領該線', 'Direct'),
        ('中石化', 'Major', '丙烯腈及其附屬產品', 'annual_report_table', '年報「2.主要產品及其營業比重」銷售比重表：丙烯腈及其附屬產品 40.14%。★前版存的 49.445% 來自年報另一張公噸單位的產銷量表，非營業比重（詳 J9 該列說明）', 'Direct'),
        ('東聯', 'Core', '乙二醇事業', 'annual_report_table', '年報營業比重表：乙二醇事業 66.00%。Core 理由（2026-09-03 因 J4-01 定義擴充使特化事業 27% 成為新候選而重跑 R023）：乙二醇為最大營收線且為公司名稱與主業所指（東聯化學之 EG 事業），特化與氣體兩事業皆建立在同一乙烯／環氧乙烷產業鏈的下游(R023②⑤)', 'Direct'),
        ('台蠟', 'Core', '精煉白蠟及微晶蠟', 'annual_report_table', '年報營業比重表：精煉白蠟及微晶蠟 65.00%，石油系蠟品；另有租賃 25% 與水產 11% 非化工業務', 'Direct'),
    ],
    "V2": [
        ('堡達', 'Core', '被動元件', 'annual_report_table', '營業比重表被動元件 98.23%，年報述「主要代理松下產業(Panasonic)主動及被動電子零組件」，屬代理通路無自有製造。', 'Direct'),
        ('日電貿', 'Core', '陶瓷電容 + 固態電容 + 電解電容', 'annual_report_table', '年報營業比重表：陶瓷電容 45.44% 加固態電容 26.31% 加電解電容 9.60%，被動元件代理合計 81.35%(R020④)。Core 理由：⑤本格 81.35%（陶瓷／固態／電解電容）大於 V1 半導體 9.99%；①年報商品項目逐條為「代理…之電子零組件」，第一條即日電貿本體代理 NIPPON CHEMI-CON、Panasonic、Sumida（★原寫「電容為主」是撰寫者推斷、年報未如此描述且 Sumida 主業為電感，2026-09-07 移除），與比重同向', 'Direct'),
        ('增你強', 'Minor', '被動元件', 'annual_report_table', '年報營業比重表：被動元件 0.96%；與其半導體八條線同為代理銷售，依產品類別歸本節點而非 V1(R031 同層互斥)', 'Direct'),
        ('敦吉', 'Minor', 'TRANSFORMER-變壓器', 'annual_report_table', '年報營業比重表：TRANSFORMER-變壓器 5.95%；商品類別表列於「機構元件（繼電器、馬達、變壓器及線圈、連接器、沖壓部品…）」。變壓器為被動元件，落本節點；同表的繼電器另建於 V6', 'Direct'),
    ],
    "V1": [
        ('禾伸堂', 'Major', '主動元件', 'annual_report_table', '比重表「主動元件」23%，年報載「代理經銷的主動元件」，內容為積體電路等主動元件，屬 V1 半導體代理通路（R022 通路用 Direct）。', 'Direct'),
        ('捷創科技', 'Major', None, 'annual_report_table', '年報「代理與自有產品銷售 22.67%」含代理 Advantest EVA100 測試機等；自有與代理合併揭露無法拆分故不計比重。依 R022 不得據所代理的測試機建立 A2-13 型製造曝險', 'Direct', 'Too_Coarse'),
        ('辛耘', 'Major', '代理', 'investor_conf', '年報商品項目 C.代理設備：代理半導體、光電、LCD、LED、太陽能等產業量測與製程設備。依 R022 不得據所代理的設備逕建 A2-xx 製造型曝險。年報比重表僅列會計科目，比重改讀 2026-08-27 法說簡報 p.36 代理營業收入：1H2026 2,958,074 仟元／48.37%（FY2025 6,647,828／58.46%，與年報營業收入淨額 11,371,368 對帳差 0.0017%）', 'Direct'),
        ('亞矽', 'Core', '處理器/核心邏輯晶片 + 網路通訊類 + 線性類比類 + 記憶體類 + 邏輯線路類', 'annual_report_table', '年報營業比重表五條半導體品項合計 99.72%：處理器/核心邏輯晶片 41.12、網路通訊類 40.55、線性類比類 13.49、記憶體類 3.89、邏輯線路類 0.67(R020④)。★「網路通訊類 40.55%」是網通用的**晶片**不是網通裝置，故在 V1 不在 V5：年報「(1)**代理銷售之半導體品牌**」逐條列出 Cortina 的 Networking Processor 與 xPON、Realtek 的 BLE Chip IC、MorseMicro 的 Wifi AH Chip、Metanoia 的 VDSL、Rafael 的 BLE/Zigbee/Sub-G Chip、Microchip 的 MCU/Analog/Interface/加密 IC、Bosch 的 MEMS 感測器。★2026-09-04 補：前版只認領 41.12% 一條且無業務範圍敘述可判，該敘述當日重抓年報後取得', 'Direct'),
        ('倍微', 'Core', None, 'annual_report_prose', '年報業務範圍載明「通路」：印度等科技產業的製造佈局,帶領出完整、及時供應支援等具競爭力條件的電子零組件通路商。在過去三十幾年來一路伴隨主機板、筆記本電腦、伺服器、顯示★2026-09-09 weight_status=Not_Extracted：年報 @394「營業比重約當95%是電子產品的銷售代理,其他5%是軟體服務」。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★2026-09-09 由 Not_Extracted 改判 Too_Coarse（R020-NE 的重新裁定）：解析取得「電子產品的銷售代理 95.0」與「軟體服務 5.0」。★95% 那條線不給本格，理由同 3033 威健：「電子產品」的範圍寬於 V1 的半導體代理，年報商品清單同時含 MPU／MCU／快閃記憶體（V1）與記憶儲存裝置、RAID 卡等資訊產品（V3），一條線橫跨兩格以上。★另一條「軟體服務 5.0」指向 U1 軟體，**本輪不建曝險**：5.0% 未達 R026 的 15% 門檻，且本公司在 U1 尚無任何既有曝險，為一條 5% 的線新建節點成員需要獨立的存在性判斷，不在本批範圍。', 'Direct', 'Too_Coarse'),
        ('光菱', 'Core', '分散式元件 + 特定應用IC + 微控制器', 'annual_report_table', '年報營業比重表：分散式元件 19.32% 加特定應用IC 68.96% 加微控制器 3.21%，全部為半導體代理銷售', 'Direct'),
        ('全科', 'Core', '無線通訊元件 + 有線寬頻元件', 'annual_report_table', '年報營業比重表：無線通訊元件 56.65% 加有線寬頻元件 23.08%，合計 79.73%(R020④)。★這兩條是網通用的**半導體元件**不是網通裝置，故在 V1 不在 V5：年報自述「本公司營運主要為代理銷售各國網路通訊之半導體產品,包括有線寬頻元件、無線通訊元件,以及光通訊元件、語音處理元件、通訊保護元件等」，並列「本公司為 IC 通路業者」、上中下游圖標「半導體零組件代理經銷商」', 'Direct'),
        ('全達', 'Core', '電子零組件', 'annual_report_table', '年報營業比重表：電子零組件 93.77%；年報自述「本公司為專業半導體零組件代理商」，商品項目列代理品牌 Microsoft、SigmaStar、Intelligo 與特定應用 IC。★年報「自行研發設計之成果」列有 Projector／Monitor Scaler，但正文明說研發工作「主要由工程部門人員負責…並未將工程部門產生的費用獨立歸為研發費用」，屬 design-in 應用支援不是 IC 設計，故不建 A4 曝險。Core 理由：代理業務占營收 93.77%(R023①⑤)', 'Direct'),
        ('利機', 'Core', '半導體產品', 'annual_report_table', '年報營業比重表：半導體產品 67.00%。Core 理由：①年報所營業務首條為「**半導體產業相關材料與設備之銷售**」，光電列第二、奈米材料**製造**列第四；商品項目大類順序亦為 (1)半導體產業 (2)光電產業 (3)綠能及其他；⑤本格 67.00% 為最大營收線。★自製銀漿屬 A1-12 且列於自述末位', 'Direct'),
        ('博士旺', 'Core', '記憶體相關零組件', 'annual_report_table', '年報營業比重表：記憶體相關零組件 25.32%；正文「本公司主要係電子IC批發買賣、區塊鏈技術軟體應用開發、消費性娛樂產品及生技產品之銷售等四大類產品為主」。原 evidence 是「通路」二字命中電玩通路那句（「(4)消費性娛樂產品:TVGame與PCGame相關產品之代理、銷售、通路經營及市場拓展」），拿另一條線的文字支撐本格，已於 2026-09-03 更正。Core 理由：候選集合目前是本格 25.32% 與 U1 軟體 17.74% 兩者，①公司自述「電子IC批發買賣、區塊鏈技術軟體應用開發、消費性娛樂產品及生技產品之銷售等四大類」以電子IC批發為首，④長期策略「計劃開發之新產品(服務) (1)電子類產品:開拓記憶體零組件之銷售」同樣開頭於電子，⑤本格 25.32% 亦大於 U1 17.74%，三項同向。★但本裁定是**暫時的**：消費性娛樂 56.91% 才是最大營收線（TV Game 與 PC Game 產品之代理、銷售、通路經營），它落在 X4 賣場／通路而該節點目前無 Product Definition，依 R031 的 mapping gate 不得先掛上去。X4 定義補上後本公司會多一個比重是本格兩倍的候選，Core 必須重裁★2026-09-15 M5：權重改由年報印出值 25.32% 解析（金額推算 25.3236）', 'Direct'),
        ('增你強', 'Core', '線性積體電路 + 記憶卡 + 模組及其他 + 功率場效應電晶體 + 邏輯及特定應用積體電路 + 數位積體電路 + 二極體 + 電晶體', 'annual_report_table', '年報營業比重表八條半導體與記憶體品項合計 99.04%(R020④)。2026-09-04 補入電晶體 0.87%，前版漏列；被動元件 0.96% 另建於 V2。Core 理由：⑤本格八條半導體與記憶體代理線合計 99.04%，V2 被動元件僅 0.96%；①年報所營業務逐條為進出口貿易與電子零件經銷，④「計劃開發之新產品**代理經銷**」亦以代理為主軸，三項同向', 'Direct'),
        ('大聯大', 'Core', '核心元件 + 記憶元件 + 離散及邏輯元件 + 類比及混合訊號元件', 'annual_report_table', '年報營業比重表四條半導體品項合計 82.28%：核心元件 37.45（AI加速器／晶片組／GPU／CPU／MCU）、記憶元件 27.68、離散及邏輯元件 8.72（二極體／電晶體／IGBT／SiC／GaN／邏輯IC）、類比及混合訊號元件 8.43(R020④)。年報自述「代理經銷之半導體品牌逾250家」。★另兩條不計於本格(R020②)：光學元件 7.74 含 LCD面板模組（V6）與 CMOS Sensor／LED／Micro LED（V1）兩類；被動、電磁及連接器元件 7.69 橫跨 V2 與 V6。★前版只認領 37.45% 一條，原因同文曄', 'Direct'),
        ('威健', 'Core', None, 'annual_report_prose', '年報業務範圍載明「通路」：定位為「必要基礎設施」及「必要業務」。在此產業環境下,威健集團長期深耕半導體通路與代理服務,於產業供應鏈中扮演關鍵橋接角色,透過穩定的產品供★2026-09-09 weight_status=Not_Extracted：年報 @31「營業比重約100%是代理電子零組件暨周邊設備之銷售」。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★2026-09-09 由 Not_Extracted 改判 Too_Coarse（R020-NE 的重新裁定）：解析成功，取得「代理電子零組件暨周邊設備 100%」。★**不給 100% 的理由是本節點自己的定義**：V1 明文「★不含被動元件（V2）」「★不含資訊產品／系統通路（V3）」，而該線的「電子零組件」涵蓋被動元件、「周邊設備」涵蓋資訊產品通路——一條線橫跨 V1／V2／V3 三格，年報未給拆分依據。★這一格是公司唯一建檔的節點，正因如此更不能把 100% 填進來（同 R019 分支④的精神：候選集合不完整時，不得讓唯一可見的格吸收全部）。', 'Direct', 'Too_Coarse'),
        ('安馳', 'Core', '類比積體電路 + 混合信號積體電路 + 射頻積體電路 + 邏輯積體電路', 'annual_report_table', '年報營業比重表：類比積體電路 36.03% 加混合信號積體電路 29.39% 加射頻積體電路 10.01% 加邏輯積體電路 9.94%，四者同為代理之 IC 產品分類、全部落在本節點，合計 85.37%(R020④)。年報自述「本公司以代理為業,近年來原廠透過併購,不斷改變代理生態,使代理商必須能提供加值服務」，★依 R022 不得因品名出現「射頻積體電路」而另建 A4-13 製造型 Direct。Core 理由：四條線皆為同一門 IC 代理生意，占營收 85.37%(R023①⑤)', 'Direct'),
        ('尚立', 'Core', '零件通路事業', 'annual_report_table', '年報營業比重表：零件通路事業 95.68%', 'Direct'),
        ('崇越', 'Core', '半導體相關產品', 'annual_report_table', '年報營業比重表：半導體相關產品 84.60%；品項列矽晶圓、再生晶圓、深紫外線光阻液、光罩、光罩基板、晶圓廠製程用石英器具、CVD/Etching 製程化學材料及特殊氣體、化學機械研磨液、FOSB/FOUP、積體電路封裝材料等，皆為半導體製程材料與設備，屬 V1 定義明文涵蓋。年報自述「代理銷售半導體、LCD、LED 等產業所需之精密材料、製程設備與零組件」。Core 理由：⑤本格 84.60% 遠大於 V8 側的 LED/LCD 3.80%；①公司自述亦以半導體列首。★工程與廠務系統 9.00% 屬工程服務不是通路，未建檔', 'Direct'),
        ('弘憶股', 'Core', None, 'annual_report_table', '年報營業比重表：數位通訊應用方案與元件 85.32%、儲存裝置應用方案與元件 12.70%、類比電子元件 0.80%。★比重不計(R020②)：每一條線的名稱裡都寫著「應用方案**與**元件」，內容也確實兩者並存——85.32% 那條的應用方案是無線網卡、Router、AP、VoIP、Desktop PC、Notebook、平板、藍芽耳機（成品，V3／V5），元件是無線網路IC、區域網路IC、LCD驅動IC、隨身碟控制IC、ARM CPU、WiFi-PA、Bluetooth IC（V1）；12.70% 那條的應用方案是液晶電視、Set-top Box、記憶卡、固態硬碟，元件是 SRAM／DDR／SPI Flash 與控制IC。年報未給任何拆分依據，故各節點 Direct 但 weight 一律 NULL。★這是高營收揭露、低產業解析度的典型：85.32% 的來源清楚，缺的是它裡面有多少屬於哪一個節點——保留 NULL 勝於編一組看起來精確的比例', 'Direct', 'Too_Coarse'),
        ('擎亞', 'Core', '行動通訊產品之關鍵零組件 + 伺服器記憶體(Server + 車用記憶體(Automotive + 晶圓(Foundry', 'annual_report_table', '年報營業比重表四條合計 82.33%：行動通訊產品之關鍵零組件 39.88、伺服器記憶體 34.27、車用記憶體 6.78、晶圓(Foundry) 1.40(R020④)。商品項目「(1)記憶體產品——三星記憶體」「(3)System LSI——Exynos 行動處理器、CIS 影像感測器、DDI／PMIC／Security IC」「(4)Foundry——代客戶投片至三星晶圓代工廠」，年報自述「本公司身為專業的半導體與記憶體零組件通路商」。★前版證據是「通路」二字命中產業上下游關聯性那段，且無比重，2026-09-04 更正。★薄膜電晶體液晶顯示器(SDC) 15.54% 為 AMOLED 面板模組，另建於 V6。Core 理由：⑤四條半導體線合計 82.33% 遠大於面板模組 15.54%，且①公司自述即以半導體與記憶體零組件通路商定位', 'Direct'),
        ('文曄', 'Core', '特定應用積體電路 + 特定應用標準產品-有線連接 + 特定應用標準產品-無線連接 + 類比積體電路 + 微元件積體電路 + 記憶體 + 分散式元件 + 特定應用標準產品-其他 + 光電子元件', 'annual_report_table', '年報營業比重表九條半導體品項合計 94.00%：特定應用IC 27.80、特應標準品-有線連接 19.00／無線連接 16.50／其他 3.10、類比IC 10.70、微元件IC 5.60、記憶體 5.10、分散式元件 3.60、光電子元件 2.60(R020④)。★前版只認領 27.80% 一條：舊 V1 定義是「文曄、大聯大、至上」三個公司名，無從判斷哪些線算數，於是取了名稱最像的那條。定義於 2026-09-03 重寫後才有併線依據。另「被動元件、連接器及機電元件 3.10%」橫跨 V2 與 V6 不可拆，不計於本格(R020②)', 'Direct'),
        ('方土昶', 'Core', '記憶體元件', 'annual_report_table', '年報營業比重表：記憶體元件 98.62%', 'Direct'),
        ('日電貿', 'Minor', '半導體 + 發光二極體', 'annual_report_table', '年報營業比重表：半導體 7.87% 加發光二極體 2.12%，合計 9.99%(R020④)。發光二極體為光電半導體，年報代理品牌含 EVERLIGHT。本公司主體是 V2 被動元件通路 81.35%（陶瓷／固態／電解電容），年報自述「本公司係屬專業電子零組件通路商,主要所營為代理買賣業務,無從事研發活動」', 'Direct'),
        ('昱捷', 'Core', '分散式元件 + 微處理器 + 記憶體 + 光電元件', 'annual_report_table', '年報營業比重表四條半導體品項合計 92.91%：分散式元件 78.25、微處理器 7.99、記憶體 4.72、光電元件 1.95(R020④)。年報自述「本公司為專業半導體零組件通路商」，代理 Synaptics／APEC／美微科。★金屬表面處理 4.24% 是製造服務不是通路，不屬 V 分支，未建檔', 'Direct'),
        ('港建*', 'Core', '電子零件、設備及材料', 'annual_report_table', '年報營業比重表：電子零件、設備及材料 92.46% 加佣金收入 3.97%，為代理銷售', 'Direct'),
        ('益登', 'Core', '積體電路 + 記憶體 + 電子組件 + CPU', 'annual_report_table', '年報營業比重表四條半導體品項合計 97.41%：積體電路 87.11、記憶體 7.39、電子組件 2.21、CPU 0.70(R020④)。★前版只認領 87.11% 一條，原因同文曄', 'Direct'),
        ('至上', 'Core', '非記憶體元件 + 記 憶 體 元 件', 'annual_report_table', '年報營業比重表：非記憶體元件 27.76% 加記憶體元件 72.24%，全部為半導體代理銷售', 'Direct'),
        ('茂綸', 'Core', '半導體電子零組件', 'annual_report_table', '年報營業比重表：半導體電子零組件 97.69%', 'Direct'),
        ('華立', 'Core', '半導體產業', 'annual_report_table', '年報營業比重表：半導體產業 31.27%；該事業群列製程用光阻、電子級特用氣體、IC Chemical、Remover、矽晶圓、先進封裝材料、Slurry、Developer、Spare Parts、Polyimide、靶材，皆為半導體製程材料，屬 V1 定義明文涵蓋。Core 理由：⑤本格 31.27% 並非合計最大——V8 非半導體電子材料通路以平面顯示器 23.73 加 PCB 12.26 得 35.99%，高出 4.7pp；但①公司自述的五大經營範疇順序是「(1)資通訊及電子構裝 (2)半導體 (3)光電/光通訊/平面顯示器」，半導體列於顯示之前，且**單一最大營收線**仍是半導體產業 31.27%（V8 那 35.99% 是兩條線相加而來，任一條單獨都不及 31.27%）。R023 將①置於⑤之前，故 Core 維持 V1。★這是一個 4.7pp 的近距離判定，若日後「電子資通訊產業 26.38%」那條混合線變得可拆，候選集合會再變，屆時須重裁', 'Direct'),
        ('華豫寧', 'Core', 'MCU(微電腦控制器 + Analog&Interface(類比元件', 'annual_report_table', '年報營業比重表：MCU(微電腦控制器) 31.98% 加 Analog&Interface(類比元件) 17.53%，合計 49.51%(R020④)。年報「(1)電子零件事業：主要係提供 IC 韌體設計與應用服務、**IC 代理及買賣**」，原廠授權代理產品線列 Microchip、BOURNS、POWER INTEGRATIONS、NIKO-SEM、EPSON。★2026-09-04 重抓年報後定案：前版 evidence 是「代理」二字命中公司治理章的董事利益迴避條款（「不得代理其他董事行使其表決權」），與生意無關；且 tw.product_narrative 只存了 2,181 字的誠信經營守則表。重抓後比重表第三列 Analog&Interface 17.53% 才進庫，四列合計 100.00%。Core 理由：⑤本格 49.51% 為最大，大於 IoT 28.19%；①公司自述兩大事業以電子零件事業列首。★IoT(智能產品) 28.19% 是自有品牌 WAFERLOCK 的電子晶片鎖、e-Home 智慧家居、e-Link 門禁與 e-Hotel 房控系統，屬**自製**非代理，但樹上目前沒有智慧門禁／智慧家居系統的節點，依 R031 的 mapping gate 不得先塞進最像的一格，未建檔', 'Direct'),
        ('蔚華科', 'Major', '半導體設備', 'annual_report_table', '年報營業比重表：半導體設備 93.77%（本輪撤下計重）；官方分類電子通路業，代理原廠測試設備。撤重的依據：比重表唯一那格「半導體設備 430,621 93.77%…」（印刷頁 p37）同時含代理銷售與自製光學檢測設備（該自製部分落在 A2-13），年報未列各自金額，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★本列的 membership 不受影響——公司仍是主要以代理銷售為營運型態的電子通路業（「本公司為電子通路業,主要營運型態為設備代理銷售業務,無固定製造製程。」，印刷頁 p24）。★role 由 Core 改為 Major：Core 依 RULING_B7_B 轉 Core Pending（該線現在明確包含原代理通路與自有產品兩條路徑，不能再視作單一路徑），本列 role 依 R026③ 為 Major。', 'Direct', 'Too_Coarse'),
        ('豐藝', 'Core', '線性/分散式元件 + 影像處理IC + 特定應用晶片', 'annual_report_table', '年報營業比重表三條半導體品項合計 75.10%：線性/分散式元件 34.28、影像處理IC 27.30、特定應用晶片 13.52(R020④)。年報商品項目逐條以「…代理」結尾，並自述「本公司之主要營業項目為電子零組件之經銷代理」。★「特定應用暨液晶面板相關產品 19.52%」橫跨 V1（特應IC）與 V6（液晶面板），不可拆故不計(R020②)', 'Direct'),
        ('青雲', 'Core', '記憶體產品', 'annual_report_table', '年報營業比重表：記憶體產品 85.28%；商品項目「(1) 品牌代理記憶體產品」。★板卡產品 10.14% 與伺服器產品 2.54% 已於 2026-09-04 依 V3 電腦通路的新定義建為該節點的 Minor。★商品項目 (1)「記憶體模組及固態硬碟」未冠「代理」二字而 (2)(3)(4) 皆有，是否含自製待查。Core 理由：記憶體代理 85.28% 為最大營收線(R023⑤)，且遠大於 V3 側的 12.68%', 'Direct'),
        ('羅昇', 'Major', '半導體設備材料銷售及服務類', 'annual_report_table', '年報營業比重表：半導體設備材料銷售及服務類 11.96%。所營業務載「在半導體產品部份主要核心業務為半導體、光電及太陽能相關設備、耗材及零組件之代理銷售及技術服務」，商品項目列高壓汞燈、晶圓傳送盒、金屬氣體過濾器、研磨粉末、去光阻液等，屬本節點定義的半導體製程設備與材料之代理銷售(R022)。不計比重：同一條線另含自製設備——營業報告載「在晶圓傳送設備方面,新增晶圓舉片機的開發,進一步擴充自製設備品項」（印刷頁 p1–p2），判讀屬 A2-09 一側；年報未拆分(R020②，與 3055 蔚華科同形)，且產製過程又載「本身並未從事生產活動」（印刷頁 p61），兩處不一致。role：Direct 且不計比重，依 R026③ 預設 Major；年報載「羅昇將專注於智動化、綠能、半導體三大領域」，半導體為公司具名的三大發展領域之一，無依據認定其為附屬業務。整條線 11.96% 不作為 role 依據。★2026-09-22 已裁定本線不屬 A2-10（代理轉售）；2026-10-07 裁定成列', 'Direct', 'Too_Coarse'),
    ],
    "A1-01": [
        ('中美晶', 'Core', '半導體晶片', 'annual_report_table', '年報營業比重表：半導體晶片 81.09%', 'Direct'),
        ('台勝科', 'Core', '矽晶圓', 'annual_report_table', '年報營業比重表：矽晶圓 100.00%', 'Direct'),
        ('合晶', 'Core', None, 'annual_report_prose', '年報業務範圍載明「矽晶圓」：,242100.003.公司目前之產品項目(1)半導體矽晶棒(2)半導體矽晶圓(單面拋光片及雙面拋光片)(3)半導體磊晶片(4)SOI晶片(★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('環球晶', 'Core', '半導體晶片產品', 'annual_report_table', '年報營業比重表：半導體晶片產品 98.10%', 'Direct'),
    ],
    "A1-02": [
        ('華旭先進', 'Major', None, 'annual_report_prose', '年報商品項目載明「半導體晶圓再生之處理與銷售」，與碳化矽晶圓並列；該 22.27% 營收線橫跨兩節點無法拆分故不計比重', 'Direct', 'Too_Coarse'),
        ('辛耘', 'Minor', '晶圓再生', 'investor_conf', '年報商品項目 B.晶圓再生（12吋矽晶圓再生），與自製設備、代理設備並列為三大商品；年報比重表僅列會計科目，比重改讀 2026-08-27 法說簡報 p.34 晶圓再生營業收入：1H2026 594,674 仟元／9.72%（FY2025 1,008,562／8.87%）。role：原列 Major 是在沒有數字的情況下判的，比重一到手即依 R026 的 15% 預設降為 Minor', 'Direct'),
        ('中砂', 'Core', '再生晶圓', 'annual_report_table', '年報營業比重表：再生晶圓 46.22%。Core 理由：⑤本格 46.22% 為單一最大營收線，大於 A1-08 鑽石碟 29.50% 與 G4-01 砂輪及刀具 23.42%。★**不得把 A1-08 與 G4-01 合併成 52.92% 來推翻本格**——R019 明文禁止為取得較大比重而跨節點合併，兩者是不同 Industry node。①年報商品首句「本公司目前之主要商品項目為砂輪、鑽石碟等研磨工具及再生晶圓」是並列列舉，未指主從；④計畫開發首項「2nm 及次世代半導體製程應用之先進鑽石碟」指向 A1-08，屬新製程開發方向而非現有主體', 'Direct'),
        ('昇陽半導體', 'Core', None, 'annual_report_prose', '年報業務範圍載明「矽晶圓」：高階技術開發(清洗製程)。(3)12吋,高平坦度測試晶圓開發。(4)12吋,矽晶圓/碳化矽晶圓載板的開發。(5)6/8吋,高功率寬能隙元件晶。Core 理由：①年報所營業務首句「本公司主要從事半導體晶圓製程服務之研發、製造及銷售，包含提供**晶圓再生**、晶圓薄化製程服務」，晶圓再生列於句首；商品項目表列順序亦為 晶圓再生 → 晶圓薄化 → 晶圓正背面金屬製程；④計畫開發首項為「12 吋，15 奈米**再生晶圓**技術開發」。⑤不可用（比重表未拆）★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
    ],
    "A1-04": [
        ('華旭先進', 'Major', None, 'annual_report_table', '年報「晶圓 22.27%」同時含碳化矽晶圓產品(本節點)與半導體晶圓再生處理(A1-02)，無法拆分故不計比重', 'Direct', 'Too_Coarse'),
        ('IET-KY', 'Core', '磷化銦(InP)磊晶片 + 砷化鎵(GaAs)磊晶片 + 銻化鎵(GaSb)磊晶片', 'annual_report_table', '年報營業比重表：磷化銦(InP) 49.56% 加砷化鎵(GaAs) 26.55% 加銻化鎵(GaSb) 6.09%，三者同為 III-V 化合物磊晶片、同屬本節點，合計 82.20%', 'Direct'),
        ('光環', 'Major', None, 'annual_report_prose', '年報業務範圍載明產品「包含磊晶、元件、次模組、光引擎/AOC 等型式」，並自述建立「從上游磊晶設計製造」的垂直整合能力；磊晶未單獨列示營業比重故不計比重。role 理由：磊晶是年報自述垂直整合鏈的起點（「從上游磊晶設計製造、中游晶粒設計生產、至下游的主動元件的封裝製造」），為自家晶粒競爭力的實質支柱故給 Major，惟不對外貢獻營收故不給比重。★2026-09-07 exposure_type 由 Direct 改 Indirect：磊晶為自家晶粒(A3-06，63.00%)的上游環節而非對外銷售之材料，年報無磊晶對外營收，該營收已計於 A3-06，依 R004 不得重複計入', 'Indirect', 'Too_Coarse'),
        ('全新', 'Core', None, 'annual_report_prose', '年報業務範圍載明「磊晶」：%3.公司目前之商品及服務項目A.異質接面雙載子電晶體(HBT)磊晶片B.假晶高電子遷移率電晶體(PHEMT)磊晶片C.磷化銦異質接面雙載子★2026-09-09 weight_status=Not_Disclosed：比重表只有「自製品收入 100.00」一列，軸是交易性質不是產品。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('嘉晶', 'Core', '磊晶晶片', 'annual_report_table', '年報營業比重表：磊晶晶片 100.00%', 'Direct'),
    ],
    "A1-05": [
        ('新應材', 'Core', None, 'annual_report_prose', '精密特用化學材料 100.00 全歸一項；主力為光阻與顯影剝離液。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報計畫開發之新商品首項「半導體先進微影製程 DUV 光阻劑」，商品項目含半導體光學元件材料與顯示器材料——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('永光', 'Major', None, 'annual_report_prose', '特用化學品 24.00 內含彩色與半導體光阻，年報不細分。★2026-09-06 由 Core 降為 Major：本格是「特用化學品 24.00%」線的一部分（年報不細分彩色光阻與半導體光阻），本身無可辨識比重，而 J7 色料化學品 36.40% 為可辨識的最大線；且致股東報告書列示事業別時「一、色料事業」列首。Core 依 R023 移至 J7。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報載明「持續擴大銷售自有品牌的各類光阻產品，包括：PSPI、低溫製程用 OC 及 BM 光阻劑」——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('達興材料', 'Core', None, 'annual_report_prose', '半導體材料 16.70 未達 20%，依利基題材絕對規模規則收錄★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('揚博', 'Major', None, 'annual_report_prose', '年報供應項目表半導體欄列「BARC、TARC、SOC等黃光製程特用化學品」，本節點定義含 BARC；年報自述「半導體材料事業聚焦於先進封裝及黃光製程相關材料…本公司除提供材料導入與供應服務外,並積極參與客戶製程問題分析與即時解決」，屬代理供應非自製，依 R022 建 Indirect 且不計比重。★2026-09-07 更正，由 Minor 改為 Major：原標 Minor 無任何依據——同一張代理供應表產生的三列（本列、A2-13、B6-02）被標成 Minor／Major／Major，而沒有一列說明差別從何而來（R026② 的典型失效）。重跑後改判 Major，理由如下。role 理由：本列屬揚博年報自列的「代理電子及特用化學品事業」，而年報「產業現況與發展」對該事業的敘述是「半導體材料事業**聚焦於先進封裝及黃光製程相關材料**」——BARC／TARC／SOC 即黃光製程材料，是公司自述的兩個聚焦方向之一；年報並載明「已公開宣布與韓國 DCT 建立**策略合作關係**，作為未來半導體材料業務深化與拓展之重要基礎」，策略資源明確投向此線。依 R026② 之策略重要性，故列 Major，而非附帶品類', 'Indirect', 'Too_Coarse'),
    ],
    "A1-06": [
        ('三福化', 'Core', '精密化學品', 'annual_report_table', '年報營業比重表：精密化學品 71.97%', 'Direct'),
        ('中華化', 'Major', '電子化學品', 'annual_report_table', '年報營業比重表：電子化學品 29.42%。★2026-09-07 由 Core 降為 Major：新增 J10 基礎化學品 45.76% 後候選成為二取一，依 R023 重跑的結果 Core 移至 J10，理由記在該列', 'Direct'),
        ('勝一', 'Core', '溶劑系列產品', 'annual_report_table', '年報營業比重表：溶劑系列產品 83.93%', 'Direct'),
        ('國精化', 'Major', '電子化學材料', 'annual_report_table', '年報營業比重表（合併欄）：電子化學材料 18.00%；年報商品項目對應「b.高速運算高頻電子材料樹脂」「c.光阻材料樹脂」。★2026-09-04 由 Core 降為 Major：先前本格是本公司唯一的曝險，而它只覆蓋 18%，樹脂本體 79% 未建檔；補建 J5 後 Core 依 R023 移至 J5。★本格的落點待查：本節點定義是「酸、鹼、溶劑、蝕刻液、清洗液、特殊化學品」，而這條線的內容是電子用**樹脂**，與 J8 電子化學（電子級化學品）孰宜需另案判定', 'Direct'),
        ('芝普', 'Core', '半導體製程用化學品及材料', 'annual_report_table', '年報營業比重表：半導體製程用化學品及材料 68.40%', 'Direct'),
        ('炎洲', 'Minor', None, 'annual_report_prose', '年報所營業務「(G)半導體生產製程用之化學品、高純度電子級溶劑、客製化配方、全回收電子級溶劑」；產品「(D)電子級高純度化學品代工生產、廢溶劑回收純化再利用、高純度特殊原料溶劑純化、黃光製程配方化學品開發」；產業概況「特化事業部主要經營之半導體與光電相關產業市場…工廠營運狀況穩定,加上近期之自有品牌推廣與全球銷售布局…擴廠計畫持續進行以提升目前產能」。本節點定義含溶劑、清洗液、特殊化學品。營收被併進「其他 7%」無法拆分故不計比重(R020②)；有實際營收故不標 Emerging。role 理由：炎洲的主要經營活動為膠帶、包材與薄膜之塑膠加工，三者合計為公司 Core；特化事業部的電子級化學品未被單獨列示，營收併入年報最小的「其他」一欄，相對於塑膠加工本業屬附帶之經濟地位，故列 Minor。該欄合計 7% 僅作佐證。★年報載明該事業部「擴廠計畫持續進行以提升目前產能」，若日後單獨列示營收須依 R026 重判', 'Direct', 'Too_Coarse'),
    ],
    "A1-07": [
        ('台特化', 'Core', '半導體特氣及精密化學材料', 'annual_report_table', '年報營業比重表：半導體特氣及精密化學材料 52.68%', 'Direct'),
        ('晶呈科技', 'Core', None, 'annual_report_prose', '精密化學品 96.73 全歸一項；主力為電子級特殊氣體。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報產品項目「(1)精密化學品(特殊氣體) 產品包含 C4F8、SiF4、C3F8、C4F6、CH2F2、F2 mixture、SF6」——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('東聯', 'Minor', '氣體事業', 'annual_report_table', '年報營業比重表：氣體事業 7.00%；商品項目「(2) 氧氣、氮氣、氬氣、二氧化碳等氣態與液態氣體產品之製造與買賣」；產業概況「本公司氣體產量除充分供應內部乙二醇(EG)及特化事業各廠所需,氧氣及氮氣並供應林園、大林及大發工業區客戶」。本節點定義首項即氮/氬/氫/氦', 'Direct'),
    ],
    "A1-08": [
        ('中砂', 'Major', '鑽石碟', 'annual_report_table', '年報營業比重表：鑽石碟 29.50%', 'Direct'),
        ('頌勝科技', 'Core', '半導體研磨墊與耗材', 'annual_report_table', '年報營業比重表：半導體研磨墊與耗材 66.92%', 'Direct'),
    ],
    "A1-09": [
        ('光洋科', 'Core', None, 'annual_report_prose', '貴金屬材料 83.58 含靶材製造與貴金屬回收，年報不細分。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報所營業務之主要內容 (1)薄膜濺鍍靶材與蒸鍍材 (2)貴金屬及稀有金屬化學品與材料——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('鑫科', 'Core', '薄膜濺鍍靶材', 'annual_report_table', '年報營業比重表：薄膜濺鍍靶材 8.53%。Core 理由：新增 L10 貴金屬(55.97%)後候選集合改變，依 R023 重跑裁定而非沿用——②價值創造位置在靶材：年報「介電靶材市佔率第一」「國內金屬靶材的領導廠商」，而貴金屬那條線年報自述僅為「純金料及銀料之加工及銷售」，全文未賦予其任何技術、IP 或製程敘述；④長期策略定位同指靶材：114 年研發主軸為「半導體靶材技術建置」、靶材尺寸擴大至 18 吋，集團與中鋼精材垂直整合「建立完整高值靶材供應鏈」；⑤營收 55.97% 支持 L10 但依規則僅為支持證據。★反否決條件已查：靶材營收確實由 571,156 降至 487,604 仟元，但公司未將資源移出——研發與資本支出仍指向靶材，故不屬「歷史遺留或已逐步退出」，A1-09 得保留 Core', 'Direct'),
    ],
    "A1-10": [
        ('光罩', 'Core', None, 'annual_report_prose', '年報無可用營業比重：三條線皆為敘述句碎片（「以外專業光罩製造公司約佔」等），已於 2026-09-02 連同解析缺陷一併清除；公司為專業光罩製造廠', 'Direct', 'Too_Coarse'),
    ],
    "A1-12": [
        ('勤凱', 'Core', '導電漿料', 'annual_report_table', '年報營業比重表：導電漿料 99.38%', 'Direct'),
        ('山太士', 'Core', None, 'annual_report_table', '年報營業比重表：半導體材料 67.80%，為最大營收線；該線併多項產品揭露無法拆分故不計比重(R020②)。年報「公司目前之產品項目」逐項列出本公司自有產品，其中屬本格者：IC 封裝製程材料、FAN OUT 封裝製程用承載膠膜、CoWoS 清針片、TGV 製程材料、TBDB 製程材料、固晶接著材料、半導體製程用保護膜、UV 切割膠膜、抗翹曲膠膜。Core 理由：⑤半導體材料 67.80% 遠大於光學材料 30.34%（該線掛 E2-02 Major）；①產業概況自述「2020 年後更致力於半導體及封測製程中各式製程膠膜及相關材料,並配合客戶需求合作開發及客製化新製程用膠膜」。★2026-09-06 曾由 Core 降為 Major，理由是當時引用的證據取自產業概況段落而非公司自述產品；2026-09-07 改以年報商品項目為據回復 Core——當初錯的是證據來源，不是本格不成立', 'Direct', 'Too_Coarse'),
        ('昇貿', 'Core', '錫棒、錫球、錫絲', 'annual_report_table', '年報營業比重表：錫棒、錫球、錫絲 67.00%', 'Direct'),
        ('長華*', 'Major', '封膠樹酯', 'annual_report_table', '年報營業比重表：封膠樹酯 29.29%', 'Direct'),
        ('利機', 'Minor', None, 'annual_report_prose', '年報商品項目「(3)綠能及其他產業: A.自製銀產品 網印銀漿(Screen Printing Paste)、客製化銀漿、燒結銀漿(Sintering Ag Paste)、導電/導熱銀漿」；所營業務「(4)奈米技術相關材料之製造與銷售」；年報另單獨繪製「製造事業部」上中下游圖。本節點定義即含 Silver Paste。依 R022 通路性質不外溢，自有製造業務線建 Direct。營收併於「其他產品 2%」內無法拆分故不計比重。role 理由：利機的主要經營活動為半導體與光電產品之代理通路，兩者為年報前兩大營收線；自製銀漿屬年報「(3)綠能及其他產業」項下，營收併入三欄中最小的「其他產品」，是公司在通路本業之外的自製材料業務，相對於主要經營活動屬附帶之經濟地位，故列 Minor。該欄合計 2% 僅作佐證', 'Direct', 'Too_Coarse'),
    ],
    "A1-13": [
        ('百容', 'Core', '半導體', 'annual_report_table', '年報營業比重表：半導體 49.00%，為導線架與 IC 沖壓件。Core 理由：最大營收線且為公司的製造本業(R023①)', 'Direct'),
        ('一詮', 'Core', '導線架(LED/SMD)', 'annual_report_table', '年報營業比重表：導線架(LED/SMD) 32.15%', 'Direct'),
        ('界霖', 'Core', 'TO導線架', 'annual_report_table', '年報營業比重表：TO導線架 29.19%', 'Direct'),
        ('長科*', 'Core', 'IC導線架', 'annual_report_table', '年報營業比重表：IC導線架 69.98%', 'Direct'),
        ('長華*', 'Core', 'IC導線架', 'annual_report_table', '年報營業比重表：IC導線架 48.91%。Core 理由：①年報商品項目表 IC 封裝類別下順序為 **導線架(Lead Frame)** → 封膠樹酯(EME) → 記憶體基板 → 覆晶基板 → 導電膠，導線架列首；④計畫開發首項為 IC 封裝類別下的新材料新設備開發；⑤本格 48.91% 大於 A1-12 封膠樹酯 29.29%。★自述稱「本公司為專業半導體封裝材料、設備及面板材料**通路商**」，但同句載明「合併營收主係來自子公司長華科技…子公司主要從事**金屬導線架之生產及銷售**」——依 R022，具名製造品且有自有產能者建 Direct，此即 R022 規則本文引用之案例', 'Direct'),
        ('順德', 'Core', '導線架及電子沖壓件', 'annual_report_table', '年報營業比重表：導線架及電子沖壓件 85.00%', 'Direct'),
    ],
    "A1-14": [
        ('九豪', 'Core', None, 'annual_report_prose', '年報業務範圍載明「陶瓷基板」：肆、營運概況一、業務內容(一)業務範圍1.所營業務之主要內容(1)氧化鋁陶瓷基板之製造、加工及買賣業務。(2)陶瓷構裝基板之製造、設計、加工★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('同欣電', 'Major', '陶瓷電路板', 'annual_report_table', '年報營業比重表：陶瓷電路板 17.56%', 'Direct'),
        ('立誠', 'Core', None, 'annual_report_prose', '年報業務範圍載明「陶瓷基板」：製造及銷售,位於LED產業供應鏈中上游位置,其透過金屬表面處理製程技術應用於陶瓷基板,與上游基板廠及其他零組件公司密切合作,開發製造高品質產★2026-09-09 weight_status=Not_Disclosed：比重表的軸是應用別（車用照明 74.40／高功率照明 7.96）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('佳總', 'Major', None, 'annual_report_prose', '年報商品項目 (3)DBC 氧化鋁/氮化鋁覆銅陶瓷基板，本節點定義即含 DBC 與氧化鋁/氮化鋁陶瓷基板（AMB 屬計畫開發之新商品，不計入）。原掛 V7-01 電子製造服務，但其證據「線路留存在印刷電路板上,成為連接所有電子元件的基板」是年報在描述印刷電路板這個產業，全份材料無任何自有組裝業務敘述。併於「印刷電路板 100.00%」一列故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "A2-01": [
    ],
    "A2-03": [
        ('川寶', 'Major', None, 'annual_report_prose', '年報半導體部門敘述含「PEALD 原子層沉積設備」，研發成果表 111 年度列「電漿輔助原子層沉積及蝕刻雙功能機台」，屬**新機製造**。★半導體部門 68.97% 一線橫跨 A2-03（PEALD 沉積設備）／A2-09（EFEM）／A2-10（關鍵零組件）／A2-15（設備再生）四格，年報無金額拆分故各格皆不計比重(R020②)；role 依 R026 判，無 weight 的 Direct 曝險以 Major 為預設，不得由 Core 反推其餘各格為 Minor。★2026-09-08 由 Core 降為 Major(R023-R)：原 Core 理由是「本格是該部門年報具名的第一項（半導體沉積 PECVD 製程設備再生、PEALD 原子層沉積設備）」——而 A2-15 建立後，該「第一項」所指的設備**再生**已經有自己的節點，本格只剩 PEALD 新機。理由所依據的那一項不在本格了，理由本身因此失效，不是 Core 換了個更好的候選而已。★原掛 A2-01 曝光／光刻設備係更早的誤置：該部門不含曝光設備，公司的曝光機(LDI)在 PCB 部門，見 B5-01。Core 依 R023 移至 A2-15。', 'Direct', 'Too_Coarse'),
        ('天虹', 'Core', '半導體主設備 + 半導體技術服務', 'annual_report_table', '年報營業比重表（114 年度印出比重）：半導體主設備 31.00% 加半導體技術服務 31.79%，服務對象為自家設備。Core 理由：①年報主要商品項目順序為 A.**物理氣相沉積設備(PVD)** B.**原子層沉積設備(ALD)** C.晶圓鍵合機及解鍵合機 D.EUV光罩護膜檢測 E.半導體設備零備件及相關服務，薄膜沉積設備列前二；④計畫開發亦為「在自製設備上也將從**薄膜區**跨足到蝕刻區」，以薄膜為現有基礎；⑤本格 62.79%（主設備 31.00 加技術服務 31.79，服務對象為自家設備）大於 A2-10 零組件 29.44%★2026-09-15 M5：權重改採年報印出比重；表列四項印出比重合計 99.48%，另 0.52% 為表外營收，故不以表列金額合計為分母（原金額推算 31.16／32.03／29.60）', 'Direct'),
    ],
    "A2-05": [
        ('弘塑', 'Major', '機台設備產品', 'annual_report_prose', '年報僅列「機台設備產品 69.86%」一線。★比重不計：同一條線橫跨前段(本節點)與後段(A2-12)，年報未給拆分依據(R020②)。這個 NULL 在 2026-09-04 A2 邊界釐清後仍然成立，但原因是這家公司真的兩段都做而年報只給一條線（filing 粗），不是 taxonomy 重疊。★現行 evidence 原寫「年報自述濕製程設備同時供應晶圓廠與先進封裝產線」，該自述在 FY2025 年報中找不到對應句（見 filing_pages_matching），依 RULING_B4_A 認定為 stale evidence citation 並撤除，不得繼續支撐 Core。本列成員資格改記可查證的依據：產品競爭情形列有「(1) 前段酸槽設備:由於本土設備商發展較晚,台灣市場目前幾乎為日本業…」（印刷頁 p48），即公司確實把前段酸槽設備列為自己的產品競爭項目，故不移除本列。★role 由 Core 改為 Major：同段並載「率超過九成。本公司主要著墨於 Wafer Reclaim 等中後段製程。」，此句正是本列不再為 Core 的理由；Core 依 R023①＋R023-R 移至 A2-12。', 'Direct', 'Too_Coarse'),
        ('辛耘', 'Major', None, 'annual_report_prose', '年報比重表僅列會計科目無產品線；自製濕製程設備。本列的成員資格依據改記為年報自己的應用列示：「(A) 批次式濕式製程設備,應用於…」與「(B) 單晶圓濕式製程設備,應用於…」兩款皆含「2. 半導體前段製程…」（皆印刷頁 p66），故不移除本列。比重不計：2026-08-27 法說 p.33 自製設備 1H2026 2,562,617 仟元／41.90% 這條線同時橫跨本節點與 A2-12 封裝設備，簡報未再拆分(R020②)。★weight_status=Not_Disclosed 的判準同 A2-12 列。★現行 evidence 的 ① 以「A2-12 定義只列 Die Bonder／Wire Bonder／Molding／Singulation／TCB、而辛耘無任一具名品項」為由排除 A2-12，該理由自 2026-09-04 A2-12 改寫後失效，已刪除。★role 由 Core 改為 Major：Core 依 R023④＋R023-R 移至 A2-12。', 'Direct', 'Not_Disclosed'),
        ('博磊', 'Minor', None, 'annual_report_prose', '年報商品項目 (6)晶圓、基板半自動、全自動清洗機，其中**晶圓**清洗機屬本節點（前段晶圓製造）；同項的基板清洗機服務後段封裝，依 A2 的製程階段界線歸 A2-12。★比重不計：併於設備產品 65.07% 揭露，年報未給拆分依據(R020②)。role 理由：年報自述主力產品為測試產品、切割機及植球機，清洗機不在其列', 'Direct', 'Too_Coarse'),
    ],
    "A2-08": [
        ('光焱科技', 'Minor', '晶圓級光電檢測', 'annual_report_table', '新列的依據：「晶圓級光電檢測儀器是一款高精度、高靈敏度、高速度的設備,可直接在多種光學影像感測器上進行晶圓級的光電參數檢測…」與「晶圓級量子效率測試設備、晶圓級斜向入射光量測設備…」（皆印刷頁 p62）；交付物為晶圓階段的光電參數檢測設備整機。weight 10.750 取自營業比重表的晶圓級光電檢測一格，該格為公司自身揭露的獨立營收線。role 依 R026①（Direct 且有 weight，純數值判定）：10.75% 低於 15%，故為 Minor，毋須另附理由。', 'Direct'),
        ('聯策', 'Major', None, 'annual_report_prose', '新列的依據（皆為公司自身的階段與功能敘述，不是品項名稱）：「針對半導體前段與後段製程,本公司提供晶圓級 AOI 檢測機,…」（印刷頁 p72）；「本公司產品應用範疇已由高階印刷電路板(PCB)與 IC 載板產業,全面拓展並成功切入半導體前段晶圓檢測(Wafer AOI)與後段先進封裝自動化領域。」（印刷頁 p73）；主要產品重要用途表逐項寫「包含符合國際 SEMI S2 與F47認證之前段晶圓外觀檢查機、晶圓分選機,以及具備預測性耗損監控之 CMP 研磨墊AI辨識系統…」（印刷頁 p86）。weight 為 NULL 是因為這三種對象併在同一條「AI機器視覺設備 505,439 31.70 856,954 41.17…」線內（印刷頁 p72）；weight_status 取 Too_Coarse：同時涵蓋的另兩個節點可具名（B5-02、A2-13）。★不得為了新增本列而把 41.17% 的一部分估給它（RULING_B7_D）。role 依 R026③：Direct 且 weight 為 NULL 者 Major 為 default，本案無附屬業務之 evidence——公司把前段晶圓檢測寫進產業概況的營運主軸敘述，非附帶業務。', 'Direct', 'Too_Coarse'),
        ('華洋精機', 'Core', '影像檢測設備', 'annual_report_table', '年報營業比重表：影像檢測設備 72.05%', 'Direct'),
    ],
    "A2-09": [
        ('川寶', 'Major', None, 'annual_report_prose', '年報半導體部門敘述含「EFEM 自動化生產製造」，集團產品線列「晶圓傳送模組(VTM, EFEM)」，研發成果表 111 年度「多載台晶圓級傳送設備前端模組」。★半導體部門 68.97% 一線橫跨 A2-03（PEALD 沉積設備）／A2-09（EFEM）／A2-10（關鍵零組件）／A2-15（設備再生）四格，年報無金額拆分故各格皆不計比重(R020②)；role 依 R026 判，無 weight 的 Direct 曝險以 Major 為預設，不得由 Core 反推其餘各格為 Minor。', 'Direct', 'Too_Coarse'),
        ('創新服務', 'Core', '半導體自動化設備', 'annual_report_table', '年報營業比重表：半導體自動化設備 98.26%', 'Direct'),
        ('均豪', 'Core', '半導體製程設備', 'annual_report_table', '年報營業比重表：半導體製程設備 78.83%。Core 理由（2026-09-07 因新增 E7-01 使候選成為二取一而依 R023 重跑，依序判定）：①該公司無 tw.product_narrative 的商品項目章節，核心產品能力無自述可據，①不定案。②價值創造位置：兩個候選是同一套製程設備能力用於半導體與顯示器兩個下游，半導體側 78.83% 為顯示器側 10.20% 的 7.7 倍，足以排除顯示器側的競爭解釋，②於此定案。⑤同向，僅作支持證據。★**本裁定為暫定**：本格的節點與品項不相符（品項是「半導體製程設備」而 A2-09 的定義是晶圓搬運／自動化），候選集合裡的這一格本身待更正；更正後須重跑（已記入 tmp/rejudge 缺口清單）', 'Direct'),
        ('家登', 'Core', '光罩載具品', 'annual_report_table', '年報營業比重表：光罩載具品 42.00%', 'Direct'),
        ('盟立', 'Core', '半導體智能自動化系統', 'annual_report_table', '年報營業比重表：半導體智能自動化系統 58.18%。Core 理由（2026-09-07 因新增 E7-03 使候選成為二取一而依 R023 重跑，依序判定）：①年報同一章的兩處自述互相矛盾——「業務範圍」敘述段以「**光電面板**智能自動化系統、半導體智能自動化系統、新物流…」為序，而「業務之主要內容」條列則以 (1)半導體智能自動化系統列首、(3)才是光電面板；①無區辨力，不定案。②價值創造位置：兩個候選是同一套智能自動化系統整合能力用於兩個下游，其毛利與工程資源所在由兩側規模決定，半導體側 58.18% 為光電面板側 10.08% 的 5.8 倍，足以排除面板側的競爭解釋，②於此定案。⑤同向，僅作支持證據', 'Direct'),
        ('迅得', 'Core', '晶圓廠(半導體)自動化設備-FBU', 'annual_report_table', '年報營業比重表：晶圓廠(半導體)自動化設備-FBU 43.59%；年報商品項目載明該 BU 為「晶圓廠自動化物流系統、自動倉儲系統、智能軌道車系統、自走車系統、移動式機器人」，即本節點定義的 Robot／Stocker／AMHS。Core 理由（2026-09-07 依 R023 的**順序**判定重跑；前版以「①⑤兩項對④一項」的計票方式裁定，而 R023 明文是依序判定、⑤只作支持證據，計票法本身不合規）：①核心產品能力無區辨力——IBU／BBU／FBU 三者是同一套產線自動化與整廠物流能力用於三個下游，年報商品項目表以 IBU 列首是表格編排而非能力主從；②價值創造位置：FBU 43.59% 對 IBU 55.94% 僅差 12.35pp，不足以排除任一側的合理競爭解釋，②不定案；③不適用（三 BU 無上下游載體關係）；④長期策略定位定案——年報「計劃開發之新商品」四項中前三項全部指向半導體側：高階半導體用晶圓機器人(針對 CoWoS 及晶圓級封裝)、次世代 OHT(「優化**半導體廠房**上部空間之物流效率」)、智慧型跨樓層高速提升機(「實現**晶圓盒(FOUP)**在不同樓層間的自動化銜接」)，僅第四項 ESG 節能型厚重板自動化收放系統屬 PCB 側。研發資源明確指向本節點，Core 由 B5-03 移入。★⑤方向相反(IBU 較大)但依 R023 只作支持證據，不得單獨推翻已定案的④', 'Direct'),
    ],
    "A2-10": [
        ('公準', 'Core', None, 'annual_report_prose', '新列的依據：「半導體類:半導體製程所需之微影製程設備之關鍵零組件、其良率檢測設備之關鍵零組件及 IC 封裝有關之工模夾治具等。」（印刷頁 p49）；交付對象為半導體設備製造商，符合本節點的交付對象與交付物兩項要件。公司在價值鏈上的位置是加工與組裝端，上下游圖把本公司一格寫成「產品/製程設計、精密…」（印刷頁 p51），下游是設備類客戶。weight 不沿用 62.270 的理由：同一條線含工模夾治具，該成分被本節點定義明文排除且全樹無落點，也不做任何拆分、扣除或估算。★★依 RULING_B11_A，本欄必須明載：這個 Granularity_Insufficient **不得被解讀為 62.27% 全部屬 A2-10**，它只表示已確認存在 A2-10 成分、整線金額不可拆。Core 為既有 Core 列的節點更正（R019(1) 恆等式，依 R023-R 於 2026-09-22 taxonomy 變動後重驗），不是重新選 Core；weight 轉為 NULL 不影響 Core，R019 的候選資格不以 weight_pct IS NOT NULL 為條件。', 'Direct', 'Granularity_Insufficient'),
        ('川寶', 'Major', None, 'annual_report_prose', '年報半導體部門敘述含「半導體零組件」，集團產品線列「關鍵零組件耗材」，研發成果表 111 年度「電漿射頻偏壓耦合匹配控制器」、112 年度「先進製程表面光學鍍膜零件」。★半導體部門 68.97% 一線橫跨 A2-03（PEALD 沉積設備）／A2-09（EFEM）／A2-10（關鍵零組件）／A2-15（設備再生）四格，年報無金額拆分故各格皆不計比重(R020②)；role 依 R026 判，無 weight 的 Direct 曝險以 Major 為預設，不得由 Core 反推其餘各格為 Minor。★同部門另有「設備零件代理銷售」一項，依 R022 通路產品線不得認定製造歸屬，未建曝險。', 'Direct', 'Too_Coarse'),
        ('有成精密', 'Core', '半導體', 'annual_report_table', '年報營業比重表：半導體 54.08%，為半導體設備精密零組件。Core 理由：兩候選中半導體為較大營收線，且公司的加工技術與客戶關係建立在設備零組件上(R023①③)', 'Direct'),
        ('慶康科技', 'Core', '半導體設備零組件', 'annual_report_table', '年報營業比重表：半導體設備零組件 100%（資料由年報業務範圍表格補得）', 'Direct'),
        ('京鼎', 'Core', '關鍵性零組件', 'annual_report_table', '年報營業比重表：關鍵性零組件 52.01%，另有半導體設備及系統組裝 45.52%，同屬本節點', 'Direct'),
        ('千附', 'Major', '精密零組件收入', 'annual_report_table', '年報營業比重表：精密零組件收入 54.21%，大於其工程收入 38.39%（見 A7-01）。★2026-09-06 由 Core 降為 Major：⑤指向本格（精密零組件 54.21%），但①④皆指向廠務系統——年報業務範圍首句為「本公司經營業務主要包含**廠務系統工程**、鞋機生產銷售、精密零組件整合製造等三大業務」，廠務列首；商品項目順序亦為 (1)廠務系統工程 (2)鞋機生產銷售 (3)千附精密；④計畫開發首項為「(1)先進廢氣處理設備及高科技廠房**廠務整改工程**」。Core 依 R023 移至 A7-01', 'Direct'),
        ('建暐', 'Core', '其他(精密零組件)', 'annual_report_table', '年報營業比重表：其他(精密零組件) 30.00%。Core 理由：①年報所營業務順序為 (1)**電子零組件製造業** (2)模具製造業 (3)一般及光學儀器製造業 (4)機械設備製造業，電子零組件列首；⑤本格 30.00% 大於 A2-13 量測儀器類 26.00%。★年報比重表第一項為「精密模具」，該線目前無任何節點認領，屬待補', 'Direct'),
        ('千附精密', 'Core', '光電及半導體設備零組件', 'annual_report_table', '年報營業比重表：光電及半導體設備零組件 72.18%', 'Direct'),
        ('台灣精材', 'Core', '陶瓷', 'annual_report_table', '年報營業比重表：陶瓷 69.60%', 'Direct'),
        ('大銀微系統', 'Core', '微米與奈米級定位系統', 'annual_report_prose', '微奈米定位系統 52.58，半導體設備用精密運動元件', 'Direct'),
        ('天虹', 'Major', '半導體客製化零組件', 'annual_report_table', '年報營業比重表：半導體客製化零組件 29.44%★2026-09-15 M5：權重改採年報印出比重（原金額推算 29.60%，表外營收 0.52% 未列入表列合計）', 'Direct'),
        ('宏碩系統', 'Core', 'AI液冷零組件', 'annual_report_table', '年報營業比重表：AI液冷零組件 60.05%', 'Direct'),
        ('家碩', 'Core', None, 'company_site', '機器設備 88.42 不指明品類；晶圓廠製程設備★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('意德士', 'Core', '半導體零組件', 'annual_report_table', '年報營業比重表：半導體零組件 88.13%', 'Direct'),
        ('日揚', 'Core', None, 'annual_report_prose', '年報僅分商品與勞務；真空設備與零組件。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報所營業務之主要內容「標準真空零組件及真空閥設計、製造及銷售」「客製化各式真空鍍膜腔體、模組與系統設計製造」——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Disclosed：比重表的軸是交易性質（商品銷貨收入 61／勞務收入 38）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('旭暉應材', 'Core', 'AI液冷零組件暨真空微波管與半導體設備零組件', 'annual_report_table', '年報營業比重表：AI液冷零組件暨真空微波管與半導體設備零組件 73.55%', 'Direct'),
        ('明遠精密', 'Core', '半導體設備技術服務 + 半導體設備子系統銷售', 'annual_report_table', '年報營業比重表：半導體設備技術服務 55.94% 加半導體設備子系統銷售 44.06%；年報自述業務為「製程設備電子副系統技術服務及銷售」，兩線是同一門生意的兩種收入認列，合計 100.00%', 'Direct'),
        ('晟田', 'Core', '航太零組件(客供料)', 'annual_report_table', '年報營業比重表：航太零組件(客供料) 48.60%', 'Direct'),
        ('瑞耘', 'Core', '半導體零組件', 'annual_report_table', '年報營業比重表：半導體零組件 100.00%', 'Direct'),
        ('翔名', 'Core', '材料類產品', 'annual_report_table', '年報營業比重表：材料類產品 83.90%', 'Direct'),
    ],
    "A2-11": [
        ('世禾', 'Core', '精密設備潔淨再生服務收入', 'annual_report_table', '年報營業比重表：精密設備潔淨再生服務收入 93.40%', 'Direct'),
        ('德揚', 'Core', '零件清洗收入', 'annual_report_table', '年報營業比重表：零件清洗收入 54.69%', 'Direct'),
        ('科嶠', 'Major', None, 'annual_report_prose', '年報所營業務主要內容：「提供半導體晶圓 FouP、先進封裝載板 FouP 清洗設備」；商品說明「其他機台」含「載板載具清洗機」；計畫開發之新商品列「半導體晶圓 FOUP 高速清洗搬運方法」「半導體封裝 Frame cassette 自動清洗設備」「半導體封裝 Magazine 自動清洗設備」。併於「其他機台 33.13%」一列故不計比重(R020②)。★節點選擇說明：A2-05 清洗設備的定義是晶圓濕製程清洗(Wet Cleaning／Single Wafer／Batch)，不含載具清洗；本節點目前無 Product Definition，選它是因為名稱「零件清洗／再生」對 FOUP／載具清洗設備最接近', 'Direct', 'Too_Coarse'),
    ],
    "A2-12": [
        ('博磊', 'Core', None, 'annual_report_table', '年報營業比重表：設備產品 65.07%。商品項目 (5)晶圓、基板切割機、(8)植球機及植球機治具零件——切割與植球皆為本節點定義具名的後段封裝設備；同條線另含 (6)晶圓、基板半自動、全自動清洗機，其中基板清洗屬本節點。★weight_status 由 Too_Coarse 改為 Granularity_Insufficient（RULING_B1_A）：該 65.07% 線另含兩項非 A2-12 交付物——「主要用於監控工廠內可燃氣體、有毒氣體或氧氣等…」、「用於半導體、製藥及化工產業」與「主要用於量子電腦的超低溫冷卻與控制系統…」（皆印刷頁 p82）——兩者在全樹皆無具名節點，故本列不屬「橫跨數個已具名節點」的粗粒度，而是 R020④ 證據不足以計算本節點權重。Core 理由：R023①，公司自述「目前主力產品以測試產品、切割機及 IC 封裝植球機為主」三者有二在本節點（印刷頁 p60）；⑤ 只作支持，Core 不依賴該未拆分整線，故權重狀態改為 GI 後唯一勝者不變。', 'Direct', 'Granularity_Insufficient'),
        ('印能科技', 'Core', '氣動與熱能製程解決方案', 'annual_report_table', '年報營業比重表：氣動與熱能製程解決方案 73.26%；先進封裝翹曲控制製程設備。★本輪補記 73.260 維持計入本節點的正面依據：主要產品重要用途表把這條線的用途界定為「藉由這樣的核心技術我們進一步從提供消除製程氣 氣動與熱能製 泡的解決方案擴展至提供…」之後的三項——封裝材料翹曲抑制、無氣泡高溫熔錫、高功率與高效能封裝晶片散熱（皆印刷頁 p48）；其核心技術界定為「核心技術是「高壓與低壓氣體在高溫與低溫下的調和運用」…」（印刷頁 p48）。同表另一列把 16.49% 那條線界定為「主係區域型自動化物料搬運系統,藉由烘烤站的印能除泡 自動化系統解 設備,搭配適用於封裝廠的新型天車系統與韌體,解決封 決方案 裝廠自動化的難題。」（印刷頁 p48），與本節點的封裝製程設備是不同的東西。★沒有證據能證明 A2-13 的老化測試機營收一定落在 73.260% 內，故不跨 node 推定，本列 weight 不改（RULING_B6_A）。★2026-09-07 已更正 evidence_kind 為 annual_report_table。Core 理由（R023①）：A2-12 與 A2-13 同屬後段，分界不是製程段而是**功能**（A2 母節點明訂「跨階段的界線是設備服務哪一段製程」而「階段之內再依功能／製程單元細分」）。主要產品重要用途表把「氣動與熱能製程解決方案」的用途逐項寫成消除製程氣泡、封裝材料翹曲抑制、無氣泡高溫熔錫與高功率與高效能封裝晶片散熱四項，**四項都是封裝製程端的功能，沒有一項是老化或測試**；而 A2-12 的定義具名翹曲控制。② 上下游一節把本公司放在設備供應商位置，兩種功能的價值創造位置相同，分不出方向。③ 兩者都是設備製造。④ 計畫開發之新商品三項（半導體製程解決方案、自動化生產解決方案、專利佈局）全指向封裝製程與封裝產線自動化，未指向測試。⑤ 只作支持：三條線中只有這一條被用途表逐項界定，而老化測試機這一項在任何一條線內都沒有被指認金額。', 'Direct'),
        ('單井', 'Core', '光電、半導體自動化設備類(SET)', 'annual_report_prose', '年報營業比重表：光電、半導體自動化設備類(SET) 8%（本輪撤下計重）。★現行 evidence 記的 weight 依據是封裝模具類 27% 加自動化設備類 8% 的合併值，應改記：封裝模具不屬本節點（A2-12 收設備；2026-09-22 條款的三分法為整機、零組件、結構件與模組，模具皆非；A2-10 亦明文排除模具），合併的前提因此消失。本格的成員資格依據只有自動化設備類一條——「LEADFRAME 自動排片機、自動切彎腳成型機、 LED 自…」、「 SMD 自動包裝機、自動點膠機…」、「自動雷射印碼機、自動料片分裝機、P/H 雷射/測試一…」（皆印刷頁 p50）；而該條同時涵蓋 A2-13 的測試設備，年報未列各品項金額，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★Core 理由改記為 R023① 的能力宣稱（印刷頁 p2），移除原有的揭露先後與比重比較兩項；⑤ 只作支持，故 weight 改為 NULL 後唯一勝者不變（RULING_B4_B）。★另一列「其他(註)」33.00% 為年報合併揭露，註明「包含出售模具、設備零組件及修繕等收入」，三者分屬不同性質且無金額拆分，依 R020② 不計入任何節點之 weight；此為年報揭露顆粒度造成的 evidence gap，非解析遺漏，故載明而不認領。', 'Direct', 'Too_Coarse'),
        ('均華', 'Core', '半導體製程設備及相關精密模具', 'annual_report_table', '年報營業比重表：半導體製程設備及相關精密模具 93.90%（本輪撤下計重）；黏晶與先進封裝製程設備。撤重的依據：「半導體製程設備 2,294,427 93.96 2,526,471 93.90 及相關精密模具…」一格（印刷頁 p57）的品項含三類——機台、A2-13 具名功能的檢測與雷射機台、以及機台用模具「(E)自動封膠機用模具(Mold Chase)…」與「(F)沖切成型機用模具與精密元件(Die Set /kit)…」（皆印刷頁 p57，模具在全樹無落點）；年報未列各品項金額，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★不套 GI，因為跨的是兩個已具名、可確認的節點（RULING_B5_A）。Core 理由（R023①）：「均華在精密取放設備長期耕耘,關鍵核心技術為精密取放(Pick and Place)、精密加工與光電整合及AOI。」與「本公司推估在 IC 晶粒挑撿機方面約有八成,在黏晶機上約有一成,在沖切成型機上約有一成。」（皆印刷頁 p68）。★2026-09-07 已更正 evidence_kind 為 annual_report_table。', 'Direct', 'Too_Coarse'),
        ('弘塑', 'Core', '機台設備產品', 'annual_report_prose', '市場占有率一節載「本公司產品主要為半導體後段封裝濕式設備,目前專攻於 200 至 300mm 之…」與「其酸槽設備(Wet Bench)在 12 吋晶圓級封裝中在台灣佔有率為六成,…」（皆印刷頁 p49）；主要產品之重要用途一節載「提供半導體 IC 封裝製程之濕式設備。濕製程機台包括清洗、顯影、蝕刻、…」（印刷頁 p54）；產品競爭情形第二項載「(2) 晶圓級封裝產業設備:本公司為主要的設備供應商,尤其在錫鉛凸塊…」（印刷頁 p48）。依 A2 分支的製程階段界線，封裝端的濕製程與清洗設備屬本節點（2026-09-04 起本節點定義明文涵蓋封裝／基板用濕製程與電漿清洗設備）。★比重不計：同一條「機台設備產品 69.86%」線橫跨前段(A2-05)與後段(本節點)，年報未給拆分依據(R020②)。Core 理由即上列 p48／p49／p54 原文（R023①，2026-09-28 RULING_B4_A）。★原 evidence 首句「年報自述其濕製程設備同時供應晶圓廠與先進封裝產線」在 FY2025 年報找不到對應句（見 filing_pages_matching），依 RULING_B4_A 認定為 stale evidence citation，**整句撤除**，不再作為本列任何部分的依據；本列的 membership 與 Core 依據已全部改由上述當期可查證原文重建。', 'Direct', 'Too_Coarse'),
        ('志聖', 'Major', '半導體製程設備', 'annual_report_prose', '新列的依據：「晶圓級乾膜貼合設備、暫時貼合設備:針對超薄晶圓提供精準溫控與壓力管理…」與「混合鍵結製程設備:實現微米級對位與無縫接合…」（皆印刷頁 p78），分別對應 A2-12 逐字具名的暫時貼合／剝離、熱壓鍵合與翹曲控制；解決晶圓翹曲的溫控壓力管理對應具名的翹曲控制。weight 為 NULL：同一條「半導體製程設備 40.00%」線（年報營業比重表）還含 A2-13 的測試燒機爐，年報未給機種金額，依 R020② 不可單獨計入本節點。role 依 R026③（Direct 且 weight 為 NULL，無附屬業務 evidence）為 Major。', 'Direct', 'Too_Coarse'),
        ('揚博', 'Major', None, 'annual_report_prose', '新列的依據（皆為代理供應項目表的逐字用途敘述，不是品項名稱）：「晶圓封裝設備(bonder) 晶片(die to die)/晶圓(wafer to wafer)鍵合設備…」與「植球機(ball ounter) IC封裝基板用植球設備…」（皆印刷頁 p52）。兩者對應 A2-12 具名的熱壓鍵合與植球，不是測試或檢測設備。exposure_type 依 R022：屬代理供應非自製，不得建製造型 Direct。weight 為 NULL 是因為代理與自製混在同一條機器設備收入線內（R020②）。role 理由（R026②，Indirect 不適用數值預設）：先進封裝製程設備與同表的檢測設備同屬公司兩大事業之一「代理電子及特用化學品事業」在半導體側的主要品類，非設備本業的附帶配套，對公司整體經濟具實質重要性，故列 Major；機器設備收入占比 60.30% 僅作佐證。★本列為新增 exposure，不是把原有 A2-13 exposure 改成 A2-12（RULING_B9_C）。', 'Indirect', 'Too_Coarse'),
        ('梭特', 'Core', 'IC半導體設備', 'annual_report_table', '年報營業比重表：IC半導體設備 88.24%（本輪撤下計重）。撤重的依據：「IC半導體設備 332,944 72.53 460,374 88.24…」一格（印刷頁 p55）同時含 A2-12 的分選機／固晶機與 A2-13 的 AOI 設備，年報未列各品項金額，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★不套 GI，因為跨的是兩個已具名、可確認的節點（RULING_B5_A）。Core 理由（R023①）：「本公司以Pick & Place 核心技術為發展基礎,深耕分選機(Mapping Sorter)與固晶…」機設備市場（印刷頁 p6）。', 'Direct', 'Too_Coarse'),
        ('竑騰', 'Core', '主製程設備', 'annual_report_table', '年報營業比重表：主製程設備 61.87%；先進封裝製程設備。★本輪補記 61.870 的內容界定：比重表的「主製程設備 651,780 56.94 1,150,325 61.87…」一格（印刷頁 p40），其內容由公司自己定義為「本公司主製程設備涵蓋客製化先進封裝應用,提供精密點膠、植均熱片或Stiffener及可程式自動化壓合設備…」（印刷頁 p2），全部落在本節點，沒有跨到 A2-13；商品項目一節同樣兩類分列「本公司營業項目為半導體自動化設備之研發、製造及銷售等業務,主要產品包括點膠植散熱片(植stiffener)機、AOI 檢測機、自動交換機及治具等。」（印刷頁 p40）。★補記這一格為何**不**改 Too_Coarse：檢測設備有自己的營收線，沒有混在本格內（RULING_B6_B）。★2026-09-07 已更正 evidence_kind 為 annual_report_table。Core 理由（R023①）：A2-12 與 A2-13 同屬後段，分界是**功能**（A2 母節點明訂階段之內再依功能／製程單元細分）。決定層的依據是公司對自己兩類產品的**定義句**：主製程設備被界定為「本公司主製程設備涵蓋客製化先進封裝應用,提供精密點膠、植均熱片或Stiffener及可程式自動化壓合設備…」，三項（點膠、貼合、壓合）都是 A2-12 具名或同義的封裝製程功能；同節另段把視覺檢測設備界定為適用於各封裝製程中及最終成品檢測。② 上下游一節把本公司放在設備製造端、零組件向協力廠商採購，兩類功能的價值位置相同。③ 兩者都是設備製造。④ 未來計畫兩類並進（設備精度／新功能與視覺技術各一節），方向不單一。⑤ 只作支持：本案兩條線都各自揭露，但**比重高低依 R023⑤ 不作決定**——決定層落在 ①，與 61.87% vs 5.43% 的金額大小無關。', 'Direct'),
        ('萬潤', 'Core', '半導體設備', 'annual_report_table', '年報營業比重表：半導體設備 96.13%（本輪撤下計重）；點膠與封裝製程設備供應先進封裝產線。撤重的依據：比重表唯一一格「半導體設備 96.82% 96.13% 96.32%…」（印刷頁 p58）同時涵蓋 A2-12 的封裝製程設備與 A2-13 的後段檢測設備，公司自述「營業範疇涵蓋檢測、封裝及製程自動化設備…」（印刷頁 p65），年報未列各品項金額，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★不套 GI，因為跨的是兩個已具名、可確認的節點（RULING_B5_A）。Core 理由（R023①）：「已於近幾年成功轉型為高階的半導體後段製程設備供應商,主要提供半導體先進封裝所需的點膠、貼合、熱壓等自動化高精密設備以及AOI檢測設備…」（印刷頁 p59）。★2026-09-07 已更正 evidence_kind 為 annual_report_table 並更正原文誤植的 96.82%（年報實為 96.13%）。', 'Direct', 'Too_Coarse'),
        ('辛耘', 'Core', None, 'annual_report_prose', '年報商品項目 A.自製設備列有「暫時性貼合及剝離設備 TBDB」，屬本節點定義具名的後段封裝設備。比重不計：TBDB 與其前段濕製程設備同屬 2026-08-27 法說 p.33「自製設備」1H2026 41.90% 一條線，簡報未拆分，且該線橫跨前段(A2-05)與後段(本節點)兩個製程階段(R020②)。★weight_status=Not_Disclosed 的判準是公司選的揭露軸：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，比重表僅列會計科目，故與 Too_Coarse 分開。Core 理由（R023④＋R023-R，2026-09-28 RULING_B4_E）：計畫研發產品六項中三項指向後段封裝（印刷頁 p67），計畫開發敘述「設備,及加強布局先進封裝的晶圓貼合,剝離設備等…」（印刷頁 p66），董事長報告「機台已成為公司重要的營收來源之一。」（印刷頁 p1）。', 'Direct', 'Not_Disclosed'),
        ('鈦昇', 'Major', None, 'annual_report_prose', '新列的依據：「晶圓背面開槽…」、「IC 封裝導線架清洗與活化、改質…」與「晶圓微蝕刻…」（皆印刷頁 p62）；用途說明為「雷射精微切割機使用於半導體元件之成型製造。」與「電漿清洗機用於 IC、LED製程中的清洗。」（皆印刷頁 p75）。雷射切割與雷射開槽機、以及封裝基板與導線架用的電漿清洗與微蝕刻設備，皆為 A2-12 定義逐字具名的後段封裝製程設備。weight 為 NULL：同一條雷射設備線還含 IC 封裝基板與 ABF 載板的雷射鑽孔（B5-01 具名工序）與玻璃基板加工，年報未列機種金額（R020②）。role 依 R026③（Direct 且 weight 為 NULL，無附屬業務 evidence）為 Major。', 'Direct', 'Too_Coarse'),
    ],
    "A2-13": [
        ('倍利科', 'Core', '自動光學檢量測設備', 'annual_report_table', '年報營業比重表：自動光學檢量測設備 95.03%。Core 理由：①年報商品項目首句「本集團目前以**自動化檢測與量測**及智能自動化設計開發為主要業務內容」，表列順序為 自動光學檢量測設備 → 智慧影像數據分析系統 → AI智慧醫療系統；⑤本格 95.03% 遠大於 U6 的 0.68%。三項同向', 'Direct'),
        ('久元', 'Major', None, 'annual_report_prose', '年報業務範圍表：機器組裝等產品銷售及其他 39.26%，為自有設備銷售；比重表未進庫故不計比重', 'Direct', 'Too_Coarse'),
        ('光焱科技', 'Major', '半導體光電轉換測試', 'annual_report_table', '年報營業比重表：半導體光電轉換測試 51.26%。本列的 weight 依據不變，補記兩點。交付物形態：所營業務寫為「光源模擬器、影像感測效率量測儀及光偵測器效率量測儀之設計、開發、製造及銷售…」（印刷頁 p47），交付的是儀器整機，故 2026-09-22 新條款的零組件分流不適用——該條款在本列的作用是確認而非改變。階段：本列對應的用途敘述「半導體光電轉換測試儀器用於量測、分析和判別多種光學感測器和光學影像感測器之光電轉換性能…」（印刷頁 p62）沒有晶圓級三個字，而同表的另一條線有，兩者因此構成階段上的互補而非重疊。★weight 維持 51.260（RULING_B10_A）：年報已把階段拆成兩條營收線，故無證據顯示 51.26% 本身又跨兩個節點，不得因公司另有 A2-08 就把整條降為 Granularity_Insufficient 或任何表示該線跨節點的粗粒度狀態。★role 由 Core 改為 Major：Core 依 RULING_B10_B 轉 Core Pending（逐層皆無法排除 A2-08，而 ① 的區辨只是名稱字面重合）；role 依 R026①，51.26% 不低於 15%，故為 Major。', 'Direct'),
        ('印能科技', 'Major', None, 'annual_report_prose', '新列的依據：公司在目前之商品項目一節把服務範圍界定為「在半導體封裝與測試製程領域,本公司提供全面的解決方案,包括氣動與熱能製程系統、自動化系統解決方案,以及相關零組件的設計、開發、製造、銷售和維修服務。」，並在同節把現行產品寫成「我們的產品涵蓋先進封裝應用中的壓力烤箱、高功率老化測試機和自動化搬運系統。」（皆印刷頁 p40）；產品發展趨勢一節再次把它列為關鍵設備：「本公司致力於提供領先的半導體製程解決方案,涵蓋關鍵設備如壓力除泡烤箱、高功率老化測試機及封測製程自動化系統。」（印刷頁 p44）；當年度開發成功之產品列「CMAC (High Power Burn In Oven)…」與「SMAC (High Power Server Rack)…」（皆印刷頁 p45）。高功率老化測試機與本節點具名的老化設備為同一種機台。★本列的 weight 為 NULL 是因為金額不可辨識，不是因為業務不重要；weight_status 取 Too_Coarse 而非 GI：年報有產品軸的比重揭露、只是比樹粗，且同時涵蓋的另一個節點可以具名（A2-12）。role 依 R026③：Direct 且 weight 為 NULL 者 Major 為 default，本案無附屬業務之 evidence——該機台與另兩個產品族在商品項目一節並列，不是附帶或配套業務。', 'Direct', 'Too_Coarse'),
        ('均華', 'Major', '半導體製程設備及相關精密模具', 'annual_report_prose', '新列的依據：商品項目光電雷射類「(B)自動化光學檢測設備(Automated Optical Inspection Machine)…」（印刷頁 p58）；主要產品用途表的視覺檢查機一列「應用外觀檢查技術配合各種封裝或晶圓的檢查,檢查的項目包括晶…」粒的瑕疵與封裝的精度（印刷頁 p70）——兩者都是獨立於取放機台的檢測機台，且後者由公司自己描述功能。★不以 IC 晶粒多面檢查挑撿機為依據：該機台檢查與挑撿雙功能，本輪仍按主功能（取放）留在 A2-12。weight 為 NULL／Too_Coarse：與 A2-12 的整機併在同一條 93.90% 線內，年報未列各品項金額（R020②）。role 依 R026③：Direct 且 weight 為 NULL，無附屬業務 evidence，故為 Major。', 'Direct', 'Too_Coarse'),
        ('建暐', 'Major', '量測儀器類', 'annual_report_table', '年報營業比重表：量測儀器類 26.00%。本列的 membership 與 weight 依據不變，role 仍為 Major。★既有 evidence 裡以揭露先後與比重比較說明降為 Major 的那兩句同樣是 R023 禁止的判準，已改為 R026① 的純數值判定（26.00% 不低於 15%，故為 Major）。★另記節點歸屬的疑問：交付物是跨產業的工件量測儀器——「因本公司此產品之用途為各產業之工件量測用…」（印刷頁 p38）——與本節點具名的機種都不相符，但全樹指不出更好的落點。依 RULING_B11_E 另立 node-landing／taxonomy issue，該案完成前不移除本列、不新增新列、不改 weight。', 'Direct'),
        ('德律', 'Major', '自動檢測設備', 'annual_report_table', '年報營業比重表只列「自動檢測設備 97.21%」一條設備線，無法拆到節點。半導體／先進封裝檢測設備業務存在，不只是計劃開發：致股東報告書載「半導體製程檢測設備是公司未」「來成長的重要策略。公司將逐年提升半導體檢測設備的營收佔全公司整體營收的比例」（原文跨行）；近五年研究發展成果列研發完成之 TR7900Q SII AOI（「為半導體/先進封裝產業檢測所設計」）、「TR7720S是一款專為半導體/先進封裝產業設計的小型AI驅動 AOI」、「TR7700Q SII-S是應用於半導體/先進封裝產業的AI驅動AOI檢測平台」、「TR7600 SIII Plus 是專為半導體、先進封裝、汽車、航太和醫療等產業應用」設計的 AXI。年報未把這些產品對應到營收線，依 R020 ②-MX 屬線索（MX-CLUE），不宣告混合線；與 B5-02 為既有共用線，維持原狀，該線待補證，兩格均不計比重。★2026-09-06 由 Core 降為 Major：①年報主要商品及服務項目順序為 (1)組裝電路板測試機(ICT) (2)電路板分析測試治具 (3)錫膏自動光學檢測機(SPI) (4)電路板自動光學檢測機(AOI) (5)在線式 3D X-Ray 自動檢測機(AXI)——前五項全部是 PCBA 側設備，無一是半導體測試設備。Core 依 R023 移至 PCB／PCBA 檢測側（2026-09-07 B5-01 拆格後為 B5-02）', 'Direct', 'Too_Coarse'),
        ('志聖', 'Major', '半導體製程設備', 'annual_report_table', '年報營業比重表：半導體製程設備 40.00%（本輪撤下計重）。撤重的依據：同一條 40% 線的主體是 A2-12 具名的封裝製程設備，本節點側只有「測試燒機爐:建構高溫動態環境,用於執行晶片的 可靠度與壽命評估,確保出貨品質。」（印刷頁 p79），對應本節點具名的老化設備。membership 因此保留，weight 則不再屬於本節點獨有；年報未給機種金額，依 R020② 不可單獨計入本節點，且不估算本節點所占的剩餘比例（RULING_B12_B）。role 依 R026③ 維持 Major。', 'Direct', 'Too_Coarse'),
        ('惠特', 'Core', None, 'annual_report_prose', '年報業務範圍載明「測試設備」：務收入等。3.公司目前之商品(服務)項目:(1)雷射二極體及其他化合物半導體測試設備:VCSEL晶粒點測機、VCSEL遠場/近場點測機、PD★2026-09-09 weight_status=Not_Disclosed：比重表的軸是受測產品與應用（半導體 42.09／LED檢測 7.42／雷射精加工 3.28），且最大欄是「其他 47.21」。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('梭特', 'Major', 'IC半導體設備', 'annual_report_prose', '新列的依據：商品項目「D. AOI設備…」（印刷頁 p55）與用途表「AOI 設備 採用影像分析技術,檢查元件是否有缺陷、汙染等瑕疵,以…」滿足客戶對品質確保之需求（印刷頁 p69）；董事長報告「帶動本公司相關檢測與分選設備(如 Sorter 及六面檢AOI)出貨表現。」（印刷頁 p4）；計畫開發亦列「E. 光學檢測設備(6S AOI):半導體晶片高精度六面自動光學瑕疵檢測。」（印刷頁 p56）——半導體晶片一詞排除了 LED 專屬的可能。★不以研發成果表的 ST642 測試分類機為依據：那是 110 年的研發成果、非現行商品項目，依 RULING_B4_D 不得以品項名稱反推 membership。weight 為 NULL／Too_Coarse：AOI 設備與 A2-12 的分選機／固晶機併在同一條 88.24% 線內（R020②）。role 依 R026③ 為 Major。', 'Direct', 'Too_Coarse'),
        ('由田', 'Core', 'SEMI檢測系統', 'annual_report_table', '年報營業比重表：SEMI檢測系統 41.77%。Core 理由（2026-09-07 在補齊 E7-02 後於完整的三取一候選集上依 R023 順序重跑）：候選為 A2-13 SEMI 檢測系統 41.77%／B5-02 PCB·IC 載板 28.45%／E7-02 LCD 檢測系統 22.47%，三線同屬 AOI 而依所服務的製程分列三格。①核心產品能力無區辨力——三者是同一套自動光學檢測能力用於三個下游，年報所營業務順序 A.PCB/IC載板 → B.TFT-LCD/OLED/觸控面板 → C.半導體 是產品目錄的編排而非能力主從，商品項目表前六項以 IC 載板與 PCB 為主同理，①不定案。②價值創造位置：41.77% 對 28.45% 僅差 13.3pp，不足以排除 PCB 側的競爭解釋，②不定案。③不適用（三者無上下游載體關係）。④長期策略定位：計畫開發表列第一項為「FOPLP 黃光製程檢測設備」，屬先進封裝即半導體側，研發指向與 PCB／面板兩側皆無關，④於此定案，Core 維持 A2-13。⑤同向，僅作支持證據。★前版註記的「候選集合殘缺」已結清——LCD 檢測系統 22.47% 現掛 E7-02', 'Direct'),
        ('竑騰', 'Minor', '視覺檢測設備', 'annual_report_table', '新列的依據（公司自身的定義句與逐項功能敘述，不是品項名稱）：「本公司之視覺檢測設備適用於各封裝製程中及最終成品檢測,以自動化辨識外觀並提升產品良率,實現2D/3D之六個視面檢測…」（印刷頁 p3）；「近年來本公司持續開發視覺檢測設備(Automatic Optical Inspection,簡稱AOI),並在點膠、植散熱片及壓合等製程後之最終封裝檢測設備(Final Visual Inspection,簡稱 FVI)已獲得成果…」（印刷頁 p54）；主要產品重要用途表在視覺檢測一列逐項寫出檢測對象與時點，例如「1.FCBGA/FCCSP die bond 後芯片偏移、崩缺、龜裂檢測…」與「8.散熱片檢測機,Lid gap/title/shift/刮傷/露銅/汙染檢測…」（皆印刷頁 p58）；同業比較表亦以兩類並列自述業務「竑騰 半導體封裝主製程及視覺檢測設備 268,886 1,859,407 62.42 28.01 20.50…」（印刷頁 p50）。weight 5.430 取自比重表的「視覺檢測設備 149,897 13.09 101,020 5.43…」一格（印刷頁 p40）。role 依 R026① 純數值路徑：Direct 且 weight 有值，5.43% < 15% 即 Minor，毋須另附理由。', 'Direct'),
        ('美達科技', 'Core', None, 'annual_report_prose', '年報業務範圍載明「測試設備」：%,營收可望達1,157億美元。後段製程設備同樣表現亮眼。SEMI預估,今年測試設備營收將達112億美元,年增高達48.1%;組裝與封裝設備★2026-09-09 weight_status=Not_Disclosed：比重表的軸是受測元件類別（混合訊號產品 77.93／影像感測產品 11.94），不是設備品項。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('致茂', 'Major', '量測儀器設備', 'annual_report_table', '年報營業比重表：量測儀器設備 95.96%（本輪撤下計重）。比重表的設備側只有一格「量測儀器設備 20,418,424 94.51 27,166,591 95.96…」（印刷頁 p61），而該格底下的產品清單分成十八個解決方案族（印刷頁 p63）。屬本節點的是半導體／IC 測試解決方案（SoC 測試系統、VLSI 測試系統、「3.IC 測試分類機(IC Test Handler)…」，印刷頁 p63）與 PXI 半導體／IC 測試系統；屬 E7-02 的是「-平面顯示器測試解決方案…」（印刷頁 p63）；另有電力電子、電動車、電池、儲能、被動元件、電氣安規、太陽光電、射頻無線與「-分子診斷自動化儀器解決方案…」（印刷頁 p63）等族在全樹無落點。年報未列各族金額，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★role 由 Core 改為 Major：Core 依 RULING_B9_B 轉 Core Pending，本列 role 依 R026③（Direct 且 weight 為 NULL，無附屬業務 evidence）為 Major。其餘無落點的產品族另記為未映射部分。', 'Direct', 'Too_Coarse'),
        ('萬潤', 'Major', '半導體設備', 'annual_report_prose', '新列的依據（皆為公司對自家機台的功能敘述，不是品項名稱）：「應用於半導體先進封裝製程,自動檢查錫球尺寸、 錫球檢查機 位置與缺失狀況,確保焊接品質與製程穩定性。」、「應用於半導體先進封裝製程,透過視覺系統以及光 六面外觀檢查機 機架構整合,自動檢查產品六個面外觀…」、「應用於半導體先進封裝製程,自動量測產品或材料 厚度量測機 厚度,確保尺寸符合規格,提升品質一致性。」（皆印刷頁 p59）；主要產品用途表把它們併為半導體設備的一類：「檢測與量測設備(如錫球檢查、六面外觀檢測、厚度量測),確保產品品質與製程穩定性。」（印刷頁 p67）；上下游一節亦自述「本公司主要產品為切割機、繞線機等被動元件設備;晶片取放、檢量測AOI、點膠塗佈、散熱貼合等先進半導體封裝設備;測試分選機及測試包裝機等 LED 設備及檢測設備。」（印刷頁 p62）。weight 為 NULL／Too_Coarse：該群機台與 A2-12 的封裝製程設備併在同一條「半導體設備 96.13%」線內，年報未列各品項金額（R020②）。role=Major 的理由是本案自身證據：Core 已由 A2-12 承擔，而該群機台是公司自述營業範疇的一部分、非附帶或偶發業務，weight 為 NULL 出於無法拆分而非重要性低（R026③）。', 'Direct', 'Too_Coarse'),
        ('蔚華科', 'Major', '半導體設備', 'annual_report_prose', '本列改為 Direct／Too_Coarse 的依據：年報業務範圍一節寫「自民國一一四年起,部分營收來自自行開發之光學檢測設備及相關之檢測服務。」（印刷頁 p37），自有產品一節寫「III.自有產品開發與生產製造…」與「公司於民國一一三年七月成立光學技術研發中心,開發出業界獨家「蔚華雷射斷層掃描(Spirox Laser…」（皆印刷頁 p38），並具名「以及化合物半導體材料缺陷檢查設備、可有效反映寬能隙材料內隱特性的三維應力分析檢測系統、系統單晶片(SoC)測試機、晶圓片的巨觀檢查(Macro Inspection) AOI 設備, 封裝器件外觀檢查(Leadscan)設備等。」（印刷頁 p38）。★舊 evidence 裡「本公司不製造」那一句不得繼續支撐本列，Not_Applicable 的判準（該曝險的營收已由另一節點完整計入）對自製那部分不成立——自製設備不是代理、不屬 V1 所描述的通路活動。改記 Too_Coarse 的理由是：membership 已成立，但沒有可從混合通路線中拆出的金額；同時涵蓋的另一個節點可具名（V1）。role 依 R026③ 維持 Major。★★`weight_item` 沿用「半導體設備」只是來源營收線身份的沿用（RULING D6），**不代表該條 93.77% 全部屬 A2-13**；本列的 weight 仍為 NULL／Too_Coarse，原因是混合通路與自有設備收入無法從原始營收線拆出 A2-13 金額。membership 的 Direct／Indirect 認定與 weight 能否量化是兩個獨立問題（R020）。', 'Direct', 'Too_Coarse'),
        ('鈦昇', 'Major', None, 'annual_report_table', '年報營業比重表：半導體設備 48.81%（本輪撤下計重）。撤重有兩個獨立理由（RULING_B12_A）：其一，比重表把半導體設備寫成類別標題，其下為雷射設備與電漿設備兩條子線，48.810 是前者的數字，故現有數字本身並非該上層 line 的完整 weight；其二，同一條線同時橫跨 A2-12、A2-13 與 B5-01 三個節點——含 B5-01 具名的載板雷射鑽孔（「IC封裝基板鑽孔…」，印刷頁 p62）——line 對 node 的映射不唯一而無機種金額可拆，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★★本欄必須保留並表述這個 distinction：**weight 不成立 ≠ membership 不成立**。membership 保留的依據是「雷射打印機用於 IC 封裝測試製程末端的識別機器。」（印刷頁 p75）與業務範圍的「半導體檢測自動化軟板設備研發、製造及銷售…」（印刷頁 p61）。★role 由 Core 改為 Major：Core 依 RULING_B12_A 轉 Core Pending（A2-12 與 A2-13 共用同一條不可拆的營收線、R023 各層皆不區辨，且現行 Core 的既有依據——該線的比重——隨撤重一併消失）；role 依 R026③ 為 Major。', 'Direct', 'Too_Coarse'),
        ('雷科', 'Core', '自製鐳射設備及代理設備', 'annual_report_table', '年報營業比重表：自製鐳射設備及代理設備 59.00%（本輪撤下計重）。撤重的依據：比重表那一格逐字為「自製鐳射設備及代理設備 657,233 59.00…」（印刷頁 p84），格名本身就寫明混合；上下游一節自述「本公司於設備產品部分,主要係 SMT 設備銷售代理及雷射設備之研發製造銷售廠商,位處 3C 電子產品產業上、中游之位置…」（印刷頁 p87）；產品趨勢一節自述「本公司近年產品銷售組合以銷售自製雷射設備與銷售代理 SMT設備、被動元件產業生產設備等佔約 60%以上…」與「代理德國原廠SMT檢測設備係台灣區獨家代理,且已代理30年以上…」（皆印刷頁 p88）。該格因此同時含代理設備（R022）與橫跨 A2-12／B5-01 的自製設備，自製部分橫跨的節點見主要產品重要用途表（印刷頁 p95）；年報未列各品項金額，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。Core 維持本節點，決定層為 R023①（RULING_B7_C）；候選集合是否被 taxonomy 截斷另列 open question。', 'Direct', 'Too_Coarse'),
        ('鴻勁', 'Core', '半導體測試及其他設備', 'annual_report_table', '年報營業比重表：半導體測試及其他設備 80.51%', 'Direct'),
        ('漢測', 'Major', None, 'annual_report_prose', '年報主要產品與服務項目 B.晶圓針測機之銷售與相關專業工程服務 C.半導體設備研發、製造與客製化產品；年報自述「主要從事半導體檢測設備及其零組件、耗材之客製化研發、製造及銷售」。併於半導體設備及零組件 57.13% 揭露且該線另含探針卡故不計比重(R020②)', 'Direct', 'Too_Coarse'),
        ('聯策', 'Major', None, 'annual_report_prose', '年報載明「針對半導體前段與後段製程,本公司提供晶圓級 AOI 檢測機,可檢測晶圓表面與側面瑕疵;以及 CMP 研磨墊監控系統」「在封測端,則提供半導體封裝自動化檢測設備,涵蓋挑片、貼標、貼膜等自動化製程」。併於「AI 機器視覺設備 41.17%」一列故不計比重(R020②)。★本輪補記該線的第三種對象：除既有的封測端檢測外，同一條線還含前段晶圓檢測（印刷頁 p73、印刷頁 p86），故本列的粗粒度同時涵蓋 B5-02 與 A2-08 兩個節點。membership、role 與 weight 皆維持不動。', 'Direct', 'Too_Coarse'),
        ('揚博', 'Major', None, 'annual_report_prose', '年報供應項目表半導體欄列「半導體檢測、製程設備及材料」「Bump檢查設備」「2.5D/3D之晶圓/晶片鍵合設備」「植球設備」，屬代理供應非自製，依 R022 建 Indirect 且不計比重。★本輪把鍵合設備與植球設備兩項的依據移出本列（改記於 A2-12）；本列保留的依據是「凸塊檢測(bump inspection)設備 面板級封裝(PLP)用金屬凸塊高度檢測設備…」與「IC測包機(IC tester) IC封裝用測試設備…」（皆印刷頁 p52），兩者都是檢測與測試設備。role 理由：本列屬揚博年報自列的「代理電子及特用化學品事業」，半導體檢測與封裝設備是供應項目表半導體欄的首項，且年報「產業上、中、下游之關聯性」在半導體封測段具名「封裝後檢測設備與探針卡用有機載板」為其供應內容；本列並非設備本業的附帶配套而是該事業在半導體側的主要品類，對公司整體經濟具實質重要性，故列 Major。機器設備收入占比 60.30% 僅作佐證。', 'Indirect', 'Too_Coarse'),
    ],
    "A3-01": [
        ('世界', 'Core', None, 'annual_report_prose', '類比與電源特殊製程晶圓代工。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報業務範圍「2025 年度業務之主要內容及其營業比重」列晶圓收入為唯一產品線——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Extracted：年報 @70「晶圓收入…46,763,229…其他…1,828,016」被 (cid:3) 佔位符切斷、欄位錯位。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★★2026-09-10 批次 B 診斷（維持 Not_Extracted，**不重建數值**）：本公司的年報有兩種同時發生的抽取損壞。(1) **每個中文字被輸出兩次**——檔案裡是「肆肆、、營營運運概概況況」「營營業業比比重重」，所以搜尋「營業比重」永遠找不到，這就是本公司長期停在 no_section 的原因；(2) 資料列與金額交錯且插滿 (cid:3)：「( ) 晶圓收入／(cid:3)(cid:3)(cid:3)／( ) 其他／1 (cid:3)(cid:3) 46,763,229／2 (cid:3)(cid:3) 1,828,016／4 8,591,245」。★字元重複可以機械還原，**但數字還原不了**：46,763,229 ＋ 1,828,016 ≠ 8,591,245，三個金額連基本的加總關係都不成立，任何「哪個金額屬於哪一列」的配對都是人工猜測。**不得為了讓 no_section 消失而做 heuristic 配對。**★已排除：pypdf 在本檔同樣找不到任何含「營業比重」的頁。★下一步工具需求：字元級重建或 OCR／視覺來源。', 'Direct', 'Not_Extracted'),
        ('力積電', 'Core', '邏輯暨特殊應用產品', 'annual_report_table', '年報營業比重表：邏輯暨特殊應用產品 61.11%。Core 理由：①年報商品項目表列順序為 **邏輯暨特殊應用產品晶圓代工**（半導體晶圓代工服務）→ 記憶體產品晶圓代工；④計畫開發首項亦為「(1)**邏輯暨特殊應用產品晶圓代工服務平台**：28奈米顯示驅動IC製程、55/80/110奈米BCD製程…」；⑤本格 61.11% 大於 A3-03 DRAM 38.89%。三項同向', 'Direct'),
        ('台積電', 'Core', None, 'annual_report_prose', '年報無營業比重章節；純晶圓代工廠。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報載明「身為專業積體電路製造服務的創始者與領導者之一，台積公司提供全面整合的積體電路製造服務」——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Extracted：tw.product_narrative 的 body 卡在 20000 字上限、結尾斷在句中，無法斷言年報有無揭露。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
        ('聯電', 'Core', None, 'annual_report_prose', '年報無營業比重章節；純晶圓代工廠。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報載明「聯電以先進的製程技術提供晶圓製造服務」——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Disclosed：業務範圍章完整，唯一的「比重」是產業趨勢語「提升委外晶圓代工的比重」。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('新唐', 'Minor', '晶圓代工收入', 'annual_report_table', '年報營業比重表：晶圓代工收入 5.00%，年報自述「另擁有 6 吋晶圓製造廠,具備多樣性製程技術能力,提供專業化晶圓代工服務」', 'Direct'),
    ],
    "A3-03": [
        ('力積電', 'Major', '記憶體產品', 'annual_report_table', '年報營業比重表：記憶體產品 38.89%', 'Direct'),
        ('南亞科', 'Core', None, 'annual_report_prose', '年報業務範圍載明「DRAM」：客戶間之良好夥伴關係。創造優質服務為本公司的核心價值之一。本公司之目標乃維持DRAM產業之服務領導地位,並相信若維持上述之服務目標與態度,有★2026-09-09 weight_status=Not_Disclosed：「主要之商品項目為 1. DRAM 晶片…」為規格條列，無金額與比重。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('華邦電', 'Major', '客製化記憶體產品收入', 'annual_report_table', '年報營業比重表：客製化記憶體產品收入 29%（26,243,596 仟元）；年報載明該線含「客製化高頻寬記憶體及元件(CUBE)」、「利基型動態隨機存取記憶體(Specialty Memory)…規格包含 64Mb~8Gb SDR & DDR/2/3/4」與「行動記憶體(Mobile Memory)…128Mb~8Gb mobile DRAM」', 'Direct'),
    ],
    "A3-04": [
        ('台半', 'Core', None, 'annual_report_table', '年報營運部門銷售表：整流器 35.79%，該部門商品含整流器、功率電晶體(MOSFET)、電源管理 IC 與小訊號產品，橫跨本節點與 A4-07 故兩格均不計比重(R020②)。Core 理由：條碼印表機 64.21% 係已獨立上市之子公司鼎翰科技(3611)所營(年報明載其已自行編製年報)，屬 Indirect 不得為 Core；本公司自身之製造與研發全在功率半導體，計畫開發新商品為超接面 MOSFET、SGT MOSFET、SiC 蕭特基等晶圓產品(R020③、R023②④)', 'Direct', 'Too_Coarse'),
        ('富致', 'Core', 'SMD型PPTC', 'annual_report_table', '年報營業比重表：SMD型PPTC 53.72%', 'Direct'),
        ('強茂', 'Core', '功率分離式元件', 'annual_report_table', '年報營運部門銷售表：功率分離式元件 90.42%，該部門主要商品為晶圓、功率分離式元件、控制模組。Core 理由：最大營收部門且為公司自有晶圓與元件製造，計畫開發新商品全數為功率器件(超接面 MOSFET、FRED、IGBT、SiC、GaN)(R023①④)', 'Direct'),
        ('德微', 'Core', '二極體及電晶體', 'annual_report_table', '年報營業比重表：二極體及電晶體 64.12%', 'Direct'),
        ('朋程', 'Core', '汽車用整流二極體', 'annual_report_table', '年報營業比重表：汽車用整流二極體 75.86%', 'Direct'),
        ('漢磊', 'Core', '元件及積體電路', 'annual_report_table', '年報營業比重表：元件及積體電路 36.35%，該事業「主要業務為高功率場效電晶體(Power MOSFET)、混合型積體電路(邏輯元件)、FRD、TVS、SiC 及 GaN 分離式元件等產品之製造」。Core 理由：磊晶事業為子公司嘉晶(3016)所營(R020③ Indirect，Indirect 不得為 Core)，本公司自身之價值創造在元件及積體電路代工製造，年報並自述該事業屬中游晶片製造(R023②③)', 'Direct'),
        ('統懋', 'Core', '二極體 + 電晶體', 'annual_report_table', '年報營業比重表兩條合計 98.48%：二極體 80.44、電晶體 18.04(R020④)，其餘「其他 1.52%」年報未指明品項，不強行分配。★★**2026-09-11 由 Not_Extracted 重新裁定，node 不變**。原 evidence 首句為「年報無營運概況標準章節」——**那句話是錯的**：年報有該章節，是選檔選錯了版本。本公司同日申報 F04(股東會年報)與 F11(股東會後修訂本)，舊的選檔規則取檔名字母序最後一個，而 narrative 定位器讀的是 F04。選檔改為「依 MOPS 文件說明取該年度中文年報中上傳時間最晚者」後，F11 的營運概況章一次讀出，比重表與業務內容同時到位。★**自有 fab 的依據換了來源**：原記「有自有晶圓廠」出自 company_site，與已被推翻的那句同在一段，依 4923 先例不得沿用，改以年報自述為準——「統懋半導體採取全產業鏈整合模式，從最基礎的基板處理到最終成品皆具備自主研發與製造能力…透過**內部化「磊晶」與「晶圓製造」**等高變異成本環節，統懋確保了產品開發的穩定性與毛利彈性」，並自述「**本公司**為專業之功率二極體及電晶體製造商」、「本公司於分離式元件電晶體及二極體製造產業已有三十多年歷史」。★★**同章另有一句「完善的 IDM(垂直整合製造)結構是降低營運風險…的戰略性武器」，那是產業概況的通則敘述，不是公司自述，不得作為自有 fab 的依據**——4923 力士那次把工研院IEK 的分類表當成年報自己的分類，就是同一個誤讀。故 A3-04(自有 fab)成立，不落 A4-22(無自有 fab 之設計公司)。Core 理由：本節點兩條線合計 98.48% 為公司全部主體，無第二候選節點(R019)。', 'Direct'),
        ('茂矽', 'Core', None, 'annual_report_prose', '年報業務範圍載明「二極體」：目(1)功率場效應電晶體(PowerMOSFET)(2)金屬氧化物半導體控制二極體(MCDMOSControlDiode)(3)瞬間電壓抑制★2026-09-09 weight_status=Not_Disclosed：只有「主要業務內容」條列六項，無百分比。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('虹揚-KY', 'Core', '橋式整流器', 'annual_report_table', '年報營業比重表：橋式整流器 56.02%', 'Direct'),
        ('麗正', 'Core', None, 'annual_report_prose', '年報業務範圍載明「二極體」：低耗損蕭特基整流器(7)高操作溫度(HighTemp.)蕭特基整流器(8)二極體整流器(9)快速整流器(10)高效整流器(11)超高效整流器★2026-09-09 weight_status=Not_Disclosed：比重表的軸是行業別（半導體業 95.63／醫療器材業 0.98），不是產品別。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
    ],
    "A3-05": [
        ('采鈺', 'Core', None, 'annual_report_prose', '年報自述致力成為「半導體光學晶圓代工夥伴」，兩類產品以同一晶圓製程產出：(一)影像感測器——「受惠於手機拍攝規格升級與電動車擴大感測應用,帶動影像感測器需求持續成長,采鈺在影像感測器產品和汽車應用領域,皆分別創下營收歷史新高」，研發段首述「在影像感測器方面」5 千萬畫素高階產品量產；(二)微型光學元件——超穎表面(Metasurface)濾波感測器已出貨於消費性穿戴裝置、矽光子微透鏡陣列應用於 AI 運算光傳輸之檢光器、光波導應用於 AR 眼鏡。年報未提供營業比重表故不計比重。★2026-09-07：微型光學那條原為 E6-01 Major，E6 解散後依 R025 放寬 A3-05 定義為「半導體光學晶圓製程」而非另開節點，兩條線因此同屬本格，不再是兩筆曝險。移出 E 的理由是製程屬半導體晶圓製造，不是「E6 沒了所以退回」★2026-09-09 weight_status=Not_Extracted：narrative 抓到 pages 5-12 的目錄與董監事持股表，不是業務內容章。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★2026-09-10 出處補註：本列引用的「半導體光學晶圓代工夥伴」一句在年報**第 8 頁的致股東報告書「重要產銷政策」段**，不在 tw.product_narrative 所存的業務範圍章之內（該表存的是 pages 66-73）。記下來是因為想驗證這句話的人會先去查 narrative 而查不到，那會讓一句真實的引用看起來像捏造的。★同日本公司的 narrative 由 pages 5-12 更正為 66-73：先前存的是**目錄頁**，locator 的 _is_toc 認不出沒有點狀引導線的目錄。', 'Direct', 'Not_Extracted'),
        ('澤米', 'Core', '半導體光學鍍膜', 'annual_report_table', '年報營業比重表：半導體光學鍍膜 54.05%，為最大營收線。年報產品及服務項目載明其內容為「感測元件鍍膜：光學屏下指紋及心律/血氧/環境光/近接感測」「晶圓級各波段濾波片應用(DAR/IR-Cut/雙帶通/ITO 導電膜)：於 CSP/COB 封裝上使用」。★2026-09-08 G3 成像光學母體逐家裁定時判出：本公司被「光學」二字帶進候選，但這條線是晶圓級光學鍍膜，依 AA 的 parent scope（半導體晶圓級光學製程與鍍膜見 A3-05）歸本節點。★2026-10-07 補寫：裁定當日未寫入本檔，provenance 調查查出後補上。另有精密光學元件製造 43.73% 掛 AA1（Too_Coarse，不計比重）、薄膜濾光片及光學鍍膜 2.22% 未另掛★Core 理由：候選為本格（半導體光學鍍膜 54.05%）與 AA1（精密光學元件製造 43.73%，三項合併揭露）。①②年報無分部毛利，兩條線共用光學鍍膜能力，不足以排除另一候選；④計畫開發之短期五項中，「NIR sensor 應用濾波片」「配合半導體製程需求之鍍膜開發」「晶圓級 CSP 封裝用濾波片之表面品質提升改善」三項指向感測與晶圓級鍍膜，指向另一側的只有「穿戴顯示裝置中 3D 波導元件/DOE 繞射元件加工及鍍膜開發」一項；⑤本格 54.05% 大於 43.73%，且後者只有 OLPF 一項屬 AA1。④與⑤同向，Core 在本格；與 2026-09-08 裁定一致', 'Direct'),
    ],
    "A3-06": [
        ('光環', 'Core', '晶粒及其零組件', 'annual_report_table', '年報營業比重表：晶粒及其零組件 63.00%。Core 理由：最大營收線，年報自述建立「從上游磊晶設計製造、中游晶粒設計生產、至下游的主動元件的封裝製造」而以中游晶粒為核心；新增 D6-01 光電轉換組件(28%)後重跑裁定仍以晶粒為大(R023②⑤)', 'Direct'),
        ('全訊', 'Core', '微波放大器', 'annual_report_table', '年報營業比重表：微波放大器 59.27%', 'Direct'),
        ('宏捷科', 'Core', '砷化鎵晶片', 'annual_report_table', '年報營業比重表：砷化鎵晶片 100.00%', 'Direct'),
        ('漢磊', 'Major', None, 'annual_report_prose', '年報「磊晶及化合物半導體事業」主要業務含 GaN 磊晶片與 SiC 磊晶片(矽磊晶部分見 A1-03)。該事業之長期業務發展計劃載明「嘉晶將持續投入 GaN-on-Silicon 與 SiC 磊晶技術開發與精進」，即由已獨立上市並自行建檔之子公司嘉晶(3016)經營，依 R020③ 列為 Indirect 且不計比重。role 理由：漢磊自身的經營活動為功率元件與積體電路之晶圓製造；子公司嘉晶(3016)之磊晶晶片是集團最大的一條營收線，且為漢磊晶圓製造的上游材料而非其附帶延伸，年報將「磊晶及化合物半導體事業」列為與元件事業並列的主要事業並載明其長期業務發展計劃，對母公司整體經濟具實質重要性，故列 Major。該線營收占比 61.52% 僅作佐證', 'Indirect', 'Too_Coarse'),
        ('環宇-KY', 'Core', None, 'annual_report_prose', '年報業務範圍載明「砷化鎵」：營運概況一、業務內容(一)業務範圍1.所營業務之主要內容..本公司主要係從事砷化鎵/磷化銦/氮化鎵高階射頻(5G及6G)及光電元件化合物半導★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('穩懋', 'Core', None, 'annual_report_prose', '年報業務範圍載明「砷化鎵」：司目前之商品(服務)項目在無線寬頻通訊的微波高科技領域中,穩懋主要提供兩大類砷化鎵電晶體製程技術:異質接面雙極性電晶體(HBT)和應變式異質★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('聯亞', 'Core', None, 'annual_report_prose', '年報業務範圍載明「砷化鎵」：用於CO2等長波長(>2000nm)氣體PD偵測器的應用也逐漸多起來。(3)砷化鎵(GaAs)雷射(LD)磊晶片:波長含括635nm~110★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
    ],
    "A3-07": [
        ('旺宏', 'Core', 'Flash', 'annual_report_table', '年報營業比重表：Flash 77.55%', 'Direct'),
        ('華邦電', 'Core', '快閃記憶體產品收入', 'annual_report_table', '年報營業比重表：快閃記憶體產品收入 35%（89,406,171 仟元中的 31,074,913）；年報自述「華邦為 IDM 記憶體公司,擁有自有晶圓製造廠、自有製程技術、與產品開發團隊」，產品含 NOR、NAND、MCP、SpiStack® 堆疊組合及 TrustME® 安全快閃記憶體。Core 理由：三條線中邏輯產品 34% 為已獨立上市之子公司新唐(4919)所營、依 R020③ 為 Indirect 不得為 Core；本公司自營的兩條線中快閃記憶體 35% 大於客製化記憶體 29%，且年報主要產品列其為首(R023①⑤)。★比重於 2026-09-03 隨 [mix] 修復補上：該表原本讀不到，因為標題是「營收比重」而解析器只找「營業比重」', 'Direct'),
    ],
    "A4-05": [
        ('世芯-KY', 'Core', 'ASIC及晶圓產品', 'annual_report_table', '年報營業比重表：ASIC及晶圓產品 98.45%', 'Direct'),
        ('創意', 'Core', '委託設計(NRE) + 晶圓產品', 'annual_report_table', '年報營業比重表：委託設計(NRE) 23.53% 加晶圓產品 75.38%，同屬 ASIC 設計服務業務（turnkey 量產晶圓）', 'Direct'),
        ('安國', 'Core', 'NRE設計服務及ASIC晶圓產品', 'annual_report_table', '年報營業比重表：NRE設計服務及ASIC晶圓產品 56.43%。Core 理由：①年報所營業務首句「本公司及子公司係以 **NRE設計服務及ASIC晶圓產品服務**、資訊及消費性產品的週邊及控制IC與無線音頻控制IC 之研發、設計與銷售為主力業務」，NRE／ASIC 列於句首；商品項目表列順序同；④計畫開發第一個產品類別段落亦為「NRE 設計服務及 ASIC 晶圓產品之開發計畫」；⑤本格 56.43% 為最大營收線。四項同向', 'Direct'),
        ('巨有科技', 'Core', '設計服務收入', 'annual_report_table', '年報營業比重表：設計服務收入 60.23%', 'Direct'),
        ('意騰-KY', 'Core', 'ASIC產品', 'annual_report_table', '年報營業比重表：ASIC產品 58.65%。Core 理由：①年報所營業務首句「主要產品可區分為 **AI 聲學處理與語音識別之ASIC 產品**以及提供 AI 智財授權暨技術服務產品」，ASIC 列於句首；⑤本格 58.65% 大於 A4-06 矽智財 41.35%。★年報另一段「目前之商品(服務)項目」的順序相反（A.AI智財授權 B.ASIC產品），兩處不一致；採用自述句與比重同向的一邊', 'Direct'),
        ('智原', 'Core', 'ASIC及晶圓產品', 'annual_report_table', '年報營業比重表：ASIC及晶圓產品 78.85%', 'Direct'),
        ('益芯科', 'Core', 'ASIC及晶圓收入 + 委託設計', 'annual_report_table', '年報營業比重表：ASIC及晶圓收入 73.24% 加委託設計 24.41%，同為 ASIC 設計服務業務，合計 97.65%', 'Direct'),
        ('聯發科', 'Major', None, 'annual_report_prose', '年報商品項目 I.消費性及企業級客製化晶片(ASIC)；產業概況另闢「C.客製化晶片(ASIC)產業」一節說明布局', 'Direct', 'Too_Coarse'),
        ('威盛', 'Major', None, 'annual_report_prose', '年報自述「威宏科技 IC 設計與生產服務業務,約占 28%」，並載明威宏「提供高階 IC 後端設計、系統級晶片整合與先進封裝技術服務」', 'Direct', 'Too_Coarse'),
        ('神盾', 'Major', 'NRE設計服務、ASIC及晶', 'annual_report_table', '年報營業比重表：NRE設計服務、ASIC及晶(圓) 28.83%，為受託設計服務與客戶專用晶片', 'Direct'),
    ],
    "A4-06": [
        ('M31', 'Core', '技術服務收入', 'annual_report_table', '年報營業比重表：技術服務收入 86.26%', 'Direct'),
        ('億而得', 'Core', '技術權利金', 'annual_report_table', '年報營業比重表：技術權利金 75.69%', 'Direct'),
        ('力旺', 'Core', '技術權利金', 'annual_report_table', '年報營業比重表：技術權利金 67.50%', 'Direct'),
        ('意騰-KY', 'Major', 'AI智財授權暨技術服務', 'annual_report_table', '年報營業比重表：AI智財授權暨技術服務 41.35%', 'Direct'),
        ('晶心科', 'Core', '矽智財授權收入 + 權利金收入 + 矽智財維護服務收入', 'annual_report_table', '年報營業比重表：矽智財授權收入 65%、權利金收入 19%、矽智財維護服務收入 16%，依 R019 同節點聚合為 100%（A4-06 定義涵蓋授權金、權利金與技術服務費）；其他收入 3,490 仟元未印比重、無進入 A4-06 的依據，不計入。★2026-09-15 M5：更正——原 80.73% 是缺少權利金列時以金額推算，分母錯置（965,403 ÷ (1,478,187 − 282,337)）', 'Direct'),
        ('芯測', 'Core', '矽智財授權收入', 'annual_report_table', '年報營業比重表：矽智財授權收入 86.06%', 'Direct'),
        ('安格', 'Minor', '矽智財', 'annual_report_table', '年報營業比重表：矽智財 1.35%，年報自述成立初期以銷售矽智財為發展方向、專注於高速傳輸介面矽智財研發', 'Direct'),
        ('神盾', 'Minor', 'IP授權', 'annual_report_table', '年報營業比重表：IP授權 9.44%', 'Direct'),
    ],
    "A4-07": [
        ('來頡', 'Core', None, 'annual_report_prose', '年報按應用別揭露；產品線為電源管理與馬達驅動IC。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報業務範圍句「本公司主要從事電源管理的類比暨混合訊號積體電路設計、測試、生產及行銷等業務」——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Disclosed：比重表的軸是應用別（資訊通訊類產品 75.38／消費性及其他 24.62）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('偉詮電', 'Core', '電源管理與類比IC', 'annual_report_table', '年報營業比重表：電源管理與類比IC 33.00%。Core 理由：⑤在本格與 V6 之間完全平手（各 33.00%），由①④決定。①年報「目前之商品(服務)項目」明確分成兩層：「(1) **自行研發及產銷之產品**：電源管理與類比IC、智慧應用IC。(2) **兼營業務**：國際知名品牌電子零組件之整合應用及代理銷售」——自有 IC 是本業、代理是自述的兼營；④計畫開發首項亦為「(1) 電源管理與類比IC」。故 Core 判在 A4-07 而非 V6', 'Direct'),
        ('力智', 'Core', '電源管理IC', 'annual_report_table', '年報營業比重表：電源管理IC 76.13%。Core 理由：①年報商品項目僅兩大類且順序為 A.**電源管理IC** B.功率元件(MOSFET)；④計畫開發首項亦列於電源管理IC 類別下（Multi-Phase Digital & Hybrid Digital Vcore PMIC for CPU and GPU）；⑤本格 76.13% 大於 A4-22 功率元件 23.87%。三項同向', 'Direct'),
        ('杰力', 'Major', '電源管理IC', 'annual_report_table', '年報營業比重表：電源管理IC 23.22%', 'Direct'),
        ('沛亨', 'Core', None, 'annual_report_prose', '年報按應用別揭露；產品線為電源管理IC。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報產業概況載明其類比 IC 業務以電源管理 IC 為主——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Disclosed：比重表的軸是應用別（電腦及周邊設備 64.10／網路產品 35.90）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('矽力*-KY', 'Core', None, 'annual_report_prose', '年報按應用別揭露；電源管理IC 為全部產品線。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報產業概況以「電源管理 IC」為其類比 IC 產品線之核心敘述（全文出現 17 次）——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Not_Disclosed：比重表的軸是應用別（消費性／工業用／汽車電子／資訊／網路通訊）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('致新', 'Core', '電源管理IC', 'annual_report_table', '年報營業比重表：電源管理IC 82.00%', 'Direct'),
        ('茂達', 'Core', '電源轉換及管理IC', 'annual_report_table', '年報營業比重表：電源轉換及管理IC 26.04%。Core 理由：★這是①④推翻⑤的案例。⑤指向 A4-17：放大及驅動IC 29.05% 大於本格電源轉換及管理IC 26.04%。但①年報主要業務範圍「研究、開發、生產、製造，銷售下列產品」順序為 A.半導體**功率IC**和其模組 B.功率元件 C.智慧型功率IC D.無線及網路通訊IC E.光電驅動IC，電源類列前三、驅動IC列末；商品項目順序亦為 (1)**電源轉換及管理IC** (2)放大及驅動IC (3)離散式功率元件；④計畫開發首個類別即「(1)**電源管理 IC**」。R023 將①④置於⑤之前，故 Core 判在 A4-07', 'Direct'),
        ('虹冠電', 'Core', '功率IC', 'annual_report_table', '年報營業比重表：功率IC 65.35%。Core 理由：①年報「研究、開發、生產、製造及銷售下列產品」順序為 A.**功率積體電路(Power IC)** B.電源模組 C.MOSFET D.快速回復二極體，Power IC 列首；④計畫開發首項「PFC+SLS 晶片方案之性能升級」為電源控制晶片；⑤本格 65.35% 大於 A4-22 功率元件 34.65%。三項同向', 'Direct'),
        ('通嘉', 'Core', '電源管理IC', 'annual_report_table', '年報營業比重表：電源管理IC 100.00%', 'Direct'),
        ('類比科', 'Core', '電源管理IC', 'annual_report_table', '年報營業比重表：電源管理IC 72.26%', 'Direct'),
        ('聯發科', 'Minor', None, 'annual_report_prose', '年報商品項目 J.電源管理及控制晶片。role 理由：十一項商品中之一項，年報未給個別比重', 'Direct', 'Too_Coarse'),
        ('強茂', 'Minor', None, 'annual_report_table', '年報營運部門「功率積體電路及元件 8.05%」，年報載明「上述主係子公司-虹冠電子工業股份有限公司(股票代號:3257)所營業務內容」，虹冠電已自行編製年報並獨立建檔於本節點，依 R020③ 列為 Indirect 且不計比重。role 理由：強茂的主要經營活動為功率分離式元件，年報以之為壓倒性的單一主力產品線，公司的技術、產能與資本支出亦集中於此；虹冠電之功率積體電路屬 IC 設計，相對於母公司的分立元件製造主業是另一條附帶產品線，非母公司自行經營亦非其能力投入所在，對母公司整體經濟重要性較低，故列 Minor。子公司該業務營收占比 8.05% 僅作佐證', 'Indirect', 'Too_Coarse'),
        ('台半', 'Minor', None, 'annual_report_prose', '年報主要產品「C、電源管理 IC 方面」列電源管理類比 IC、LDO、DC/DC 轉換器、LED 驅動 IC、霍爾感應 IC 等九項；該產品線併於整流器部門 35.79% 揭露無法拆分故不計比重。role 理由：整流器部門內以整流器與功率電晶體為主(商品表列 22 項)，電源管理為次要產品線', 'Direct', 'Too_Coarse'),
    ],
    "A4-08": [
        ('矽統', 'Major', None, 'annual_report_prose', '年報商品項目「(7) 混合訊號微控制器產品線」；年報比重僅「積體電路約100%」一條粗線故不計比重', 'Direct', 'Too_Coarse'),
        ('華邦電', 'Major', None, 'annual_report_table', '年報「邏輯產品收入 34%」全數來自合併子公司新唐科技(4919)，該公司已於本節點獨立建檔，母公司再計比重會在市場層重複計數，故不計比重。role 理由：邏輯產品與快閃記憶體、客製化記憶體並列為華邦電年報揭露的三大產品線，規模相當而無主從之分，並非附帶於記憶體主業的延伸產品線；該業務雖由子公司新唐獨立經營，對母公司整體經濟仍具實質重要性，故列 Major。該線營收占比 34% 僅作佐證', 'Indirect', 'Too_Coarse'),
        ('迅杰', 'Core', None, 'annual_report_prose', '年報自述「公司的核心本業兩大產品主軸為內嵌式控制器(EC)產品線」，商品項目首列筆電內嵌式控制器 IC。年報比重僅「電腦及週邊消費電子相關應用IC 99.85%」一條粗線故不計比重。原掛 A4-11，該證據出自「為因應USB Type-C/PD趨勢,持續投入研發」屬延伸產品線', 'Direct', 'Too_Coarse'),
        ('普誠', 'Minor', None, 'annual_report_prose', '年報商品項目列有車用 Body LED 與馬達 MCU，但非主要營業項目。role 理由：年報自述主要營業項目為 TFT／馬達／LED 三種驅動 IC，MCU 僅商品表一列', 'Direct', 'Too_Coarse'),
        ('九齊', 'Core', '微控制器IC', 'annual_report_table', '年報營業比重表：微控制器IC 86.41%', 'Direct'),
        ('佑華', 'Major', None, 'annual_report_prose', '年報業務範圍載明「微控制器」：IC153,782100%3.公司目前之商品(服務)項目可分為語音控制IC、微控制器IC及32位元高階應用IC三大類。(1)語音控制IC為單。★2026-09-06 由 Core 降為 Major：①年報商品項目首句「可分為**語音控制IC**、微控制器IC及32位元高階應用IC三大類」，語音列首而微控制器列第二；④計畫開發首項「(1)**錄音類產品**：以現有產品為架構，朝向更高錄音秒數、更好錄音品質」亦屬語音。Core 依 R023 移至 A4-14★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('偉詮電', 'Major', None, 'annual_report_prose', '年報業務範圍載明「微控制器」：機(POS)之磁條解碼IC與晶片卡介面IC。-一系列之通用型8位元與32位元微控制器。(3)客戶專用客製化IC(ASIC)依客戶需求而訂製的★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('凌通', 'Core', None, 'annual_report_prose', '年報商品項目 G.無線充電、觸控、遙控器及醫療量測等微控制器晶片；營收僅「IC 產品收入 98.9%」一條粗線故不計比重(R020②)。Core 理由：年報所營業務首列「語音錄放、辨識與聲音合成控制晶片」，而該類產品之計畫開發新商品全為 4/8/16 位元 CPU 語音合成控制 OTP/Flash 晶片，即以自有 CPU 核心之微控制器為貫串消費性、MCU 與多媒體三條產品線的技術主軸(R023①②)', 'Direct', 'Too_Coarse'),
        ('凌陽創新', 'Major', None, 'annual_report_prose', '年報業務範圍載明「微控制器」：司屬於IC產業鏈上游的IC設計公司,主要業務為自行研發、設計與銷售影像處理及微控制器IC晶片,營運流程為由本公司先依據市場及品牌廠需求設計開★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('應廣', 'Core', '微控制器', 'annual_report_table', '年報營業比重表：微控制器 99.66%', 'Direct'),
        ('新唐', 'Core', None, 'annual_report_prose', '年報商品項目 1)微控制器(MCU)/微處理器(MPU) 列於首位。營收「一般IC 收入 93%」一條粗線橫跨十五項 IC 商品故不計比重(R020②)。Core 理由：商品項目以 MCU/MPU 列首，產業概況首節即為「a. MCU 及 Audio」並自述已推出首款 AI MCU、算力較傳統 MCU 提升 10 倍，技術與策略主軸在 MCU(R023①④)', 'Direct', 'Too_Coarse'),
        ('松翰', 'Core', '消費性IC', 'annual_report_table', '年報營業比重表：消費性IC 52.00%，年報自述「本公司消費性 IC 的核心為具有語音功能的微控制器,亦即語音控制 IC」。Core 理由：最大營收線，且年報自述公司創立之初即以語音控制 IC 為主要產品、微控制器核心技術歷經十餘年發展(R023①②⑤)', 'Direct'),
        ('盛群', 'Core', '微控制器IC', 'annual_report_table', '年報營業比重表：微控制器IC 76.00%', 'Direct'),
        ('研通', 'Core', '微處理器IC應用產品', 'annual_report_table', '年報本集團營業比重表：微處理器IC應用產品 21.20%。Core 理由：①決定性且為最強形式——年報「4.公司目前之商品(服務)項目」開頭即「本公司為專業 IC 應用設計服務公司，以研發技術為主要核心資源，**目前主要商品是微處理器(Micro Control Unit; MCU)產品之應用設計**」，直接指名主要商品；商品項目條列亦以 (1)微處理器 IC應用產品 列首。★⑤指向另一邊：本集團比重表影像視訊控制IC 37.50% > 微處理器IC 21.20%（本公司個體表為 56.16% > 31.07%，兩套基準同向）。R023 將①置於⑤之前，最大營收業務線不單獨決定 Core。★2026-09-07 更正：原 Core 判在 A4-16，理由用「④⑤取二比一」的計票法，既違反 R023 的依序判定，也漏掉了上述自述句——由 fresh-context read-back 抓出★2026-09-15 M5：分類依據由同頁「2.本公司營業比重」個體表改為「3.本集團營業比重」合併表（P22-2）；Core 依 R023 ①保留於本格，不以最大比重移動', 'Direct'),
        ('笙泉', 'Core', '微控制器', 'annual_report_table', '年報營業比重表：微控制器 100.00%', 'Direct'),
        ('聯陽', 'Major', None, 'annual_report_prose', '年報業務範圍載明「單晶片」：列IC(EmbeddedControl)、高速影音介面相關晶片系列IC、系統單晶片SoC、其他客製化應用晶片。4.計劃開發之新產品(1)配合★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('通泰', 'Core', None, 'annual_report_table', '年報主要產品系列表：健康醫療系列 25.89%、3C 家電及週邊系列 10.51%，應用範圍列 Thermometer Series、R2F Type MCU、Heart Rate Detector、LCD Type1 MCU、IO Type MCU、A/D Type MCU(8-bit SAR ADC) 等消費性與醫療量測微控制器。Core 理由：年報「長、短期業務發展計畫」載明「公司短期發展的首要目標…以鞏固 MCU 系列產品以及消費性產品之市場」，長期目標為「以已有獲利成績之產品為根,繼續發展其枝葉」——公司自述之策略主軸為 MCU 系列(R023①④)。★本格 2026-09-03 曾一度改判為 A4-15(觸摸系列 55.40% 最大)，係漏讀本段而誤認 ①～④ 無區辨力；R023 明定 ④ 優先於 ⑤，故回復 A4-08 為 Core★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('金麗科', 'Core', '微控制器半導體元件(晶片)', 'annual_report_table', '年報營業比重表：微控制器半導體元件(晶片) 100.00%', 'Direct'),
        ('雅特力-KY', 'Core', '微控制器', 'annual_report_table', '年報營業比重表：微控制器 99.94%', 'Direct'),
    ],
    "A4-09": [
        ('天擎', 'Core', None, 'annual_report_table', '年報營業比重表三條主線皆為影像監控錄影 SoC(無線車用監控 SoC 64.49%、無線網路安防監控錄影 SoC 22.83%、無線嬰兒監視器 SoC 6.21%，合計 93.53%)；該表未進入本資料庫故不計比重。原掛 A4-10 網通 IC，但年報「整合各型WiFi模組進行特規通訊協議開發」是採購模組整合，非設計網通晶片。Core 理由：三條 SoC 線合計 93.53%，計畫開發新產品亦以無線 8/16 通道 DVR 錄影監控 SoC 為主(R023④⑤)', 'Direct', 'Too_Coarse'),
        ('凌通', 'Major', None, 'annual_report_prose', '年報商品項目 D.高品質影像擷取、降噪、壓縮、播放及儲存之多媒體 SoC 晶片 E.具深度學習的影像辨識、影像處理、移動追蹤等功能智慧多媒體 SoC 晶片；計畫開發新商品列新一代 22 奈米多媒體應用 SoC', 'Direct', 'Too_Coarse'),
        ('凌陽', 'Core', None, 'annual_report_prose', '年報業務範圍載明「多媒體」：LCD升壓及控制電路(b)支持寬電壓操作OTP記憶體,可快速更新程式碼(2)多媒體產品線A.新一代22奈米多媒體應用SoC(a)內建凌通自行。Core 理由：④長期策略決定性——年報「計劃開發之新商品(服務)」表格第一列（凌陽科技）第一項為「(1)**車用信息娛樂系統晶片**」，屬多媒體／影音 SoC；①所營業務僅泛稱「各種積體電路之設計、製造、測試及銷售」，年報無獨立商品項目段落，不區辨。⑤不可用：比重表只有「IC收入 → 其他」兩列，三個候選（本格、A4-11、A4-16）權重全 NULL★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('太欣', 'Core', '數位/無線網路錄影系統單晶片(SoC', 'annual_report_table', '年報營業比重表：數位/無線網路錄影系統單晶片(SoC) 46.5%。Core 理由：最大營收線，年報自述「憑藉多年來在數位影像控制晶片設計及系統整合的經驗」，主要產品六項中五項為各類 SoC(R023②⑤)', 'Direct'),
        ('揚智', 'Core', '數位機頂盒晶片', 'annual_report_prose', '年報自述「本公司主要產品為數位機頂盒(Set-top Box, STB)晶片,占整體營收約 85%」，商品項目為衛星／地面廣播／有線／IP 之 STB 整合晶片。年報未提供營業比重表故不計比重。Core 理由：占營收約 85%，且年報自述為「全球第一大的廣播電視機頂盒供應商,整體市占約 40%」(R023①③⑤)★2026-09-09 weight_status=Not_Extracted：年報 @214「數位機頂盒(STB)晶片,占整體營收約85%,網路相關設備約占15%」。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★★2026-09-09 由 Not_Extracted 改判 **WEIGHTED**（R020-NE 的重新裁定，本批五列中唯一一筆）：解析取得「數位機頂盒晶片 85.0」，與本節點的多媒體／影音 SoC 定義直接對應，且本列 evidence 早已引用同一個數字，只是先前無法從年報自動取得。R020①-W 四條件逐條成立：(a) 85.0 來自年報自述且與 evidence 一致；(b) weight_item「數位機頂盒晶片」可回溯到年報那一句；(c) 商品項目章列出衛星／地面廣播／有線／IP 之 STB 整合晶片，支持本 mapping；(d) 無重複計數之虞。', 'Direct'),
        ('瑞昱', 'Major', None, 'annual_report_prose', '年報商品項目「多媒體產品」列高端智慧連網電視系統單晶片、智慧顯示系統單晶片、8K 影像解碼與處理晶片，另有超高清 HDR 多媒體機上盒晶片系列', 'Direct', 'Too_Coarse'),
        ('聯發科', 'Major', None, 'annual_report_prose', '年報商品項目 C.智慧電視晶片、F.智慧家庭裝置晶片', 'Direct', 'Too_Coarse'),
        ('松翰', 'Major', '多媒體IC', 'annual_report_table', '年報營業比重表：多媒體IC 48.00%，年報自述多媒體影像「主要產品包括影像處理晶片及無線影音方案」，本節點定義即含數位影像控制晶片', 'Direct'),
        ('聯詠', 'Major', 'SoC晶片', 'annual_report_table', '年報營業比重表：SoC晶片 36.47%，該類別主要產品為面板時序控制晶片、數位電視系統控制單晶片、監視器控制晶片、AI 機器視覺與智能影像處理單晶片、NVR/DVR AI 網路儲存系統單晶片、8K 電視系統控制單晶片等', 'Direct'),
    ],
    "A4-10": [
        ('九暘', 'Core', '乙太網路IC', 'annual_report_table', '年報營業比重表：乙太網路IC 92.20%', 'Direct'),
        ('亞信', 'Core', '高速網路/輸出入控制晶片', 'annual_report_table', '年報營業比重表：高速網路/輸出入控制晶片 99.85%', 'Direct'),
        ('宏觀', 'Major', None, 'annual_report_prose', '年報業務範圍載明「網通」：(BLE5.4)無線收發晶片。(20)2.4GHz私有協議無線收發晶片。物聯網通訊部分(21)2.4GHzZigBee/BLE/Thread★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('瑞昱', 'Core', None, 'annual_report_prose', '年報商品項目「通訊網路暨聯網多媒體產品」列乙太網路控制器、實體層收發器、多埠交換器晶片組、WLAN/藍牙控制晶片、PON 晶片組等二十餘項。營收僅「積體電路產品 99.76%」一條粗線橫跨全部產品線故不計比重(R020②)。Core 理由：年報商品項目以通訊網路類列於首位且項數最多，計畫開發新產品亦以車用乙太網路與交換器為主軸(R023①④)', 'Direct', 'Too_Coarse'),
        ('立積', 'Major', 'Wi-Fi 產品', 'annual_report_table', '同一線「Wi-Fi 產品 83.59%」的網通應用部分；與 A4-13 為同一營收線，兩格均不計比重', 'Direct', 'Too_Coarse'),
        ('聯傑', 'Core', '區域網路晶片組(LAN)', 'annual_report_table', '年報營業比重表：區域網路晶片組(LAN) 89.00%', 'Direct'),
        ('聯發科', 'Major', None, 'annual_report_prose', '年報商品項目 B.Wi-Fi 6 及 Wi-Fi 7 晶片、H.被動式光纖網路(PON)晶片。營收僅「晶片 98.22%」一條粗線橫跨 A~K 全部產品項目故不計比重(R020②)', 'Direct', 'Too_Coarse'),
        ('達發', 'Core', '無線及寬頻通訊晶片等', 'annual_report_table', '年報營業比重表：無線及寬頻通訊晶片等 99.95%', 'Direct'),
    ],
    "A4-11": [
        ('君曜', 'Core', None, 'annual_report_prose', '年報商品項目：(1)高速傳輸介面影像橋接IC (Display Bridge IC)。單一營收線「積體電路設計 100%」橫跨橋接與觸控兩節點故不計比重。原掛 A4-12 顯示器驅動 IC，其證據「從基本驅動IC初始化」講的是橋接晶片去初始化面板端 DDI，不是自家產品。Core 理由：①年報商品項目首句「本公司**專注於設計與研發高速傳輸介面、影像訊號處理**和高感度觸控 IC 相關產品」，條列順序為 (1)**高速傳輸介面影像橋接IC (Display Bridge IC)** (2)觸控IC，介面 IC 列首；④計畫開發首項為 TDDI（觸控顯示整合），屬新方向。⑤不可用（比重表未拆）', 'Direct', 'Too_Coarse'),
        ('威盛', 'Major', None, 'annual_report_prose', '年報自述「威鋒電子 USB 相關高速傳輸控制晶片業務,約占 16%」，威鋒電子(6756)已獨立上市並自行建檔於本節點，依 R020③ 列為 Indirect 且不計比重。role 理由：威盛自身的經營活動為嵌入式平台處理器與 ASIC 設計服務；威鋒電子之 USB 高速傳輸控制晶片並非該等業務的延伸或載體，而是集團內另一條獨立且已單獨上市的產品線——年報在全無營業比重表的情況下仍單獨具名揭露此一業務及其規模，公司自身即視其為需要交代的重要事業，故列 Major。年報自述約占 16% 僅作佐證。★本列的重要性依據較其他 Indirect 列薄弱（威盛年報無比重表，全部曝險的 weight 皆為 NULL），若日後取得部門別揭露須複核', 'Indirect', 'Too_Coarse'),
        ('凌陽', 'Major', None, 'annual_report_prose', '年報業務範圍載明「USB」：owsHelloHumanPresence筆電嵌入式AI攝像頭控制晶片。B.USB外接式網路攝影機ISP晶片(a)USB3.0高品質4K2K★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('創惟', 'Core', None, 'annual_report_prose', '年報業務範圍載明「USB」：千元。(2)最近年度開發成功之技術或產品:開發期間開發成功之技術或產品USB3.2Gen1/PD整合型集線器控制晶片USB4®實體層技術11★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('嘉雨思-創', 'Core', '高速通訊 IC', 'annual_report_table', '年報營業比重表：高速通訊 IC 100.00%', 'Direct'),
        ('威鋒電子', 'Core', 'USB相關控制晶片', 'annual_report_table', '年報營業比重表：USB相關控制晶片 99.99%', 'Direct'),
        ('安國', 'Major', '資訊及消費性產品的週邊及控制IC', 'annual_report_table', '同一線 33.08% 的週邊(I/O)部分；與 A4-19 為同一營收線，兩格均不計比重', 'Direct', 'Too_Coarse'),
        ('安格', 'Core', 'IC晶片', 'annual_report_table', '年報營業比重表：IC晶片 98.65%，主要產品為 USB-C(DP ALT Mode)對 HDMI/VGA 視訊轉換晶片、HDMI 訊號增強與切換器晶片及 USB 記憶卡/智慧卡讀寫晶片。Core 理由：最大營收線，年報自述「確認以單晶片研發銷售為主要營運方向」並以視訊轉換的單晶片解決方案為主要產品方向(R023①⑤)', 'Direct'),
        ('旺玖', 'Core', '數位系統應用產品', 'annual_report_table', '年報營業比重表：數位系統應用產品 56.69%，該類別產品為工業規格 USB 串列介面(UART/SPI/I2C)橋接器控制晶片、USB 2.0 Hub 集線器控制晶片、讀卡器控制晶片及 USB 1.1/2.0/3.0 Host to Host 橋接晶片。Core 理由：最大營收線，計畫開發新產品亦以 USB3.2 Host to Host 橋接晶片與 Sensor Hub 為主(R023④⑤)', 'Direct'),
        ('晶焱', 'Core', 'ESD', 'annual_report_table', '年報營業比重表：ESD 95.51%', 'Direct'),
        ('祥碩', 'Core', '高速介面控制晶片', 'annual_report_table', '年報營業比重表：高速介面控制晶片 78.81%', 'Direct'),
        ('精拓科', 'Core', '混合式IC', 'annual_report_table', '年報營業比重表：混合式IC 65.15%', 'Direct'),
        ('聯陽', 'Core', None, 'annual_report_prose', '年報業務範圍載明「介面IC」：提供及時到位的技術服務,終能贏得客戶的信任,維持市場領先地位。(2)高速影音介面IC隨著Gaming遊戲與串流影音市場的蓬勃發展,消費者對於。Core 理由：①年報商品項目原文「本公司主要產品為桌上型電腦**輸出入控制晶片系列IC(Super IO)**、筆記型電腦內嵌式控制晶片系列IC(Embedded Control)、高速影音介面相關晶片系列IC、系統單晶片SoC」——輸出入控制晶片列首；所營業務亦列「B.超級/特定用途**輸出入積體電路**及模組」；④計畫開發首項為「配合 Intel/AMD 晶片支援 eRPMC 功能…桌上型電腦**輸出入控制IC**晶片」。⑤不可用：兩個候選權重皆 NULL★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('譜瑞-KY', 'Core', '高速傳輸介面晶片', 'annual_report_table', '年報營業比重表：高速傳輸介面晶片 85.62%', 'Direct'),
        ('瑞昱', 'Major', None, 'annual_report_prose', '年報商品項目列 USB 存儲橋接晶片、USB 3.2 Gen1/Gen2 集線器、USB4 集線器控制晶片、USB Type-C 與電源傳輸(PD)控制晶片、eUSB2 Repeater、I3C 集線器控制器等系列', 'Direct', 'Too_Coarse'),
        ('鈺創', 'Minor', None, 'annual_report_prose', '年報「目前之商品(服務)項目」載明集團晶片產品含 USB Type-C Switch 與電力傳輸(Power Delivery)晶片，計畫開發新商品亦列 Type-C Switch 與 USB Power Delivery。role 理由：系統晶片為記憶體以外之次要產品線，年報僅一條 99.89% 粗線無法量化', 'Direct', 'Too_Coarse'),
        ('神盾', 'Major', '資訊及消費性產品的週邊', 'annual_report_table', '年報營業比重表：資訊及消費性產品的週邊(及控制 IC) 18.99%；商品說明「透過 USB 介面裝置之需求與相關控制 IC…包括儲存裝置控制 IC、記憶卡讀卡機控制 IC、智慧卡讀卡機控制 IC、集線器控制 IC、輸出輸入裝置控制 IC」，屬本節點定義的 USB／橋接介面 IC', 'Direct'),
    ],
    "A4-12": [
        ('普誠', 'Major', None, 'annual_report_prose', '年報商品項目「顯示器驅動積體電路系列」列 TFT LCD Driver IC、LCD Driver IC、VFD Driver IC、OLED Driver IC。年報比重僅「積體電路(IC)產品 99.48%」一條粗線故不計比重(R020②)。本格原為 Core，2026-09-03 依 R023 反否決條件改判降為 Major：年報載明既有 VFD/STN/TFT 顯示驅動產品「以最有效率的資源維持穩定的產品現金流收益」，且「顯示器驅動研發團隊的研發重心已成功轉型,全面聚焦於…印表機噴墨頭驅動晶片(PHD)」（該業務見 A4-27）、「未來,PHD 將取代傳統顯示驅動」——研發資源已移出本節點，構成歷史遺留業務，不得為 Core', 'Direct', 'Too_Coarse'),
        ('凌通', 'Major', None, 'annual_report_prose', '年報商品項目 C.液晶顯示控制晶片；計畫開發新商品列新一代 8 位元 CPU 液晶顯示控制晶片(內建 LCD 升壓及控制電路)', 'Direct', 'Too_Coarse'),
        ('力領科技', 'Core', '顯示器驅動晶片', 'annual_report_table', '年報營業比重表：顯示器驅動晶片 100.00%', 'Direct'),
        ('天鈺', 'Core', '顯示器驅動IC', 'annual_report_table', '年報營業比重表：顯示器驅動IC 58.26%', 'Direct'),
        ('奕力-KY', 'Core', '顯示器驅動IC', 'annual_report_table', '年報營業比重表：顯示器驅動IC 100.00%', 'Direct'),
        ('敦泰', 'Core', None, 'annual_report_table', '年報營業比重僅「人機介面應用晶片 100.00%」一條粗線，橫跨顯示驅動與觸控/指紋兩節點故兩格均不計比重(R020②)。Core 理由：年報業務之主要內容為「顯示驅動、觸控、指紋等人機介面解決方案」，顯示驅動列於首位；商品項目首列之觸控驅動整合單晶片(IDC/TDDI)本身即為整合顯示驅動之產品(R023①②)', 'Direct', 'Too_Coarse'),
        ('晶宏', 'Core', '顯示器驅動IC', 'annual_report_table', '年報營業比重表：顯示器驅動IC 91.46%', 'Direct'),
        ('瑞鼎', 'Core', '顯示器驅動IC', 'annual_report_table', '年報營業比重表：顯示器驅動IC 96.37%', 'Direct'),
        ('矽創', 'Core', None, 'annual_report_prose', '年報業務範圍載明「顯示驅動」：技術及產品年度技術內容AMOLEDCOG極致邊框顯示器驅動晶片。防水型觸控暨顯示驅動整合晶片。長波長(發光元件光波長為1300nm)三合一O。Core 理由：④長期策略決定性——年報「計畫開發之新產品及服務」首項為「(1)物聯網..**TFT-LCD AOD 觸控暨顯示驅動整合晶片**」，屬顯示驅動；①所營業務僅泛稱「各種積體電路之設計、製造、測試及銷售」不區辨，年報無「商品項目」段落。⑤不可用：年報比重表為應用別（AIoT／工控／感測器／車用），非產品類別，不可據以判定節點★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('聯詠', 'Core', '平面顯示器驅動晶片', 'annual_report_table', '年報營業比重表：平面顯示器驅動晶片 62.87%。Core 理由：最大營收線，主要產品表以平面顯示驅動晶片列首，計畫開發之產品十六項中九項為顯示驅動或 TDDI 晶片(R023①④⑤)', 'Direct'),
        ('研通', 'Major', '液晶驅動器', 'annual_report_table', '年報本集團營業比重表：液晶驅動器 16.62%。A4-12 定義列「TFT-LCD 與 AMOLED 顯示驅動」；公司商品項目亦列「(2)液晶驅動器IC及TFT LCD面板」。依 R026 15% 預設為 Major。★2026-09-15 M5：新增——個體表僅 0.05%，分類依據改為合併表（P22-2）後浮現；合併表其餘品項（記憶IC 10.94%、電腦週邊控制IC 5.76%、語音控制IC 3.99%、其他 3.99%）無足夠定義依據新增曝險', 'Direct'),
    ],
    "A4-13": [
        ('宏觀', 'Core', None, 'annual_report_table', '年報營業比重「射頻積體電路(RF IC) 97.08%」一線同時含電視/機上盒調諧器、衛星通訊、光纖通訊與物聯網通訊四類商品，橫跨多節點故不計比重(R020②)。Core 理由：年報自述「本公司主要從事射頻晶片(RF IC)之研究、設計、開發及銷售」，並說明射頻晶片為訊號接收端第一道關卡；反否決檢查：光通訊與物聯網係「近年亦積極投入研發資源拓展多元產品線」之新增，非射頻資源移出(R023①②)', 'Direct', 'Too_Coarse'),
        ('立積', 'Core', 'Wi-Fi 產品', 'annual_report_table', '年報僅列「Wi-Fi 產品 83.59%」一線，射頻前端元件與網通應用合列，無法拆到節點。Core 理由：①年報商品項目順序為 A.**Wi-Fi 無線網路使用之 RF IC 射頻前端晶片**（功率放大器、低雜訊放大器、天線開關）B.無線影音傳輸 RF IC C.4G/5G RF IC，前三項皆為射頻前端；④計畫開發首項「第二代線性 Wi-Fi 7 **FEM** 高功率應用之相關前端射頻晶片」。⑤不可用（比重表未拆）', 'Direct', 'Too_Coarse'),
        ('笙科', 'Core', 'RFIC(無線射頻積體電路)', 'annual_report_table', '年報營業比重表：RFIC(無線射頻積體電路) 100.00%', 'Direct'),
    ],
    "A4-14": [
        ('佑華', 'Core', None, 'annual_report_prose', '年報業務範圍載明「音效」：使用高壓縮演算法,完成高音質教育性產品、及超長秒數語音播放產品。b.使用聲音效果器演算法,完成變音相關產品。c.使用語音辨識演算法,可整合至。Core 理由：①年報「公司目前之商品(服務)項目」首句即「可分為**語音控制IC**、微控制器IC及32位元高階應用IC三大類」，條列順序 (1)語音控制IC (2)微控制器產品 (3)32位元高階應用IC，語音列首；④計畫開發首項為「(1)**錄音類產品**：朝向更高錄音秒數、更好錄音品質、並整合高附加價值功能之系列產品開發」。⑤不可用：兩個候選權重皆 NULL★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('瑞昱', 'Major', None, 'annual_report_prose', '年報商品項目「電腦週邊暨智慧互聯產品」列 HD-A 音訊編碼解碼晶片、高電壓音訊功率放大器、SoundWire 音訊編解碼與功率放大器、車用音訊數位訊號處理器等系列', 'Direct', 'Too_Coarse'),
        ('鈺太', 'Core', '微機電麥克風晶片', 'annual_report_table', '年報營業比重表：微機電麥克風晶片 88.41%', 'Direct'),
        ('鈺寶-創', 'Core', '晶片', 'annual_report_table', '年報營業比重表：晶片 42.00%；產業概況「本公司為無線音訊控制晶片及模組之 IC 設計公司,長期居於高效能、低功耗、低延遲無線音訊傳輸技術之領先地位」「本公司獨創之SynicSA® 空間音效演算法」。★原 Core 掛 F7-01 記憶體模組，係「模組」二字命中無線音訊傳輸模組所致——「記憶體」三字在該公司年報全文查無，已於 2026-09-03 移除。Core 理由（2026-09-03 因新增 F8 無線模組使模組 58% 成為合法候選而重跑 R023）：公司自述為 IC 設計公司，核心價值在自有無線音訊傳輸協定與 SynicSA® 演算法，模組是該晶片的出貨載體而非另一門生意，依 R024（一）Core 留在上游 IC，不因下游模組營收 58% > 42% 而移轉(R023①②)', 'Direct'),
        ('鑫創', 'Core', '音訊IC及其他', 'annual_report_table', '年報營業比重表：音訊IC及其他 85.27%', 'Direct'),
        ('驊訊', 'Core', None, 'annual_report_prose', '年報業務範圍載明「音訊」：。驊訊電子USBAudio晶片產品線涵蓋:\uf0b7入門級USBCodec–針對標準音訊應用,提供高性價比方案。-70-\uf0b7中高階USBCodec–具★2026-09-09 weight_status=Not_Disclosed：比重表的軸是交易性質（銷貨收入 99.67／勞務收入／權利金收入）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('聯發科', 'Minor', None, 'annual_report_prose', '年報商品項目 G.藍牙音訊晶片。role 理由：十一項商品中之一項，年報未給個別比重', 'Direct', 'Too_Coarse'),
        ('新唐', 'Major', None, 'annual_report_prose', '年報商品項目 2)音訊系統晶片 3)音效增強晶片 4)音頻轉換/放大晶片；年報自述「佈建完整的音訊晶片平台」，涵蓋 Audio CODEC、Audio MCU、D 類與智慧音訊功率放大器及 DSP 音訊方案', 'Direct', 'Too_Coarse'),
        ('凌通', 'Major', None, 'annual_report_prose', '年報商品項目 A.數位音訊解碼播放控制晶片 B.語音辨識與合成控制晶片，為年報所營業務所列之第一項產品', 'Direct', 'Too_Coarse'),
        ('太欣', 'Minor', '數位電腦耳機IC', 'annual_report_table', '年報營業比重表：數位電腦耳機IC 11.5%', 'Direct'),
        ('神盾', 'Minor', '無線音頻控制IC', 'annual_report_table', '年報營業比重表：無線音頻控制IC 5.67%；商品說明「無線音頻控制 IC 及模組、智慧遙控器、控制訊號傳輸晶片及模組」，應用於無線耳機、無線麥克風、可攜式音響', 'Direct'),
    ],
    "A4-15": [
        ('君曜', 'Major', None, 'annual_report_prose', '年報商品項目：(2)觸控IC(Touch Panel/Pad IC)。與橋接 IC 共用單一 100% 營收線，無法拆分故不計比重', 'Direct', 'Too_Coarse'),
        ('矽統', 'Core', None, 'annual_report_prose', '年報自述「自2011年開始發展投射式電容觸控晶片、及人機介面相關技術內嵌式(In cell)等觸控」，屬觸控與人機介面 IC。年報比重僅「積體電路約100%」一條粗線故不計比重。原掛 A4-05 ASIC 設計服務係關鍵字誤中「MEMS麥克風ASIC」。Core 理由：①年報「集團目前商品、服務項目」順序為 **投射式電容觸控晶片**、觸控晶片控制電路板、SoC 整合與設計服務、觸控面板應用模組、主動筆控制晶片、微機電麥克風晶片、混合訊號微控制器——觸控晶片列首，混合訊號微控制器（A4-08）列於末段；所營業務亦把 (4)觸控晶片控制電路板 (5)主動筆控制晶片 具名列出；④計畫開發首項為「新一代投射式多點電容**觸控晶片**」。⑤不可用：兩個候選權重皆 NULL', 'Direct', 'Too_Coarse'),
        ('久昌', 'Core', 'HALLIC', 'annual_report_table', '年報營業比重表：HALLIC 96.68%', 'Direct'),
        ('昇佳電子', 'Core', '感測晶片', 'annual_report_table', '年報營業比重表：感測晶片 99.87%', 'Direct'),
        ('矽創', 'Major', None, 'annual_report_prose', '年報業務範圍載明「感測晶片」：低功耗效能,應用覆蓋本公司物聯網、工控設備、與穿戴等顯示器驅動晶片產品線。在感測晶片市場上,面對供應鏈的結構性改變、設計朝向微型輕薄化、製造★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('義隆', 'Core', '消費性觸控積體電路', 'annual_report_table', '年報營業比重表：消費性觸控積體電路 27.72%；其筆電輸入裝置模組 69.92% 另計於 C11-01，本業為觸控/HMI 晶片設計，Core 依商業本質判定而非最大營收線（R023）。Core 理由：★這是①推翻⑤的案例，亦為 R019 規則本文所引用者。⑤指向 C11-01：筆電輸入裝置模組 69.92% 遠大於本格消費性觸控積體電路 27.72%。但①年報所營業務首句列舉順序為「(a)**觸控螢幕晶片** (b)帶筆功能的觸控螢幕晶片 (c)觸控板模組 (d)指向裝置 (e)生物辨識晶片」——晶片列前兩位、模組列第三；②價值創造位置同向（★2026-09-07 標明：此為推論非年報引證，年報未揭露分部成本結構）。R023 將①置於⑤之前，故 Core 判在 A4-15', 'Direct'),
        ('禾瑞亞', 'Core', '觸控面板控制器/控制晶片', 'annual_report_table', '年報營業比重表：觸控面板控制器/控制晶片 96.00%', 'Direct'),
        ('通泰', 'Major', None, 'annual_report_table', '年報主要產品系列表：觸摸系列 55.40%，為五個產品系列中最大者；「長、短期業務發展計畫」之產品系列表列該系列應用範圍為 Light Control Series 與 Touch pad Series (1~16 Keys)。該比重表未進入 tw.product_revenue(屬 check_industry [mix] 待修清單)故本輪不計比重★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('敦泰', 'Major', None, 'annual_report_prose', '年報商品項目 (3)電容式觸控晶片 (4)指紋辨識晶片及模組 (7)NB 觸控板(Touchpad)；年報自述為「全球觸控與顯示驅動技術的領導品牌」', 'Direct', 'Too_Coarse'),
        ('神盾', 'Core', '生物辨識感測IC及其應用', 'annual_report_table', '年報營業比重表：生物辨識感測IC及其應用 36.25%；商品說明「(1)生物辨識感測 IC：針對客戶提供多種規格的 IC 與模組…另外還提供**指紋辨識**之應用軟體」，本節點定義具名含 Fingerprint。★2026-09-04 由 A4-16 影像 IC 移入：A4-16 於當日定義為「以取得影像為目的之感測」，指紋等非成像感測明文歸本節點，前版歸屬在該定義寫出來前無從判別。Core 理由：⑤本格 36.25% 為最大營收線；①公司自述的商品順序亦以生物辨識感測 IC 列首', 'Direct'),
    ],
    "A4-16": [
        ('傑霖科技', 'Core', '影像處理系統單晶片', 'annual_report_table', '年報營業比重表：影像處理系統單晶片 86.07%', 'Direct'),
        ('凌陽', 'Major', None, 'annual_report_prose', '年報業務範圍載明「影像感測」：DSP)、微控制器(MCU)及產品,第三類為SmartUSB介面等,功能為將影像感測器接收之影像訊HMI控制晶片。號進行處理與壓縮,再傳送至★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('凌陽創新', 'Core', None, 'annual_report_prose', '年報業務範圍載明「影像處理」：目前主要產品線分為兩大類:(1)筆電嵌入式ISP晶片及USB外接式網路攝影機影像處理控制晶片。(2)個人電腦週邊裝置微控制晶片及低功耗IOT。Core 理由：①年報「目前主要產品線分為兩大類」順序為 (1)筆電嵌入式 **ISP晶片**及 USB 外接式網路攝影機**影像處理控制晶片** (2)個人電腦週邊裝置微控制晶片，影像列首；④計畫開發首項亦為「(1)**影像處理控制晶片**產品 A.筆電嵌入式攝影機 ISP 晶片」。⑤不可用（比重表未拆）★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('原相', 'Core', 'CMOS影像感測元配件', 'annual_report_table', '年報營業比重表：CMOS影像感測元配件 98.17%', 'Direct'),
        ('晶相光', 'Core', 'CMOS影像感測元件', 'annual_report_table', '年報營業比重表：CMOS影像感測元件 97.99%', 'Direct'),
        ('研通', 'Major', '影像視訊控制IC應用產品', 'annual_report_table', '年報本集團營業比重表：影像視訊控制IC應用產品 37.50%，大於其微處理器IC 21.20%（見 A4-08）。Core 理由：④與⑤同向而①指向另一邊，取二比一。①年報商品項目順序為 (1)**微處理器 IC應用產品** (2)液晶驅動器IC及TFT LCD面板 (3)記憶IC (4)多媒體IC (5)無線通訊，微處理器列首；但④計畫開發首項為「無線通訊結合**高品質影音技術**之應用研發：RF、Wifi、BT 等無線通訊用於 **FHD 影像**之傳輸及高音質音頻訊號之傳輸」，指向影像視訊；⑤本格影像視訊控制IC 37.50%（個體表 56.16%）亦大於 A4-08 微處理器IC 21.20%（個體表 31.07%）。★2026-09-07 由 Core 降為 Major：原裁定用「取二比一」計票，方法本身就違反 R023 的**依序判定**；更關鍵的是漏引了年報中唯一的直接自述句——「4.公司目前之商品(服務)項目：本公司為專業 IC 應用設計服務公司，以研發技術為主要核心資源，**目前主要商品是微處理器(Micro Control Unit; MCU)產品之應用設計**」。那句明確指定主要商品，是①的最強形式。Core 依 R023 移至 A4-08★2026-09-15 M5：改以本集團合併表為分類依據（P22-2），角色維持 Major', 'Direct'),
        ('芯鼎', 'Core', '智慧影像處理系統晶片', 'annual_report_table', '年報營業比重表：智慧影像處理系統晶片 92.00%', 'Direct'),
        ('鈺創', 'Minor', None, 'annual_report_prose', '年報載明集團晶片產品含 3D 雙目深度圖視覺處理晶片、3D 雙目影像擷取控制晶片與影像及人工智慧次系統解決方案。role 理由：系統晶片為記憶體以外之次要產品線，年報僅一條 99.89% 粗線無法量化', 'Direct', 'Too_Coarse'),
    ],
    "A4-17": [
        ('偉詮電', 'Major', None, 'annual_report_prose', '年報業務範圍載明「馬達驅動」：pervisorIC)。(2)智慧應用IC:-氣冷散熱方案如高整合度智慧風扇馬達驅動與控制IC。-液冷散熱方案如水泵馬達控制IC、漏液偵測I★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('普誠', 'Core', None, 'annual_report_prose', '年報自述「本公司目前主要營業項目為TFT驅動IC、馬達驅動IC、LED照明驅動IC」，馬達驅動列名第二；商品項目「馬達驅動積體電路系列」列直流馬達、步進馬達、伺服馬達、三相無刷直流馬達驅動器/控制器與柵極驅動 IC。年報比重僅「積體電路(IC)產品 99.48%」一條粗線故不計比重(R020②)。Core 理由（2026-09-03 於新增 A4-27 後依 R023 第二次重跑，候選集合＝A4-12/A4-17/A4-18/A4-27/A4-08）：① 原 Core A4-12 因反否決條件出局（研發重心已全面轉向 A4-27 噴墨頭驅動、既有面板驅動僅「維持現金流」）；② 剩餘候選中，年報兩份獨立清單（主要營業項目、研發主要重點）皆將馬達驅動 IC 排在 LED 照明驅動 IC 之前，而 A4-27 不在主要營業項目之列，故 R023① 指向本節點；③ A4-27 在 R023②④ 上其實最強（高壓製程精密控制、與全球領先大廠訂製合作、消耗品特性形成天然屏障），但年報將其定位寫成未來式且限縮於顯示器驅動團隊——「**未來**,PHD 將取代傳統顯示驅動,成為顯示器驅動研發團隊**中長期**最具潛力的高毛利獲利基石」，資源移出 A4-12 不等於目的地已成為公司當下的重心，R023 又明定 ① 優先於 ②④，故 Core 判給 A4-17；④ 馬達驅動線正「由單一驅動IC供應商,全面轉型為結合『驅動、控制與演算法』的系統級整合方案提供者」，資源在投入而非移出，反否決條件不成立。★複查點：若下一年度年報把 PHD 寫進主要營業項目或給出獨立比重，本裁定應再跑一次', 'Direct', 'Too_Coarse'),
        ('茂達', 'Major', '放大及驅動IC', 'annual_report_table', '年報營業比重表：放大及驅動IC 29.05%，含筆電散熱風扇馬達驅動與控制IC', 'Direct'),
        ('點晶', 'Minor', '轉動移動偵測與馬達驅動晶片', 'annual_report_table', '年報營業比重表：轉動移動偵測與馬達驅動晶片 6.90%（原存 100% 為解析取錯欄，2026-09-02 更正）', 'Direct'),
        ('旺玖', 'Major', None, 'annual_report_table', '年報「微機電整合應用產品 41.96%」之機電整合控制晶片為馬達驅動晶片與霍爾磁感應晶片，計畫開發新產品列數位風扇馬達晶片、車用三相風扇馬達驅動晶片、整合 LIN-BUS 之風扇馬達驅動晶片；該線另含 PDU 電力分配器與電力計量等綠色能源產品，橫跨節點無法拆分故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "A4-18": [
        ('聚積', 'Core', 'LED驅動積體電路', 'annual_report_table', '年報營業比重表：LED驅動積體電路 99.76%', 'Direct'),
        ('點晶', 'Core', '靜態偵測與驅動混合數位及類比晶片', 'annual_report_table', '年報營業比重表：靜態偵測與驅動混合數位及類比晶片 93.10%，即其年報所稱靜態混合訊號系列（LED 驅動管理晶片）。Core 理由：①年報商品項目首項為「**LED顯示屏驅動晶片**(定電流輸出、灰階控制)」；④計畫開發首項「混合數位及類比晶片：網路化高品質 **LED顯示驅動晶片**」；⑤本格 93.10% 遠大於 A4-17 馬達驅動 6.90%。三項同向', 'Direct'),
        ('普誠', 'Major', None, 'annual_report_prose', '年報自述主要營業項目第三項為「LED照明驅動IC」，商品項目列發光二極體驅動積體電路(LED Driver IC)，並自述已建構完整車用/商用 LED 產品線(DC/DC 升降壓、恆壓/恆流控制及線性驅動器)且取得 AEC-Q100 車規認證', 'Direct', 'Too_Coarse'),
        ('凱鈺', 'Minor', None, 'annual_report_prose', '年報商品表「能源產業」列安控紅外線 LED 照明之 DC/DC 驅動 IC 與車用 LED 燈之 DC/DC 驅動 IC；能源產業 13.91% 另含 10 kW 小型風力發電系統(I2-01)，兩節點共用一線故均不計比重(R020②)。role 理由：同 I2-01——凱鈺的主要經營活動為光通訊 IC 與房地銷售；安控與車用 LED 之 DC/DC 驅動 IC 併於第三大的「能源產業」一欄，與小型風力發電系統共用一線且無拆分依據，相對於通訊 IC 本業屬附帶之經濟地位，故列 Minor。該欄合計 13.91% 僅作佐證', 'Direct', 'Too_Coarse'),
    ],
    "A4-19": [
        ('安國', 'Major', '資訊及消費性產品的週邊及控制IC', 'annual_report_table', '年報「資訊及消費性產品的週邊及控制IC 33.08%」一線橫跨控制 IC 與 I/O，無法拆到節點', 'Direct', 'Too_Coarse'),
        ('群聯', 'Core', '控制晶片', 'annual_report_table', '年報營業比重表：控制晶片 19.26%。Core 理由：★這是①②④共同推翻⑤的案例。⑤指向 F7-01：快閃記憶體模組 72.41% 遠大於本格控制晶片 19.26%。但①年報業務範圍首句為「本集團從事**快閃記憶體控制晶片**及其應用產品整合之設計、製造及銷售業務」，其下四條業務逐條皆為「**控制晶片**及模組產品」且控制晶片列前；「目前之主要產品(服務)項目」四項亦皆以控制晶片起首；產業概況段更直言「本集團目前係**專注於快閃記憶體控制晶片**及週邊應用整合之設計等業務」。②價值創造位置同向：模組營收包含外購 NAND 顆粒的轉嫁成本，公司的技術價值在控制晶片。④計畫開發四項中三項為控制晶片（更微型高容量省電之控制晶片方案、車規控制晶片、Redriver/Retimer IC）。R023 將①②④置於⑤之前，故 Core 判在 A4-19。★**出處**：本列引用的年報原文出自 2025_8299_20260527F04.pdf **第 72 頁**，不在 tw.product_narrative——該表存的是公司治理章節，因為這份年報每個字元都重複輸出（「肆肆、、營營運運概概況況」），章節定位器比對不到標題。②模組營收含外購 NAND 轉嫁成本一句為推論，年報未揭露分部成本結構', 'Direct'),
        ('點序', 'Core', 'Flash Controller + Packaged Controller', 'annual_report_table', '年報營業比重表：Flash Controller 96.65% 加 Packaged Controller 0.35%（原存「其它 100%」為解析取到最小線，2026-09-02 更正）', 'Direct'),
    ],
    "A4-21": [
        ('愛普*', 'Core', '記憶體積體電路', 'annual_report_prose', '年報業務範圍載明「記憶體IC」：體相關的研發、設計、製造與銷售,並提供矽智財的技術支援與授權,為全球非標準型記憶體IC設計之領導廠商,以優越的品質與整合性的服務,協助客戶實★2026-09-09 補上 weight_item：年報比重表列「記憶體積體電路 90.94%」，與本節點記憶體 IC 設計的定義直接對應。', 'Direct'),
        ('晶豪科', 'Core', None, 'annual_report_prose', '年報業務範圍載明「記憶體IC」：省電或避免元件過熱的問題,以製程技術進行低電壓化是必要的。D.晶粒尺寸縮小化記憶體IC產品售價之持續下降,係產業之特性,為維繫產品的競爭地位★2026-09-09 weight_status=Not_Extracted：年報 @154「積體電路(IC)銷售收入為 14,575,272 仟元,其占營業比率為 100%」。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★2026-09-09 由 Not_Extracted 改判 Too_Coarse（R020-NE 的重新裁定）：頁面篩選補上「營業比率」後解析成功，取得的是**單一一條「積體電路(IC) 100%」**。那是產品軸，但粗到涵蓋公司全部業務——年報自述「主要業務為 DRAM、Flash Memory、**類比積體電路、類比與數位混合積體電路**」，本節點只收記憶體 IC，該線同時涵蓋本格與類比／混合訊號 IC，年報未給拆分依據。★**取得數字不等於數字屬於這一格**：100% 是公司的，不是 A4-21 的。', 'Direct', 'Too_Coarse'),
        ('鈺創', 'Core', None, 'annual_report_prose', '年報自述「主要業務為專精型緩衝記憶體產品(Specialty Buffer Memory)與系統晶片(System Chip)產品之設計開發」，記憶體產品含 DRAM、SPI NAND Flash 與 e.MMC。營收僅「記憶體及類比邏輯系統 ICs 99.89%」一條粗線故不計比重(R020②)。Core 理由：年報自述兩大業務中記憶體列於首位，MemorAiLink 一站式開發平台亦以客製化記憶體組合與記憶體介面 IP 為核心(R023①④)', 'Direct', 'Too_Coarse'),
    ],
    "A4-22": [
        ('力士', 'Core', 'MOSFET + ESD + Diode + Transistor', 'annual_report_table', '年報營業比重表四條合計 75.79%：MOSFET 65.91、ESD 7.18、Diode 1.41、Transistor 1.29(R020④)，四者皆為本節點宣告之功率分立元件。★★**本列於 2026-09-10 由 A4-07 電源管理 IC 移入，原分類是錯的**。原列的 evidence 開頭寫「年報無比重表；類比電源管理IC 設計」——而那句話本身就是錯的：年報有比重表，只是 scrapers.annual_report_revenue 的選檔規則把年報代碼硬編成 F04／FE4，而本公司的年報是 **F19**，於是被記成「公司沒有申報年報」。**一個建立在「查不到資料」之上的分類，在資料查到之後被自己的資料推翻。**★這是 R020-NE → R023-R 的第一個完整實例：weight_status 的 lifecycle 檢查([weight_status_lifecycle]) 在來源恢復可讀時亮燈，而重新裁定的結果不是換一個 weight，是換一個節點。★**無自有 fab，故不進 A3-04**（A4-22 與 A3-04 的分界就是這一條）。依本公司同日補抓到的年報業務章：「本公司自行設計或接受客戶委託，完成半導體電路之布局，屬半導體產業鏈中之**上游**」、「上游之半導體設計公司設計出製程所需的布局後…再經**晶圓代工廠**製造出晶圓半成品」、「**無晶圓廠**之半導體設計公司能將製造委由專業晶圓代工廠進行」、「強化與各**代工廠**合作關係，並了解磊晶矽晶圓片供貨狀況」。年報並自述功率半導體分為「功率分離式元件」與「功率積體電路」兩類，本公司產品屬前者。Core 理由：本節點四條線合計 75.79% 為公司主體，且為年報自述之產品類別(R019／R023①⑤)。★★**「Wafer 24.21%」不計入本格，也不因為叫 Wafer 就進 A3-04**（2026-09-10 裁定）：**有 Wafer 產品或 Wafer 銷售，與自己擁有晶圓廠、直接製造晶圓，是兩件事**。該線的經濟實質（自有設計＋委外投片的半成品晶圓銷售？或其他模式）尚未判定，依 R016 產品名稱本身不足以決定 Industry 歸屬，另開獨立裁定，本輪不建曝險。', 'Direct'),
        ('全宇昕', 'Core', 'MOSFET + 電晶體 + 二極體', 'annual_report_table', '年報營業比重表：MOSFET 84.87% 加電晶體 8.37% 加二極體 6.07%，三者同為功率分立元件、本節點定義即含二極體與整流器，合計 99.31%', 'Direct'),
        ('力智', 'Major', '功率元件(MOSFET)', 'annual_report_table', '年報營業比重表：功率元件(MOSFET) 23.87%', 'Direct'),
        ('博盛半導體', 'Core', 'MOSFET', 'annual_report_table', '年報營業比重表：MOSFET 100.00%', 'Direct'),
        ('大中', 'Core', '功率半導體產品', 'annual_report_table', '年報營業比重表：功率半導體產品 100.00%', 'Direct'),
        ('富鼎', 'Core', '低壓金氧半功率場效電晶體', 'annual_report_table', '年報營業比重表：低壓金氧半功率場效電晶體 49.07%', 'Direct'),
        ('尼克森', 'Core', '功率金氧半場效電晶體', 'annual_report_table', '年報營業比重表：功率金氧半場效電晶體 99.94%', 'Direct'),
        ('廣閎科', 'Core', '功率半導體元件', 'annual_report_table', '年報營業比重表：功率半導體元件 86.66%', 'Direct'),
        ('杰力', 'Core', '功率元件', 'annual_report_table', '年報營業比重表：功率元件 76.78%。Core 理由：①年報「合併公司主要業務係設計及銷售**功率元件(Power Device)**及電源管理積體電路」，功率元件列於句首；商品項目表列順序同為功率元件、電源管理IC；④計畫開發首項為「(1)功率元件 A.新世代第六代 S.G 元件製程之低壓 N-type 溝渠式功率金氧半場效電晶體」；⑤本格 76.78% 大於 A4-07 的 23.22%。三項同向', 'Direct'),
        ('茂達', 'Major', None, 'annual_report_table', '年報離散式功率元件 44.91% 為合併數，來自並列入合併的大中。role 理由：該業務對茂達具實質經濟重要性（合併營收占比最高的一條線），故為 Major。★★**2026-09-11 複核：本列的 Not_Applicable 理由＝合併治理，不是度量不適用**。四個 weight_status 沒有一個套得上，而這**不足以新增第五個狀態**（母體 n=1）：度量完全適用（公司在合併基礎上確實從事該業務）、數字已揭露且已取得（44.91%）、粒度也足夠（那條線就是本節點）。不給比重的真正理由是**同一筆經濟活動在兩個獨立上市個體上都可成為 exposure**：子公司 **6435 大中**已於本節點獨立建檔為 Core 100%（2026-09-11 查證：6435 在市、security_type=STOCK、A4-22 唯一 exposure），母公司再計會在市場層重複計數。★這是 **entity-level ownership boundary 與 exposure 的衝突**，屬分類治理問題，已具名記於 db/TAXONOMY_GAPS.md 的「What is *not* a gap」表。★**不得把本列讀成「營收比重對它不適用」**——那會讓日後的讀者分不出這是 measurement semantics 還是 consolidation governance，而兩者需要的後續工作完全不同。', 'Indirect', 'Not_Applicable'),
        ('虹冠電', 'Major', '功率元件', 'annual_report_table', '年報營業比重表：功率元件 34.65%', 'Direct'),
    ],
    "A4-23": [
        ('凱鈺', 'Core', None, 'annual_report_table', '年報營業比重表：通訊產業 48.85%。年報「3. 公司目前之商品」表列該產品線為光纖收發模組之雷射/LED 驅動器、限幅放大器、轉阻放大器(TIA)、數位監控控制器及升壓 IC、GPON/EPON 光纖盒與 4G/5G 基地台回傳光纖網路，另含數位機上盒/卡拉OK 的 Echo chip 與工業 4.0 工廠自動化設備控制及資料傳輸兩項非光通訊產品。該比重表未進入 tw.product_revenue(屬 check_industry [mix] 待修清單)故本輪不計比重。Core 理由：通訊產業為最大營收線且該線以光纖收發相關 IC 為主體(R023⑤②)；計畫開發之新商品首列 10Gbps APD TIA 與 25Gbps/100Gbps TIA、10Gbps 整合型晶片解決方案，研發資源集中於光通訊 IC(R023④)。反否決檢查：房地銷售 35.49% 次大，但研發全數投入通訊與能源 IC，通訊非資源移出之歷史業務★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('宏觀', 'Major', None, 'annual_report_prose', '年報商品項目「光纖通訊部分」列應用於衛星 RF 光纖傳輸之轉阻放大器、10G APD/PIN 轉阻放大器、25G SR 與 4X25G SR4 超高速光通信晶片組、HDMI AOC 晶片組；年報另載明針對網路基礎建設開發對稱式 10G-PON 驅動晶片', 'Direct', 'Too_Coarse'),
    ],
    "A5-05": [
        ('瑞峰', 'Core', '晶圓封裝收入', 'annual_report_table', '年報營業比重表：晶圓封裝收入 88.22%。受託證據為公司自述：「本公司也提供**客戶**一站式(Turnkey Solution)封裝及測試服務，藉由結合策略聯盟的專業測試廠商及封裝廠商，提供**客戶完整後段製程代工服務**」，並自述與知名晶圓代工廠的策略聯盟、國際客戶合作經驗。★「其他營業收入 11.78%」**不納入**：年報未說明其內容，沒有證據支持它符合本節點 Definition 就不得計入——公司符合 A5-05 不等於公司 100% 營收都符合 A5-05，分類單位是營收線不是公司身分。★★**2026-09-11 由 A5-02 先進封裝移入本節點**（R031 兩軸共存判準的第一個裁定案例）。A5-02 是**技術軸**、本節點是**商業模式軸**，而「晶圓級封裝服務」這一條營收線**同時完整落入兩者的 Definition**——依 R031 新增條款，共存不再中性，必須裁決。裁決依母體而非偏好：本節點 16 家成員共享同一個 identity（受託為客戶封裝／測試），而 A5-02 的三家成員全部也是這個 identity；A5-02 自己的定義指出真正技術軸專屬的情形是「自有晶圓製造者做自己的先進封裝」，該母體**實際觀測為 0**。', 'Direct'),
        ('台星科', 'Core', '晶圓級封裝服務 + 測試服務', 'annual_report_table', '年報營業比重表兩條合計 99.04%：晶圓級封裝服務 60.06、測試服務 38.98(R020④)；其他 0.96% 未指明不納入。受託證據為公司自述：「**本公司為專業之 IC 封測廠**，主要之**服務**項目為晶圓及積體電路之測試服務、產品可靠度驗證服務及失效分析服務、晶圓級晶片尺寸封裝(WLCSP)**服務**、晶圓凸塊(Bumping)封裝**服務**」（年報第 61 頁）。★★**原記 60.06% 是技術軸造成的量測截斷**：掛在 A5-02 時只有「封裝」這條線符合「先進封裝」的技術定義，而測試服務 38.98% 明明同樣是受託服務卻被排除在外。**技術格不只是與商業模式重疊，它會把公司的營收裁剪成自己的形狀。**★★**2026-09-11 由 A5-02 先進封裝移入本節點**（R031 兩軸共存判準的第一個裁定案例）。A5-02 是**技術軸**、本節點是**商業模式軸**，而「晶圓級封裝服務」這一條營收線**同時完整落入兩者的 Definition**——依 R031 新增條款，共存不再中性，必須裁決。裁決依母體而非偏好：本節點 16 家成員共享同一個 identity（受託為客戶封裝／測試），而 A5-02 的三家成員全部也是這個 identity；A5-02 自己的定義指出真正技術軸專屬的情形是「自有晶圓製造者做自己的先進封裝」，該母體**實際觀測為 0**。', 'Direct'),
        ('精材', 'Core', '晶圓級尺寸封裝 + 晶圓級後護層封裝 + 測試服務', 'annual_report_table', '年報營業比重表三條合計 98.02%：測試服務 47.86、晶圓級尺寸封裝 44.98、晶圓級後護層封裝 5.18(R020④)；其他 1.98% 未指明不納入。受託證據為公司自述：「對**客戶**需求能夠提供高度彈性與**客製化封裝服務**」、「12吋方面，目前有數家穿戴裝置相關元件**客戶**…**客製化的技術服務能力**」。★★**原記 50.16% 同樣是技術軸截斷**：最大的一條「測試服務 47.86%」因不符「先進封裝」而被排除，使本公司的 exposure 少掉近一半。★★**2026-09-11 由 A5-02 先進封裝移入本節點**（R031 兩軸共存判準的第一個裁定案例）。A5-02 是**技術軸**、本節點是**商業模式軸**，而「晶圓級封裝服務」這一條營收線**同時完整落入兩者的 Definition**——依 R031 新增條款，共存不再中性，必須裁決。裁決依母體而非偏好：本節點 16 家成員共享同一個 identity（受託為客戶封裝／測試），而 A5-02 的三家成員全部也是這個 identity；A5-02 自己的定義指出真正技術軸專屬的情形是「自有晶圓製造者做自己的先進封裝」，該母體**實際觀測為 0**。', 'Direct'),
        ('典範', 'Core', '先進基板或導線架型積體封裝及測試', 'annual_report_table', '年報營業比重表：先進基板或導線架型積體封裝及測試 65.23%', 'Direct'),
        ('力成', 'Core', 'IC封裝服務', 'annual_report_table', '年報營業比重表：IC封裝服務 61.76%', 'Direct'),
        ('南染', 'Core', '半導體封裝測試', 'annual_report_table', '年報營業比重表：半導體封裝測試 67.98%', 'Direct'),
        ('南茂', 'Core', '封裝', 'annual_report_table', '年報營業比重表：封裝 28.53%', 'Direct'),
        ('同欣電', 'Core', '影像產品 + 混合積體電路模組', 'annual_report_table', '年報營業比重表：影像產品 49.50%（CIS 封裝）加混合積體電路模組 20.10%，封裝業務合計 69.60%，大於陶瓷電路板 17.56%。Core 理由：①年報商品項目段首句即「**同欣致力於成為世界級的專業封裝測試代工廠**」，為公司對自身定位的直接陳述；⑤本格 69.60%（影像產品 49.50 加混合積體電路模組 20.10，R020④）大於 A1-14 陶瓷電路板 17.56%', 'Direct'),
        ('捷敏-KY', 'Core', '封裝及測試服務', 'annual_report_table', '年報營業比重表：封裝及測試服務 97.25%', 'Direct'),
        ('日月光投控', 'Core', '封裝服務', 'annual_report_table', '年報營業比重表：封裝服務 47.78%', 'Direct'),
        ('福懋科', 'Core', 'IC封裝測試', 'annual_report_table', '年報營業比重表：封裝測試 74.00%。Core 理由：①年報「本公司封測模組主要服務項目」整段皆為封裝形式（TSOP、SOP、TQFP、BGA、QFP、TFBGA…共 33 項），無一非封測；⑤本格 74.00% 大於 V7-01 模組 26.00%。★所營業務登記事項把「接受委託各型積體電路之封裝、測試」列在第四條，但該處為公司登記營業項目清單而非業務主從陳述，不作為①的依據★2026-09-09 weight_item 由「封裝測試」改為「IC封裝測試」：同上，PDF 文字層在「IC」與「封裝測試」之間斷行，舊解析只讀到後半，折疊空白後取得年報的完整品項名。★這兩列是一個要記住的耦合：**改善品項名稱會使既有的 weight_item 參照失效**，而 [stale] 是唯一會看見它的檢查——沒有它，這兩筆 weight 會安靜地消失。', 'Direct'),
        ('立衛', 'Core', None, 'company_site', '年報無比重表；專業封裝測試廠★2026-09-09 weight_status=Not_Extracted：product_narrative_status 亦為 no_section，庫內零文字。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
        ('聯鈞', 'Core', '功率半導體封裝及測試', 'annual_report_table', '年報營業比重表：功率半導體封裝及測試 62.00%', 'Direct'),
        ('華東', 'Core', '封裝', 'annual_report_table', '年報營業比重表：封裝 64.00%', 'Direct'),
        ('華泰', 'Core', '封裝及測試', 'annual_report_table', '年報營業比重表：封裝及測試 58.66%', 'Direct'),
        ('菱生', 'Core', '封裝測試服務', 'annual_report_table', '年報營業比重表：封裝測試服務 99.00%', 'Direct'),
        ('訊芯-KY', 'Core', 'SiP&感測器', 'annual_report_table', '年報營業比重表：SiP&感測器 38.45%；其光收發模組 61.55% 是拿同一套 SiP 封裝能力做出的產出，非獨立生意，故 Core 留封裝（R024）。Core 理由：★這是①推翻⑤的案例。⑤指向 D6-01：高速光纖收發模組 61.55% 大於本格 SiP&感測器 38.45%。但①年報所營業務首句為「本集團主要從事**系統模組封測(System in Package;SiP)產品**、高速光纖收發模組、光學通訊系統、金屬導電架、功率器件及其他各型積體電路模組之**封裝、測試及銷售**」——SiP 封測列於句首，且整句把所有產品線都框在「封裝、測試及銷售」這個業務性質下；商品項目順序亦為 A.半導體積體電路封裝及測試服務 B.晶圓減薄切割 C.感測器封裝測試 D.光纖收發模組封裝測試。②價值創造位置同向：其光纖模組收入亦是封測代工而非自有光模組品牌。★④指向另一邊且原先未處理（2026-09-07 補）：計畫開發 25 項以 A.102.4T 光電共封裝、B.3.2T 光引擎封裝領頭，偏向 D6-01；但 R023 的順序是①在④之前，且①講的是現有業務性質而④是尚未量產的開發方向。故 Core 判在 A5-05。★未解的張力：若光模組收入確為封測代工，D6-01 那 61.55% 以 Direct 掛著是否恰當，屬節點歸屬待另案', 'Direct'),
        ('超豐', 'Core', None, 'company_site', '年報無比重表；專業封裝測試廠★2026-09-09 weight_status=Not_Extracted：product_narrative_status 亦為 no_section，庫內零文字。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
        ('頎邦', 'Core', '封裝及測試', 'annual_report_table', '年報營業比重表：封裝及測試 72.83%', 'Direct'),
    ],
    "A6-01": [
        ('久元', 'Core', None, 'annual_report_prose', '年報業務範圍表：測試代工收入 51.03%，另切割挑檢代工 10.52%；年報自述「專業半導體及 LED 產品代工」。比重表在業務範圍章內、未進 tw.product_revenue，故不計比重（見 check_industry [mix]）。Core 理由：①年報商品項目首句「本公司為專業半導體及 LED(光電)產品**代工**及自有產品之研發、製造與銷售服務廠」，代工列於自有產品之前；⑤同向：年報業務範圍表列測試代工收入 51.03% 加切割挑檢代工 10.52%，合計 61.55%，大於 A2-13 的機器組裝等產品銷售 39.26%。★該比重表在業務範圍章內而未進 tw.product_revenue，故兩格庫內權重皆 NULL（見 [mix]）', 'Direct', 'Too_Coarse'),
        ('京元電子', 'Core', '積體電路測試服務 + 晶圓測試服務', 'annual_report_table', '年報營業比重表：積體電路測試服務 66.43% 加晶圓測試服務 30.56%，同屬 IC 測試服務（本節點定義即含 Wafer Sort），合計 96.99%。原存 58.78/35.80 取自非年報的另一張表（合計 103 億 vs 年報 349 億），2026-09-03 以年報表更正', 'Direct'),
        ('微矽電子-創', 'Core', '積體電路測試', 'annual_report_table', '年報營業比重表：積體電路測試 72.36%', 'Direct'),
        ('精材', 'Major', '測試服務', 'annual_report_table', '年報營業比重表：測試服務 47.86%。★weight_pct 為 NULL、weight_status=Not_Applicable：該條營收線已由 A5-05 封裝測試代工(OSAT) 計入——A5-05 的定義明訂 eligible revenue 是受託服務的每一條線、不只封裝那一條，故其 98.020 已含本條測試服務；在本節點再給比重即為市場層的重複統計（R020①-S B. Counting、R020②）。★membership 保留：公司確實受託提供量產測試服務，依 A6 的服務軸落在 A6-01。本次僅處理重複計重，**不預判 A5／A6 的 taxonomy boundary overlap**——R020 的 NULL 是「無法／不應給這格獨立比重」的資料表達，不得反過來替該 overlap 做裁決。★2026-09-29 更正 stale reference：原 evidence 寫「晶圓級封裝為其 Core，見 A5-02」，而 A5-02 已於 2026-09-11 隨 A5 分支改為單軸而移除，本公司現行 Core 為 A5-05。★★理由＝他節點已計入（2026-10-01 契約定案）：本列的 membership 成立且度量本身適用（47.86% 已揭露並取得），不給比重的原因是同一公司的 A5-05 已把這條測試服務線納入其 weight_item（晶圓級尺寸封裝 + 晶圓級後護層封裝 + 測試服務），故在本節點再給比重即同一經濟活動在同一法人內計兩次。與「度量不適用」的分界：本公司確實受託提供量產測試服務，拿掉 A5-05 那一列本列仍可有比重，故非無對象。與「合併治理」的分界：吸收該線的是**同一 stock_id 的另一列**，不是另一家獨立上市個體。', 'Direct', 'Not_Applicable'),
        ('欣銓', 'Core', '晶圓測試 + 成品測試', 'annual_report_table', '年報營業比重表：晶圓測試 9,989,372 仟元 71.21%、成品測試 3,998,084 仟元 28.50%（皆印刷頁 p94），weight_pct 99.710 為兩條線的加總，不是估算。合併的依據：商品服務項目把兩者並列——「記憶體IC 之晶圓測試。」與「數位訊號 IC 及混合訊號 IC 之晶圓及成品測試。」（皆印刷頁 p94）；而本節點的定義同時具名 Wafer Sort 與 Final Test，兩條線因此不屬於不同節點，依 R020④ 應先聚合再計重。★本案定性為 weight／evidence correction，不是重新發明 membership（RULING_B12_C）。membership、Core 與 role 全部維持。', 'Direct'),
        ('矽格', 'Core', '測試', 'annual_report_table', '年報營業比重表：測試 80.87%', 'Direct'),
        ('逸昌', 'Core', '類比晶圓測試及IC測試', 'annual_report_table', '年報營業比重表：類比晶圓測試及IC測試 99.60%', 'Direct'),
    ],
    "A6-02": [
        ('中探針', 'Core', None, 'annual_report_prose', '年報自述「本公司主要產品為 POGO PIN 連接器及高端 IC 測試探針」，測試探針應用於印刷電路板測試、晶圓測試、IC 封裝測試等。營收「探針 69.76%」一線同時含測試探針與 POGO PIN 連接器故與 F2-01 均不計比重(R020②)。Core 理由：R023① 無區辨力——年報三處(所營業務、主要產品、產業概況)皆以並列句同時列出探針式連接器與測試探針，未指明何者為主；因該營收線已宣告不可拆，⑤ 亦無從援用。決定性證據在 ④：計畫開發之新產品五項(微距高頻測試模組、大電流測試針與軟包電池測試工具、鍍層技術、高速連接器測試針、高頻測試用 PCR 導電橡膠)全數落在測試探針/測試模組一側，無一為 POGO PIN 連接器產品，研發資源集中於測試側(R023④)', 'Direct', 'Too_Coarse'),
        ('勵威', 'Core', '探針卡', 'annual_report_table', '年報營業比重表：探針卡 44.03%。Core 理由（R023①）：計劃開發的五項全部圍繞探針卡的針體與組裝技術——「CIS MEMS probe 架構開發…」、「懸臂式探針卡自動彎針剪針設備開發…」與「MEMS自動插針設備開發…」（皆印刷頁 p39）。本公司投入的技術方向是探針與探針卡本體，而本輪新增的 A6-03 那一側（測試載板與 IC 測試座）在計劃開發裡完全沒有對應項目。② 價值創造位置同為測試接觸介面產品的製造；③ 商業本質同為自製銷售；④ 公司定位為探針卡與半導體耗材供應商——②③④ 不具區辨力，決定性的是 ①。', 'Direct'),
        ('旺矽', 'Core', '晶圓探針卡', 'annual_report_table', '年報營業比重表：晶圓探針卡 72.22%', 'Direct'),
        ('精測', 'Core', '晶圓測試卡', 'annual_report_table', '年報營業比重表：晶圓測試卡 64.76%，大於其 IC測試板 25.12%（見 A6-03）。Core 理由：①年報「目前之商品(服務)項目」順序為 (1)**晶圓測試卡(包含探針卡)** (2)IC測試板 (3)技術服務與其他，探針卡列首；⑤本格 64.76% 大於 A6-03 IC測試板 25.12%。②同向：④計畫開發首項講的是高頻高速材料與 HDI 增層技術，屬測試介面本體的製程能力', 'Direct'),
        ('穎崴', 'Major', '探針卡ProbeCard', 'annual_report_table', '年報營業比重表：探針卡ProbeCard 30.00%；測試座為其 Core，見 A6-03', 'Direct'),
        ('漢測', 'Core', None, 'annual_report_prose', '年報「主要產品與服務項目」A.探針卡(Probe Card)設計、製造與清潔材料列於首位；年報所營業務自述為「半導體檢測設備及其零組件、耗材之客製化研發、製造及銷售」，探針卡即該句之零組件/耗材。營收「半導體設備及零組件 57.13%」一線同時含探針卡與晶圓針測機故與 A2-13 均不計比重(R020②)。Core 理由：產品清單以探針卡列首(R023①)，且計畫開發之新商品兩項全為探針卡(微細間距高密度垂直探針卡、極微細間距高產能探針卡)，研發資源全數投入探針卡(R023④)', 'Direct', 'Too_Coarse'),
        ('雍智科技', 'Major', '半導體晶圓測試-前段測試載板', 'annual_report_table', '新列的依據：年報把該線的用途逐字寫為晶圓探針卡測試載板，即接觸尚未切割晶圓的測試介面（印刷頁 p63），符合本節點「接觸的是尚未切割的晶圓」之界定。weight 36.460 取自年報比重表的前段測試載板一格，非估算。role 依 R026①（Direct 且有 weight，純數值判定）：36.46% 不低於 15%，故為 Major。', 'Direct'),
    ],
    "A6-03": [
        ('勵威', 'Major', None, 'annual_report_prose', '新列的依據：「公司目前之商品項目:探針卡、測試載板、IC測試座、耗材、檢測設備及中古設備。」（印刷頁 p39）——測試載板與 IC 測試座分別對應本節點具名的 Load Board 與 Test Socket。weight 為 NULL／Granularity_Insufficient：兩項產品沒有自己的營收格，必然含於半導體耗材或其他之內，而那兩格另含全樹無落點的耗材；依 R020 不得把整格計入本節點，也不做任何拆分或估算。★狀態選擇的判準（RULING_B12_E）：目前不是跨兩個已具名節點，而是**單一可辨識節點成立、但無法取得可靠 weight**，故用 GI 而非 Too_Coarse。role 依 R026③（Direct 且 weight 為 NULL，無附屬業務 evidence）為 Major。Core 維持 A6-02——計劃開發的技術方向全部在探針與探針卡本體，本列依 R023① 未能勝出。', 'Direct', 'Granularity_Insufficient'),
        ('博磊', 'Major', '測試產品', 'annual_report_table', '年報營業比重表：測試產品 22.23%。★2026-09-07 更正，由 filing_too_coarse／Minor 改為計入比重／Major：原列標 filing_too_coarse 卻未說明該線橫跨哪些節點，複查年報商品項目後不成立——「測試產品」對應商品項目前四項 (1)半導體測試治具 (2)半導體測試介面板 (3)積體電路預燒用測試連接座 (4)積體電路測試用測試連接座，四項**全部**落在本節點定義的 Test Socket／Load Board／Test Interface 之內，無跨節點情事，故該線可計比重。「預燒用」描述的是連接座的使用場景，四項品名的產品本體都是測試連接座、治具與介面板，逐字命中本節點定義。計入 22.23% 後依 R026① 的數值預設（>=15%）role 為 Major，原 Minor 係在 weight 缺席下逕行判定，本身即無依據。★原掛 A6-02 探針卡，但年報載明探針卡係「爭取為邏輯IC廠商需求設計」之開發中商品，非現有營收。★Taxonomy follow-up（非本次分類依據）：A6-04 Burn-in 尚未完成 Definition，待該 sibling boundary 補齊後對本列進行反向 overlap review。依 R031 的 2026-09-07 增訂條款，sibling 無定義或無成員不得作為排除該 sibling 的分類依據，故本列的歸屬僅建立在 A6-03 自身定義的正面命中之上；原評述曾以「A6-04 無定義且無成員」佐證，已於同日移除', 'Direct'),
        ('穎崴', 'Core', '測試治具TestSockets', 'annual_report_table', '年報營業比重表：測試治具TestSockets 56.00%。Core 理由：①年報商品項目順序為 A.**高效能半導體測試治具(Advanced Test Socket)** B.接觸元件(探針／導電橡膠) C.測試機台轉換治具 D.感光元件／高頻／記憶模組測試治具，測試座列首；⑤本格 56.00% 大於 A6-02 探針卡 30.00%。★④計畫開發首項為「微間距探針卡(Fine Pitch Probe Card)」指向 A6-02，屬新開發方向；①⑤同向且為現有主體，故 Core 維持 A6-03', 'Direct'),
        ('精測', 'Major', 'IC測試板', 'annual_report_table', '年報營業比重表：IC測試板 25.12%', 'Direct'),
        ('雍智科技', 'Core', '半導體晶圓測試-後段測試載板', 'annual_report_table', '年報業務範圍載明「品及服務主要為製造及銷售半導體晶圓測試之各種測試載板,包含前段測試載板(晶圓探針卡測試載板)、後段測試載板(IC測試載板及IC…」老化測試載板（印刷頁 p63）。★本列的依據更正為只含後者：後段測試載板（IC 測試載板及 IC 老化測試載板）接觸封裝後的元件，屬本節點；前段測試載板依測試階段應落 A6-02，故不再由本節點承載。weight 61.580 取自年報比重表的後段測試載板一格。★原 evidence 引「年報業務範圍載明探針卡」一語已撤除——公司製造的是探針卡**用的測試載板**，不是探針卡本身，該引用會讓本列看起來像 A6-02。Core 理由（維持本節點，RULING_B12_F）：新增的 A6-02 為 Direct 葉節點故進入候選集合，依 R023① 的產品身份未能勝出；不採「兩列同為載板所以 ① 無區辨力」的反讀，因為載板是較上位的材料／形態詞，不能抬高到產品身份層級後再說沒有區辨力。階段拆分不自動觸發 Core Pending（RULING_B1_B2）。', 'Direct'),
        ('中探針', 'Major', None, 'annual_report_prose', '年報產品項目載明「還有測試治具 (Test Socket),已針對高頻傳輸、高速運算、AI 等應用場景的半導體測試需求來進行晶片封裝後測試」；營業比重「其他 20.69%」註記為「主要係指治具、針盤、配件及代購之探針」，該線橫跨本節點與 A6-02 故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "A6-05": [
        ('宜特', 'Major', None, 'annual_report_prose', '年報業務範圍載明其第三方驗證分析服務「涵蓋⋯故障分析、材料分析、**可靠度驗證**、訊號測試、化學分析」，可靠度驗證為公司自述的服務項目之一，逐字落在本節點定義。★比重不計：年報比重表僅「檢測服務收入 100.00%」一條，同時含 A6-06 的失效與材料分析，無拆分依據(R020②)', 'Direct', 'Too_Coarse'),
        ('汎銓', 'Major', '分析服務收入', 'annual_report_table', '新列的依據是主要產品之重要用途表的可靠度分析格（印刷頁 p65）：「晶片ESD試驗…」「晶片高/低溫壽命試驗…」「晶片封裝品質與應力試驗…」「晶片耐候性試驗…」「第三代半導體壽命試驗…」「板級導通性試驗…」「電路板離子遷移試驗…」「產品壽命預估…」；該表標題為「1.主要產品之重要用途:分析服務。」，故屬公司對自身服務的陳述而非產業敘述。八項試驗逐字屬 A6-05 定義的壽命、環境與電性應力驗證。weight 為 NULL／Too_Coarse：與 A6-06 的各項分析併在同一條「分析服務收入 100%」線內，年報未給拆分依據（R020②）。role 依 RULING_B3_F 為 Major——理由不是既有兩列皆為 Major（先例不是依據），而是本案證據足以排除 Core 卻不足以證明它只是 Minor：weight_pct=NULL 的原因是無法拆分，不是業務重要性低。weight_pct=NULL 不否定 exposure 的存在（membership、weight、role 分開判定）。', 'Direct', 'Too_Coarse'),
        ('閎康', 'Major', None, 'annual_report_prose', '年報「目前之商品(服務)項目」自述主要服務為「**可靠度測試(RA)**、故障分析(FA)及材料分析(MA)服務」，可靠度測試為公司並列的三大服務之一；計畫開發之新商品亦含「PIC 光子積體電路晶片可靠度測試」。★比重不計：年報比重表僅「檢測服務收入 100.00%」一條，同時含 A6-06 的 FA 與 MA，無拆分依據(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "A6-06": [
        ('宜特', 'Core', None, 'annual_report_prose', '年報業務範圍自述「本公司專注於提供半導體產業上、中、下游供應鏈於產品生命週期各階段所需的第三方驗證分析服務。服務涵蓋 FIB 電路修改、工程樣品製備、故障分析、材料分析、**可靠度驗證**、訊號測試、化學分析」。★2026-09-07 由計入 100% 改為不計比重(R020②)：補齊 A6-05 定義後，該條「檢測服務收入 100.00%」同時橫跨本節點與 A6-05，年報未給拆分依據，兩格皆為 NULL。Core 理由（因新增 A6-05 使候選成為二取一，依 R023 順序重跑）：①年報服務清單的列舉順序為 FIB 電路修改 → 工程樣品製備 → 故障分析 → 材料分析 → 可靠度驗證，前四項全屬本節點且可靠度列於其後；②價值創造位置同向——公司自述的技術平台是 FIB 與樣品製備，即失效分析與材料分析的核心設備。①②同指本節點。⑤不可用（兩格皆 NULL）', 'Direct', 'Too_Coarse'),
        ('汎銓', 'Core', '分析服務收入', 'annual_report_table', '年報營業比重表：分析服務收入 100.00%（本輪撤下計重）。撤重的依據：唯一那條「分析服務收入 100%」同時涵蓋本節點的材料分析、故障分析、「表面分析…」與矽光子積體光路分析四格，以及 A6-05 的可靠度分析一格（皆印刷頁 p65），年報未給拆分依據，依 R020② 不可單獨計入本節點，不做任何拆分／扣除／估算。★不改成 Granularity_Insufficient（RULING_B3_B）——與 3581 不同，本案已能明確指出至少兩個具名節點，是跨具名節點而無法拆分，故為 Too_Coarse。本節點的成員資格依據不變。Core 理由（維持本節點；決定層：R023① 的能力宣稱），逐字為：「本公司為專業半導體材料分析及故障分析之技術服務公司,目前市場上並無與本公司完…」全相同之專業分析公司、「本公司設立迄今專注於技術層次最高之材料分析,與國內同業相…」較無論在材料分析之品質、交期乃至市場佔有率均處於領先地位、「本公司專注於材料分析與故障分析核心技術,長期投入研發資源建立高階分析技術…」能力（皆印刷頁 p59），另見「本公司專注於半導體先進製程之材料分析與故障分析技術研發…」（印刷頁 p63）。★這四句指名的是材料分析與故障分析（本節點具名的兩項服務），**未提可靠度分析**（A6-05），故 ① 對本節點與 A6-05 之間具區辨力。⑤ 只作支持，故權重改為 NULL 後唯一勝者不變，不觸發 Core Pending。', 'Direct', 'Too_Coarse'),
        ('閎康', 'Core', None, 'annual_report_prose', '年報「目前之商品(服務)項目」自述「本公司目前主要服務項目係提供各產業新產品開發及量產品質率改善所需之**可靠度測試(RA)、故障分析(FA)及材料分析(MA)服務**」，三種服務並列。★2026-09-07 由計入 100% 改為不計比重(R020②)：補齊 A6-05 可靠度驗證服務的定義後，該條「檢測服務收入 100.00%」同時橫跨本節點(FA+MA)與 A6-05(RA)，年報未給拆分依據，兩格皆為 NULL。Core 理由（因新增 A6-05 使候選成為二取一，依 R023 順序重跑）：①核心產品能力定案——年報自述「以**IC 材料及結構等專業分析能力**提供中下游 IC 製造與封裝測試等產業相關製程之問題分析」，公司自我定位的能力是材料與結構分析而非應力驗證；④同向：計畫開發五項中「矽光子晶片光損特性與失效分析」「LEO 高頻通訊晶片之先進材料分析」屬本節點，僅「PIC 光子積體電路晶片可靠度測試」屬 A6-05。⑤不可用（兩格皆 NULL）', 'Direct', 'Too_Coarse'),
    ],
    "A7-01": [
        ('上品', 'Core', '氟素內襯設備', 'annual_report_table', '年報營業比重表：氟素內襯設備 59.12%', 'Direct'),
        ('亞翔', 'Core', '機電工程 + 無塵室工程 + 製程管路工程 + 其他(含統包工程)', 'annual_report_table', '年報營業比重表：四條工程線合計 100%，統包工程即其無塵室 EPC 本業', 'Direct'),
        ('信紘科', 'Core', '高科技產業製程之廠務供應系統整合', 'annual_report_table', '年報營業比重表：高科技產業製程之廠務供應系統整合 92.37%', 'Direct'),
        ('創控', 'Core', None, 'annual_report_prose', '年報業務範圍載明「AMC」：監測數據之可信度。4.計劃開發之新商品(服務)(1)強化現有產品:MiTAPAMC/VOCs分析技術1縮短設備分析時間:能在更短時間內完成量★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('千附', 'Core', '工程收入', 'annual_report_table', '年報營業比重表：工程收入 38.39%。Core 理由：★這是①④推翻⑤的案例。⑤指向 A2-10：精密零組件 54.21% 大於本格工程收入 38.39%。但①年報業務範圍首句「本公司經營業務主要包含**廠務系統工程**、鞋機生產銷售、精密零組件整合製造等三大業務」把廠務列於三大業務之首；商品項目順序同為 (1)廠務系統工程 (2)鞋機 (3)千附精密；④計畫開發首項「(1)先進廢氣處理設備及高科技廠房**廠務整改工程**」。R023 將①④置於⑤之前，故 Core 判在 A7-01', 'Direct'),
        ('巨漢', 'Core', '無塵室及機電空調統包工程收入', 'annual_report_table', '年報營業比重表：無塵室及機電空調統包工程收入 100.00%', 'Direct'),
        ('帆宣', 'Core', '自動化供應系統業務', 'annual_report_table', '年報營業比重表：自動化供應系統業務 38.94%', 'Direct'),
        ('擎邦', 'Major', None, 'annual_report_prose', '年報服務項目第 3 項「高科技產業廠房興建之整廠機電系統整合」，針對電子、半導體、光電、通訊、資料中心廠房提供無塵室工程、管線、儀控、電氣、空調等服務。該業務無獨立營收列，併於「捷運、發電廠、空調機電系統及公共工程 42.51%」一列，橫跨本節點與 Q1 故不計比重(R020②)', 'Direct', 'Too_Coarse'),
        ('朋億*', 'Core', '高科技產業製程供應系統設備銷售', 'annual_report_table', '年報營業比重表：高科技產業製程供應系統設備銷售 49.32%', 'Direct'),
        ('洋基工程', 'Core', '無塵室及機電空調統包工程收入', 'annual_report_table', '年報營業比重表：無塵室及機電空調統包工程收入 100.00%', 'Direct'),
        ('漢唐', 'Core', None, 'annual_report_prose', '年報業務範圍載明「廠務」：帶動先進製程產能、先進封裝、以及成熟或特殊製程產能的建置與升級需求。相應地,廠務設施工程與廠房興建需求同步成長。為因應客戶的擴產計畫,本公司★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('漢科', 'Core', '製程管路工程設計施工', 'annual_report_table', '年報營業比重表：製程管路工程設計施工 81.47%', 'Direct'),
        ('濾能', 'Core', 'AMC化學濾網及配件', 'annual_report_table', '年報營業比重表：AMC化學濾網及配件 61.21%', 'Direct'),
        ('矽科宏晟', 'Core', '高科技產業製程供應系統設備及相關系統 + 高科技產業製程廠務系統整合工程', 'annual_report_table', '年報營業比重表（114 年度）：高科技產業製程供應系統設備及相關系統 72.71% 加高科技產業製程廠務系統整合工程 19.99%，依 R019 同節點聚合為 92.70%；高科技產業製程維護及保養 7.30% 不在 A7-01 列舉內、不計入。★2026-09-15 M5：更正——原 74.10% 為 112 年度欄的單一品項', 'Direct'),
        ('聖暉*', 'Core', '工程收入', 'annual_report_table', '年報營業比重表：工程收入 97.00%', 'Direct'),
        ('聚賢研發-創', 'Core', '高科技產業廠務供應系統工程', 'annual_report_table', '年報營業比重表：高科技產業廠務供應系統工程 97.24%', 'Direct'),
        ('華景電', 'Core', '晶圓製程AMC微污染防治設備', 'annual_report_table', '年報營業比重表：晶圓製程AMC微污染防治設備 90.63%', 'Direct'),
        ('銳澤', 'Core', '工程合約收入', 'annual_report_table', '年報營業比重表：工程合約收入 97.27%', 'Direct'),
        ('隆銘綠能', 'Core', '機電系統整合工程', 'annual_report_table', '年報營業比重表：機電系統整合工程 96.43%', 'Direct'),
    ],
    "B1-01": [
        ('南亞', 'Core', '銅箔基板', 'annual_report_prose', '年報無比重表；集團最大一般用途 CCL 產能。Core 理由：候選四格（本格一般 CCL、B1-02 玻纖布、B1-03 樹脂、B1-05 銅箔）皆為年報「電子材料產品」一條線下的細項，①②③④在四格之間無區辨力——年報商品順序為 (1)塑膠加工產品 (2)塑膠原料產品 (3)電子材料產品 (4)聚酯產品，只說明電子材料整體排第三，不指哪一格。⑤可用且決定性：年報品項表列銅箔基板 10.91% > 環氧樹脂 8.29% > 銅箔 5.35% > 玻纖布 1.82%，本格（銅箔基板）為四者最大。★四列的 weight_item 目前皆為 None，故庫內權重全 NULL；年報數字存在，屬待填不是待判。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報業務比重表逐項列出「銅箔基板 千張 56,851／28,352,599／10.91%」——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立。★2026-09-07 候選集合變動註記：新增 J1-01 乙二醇 9.51% 一格，依 R023★★ 重跑——該線為南亞塑膠原料事業的單一產品、比重不足一成，而本格所屬的電子材料事業是公司自述的核心，①～⑤無一指向乙二醇側，原裁定不變★2026-09-09 補上 weight_item：本列原為 weight NULL 並被 [weight_reason] 列為「未說明為何沒有比重」，但年報比重表逐項列出「銅箔基板 10.91%」——數字一直都在，缺的只是沒有人把它寫進 weight_item。★**本列 evidence 開頭原寫「年報無比重表」，那是錯的**：該年報有 21 條產品線的比重表，同一段 evidence 的後半也自述「年報業務比重表逐項列出『銅箔基板…10.91%』」，前後矛盾，以後者為準。', 'Direct'),
        ('聯致', 'Major', '銅箔基板', 'annual_report_table', '年報營業比重表：銅箔基板 30.00%', 'Direct'),
        ('騰輝電子-KY', 'Core', '銅箔基板', 'annual_report_table', '年報營業比重表：銅箔基板 1,605,925 仟元／37.66%（金額推導）。Core 理由：⑤本格 37.66% 為最大營收線，大於鋁基板 27.48% 與 B1-08 膠片 19.22%', 'Direct'),
        ('長興', 'Major', None, 'annual_report_prose', '年報產品項目 (4)銅箔基板，產業概況「(2)電子材料」下設「B.銅箔基板」一節；併於「電子材料 26%」一列，該線另含乾膜光阻劑與真空壓膜機故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "B1-02": [
        ('南亞', 'Minor', '玻纖布', 'annual_report_prose', '年報業務範圍載明「玻纖」：異辛醇等。(3)電子材料產品:銅箔基板、基材、銅箔、環氧樹脂、電子級及工業級玻纖布、液晶顯示器、電容式觸控面板、印刷電路板、玻纖絲。(4)聚★2026-09-09 補上 weight_item：年報比重表列「玻纖布 1.82%」。★另有「玻纖絲 0.64%」未計入本格：玻纖絲是織成玻纖布的紗，兩者是上下工序而非同一產品，年報亦分列兩項；要合併需先確認本節點收不收紗，本輪不擴充定義。★2026-09-09 role 由 Major 改為 Minor：本列取得比重之前 weight 為 NULL，依 R026 的不對稱走 default Major；補上年報數字後 1.82% 低於 15%，回到數值預設。★**這正是 R026 那句「15% 是 Exposure Role 的操作門檻，不是產業重要性」的實例**：南亞是台灣最大的玻纖布生產者之一，但這門業務**對南亞而言**只佔個位數營收——兩個量方向相反，本欄記的是後者。', 'Direct'),
        ('台玻', 'Major', None, 'annual_report_table', '年報營業比重表：玻璃纖維 31.31%。★★**2026-09-11 由 Core 降為 Major，並由 Not_Extracted 改為 Too_Coarse；本公司改列 Core pending**。原 evidence 首句「年報無營運概況標準章節」**是錯的**：年報有該章節，是選檔取到了被取代的版本(同日 F04 與 F11，舊規則取檔名字母序最後一個)。選檔改依 MOPS 文件說明與上傳時間後，F11 一次讀出比重表——而那張表推翻了本列原本的 Core 地位。★**R023 ①④⑤ 全部指向平板玻璃，不指向玻纖布**：①所營業務 17 項中 (1)~(10) 為平板／壓花／鐵絲網／反射／強化膠合／鏡板／複層／軌道車廂／磨邊玻璃，(11)(12) 為容器與耐熱容器，玻璃纖維絲及布排在第 (13) 項；④計畫開發之新商品三項為 Low-E 鍍膜玻璃、耐熱奶瓶與高白烈酒瓶、TG 品牌生活器皿，無一與玻纖有關；⑤平板玻璃 59.63% > 玻璃纖維 31.31% > 玻璃器皿 9.06%。★★**weight 不填 31.31%，記 Too_Coarse(R020②乙型)**：年報自己把兩種用途分開描述——「玻布主要作為**印刷電路板(PCB)上游原料**」、「**玻纖**產品之應用以熱塑性用途具有…」，產業關聯圖中玻纖流向管道業、桶槽業、建材業、傘具業、運動器材業與風力發電葉片業——但營收表把兩者合併成一條「玻璃纖維」，**年報未給拆分依據**。知道這條線含有本節點，不等於知道本節點的比重。**不得由人工判斷「大概多少是玻布」補數字**。★★★**Core pending 的原因是樹的缺陷不是證據不足**(R019④)：B1 整棵是 PCB材料，平板玻璃 59.63% 與玻璃器皿 9.06% 在其下沒有任何位置；樹中已有 P2「玻璃」(L2，主要產品欄寫平板/建築/汽車/光電/特殊玻璃)但**無 Definition、無子節點、無成員**。依 R025 先記 taxonomy gap，不以單一公司的營收反推節點(見 db/TAXONOMY_GAPS.md)。母體佐證：玻璃陶瓷類另有 1806 冠軍、1809 中釉、1810 和成、1817 凱撒衛尚未 mapping，非為 n=1 開節點。', 'Direct', 'Too_Coarse'),
        ('富喬', 'Core', '玻璃纖維布 + 玻璃纖維紗', 'annual_report_table', '年報營業比重表：玻璃纖維布 59.77% 加玻璃纖維紗 40.23%，本節點定義即為「玻璃紗、玻纖布」兩者，合計 100.00%(R020④)。Core 理由：公司全部營收皆在本節點(R023⑤)', 'Direct'),
        ('建榮', 'Core', '玻璃纖維布', 'annual_report_table', '年報營業比重表：玻璃纖維布 100.00%', 'Direct'),
        ('德宏', 'Core', '玻璃纖維布', 'annual_report_table', '年報營業比重表：玻璃纖維布 98.40%', 'Direct'),
    ],
    "B1-03": [
        ('南亞', 'Minor', '環氧樹脂', 'annual_report_prose', '年報業務範圍載明「環氧」：可塑劑、丙二酚、酜酸酐、異辛醇等。(3)電子材料產品:銅箔基板、基材、銅箔、環氧樹脂、電子級及工業級玻纖布、液晶顯示器、電容式觸控面板、印刷★2026-09-09 補上 weight_item：年報比重表列「環氧樹脂 8.29%」。★2026-09-09 role 由 Major 改為 Minor：本列取得比重之前 weight 為 NULL，依 R026 的不對稱走 default Major；補上年報數字後 8.29% 低於 15%，回到數值預設。★**這正是 R026 那句「15% 是 Exposure Role 的操作門檻，不是產業重要性」的實例**：南亞是台灣最大的環氧樹脂生產者之一，但這門業務**對南亞而言**只佔個位數營收——兩個量方向相反，本欄記的是後者。', 'Direct'),
        ('雙鍵', 'Major', None, 'company_site', '年報無營運概況標準章節；環氧樹脂硬化劑為主要產品線。★2026-09-06 由 Core 降為 Major，本公司轉為 **Core pending**（R019：無法唯一判定則不建立 Core row）。理由：本格與 B1-07 聚亞醯胺樹脂的權重皆為 NULL，⑤完全不可用；而①②③④亦不可得——年報 PDF（2025_4764_20260625F04.pdf，91 頁）使用無 ToUnicode 對照表的內嵌字型，每個字元都抽取成 (cid:NNN)，文字工具無法讀取任何內容（與 4989 榮科的方框字同類）。在取得可讀的業務內容之前，任何 Core 都會是編出來的★2026-09-09 weight_status=Not_Extracted：PDF 內嵌字型無 ToUnicode 對照表，每字抽成 (cid:NNN)。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★★2026-09-10 批次 B 診斷（維持 Not_Extracted）：本公司年報 91 頁的內嵌字型沒有 ToUnicode 對照表。pdfplumber 抽出 683 個 (cid:NNN) 佔位符；**pypdf 解得出 CID，但解出來的是錯的語系**——「ុ໒ว 5G / 6G」是高棉／寮文碼位，中文內容被字型宣告的錯誤 Unicode 對照渲染。★**這不是換 parser 能解決的**：兩個抽取器都「成功」了，只是其中一個成功地讀出錯字。需要的是字型／CID → 正確中文字元的重映射表，屬獨立的研究工作。★本公司的 tw.product_narrative 亦為 no_section（連業務章節都定位不到），整份文件的文字層都不可用。', 'Direct', 'Not_Extracted'),
    ],
    "B1-04": [
        ('亞電', 'Core', '保護膠片', 'annual_report_table', '年報營業比重表：保護膠片 89.45%', 'Direct'),
        ('台虹', 'Core', None, 'annual_report_prose', '年報業務範圍載明「軟性銅箔基板」：電路板之上游基材原料業。軟性銅箔積層板(FCCL)主要分為兩大類:有膠系三層軟性銅箔基板(3L-FCCL)與無膠系雙層軟性銅箔基板(2L-F★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('律勝', 'Core', '軟板基材及保護膠片', 'annual_report_table', '年報營業比重表：軟板基材及保護膠片 87.00%', 'Direct'),
        ('達邁', 'Core', '聚醯亞胺薄膜', 'annual_report_table', '年報營業比重表：聚醯亞胺薄膜 99.78%；商品項目「聚醯亞胺薄膜之製造與銷售」。本節點定義首項即 PI Film。原掛 B1-07 聚亞醯胺樹脂，但該節點定義是 PI Resin／PI Varnish，達邁賣的是薄膜不是樹脂。Core 理由：單一產品線占營收 99.78%(R023⑤)', 'Direct'),
    ],
    "B1-05": [
        ('南亞', 'Minor', '銅箔', 'annual_report_prose', '年報業務範圍載明「銅箔」：塑膠原料產品:乙二醇、可塑劑、丙二酚、酜酸酐、異辛醇等。(3)電子材料產品:銅箔基板、基材、銅箔、環氧樹脂、電子級及工業級玻纖布、液晶顯示器★2026-09-09 補上 weight_item：年報比重表列「銅箔 5.35%」，與同表的「銅箔基板 10.91%」(B1-01) 是兩條分列的線，不重複。★2026-09-09 role 由 Major 改為 Minor：本列取得比重之前 weight 為 NULL，依 R026 的不對稱走 default Major；補上年報數字後 5.35% 低於 15%，回到數值預設。★**這正是 R026 那句「15% 是 Exposure Role 的操作門檻，不是產業重要性」的實例**：南亞是台灣最大的銅箔生產者之一，但這門業務**對南亞而言**只佔個位數營收——兩個量方向相反，本欄記的是後者。', 'Direct'),
        ('榮科', 'Core', None, 'annual_report_prose', '年報業務範圍載明「銅箔」：目前之商品(服務)項目(1)PK-HTE-LP3:低稜線高溫延展性粉紅色電解銅箔,厚度8μm~140μm(2)PK-HTE-RTF:反面處理★2026-09-09 weight_status=Not_Extracted：年報 @107「電解銅箔 `����,���` `���.��`」——數字全部抽成替代字元。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★★2026-09-10 批次 B 診斷（維持 Not_Extracted，**不重建數值**）：年報第 71 頁的比重表**中文完全正常、只有數字全毀**——「主要產品／營業收入／占營業額比率」三欄讀得出來，資料列是「電解銅箔 �,���,��� ���.�� �,���,��� ���.��」，32 個 U+FFFD 替代字元。★**已排除的修法**：pdfplumber 與 pypdf 兩個抽取器給出**完全相同**的損壞（cid=0 fffd=32 digits=70 cjk=259），換 parser 無效——字型的數字字符沒有可用的 Unicode 對照，這是 PDF 的問題不是抽取器的問題。★**唯一推測不等於可靠抽取**（2026-09-10 裁定）：比率欄的形狀是三位整數加兩位小數，而占比不可能超過 100，因此該值在語意上只可能是 100.00。**但那只證明「損壞字串很可能代表 100.00」，不證明「PDF 的原始字元序列就是 100.00」**，仍屬從損壞來源反推原始數值，依本專案「不編造 weight」的紀律不得寫入。資料庫保存的是事實，不是我們對損壞文件的最佳猜測。★下一步工具需求：OCR（本機無 pytesseract／PyMuPDF）。', 'Direct', 'Not_Extracted'),
        ('金居', 'Core', '電解銅箔', 'annual_report_table', '年報營業比重表：電解銅箔 100.00%', 'Direct'),
    ],
    "B1-06": [
        ('台光電', 'Core', '銅箔基板', 'annual_report_table', '年報營業比重表：銅箔基板 56.24%。Core 理由：⑤本格 56.24% 大於 B1-08 膠片 42.85%；①②銅箔基板為公司主體產品，膠片是其上游中間材而非另一門生意', 'Direct'),
        ('台燿', 'Core', '銅箔基板', 'annual_report_table', '年報營業比重表：銅箔基板 61.83%，年報不分等級；高頻高速材料領導廠。★2026-09-07 更正 evidence_kind 由 investor_conf 改為 annual_report_table：理由同 6213 聯茂——evidence 未引用法說，weight_item 逐字對應年報比重表，且庫內無本公司法說資料', 'Direct'),
        ('聯茂', 'Core', '銅箔基板(CCL)', 'annual_report_table', '年報營業比重表：銅箔基板(CCL) 72.20%，年報不分等級；高速材料出貨為主。★2026-09-07 更正 evidence_kind 由 investor_conf 改為 annual_report_table：本列的 evidence 從未引用任何法說簡報，weight_item「銅箔基板(CCL)」逐字對應年報比重表，且 tw.conference_product_mix 中沒有本公司任何一場法說資料。錯標的後果是 _dates_for 走法說分支找不到資料而掉回年報日，evidence_period_end 則因 investor_conf 不在 DATED_EVIDENCE 內而留 NULL', 'Direct'),
    ],
    "B1-07": [
        ('長興', 'Major', None, 'annual_report_prose', '年報業務範圍載明「聚亞醯胺」：電路板、封裝基板..等產業。B.保護絕緣材料上游材料主要由電子級環氧壓克力、聚亞醯胺、樹酯、無機填充物等原料組成。下游產業主要應用電子產品金★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('雙鍵', 'Major', None, 'company_site', '年報無營運概況標準章節；PI 樹脂與軟板基材用樹脂供應商★2026-09-09 weight_status=Not_Extracted：PDF 內嵌字型無 ToUnicode 對照表，每字抽成 (cid:NNN)。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★★2026-09-10 批次 B 診斷（維持 Not_Extracted）：同本公司 B1-03 列——內嵌字型無 ToUnicode，pdfplumber 得 (cid:NNN)、pypdf 得高棉／寮文碼位，兩者都不可用，需字元重映射表。', 'Direct', 'Not_Extracted'),
    ],
    "B2-01": [
        ('佳總', 'Core', None, 'annual_report_table', '年報僅列「印刷電路板 100.00%」一條線，涵蓋基板製造與組裝加工，無法拆到單一節點，故不計比重。Core 理由：⑤年報比重表僅一條線「印刷電路板 100.00%」；①商品項目順序為 (1)MCPCB 鋁／銅基板 (2)FR4 多層電路板 (3)DBC 覆銅陶瓷基板 (4)內埋式複合型電路板，前兩項為金屬基與多層板，陶瓷基板(A1-14)列第三', 'Direct', 'Too_Coarse'),
        ('健鼎', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 99.63%', 'Direct'),
        ('博智', 'Core', None, 'company_site', '年報無營運概況標準章節；高多層與高頻印刷電路板製造商★2026-09-09 weight_status=Not_Extracted：product_narrative_status 亦為 no_section，庫內零文字。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
        ('宇環', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 99.72%', 'Direct'),
        ('定穎投控', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 99.70%', 'Direct'),
        ('志超', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 100.00%', 'Direct'),
        ('慶生', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 100.00%', 'Direct'),
        ('敬鵬', 'Core', None, 'annual_report_prose', '年報業務範圍載明「印刷電路板」：的總碳排量已較基準年2021年減少10%。(b)行業基礎指標之資訊:本公司在印刷電路板產業。其特定經營模式、活動與其他共同特性有關之行業基礎★2026-09-09 weight_status=Not_Extracted：narrative 抓到 pages 99-104 的氣候風險情境分析章節，不是業務內容章。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
        ('新復興', 'Core', '高頻微波通訊基板', 'annual_report_table', '年報營業比重表：高頻微波通訊基板 90.66%', 'Direct'),
        ('晟鈦', 'Core', '多層印刷電路板', 'annual_report_table', '年報營業比重表：多層印刷電路板 20.71%。Core 理由：①年報業務範圍首句「本公司主要從事**硬性印刷電路板**、磷銅球製造及加工買賣業務，以小量多樣PCB板提供客戶生產彈性，具靈活即時生產管理可達到快速交件，產品應用多元化」——PCB 列於句首，且整句的闡述對象只有 PCB；④計畫開發首項為「HDI 任意層互連製程研發」，同屬 PCB 製程。★⑤指向另一邊且差距很大：磷銅球 61.52% 為多層印刷電路板 20.71% 的三倍。R023 是**依序判定不是計票、也不設比重的例外門檻**，①④同向即定案。★本格比重可能被低估：年報同表另有單面板 1.00%、雙面板 6.22%、鑽孔代工 9.35% 未進任何曝險列，若確屬硬板同節點則依 R020④ 應聚合為 27.93%，屬待補。★2026-09-07 更正，原 Core 判在 B6-01★2026-09-15 M5：權重改由年報印出值 20.71% 解析（金額推算 20.7113）', 'Direct'),
        ('松上', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 41.70%', 'Direct'),
        ('柏承', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 100.00%，為公司全部營收。★2026-09-09 更正：本列原記 weight_item「全球PCB銷售產值」，那不是本公司的營收品項而是**產業統計數字**，來自 tw.product_revenue 的一筆解析錯誤——舊解析器把年報產業概況章的全球 PCB 產值表當成營業比重表讀進來，本列的 evidence 又照抄了那個品項名，於是一個 100% 的 weight 建立在一個不存在的營收線上。84 家重跑（save() 改為 replace 語意）把該筆假資料清掉後，[stale] 檢查立刻指出本列引用的品項已不存在，本列隨之更正為年報實際的「印刷電路板 100%」。★教訓記於此：**解析層的錯誤會沿著 weight_item 傳進分類層的 evidence**，而在 replace 語意存在之前，沒有任何檢查看得到它', 'Direct'),
        ('楠梓電', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 77.08%。Core 理由：①年報所營業務原文為「各種**印刷電路板**之製造、加工、組裝及銷售」，製造列於組裝之前；產品順序亦為 印刷電路板 → 印刷電路板組裝 → 商品及其他；⑤本格 77.08% 大於 V7-01 印刷電路板組裝 22.92%', 'Direct'),
        ('欣興', 'Major', None, 'annual_report_table', '年報僅列「印刷電路板 99.07%」一條線，涵蓋硬板、HDI 與 ABF 載板，無法拆到單一節點，故不計比重', 'Direct', 'Too_Coarse'),
        ('泰鼎-KY', 'Core', '多層板', 'annual_report_table', '年報營業比重表：多層板 74.36%', 'Direct'),
        ('瀚宇博', 'Core', 'PCB', 'annual_report_table', '年報營業比重表：PCB 85.15%。Core 理由（2026-09-07 因新增 V7-01 使候選成為二取一而依 R023 重跑）：①年報所營業務條列 (1)**印刷電路板製造及銷售** (2)電子產品製造服務(EMS)，PCB 列首；②價值創造位置同向——EMS 段自述「以基板組裝加工為主」，是把自家 PCB 往下游延伸的加工服務，PCB 產能才是公司的資本與技術所在。①②同指本節點。⑤同向作為佐證：PCB 85.15% 為 EMS 14.31% 的六倍', 'Direct'),
        ('燿華', 'Core', '十層板以上', 'annual_report_table', '年報營業比重表：十層板以上 52.86%', 'Direct'),
        ('相互', 'Minor', '硬板', 'annual_report_table', '年報營業比重表：硬板 12.00%。★2026-09-07 由 Core 降為 Minor：原列把年報最大的一條線「軟硬複合板 88.00%」掛在本節點，但本節點是整片皆硬的板子，而 B2-03 軟硬結合的定義逐字寫著「同一片同時含硬區與軟區者屬本節點」——該線是正面命中 B2-03，Core 依 R023 移至該格；留在本格的是年報另一條「硬板 12.00%」，依 R026① 的數值預設（<15%）role 為 Minor', 'Direct'),
        ('競國', 'Core', '多層印刷電路板', 'annual_report_table', '年報營業比重表：多層印刷電路板 92.43%', 'Direct'),
        ('精成科', 'Core', 'PCB', 'annual_report_table', '年報營業比重表：PCB 74.93%。Core 理由：①年報所營業務順序為「(1)**印刷電路板(PCB)產銷** (2)專業電子製造服務(EMS)」，商品項目同序；④計畫開發首項為「(1)**PCB事業群**研發重點為 6 盎司厚銅產品製程開發…」；⑤本格 74.93% 大於 V7-01 EMS 24.28%。三項同向', 'Direct'),
        ('育富', 'Core', '多層板', 'annual_report_table', '年報營業比重表：多層板 99.74%', 'Direct'),
        ('臻鼎-KY', 'Major', None, 'annual_report_table', '年報僅列「各式印刷電路板 100.00%」一條線，涵蓋軟板、硬板與載板，無法拆到單一節點，故不計比重', 'Direct', 'Too_Coarse'),
        ('華通', 'Core', None, 'annual_report_prose', '年報業務範圍載明「印刷電路板」：四、營運概況(一)業務內容1.業務範圍華通電腦以生產印刷電路板(PrintedCircuitBoard,PCB)為核心業務。印刷電路板是各。Core 理由：①年報業務範圍首句「華通電腦以生產**印刷電路板(PCB)**為核心業務」，續句「主要的業務範圍包含**高密度電路板(HDI)**、高層次板(HLC)、軟板(FPC)與軟硬板(Rigid-Flex PCB)」——HDI 與高層次板列於軟板與軟硬板之前，而本格定義即含一般硬板、多層板與 HDI。⑤不可用：年報無產品營收比重表，三個候選（本格、B2-02 軟板、B2-03 軟硬結合）權重全 NULL★2026-09-09 weight_status=Not_Disclosed：業務範圍章完整，直接跳到設備評估清單收尾，無任何比重。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('邑昇', 'Core', '多層印刷電路板', 'annual_report_table', '年報營業比重表：多層印刷電路板 72.69%', 'Direct'),
        ('金像電', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 100.00%', 'Direct'),
        ('霖宏', 'Core', '多層印刷電路板', 'annual_report_table', '年報營業比重表：多層印刷電路板 51.55%', 'Direct'),
        ('高技', 'Core', '多層印刷電路板', 'annual_report_table', '年報營業比重表：多層印刷電路板 98.82%', 'Direct'),
        ('南電', 'Major', None, 'annual_report_prose', '年報產業概況與研發章皆分「(1)一般電路板」與「(2)IC 載板」兩塊撰寫，前者載明「一般電路板持續朝高層數、細線路發展」「HDI 產能大幅擴張…促使 HDI 廠商近年轉向 AI 伺服器等高階應用產品」。併於「印刷電路板製造及銷售 97.23%」一條粗線故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "B2-02": [
        ('台郡', 'Core', '軟式印刷電路板', 'annual_report_table', '年報營業比重表：軟式印刷電路板 94.91%。Core 理由：公司自述業務為軟性印刷電路板「之設計、開發、製造及銷售,以及組裝打件模組等業務」，該線占營收 94.91%(R023①⑤)', 'Direct'),
        ('同泰', 'Core', '軟性印刷電路板', 'annual_report_table', '年報營業比重表：軟性印刷電路板 99.92%', 'Direct'),
        ('嘉聯益', 'Core', None, 'annual_report_prose', '年報業務範圍載明「軟性印刷電路板」：軟性電路板(FPC)之設計、開發、製造、加工及買賣之業務,為多應用產品用途之軟性印刷電路板。4.計劃開發之新商品(服務)軟性印刷電路板將隨著★2026-09-09 weight_status=Not_Disclosed：年報**有**比重表，但軸是地區別（美國 22.84／中國大陸 43.01…）。★判準是**公司選的揭露軸**：本列不是「產品軸太粗」而是「根本沒有沿產品軸揭露」，故與 Too_Coarse 分開——後者等公司揭露變細就會有數字，本列不會。', 'Direct', 'Not_Disclosed'),
        ('圓裕', 'Core', 'FPC軟性印刷線路板', 'annual_report_table', '年報營業比重表：FPC軟性印刷線路板 90.29%', 'Direct'),
        ('旭軟', 'Core', '各式軟性印刷電路板', 'annual_report_table', '年報營業比重表：各式軟性印刷電路板 97.46%', 'Direct'),
        ('毅嘉', 'Core', None, 'annual_report_table', '年報營業比重表：電子整合元件產品(含商品+模具+模組) 80.00% 為最大營收線，商品項目載明「電子模組：FPC、PCB、SLP 之佈線設計、測試,可配合SMT製程與其他零組件組裝」；計畫開發之新商品 18 項中 10 項為軟板類（多層軟板、極細線路軟板、雙面COF板、光通訊軟板、軟板基材、高速訊號軟板、CCM&OLED軟硬結合板、TGP散熱片軟板）。Core 理由：⑤本列 80.00% 為最大營收線，遠大於 F4-01 機構整合元件產品 20.00%；④計畫開發之新商品 18 項中 10 項為軟板類，方向與本格一致；①商品項目首列即「電子模組：FPC、PCB、SLP 之佈線設計、測試」。三項同向。★該列名稱橫跨 FPC／PCB／SLP 與 SMT 組裝故不計比重(R020②)，其中軟硬結合部分另見 B2-03 待補。★2026-09-07 由 Major 升 Core：2026-09-06 降為 Major 時認定「①④⑤無一支持本格」，但當時未察 80.00% 那列無任何節點認領、且其商品說明本身即 FPC／PCB／SLP 佈線，⑤與④實際皆指向本格；Core 依 R023⑤ 自 F4-01(20.00%) 移入', 'Direct', 'Too_Coarse'),
        ('臻鼎-KY', 'Core', None, 'annual_report_prose', '年報業務範圍載明「軟性印刷電路板」：業/複合專業背景,能為客戶開發出\uf075系統整合度兼具需求與市場趨勢之高端産品,如軟性印刷電路板(FPC)、高密度連接板(HDI)、高多層印刷電路。Core 理由：④長期策略決定性——年報「計畫開發之新商品」首項為「高頻寬天線 **FPC** 開發」；①商品項目按終端應用分列（手機／個人電腦／伺服器／儲存器／網通基站相關印刷電路板）不區辨硬板軟板，無法用以判別。⑤不可用：年報比重表未拆硬板與軟板(R020②)★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('華通', 'Major', None, 'annual_report_prose', '年報業務範圍「主要的業務範圍包含高密度電路板(HDI)、高層次板(HLC)、軟板(FPC)與軟硬板(Rigid-Flex PCB)」；研發成果列「完成 FPC 線圈板類 MSAP 技術開發」「完成軟板 L/S=25/25um & 20/20um 產品製程設立」。年報無比重表故不計比重', 'Direct', 'Too_Coarse'),
    ],
    "B3-01": [
        ('南電', 'Core', None, 'annual_report_prose', '年報業務範圍載明「載板」：用屬利基市場,應用產品利潤較佳,為一般電路板供應商積極切入之目標。(2)IC載板:因智慧型行動裝置銷售動能趨緩,且平均售價持續下滑,相關應用。Core 理由：與 3037 欣興同型。①不可用：年報「目前之主要商品(服務)項目」僅「印刷電路板。」一項、④僅「新世代各式高階電路板」，皆為總稱不區辨一般板與載板。②價值創造位置決定：年報產業概況與研發章明確分「(1)一般電路板」與「(2)**IC載板**」兩塊撰寫，本公司的 ABF 載板產能與其在 AI／HPC 供應鏈的地位是市場定位核心。⑤不可用：併於「印刷電路板製造及銷售 97.23%」一條粗線(R020②)，兩格皆 NULL★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('恆勁科技', 'Core', '特殊載板', 'annual_report_table', '年報營業比重表：特殊載板 59.00%', 'Direct'),
        ('景碩', 'Core', None, 'annual_report_table', '年報「載板部門 82.11%」是部門名稱，其下 13 項產品橫跨覆晶式基板／無核心基板(本節點)、PBGA 與 CSP(Mini-BGA)(B3-04)、覆晶式薄膜 COF(B4-01)，故三格均不計比重(R020②)。年報產品項目 (5)覆晶式基板(Flip Chip Substrates) (7)無核心(Core-less)基板 (8)全加成(All layer build up)基板。Core 理由：先進封裝用高階載板為公司產品清單主體，產業概況以 ABF 載板與先進封裝為主軸(R023①④)', 'Direct', 'Too_Coarse'),
        ('欣興', 'Core', None, 'annual_report_table', '年報僅列「印刷電路板 99.07%」一條線，涵蓋硬板、HDI 與 ABF 載板；業務範圍章載明「印刷電路板之製造加工(含載板)」，可確認有載板業務但拆不出比重，故與 B2-01 同為不計比重的 Direct。Core 理由：①的「印刷電路板」在該句中是**總稱**不是與載板並列的一格——年報首句「本公司主要營業項目係從事印刷電路板(PCB)、高密度連接板(HDI)、軟板(FPC)、軟硬複合板、**載板(IC Carrier)**之開發、製造」，五者皆為 PCB 大類下的次類，故該句的順序不構成①對 B2-01 的支持，①在兩格之間無區辨力。②價值創造位置決定：本公司在 ABF 載板的產能與技術地位是其市場定位核心，年報業務範圍亦特別載明「印刷電路板之製造加工(**含載板**)」。⑤不可用：年報僅一條「印刷電路板 99.07%」橫跨硬板、HDI 與 ABF 載板(R020②)，兩格皆 NULL', 'Direct', 'Too_Coarse'),
    ],
    "B3-02": [
        ('臻鼎-KY', 'Major', None, 'annual_report_prose', '年報業務範圍載明「載板」：戴裝置相關印刷電路板\uf075深跳孔HLC產品技術開發8485\uf075高階層並列式內埋線路載板技術開發值將進一步成⻑至957.8億美元,年增12.5%。P★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
    ],
    "B4-01": [
        ('易華電', 'Core', '捲帶式高階覆晶薄膜IC基板', 'annual_report_prose', '年報業務範圍載明「COF」：,22298(ReeltoReelchiponFilm;簡稱Tape-COF)其他30,270234,7652合計1,959,5331001★2026-09-09 補上 weight_item：年報比重表列「捲帶式高階覆晶薄膜IC基板 98.00%」，即本節點的捲帶封裝載板。', 'Direct'),
        ('景碩', 'Major', None, 'annual_report_prose', '年報產品項目 (6)覆晶式薄膜 COF(Chip on Flex) 之製造與銷售；本節點定義即為 COF/COP/COG。併於「載板部門 82.11%」一列故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "B5-01": [
        ('東台', 'Minor', '印刷電路板加工機', 'annual_report_table', '年報營業比重表：印刷電路板加工機 9.84%；商品說明「以數值控制單元配合高速主軸、自動夾持、自動換刀、自動偵測等機構組件，可從事印刷電路板 PCB 加工機之鑽孔／成型」，另有雷射加工機「利用雷射源聚焦在印刷電路板上，將材料汽化移除」。與 1528 恩德同型：PCB 鑽孔機與成型機屬本節點。role 依 R026 數值預設：9.84% 低於 15% 故 Minor。本公司 Core 在 G5 工具機(70.68%)', 'Direct'),
        ('川寶', 'Major', 'PCB部門', 'annual_report_table', '年報營業比重表：PCB部門 31.03%，自述為設計研發生產曝光機及 DI 直接成像設備', 'Direct'),
        ('亞泰金屬', 'Core', '印刷電路板基材製程設備', 'annual_report_table', '年報營業比重表：印刷電路板基材製程設備 92.78%', 'Direct'),
        ('大量', 'Core', '設備', 'annual_report_table', '年報營業比重表：設備 96.01%', 'Direct'),
        ('志聖', 'Core', '印刷電路板製程設備', 'annual_report_table', '年報營業比重表：印刷電路板製程設備 50.00%。membership 與 weight 的依據不變，並補上本輪才取得的工序證據：「光阻(乾、濕膜)製程設備:自動/手動壓膜系統、真空壓膜系統、濕膜塗佈系統…」（印刷頁 p79），對應 B5-01 具名的壓合與濕製程工序。Core 理由改記為 R023① 的核心技術句：「志聖專注AI產業供應鏈的技術發展與趨勢前景,延伸在印刷電路板壓膜製程之核心技術…」（印刷頁 p79）。★原以本格比重大於 A2-13 一格（10pp）作為 Core 理由的敘述已撤除——比重比較為 R023 所禁，⑤ 只作支持。★原「業務內容章節未被解析器擷取、故僅依⑤」的限制已解除：本輪已取得印刷頁 p78–p79 的業務內容原文。', 'Direct'),
        ('恩德', 'Major', '機械', 'annual_report_table', '年報商品項目「機械」線載「印刷電路板鑽孔機、成型機與檢測機」，其中鑽孔機與成型機屬本節點定義之鑽孔、成型製程設備；同線另含 G5 工具機、G4-03 刀具、B5-02 檢測機與全樹無節點之零配件，年報未拆分，依 R020 ②-MX，含未定組成的混合（MX-OPEN），不計比重。', 'Direct', 'Granularity_Insufficient'),
        ('敘豐', 'Core', 'IC高階載板製程設備', 'annual_report_table', '年報營業比重表：IC高階載板製程設備 91.77%', 'Direct'),
        ('易發', 'Core', None, 'company_site', '年報無比重表；PCB 鑽孔與成型設備★2026-09-09 weight_status=Not_Extracted：連年報都沒抓到：兩個 status 皆 no_report，source_file 與 filing_date 全 NULL。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
        ('暉盛-創', 'Core', '機台銷售', 'annual_report_table', '年報營業比重表：機台銷售 84.84%', 'Direct'),
        ('港建*', 'Major', None, 'annual_report_table', '代理銷售之設備類別，非自製；另有佣金收入 3.97% 為代理商特徵。role 理由：設備代理是港建* 的主要生意（V1 92.46%），對公司具實質經濟重要性，故為 Major。★2026-09-09 weight_status=Not_Applicable。★★**2026-09-11 複核通過，理由＝度量不適用**：與 3055 蔚華科同形——B5-01 描述的是 PCB／載板製程設備的**製造**，本公司不製造而是代理，經濟參與已完整計於 V1（電子零件、設備及材料 92.46%）。佣金收入 3.97% 是代理身分的獨立佐證：代理商賺的是佣金，製造商不會有這一條。拿掉 Not_Applicable **指不出任何替代狀態**(R020③／R004)。', 'Indirect', 'Not_Applicable'),
        ('科嶠', 'Core', '輸送爐 + 烤箱', 'annual_report_table', '年報營業比重表：輸送爐 35.21% 加烤箱 14.51%，皆為 PCB／載板製程用自動化乾燥設備，合計 49.72%(R020④)。另一列「其他機台 33.13%」含載板載具清洗機(A2-11)與 UV 光固化機、板翹反直機(本節點)，橫跨兩節點故不計入(R020②)。Core 理由：輸送爐與烤箱為公司自述之核心自動化乾燥設備，年報自述「專注於自動化乾製程設備之研發、設計與精密製造」(R023①②)', 'Direct'),
        ('群翊', 'Core', '設備', 'annual_report_table', '年報營業比重表：設備 96.98%', 'Direct'),
        ('聯策', 'Major', '濕製程智慧化方案', 'annual_report_table', '年報營業比重表：濕製程智慧化方案 29.04%；年報所營業務列 b.濕製程智慧化設備，濕製程為本節點定義具名品項。★2026-09-07 B5-01 移出 AOI／AVI 後，公司的檢測設備線改掛 B5-02，Core 隨之移往 B5-02', 'Direct'),
        ('達航科技', 'Major', '設備產銷', 'annual_report_table', '年報營業比重表：設備產銷 21.79%；年報商品項目 (2)「本公司擁有自有品牌高精密鑽孔及成型設備」，鑽孔與成型為本節點定義具名品項。★2026-09-07 由 Core 降為 Major：原 Core 掛在「專業服務 75.10%」那條線上，但該線是雷射加工受託加工服務不是設備銷售，Core 依 R023 移至 B2-07', 'Direct'),
        ('揚博', 'Core', None, 'annual_report_prose', '年報自述自製品「PCB 製程自製機器產品項目包括真空蝕刻機、常溫超級乾燥機、剝膜熱回收再生加熱系統、覆晶基板濕製程設備、印刷電路板細線路蝕刻補償系統、藥水自動分析添加儀等」，本節點定義含電鍍、蝕刻、濕製程等 PCB 製程設備（2026-09-07 起 AOI 已移出本節點至 B5-02；揚博自製品中無檢測機，歸屬不受影響）。營收「機器設備收入 60.30%」一線混有代理的半導體與 PCB 設備，無法拆分故不計比重(R020②)。Core 理由：機器設備為最大營收線且為公司唯一的自製業務，代理業務依 R022 不得建製造型 Core(R023②⑤)', 'Direct', 'Too_Coarse'),
        ('長廣', 'Core', '設備', 'annual_report_table', '年報營業比重表：設備 93.74%（印刷頁 p52）。本列的依據：上下游關聯一節寫「本公司及子公司從事 ABF載 板用真空壓膜機的研發及製造…」與「…之上游生產製程設備供應商,其餘包括銅箔及各種特用化學品供應商;下游客戶則涵蓋 IC 封裝產業以及電子產品組裝(EMS)廠商。」（皆印刷頁 p55）；客戶身分寫「本公司在真空壓膜機領域具有市場主導地位,並且世界主要 ABF 載板生產商都是本公司重要客戶。」（印刷頁 p55）；主要產品寫「本公司專注於真空壓膜技術之研發與應用,主要產品包括 ABF 載板真空壓膜機等高端精密製造設備。」（印刷頁 p56）；主要產品重要用途表首列 IC 封裝載板：「除 IC 封裝載板外,也適用 PCB、FPC、NCF、LED 及半導 真空壓膜機 體等相關製程。」（印刷頁 p63）；市場定位寫「於高階載板真空壓膜設備之市場占有率名列前茅,居於行業領先地位。」（印刷頁 p57）。壓合為本節點定義具名的工序。★本列於 2026-09-30 由 A2-12 就地改掛本節點（RULING_B6_C、RULING_B6_D）：A2-12 不保留該列，因為年報沒有任何現行營收線落在封裝廠端的封裝製程設備——晶圓級真空壓膜設備是計畫開發、PLP 用壓膜機是研發成果，依 RULING_B4_D 不構成 membership。★原 evidence 記「2026-09-07 由 B5-01 移入，理由為該公司年報通篇未提 PCB 或載板」，此前提在 FY2025 年報不成立（產業概況整節為 IC 載板，上下游圖為 IC 載板產業，競爭對手為 PCB 與面板設備廠，皆印刷頁 p55），已撤除。★原 evidence 所引的「3、產品及服務項目: 本公司提供電子、半導體設備及電子零組件之製造、安裝及銷售,其中主要包 括應用於半導體封裝、電子元件製造等領域。」（印刷頁 p52）是**下游應用語**——ABF 載板的下游正是 IC 封裝——依 R031 不足以單獨支撐 A2-12 的 exposure。Core 隨本列落在本節點（R023①＋R023②，依 R023-R 重驗）。', 'Direct'),
        ('和椿', 'Core', '設備及模組', 'annual_report_table', '年報營業比重表：設備及模組 50.79%。商品項目載「以電子製造(SMT/PCBA)製程關鍵設備與產線自動化解決方案為核心,主要包括PCBA 製程設備(如七化高性能切板機、下切割切板機、雷射切板機、二次除塵設備)」，自行開發產製項目首列「PCBA 製程設備:專精於 SMT 生產線之後段製程,提供 PCBA 切板、收板及品質檢測等自動化裝置」（印刷頁 p104）——切板改變組裝板本身，依 B5 的分節點軸屬本節點。比重不計：同一條線另含智慧產線周邊系統（「SMT 智慧倉儲系統、升降滾筒機器人」，屬 B5-03 一側）與「半導體自動化模組業務」（A2-09 一側），年報未拆分(R020②)。role 依 R026③ 預設 Major。★Core 理由(2026-10-07 改判，原記 Core Pending)：年報自述「本公司從自動化設備零組件代理商成功跨足設備製造與智慧機器人領域,目前正積極轉型」，並稱其設備為「主力 PCBA 切板機」（印刷頁 p100）；未來研發計畫七項中四項為切板機與 SMT 產線設備、模組（印刷頁 p120），重大資本支出係「為應對智慧製造趨勢及自有品牌產品之成長需求」而擴充組裝與測試空間（印刷頁 p118）——核心技術、研發與資本支出皆指向自製設備(R023①④)，代理為其轉型前之本業。另一候選 V4（自動化零件 38.11%）為代理線；風險段仍稱「本公司主要從事傳動、驅動等自動化關鍵零組件之代理銷售」（印刷頁 p123），與上開各處不一致，依研發與資本支出之去向判定。本線 50.79% 為最大營收線，僅作佐證(⑤)。★未掛：機器人及其他 11.10%（代理 PUDU 機器人、自研搬運機器人、機器人模組代工與綠風百葉窗併為一線）', 'Direct', 'Too_Coarse'),
    ],
    # 2026-09-07: B5-01 used to hold every PCB equipment maker regardless of what
    # the machine did. Split along the same functional line A2 already draws.
    "B5-02": [
        ('恩德', 'Major', '機械', 'annual_report_table', '年報商品項目「機械」線載「印刷電路板鑽孔機、成型機與檢測機」，其中檢測機為印刷電路板之檢測設備，屬本節點（原 B5-01 列記「不另建 B5-02 列」，依 R020 ②-MX (5) 改為建列）；依 R020 ②-MX，含未定組成的混合（MX-OPEN），不計比重；role 依 R026 預設 Major。', 'Direct', 'Granularity_Insufficient'),
        ('川寶', 'Major', None, 'annual_report_prose', '年報業務範圍載明 PCB 部門除曝光機外並提供「相關載板用檢查分類、AVI 外觀檢查機械設備製造與買賣」，研發成果表 113、114 兩年度皆列「Lead Frame AVI 外觀檢查機」，長期目標亦載「AVI 佈局延伸：利用光學成像優勢,從 PCB 延伸至載板及半導體導線架(Lead Frame)檢測」。role 理由：AVI 檢測機為公司自有研發並連兩年列為年度研發成果，屬實質產品線而非附帶項目故給 Major。★B5-01 於 2026-09-07 將 AOI／AVI 移出改掛本節點，本筆為該次定義變更的連帶補列。★★**2026-09-11 由 Indirect／Not_Applicable 改判為 Direct／Too_Coarse**。原記「其營收含於 PCB 部門 31.03% 之內、已計於 B5-01，依 R004 不重複計比重故為 Indirect」，**兩個標記都被誤用**：(甲) **Indirect 被拿來表達「不要重複計數」**——但本公司是**自己製造**這些檢測機，不是代理或轉投資，exposure_type 回答的是「這家公司是否直接從事該業務」，不是「這筆比重該不該記」。用 exposure_type 承載 weight 的問題，與 evidence_kind 曾經兼差當 reason 是同一種病。(乙) **Not_Applicable 吸收掉了 Too_Coarse**——營收比重對本列在結構上完全可定義，缺的是拆分依據：年報只揭露到「PCB部門 31.03%」，而該部門**同時包含製程設備(B5-01)與檢測設備(本節點)**，年報未給兩者的拆分比例。把整條 31.03% 給 B5-01、本節點給零，本身就是一個粒度決定，不是結構上的不可能。這正是 R020② 乙型（揭露粒度型），與 1802 台玻「玻璃纖維 31.31% 混了 PCB 級玻布與熱塑性玻纖」同形。★判準：拿掉 Not_Applicable 之後**指得出另一個正確狀態**，就代表 NA 以前是在吞問題。★同一家公司的另外四筆 exposure(A2-03／A2-09／A2-10／A2-15)早已是 Too_Coarse，本列原本與它們不一致。**不得由人工判斷「大概多少是檢測設備」補數字**。', 'Direct', 'Too_Coarse'),
        ('牧德', 'Core', '其他檢測系列 + 線路檢測系列 + 外觀檢測系列', 'annual_report_table', '年報營業比重表：其他檢測系列 34.00% 加線路檢測系列 33.00% 加外觀檢測系列 25.00%，三線同屬 PCB 檢測設備，合計 92.00%(R020④)。年報商品項目前六項全為檢測與量測機台：高速孔位精度量測機 Hole-AOI、高速驗孔機、泛用型尺寸量測機 EZ3D、底片線路檢查機、線寬線距量測儀、內外層線路檢查機。★2026-09-07 由 B5-01 移入：公司無任何改變板材的製程機台，舊 B5-01 定義因含 AOI／AVI 才收得下它', 'Direct'),
        ('聯策', 'Core', None, 'annual_report_prose', '年報所營業務列 a.AI 機器視覺設備 b.濕製程智慧化設備 c.生產智動化整合方案；本節點的依據為「針對高階載板(如CSP/ABF)、先進封裝與高階 PCB,提供全自動AOI/AVI全檢設備與 3D量測系統。」（印刷頁 p86）。「AI 機器視覺設備 41.17%」一線同時含本節點、A2-13 與 A2-08 三種對象，橫跨節點故不計比重(R020②)。★本輪補記第三種對象 A2-08（印刷頁 p73、印刷頁 p86）。Core 理由（R023①）：年報自述以 AVRIOT 技術架構之外觀檢測「具備業界領先的瑕疵檢出率」——被具名為技術優勢的是檢測而非濕製程，①足以排除 B5-01 的競爭解釋，於此定案。★⑤不可用於本裁定：該線 41.17% 已依 R020② 判為橫跨多節點而不計比重，一條不能歸屬本節點的線不得以全額充當本節點的營收支持證據。', 'Direct', 'Too_Coarse'),
        ('德律', 'Core', '自動檢測設備', 'annual_report_table', '同一線「自動檢測設備 97.21%」的 PCBA 檢測部分；與 A2-13 為同一營收線，兩格均不計比重。Core 理由：①年報「公司目前主要商品及服務項目」前五項全為 PCBA 檢測設備：(1)組裝電路板測試機(ICT) (2)電路板分析、測試治具 (3)錫膏自動光學檢測機(SPI) (4)電路板自動光學檢測機(AOI) (5)在線式 3D X-Ray 自動檢測機(AXI)；④計畫開發首項「多核心高速高點數**組裝電路板測試機**，全自動化大尺寸在線式組裝電路板測試系統」同屬 PCBA。⑤不可用：年報僅一條「自動檢測設備 97.21%」，與 A2-13 為既有共用線；半導體側只有營收佔比陳述與研發完成機型、未對應到營收線（R020 ②-MX 之 MX-CLUE），該線待補證，兩格皆 NULL', 'Direct', 'Too_Coarse'),
        ('由田', 'Major', 'PCB/IC載板', 'annual_report_table', '年報營業比重表：PCB/IC載板 28.45%；年報商品項目前六項含 IC 載板出貨前成品 AOI、IC 載板製程中 AOI、PCB 線路檢查設備、PCB AFVI 自動最終光學檢測系統', 'Direct'),
        ('鏵友益', 'Core', None, 'annual_report_prose', '年報業務範圍首句「本公司專注於研發、生產及銷售智能自動化設備及 AOI 機械視覺檢測設備,核心技術為 AOI(自動光學檢測)、Automation(智能自動化設備)及 AI,主要提供**半導體、PCB 及光電**客戶」。★2026-09-07 由 42.78% 改為不計比重(R020②)：設備分支的界線是所服務的製程，而「AOI檢測設備 42.78%」一線同時服務半導體(A2-13)、PCB(本節點)與光電面板(E7-02)三側，年報未給拆分依據。節點歸屬本身亦僅由「PCB」在該句中被具名支撐，屬待查。Core 理由：①核心產品能力即年報首句具名的 AOI 機械視覺檢測，公司三條營收線中唯一被自述為核心技術者；②價值創造位置同在自研 AOI 演算法與機構，OEM 設備 36.36% 為受託組裝他人設備、不創造自有 IP。①②同指檢測設備側，故 Core 留在檢測格；跨下游的拆分待證據補齊', 'Direct', 'Too_Coarse'),
    ],
    "B5-03": [
        ('聯策', 'Minor', '生產智動化整合', 'annual_report_table', '年報營業比重表：生產智動化整合 6.58%；年報所營業務列 c.生產智動化整合方案', 'Direct'),
        ('迅得', 'Major', '載板(印刷電路板)自動化設備-IBU', 'annual_report_table', '年報營業比重表：載板(印刷電路板)自動化設備-IBU 55.94%；年報商品項目載明該 BU 為「IC 載板印刷電路板、軟板、HDI、陶瓷基板等電子產業自動化設備與智慧物流系統(AMSH)」。★2026-09-07 兩項變動：(a) 由 B5-01 移入本節點（B5-01 拆出自動化功能）；(b) 由 Core 降為 Major——依 R023 的順序判定重跑後 ④(計畫開發四項中前三項指向半導體側)定案，Core 移至 A2-09，理由記在該列。本列 55.94% 雖為最大營收線，但 R023⑤ 只作支持證據，不得單獨決定 Core', 'Direct'),
        ('陽程', 'Core', 'CCL及PCB自動化設備', 'annual_report_table', '年報營業比重表：CCL及PCB自動化設備 74.30%。Core 理由（依 R023 順序判定）：①兩個候選是**同一套產線自動化能力**分別用於 CCL／PCB 與 FPD／觸控兩個下游，該能力持續創造經濟價值的所在即由兩側規模決定——CCL 及 PCB 側 74.30% 對面板側 5.35%，十四倍差距足以排除面板側為核心能力所在的合理競爭解釋，①於此定案。⑤同向，僅作支持證據。★該公司無 tw.product_narrative（年報業務內容章節未取得），①的判斷僅能由比重表的品項命名支撐；若日後取得該章節須複核', 'Direct'),
    ],
    # 2026-09-07: panel/display manufacturing equipment had no node in the tree.
    "E7-01": [
        ('均豪', 'Minor', '顯示器製程設備', 'annual_report_table', '年報營業比重表：顯示器製程設備 10.20%', 'Direct'),
    ],
    "E7-02": [
        ('晶彩科', 'Core', '自動光學檢測機及維修收入', 'annual_report_table', '年報營業比重表：自動光學檢測機及維修收入 93.09%。年報商品項目前六項全為面板側檢測與量測機台：低解析度 AOI、高解析度 AOI、素玻璃 AOI、突起物 AOI、光罩 AOI、ADSI 自動線寬量測機；產品種類欄為「TFT Array、TP、LTPS、3D Lens 檢測與量測設備」。★2026-09-07 由 A2-13 移入：先前的裁定已載明「節點歸屬本身待查……樹上目前沒有面板製程／檢測設備的節點」，本節點建立後該待查事項結清', 'Direct'),
        ('由田', 'Major', 'LCD檢測系統', 'annual_report_table', '年報營業比重表：LCD檢測系統 22.47%；年報所營業務列 B.TFT-LCD／OLED／觸控面板 AOI。★2026-09-07 新增：此線先前超過 R026 的 15% 門檻卻無任何節點可掛，是本節點成立的兩個 driver 之一', 'Direct'),
    ],
    "E7-03": [
        ('盟立', 'Minor', '光電面板智能自動化系統', 'annual_report_table', '年報營業比重表：光電面板智能自動化系統 10.08%；年報業務範圍自述其自動化系統含「光電面板智能自動化系統」', 'Direct'),
        ('陽程', 'Minor', 'FPD及觸控面板設備', 'annual_report_table', '年報營業比重表：FPD及觸控面板設備 5.35%', 'Direct'),
    ],
    "B6-01": [
        ('凱崴', 'Core', '鑽頭', 'annual_report_table', '年報營業比重表：鑽頭 55.00%，本節點定義首項即鑽針。Core 理由（2026-09-03 因新增 B2-07 使代工成為新候選而重跑 R023）：鑽頭為最大營收線且為自製耗材；代工那條線 45% 與代理 CCL 混列無法量化，且其中的代理部分依 R022 不構成製造型候選(R023⑤)', 'Direct'),
        ('尖點', 'Core', '精密金屬製品及加工服務', 'annual_report_table', '年報營業比重表：精密金屬製品及加工服務 97.40%', 'Direct'),
        ('晟鈦', 'Major', '磷銅球', 'annual_report_table', '年報營業比重表：磷銅球 61.52%。Core 理由：★①④與⑤方向相反，取⑤，理由如下。①年報業務範圍首句「本公司主要從事**硬性印刷電路板、磷銅球**製造及加工買賣業務」是兩項並列列舉，PCB 雖列前但未表明主從；④計畫開發首項「HDI 任意層互連製程研發」指向 B2-01。⑤本格 61.52% 為 B2-01 多層印刷電路板 20.71% 的三倍。並列句不足以推翻三倍的比重差距，故 Core 維持 B6-01。★若日後年報自述明確指出主從，本裁定須重跑。★2026-09-07 由 Core 降為 Major：原裁定以「①的並列句不足以推翻三倍比重差距」保留本格，但同一批裁定對 8383 千附的同型並列句（「經營業務主要包含廠務系統工程、鞋機、精密零組件整合製造等三大業務」）卻判定足以推翻 15.9pp 差距——同型證據兩套標準，由 fresh-context read-back 抓出。統一標準後：①年報業務範圍首句「本公司主要從事**硬性印刷電路板**、磷銅球製造及加工買賣業務」PCB 列前，且該句其後的整段闡述（「以小量多樣PCB板提供客戶生產彈性，具靈活即時生產管理可達到快速交件」）全部在講 PCB 的經營模式；④計畫開發首項「HDI 任意層互連製程研發」同指 PCB。①④同向，Core 依 R023 移至 B2-01★2026-09-15 M5：權重改由年報印出值 61.52% 解析（金額推算 61.5265，差異為公司自身捨入）', 'Direct'),
        ('鉅橡', 'Core', '墊板', 'annual_report_table', '年報營業比重表：墊板 55.57%', 'Direct'),
    ],
    "B6-02": [
        ('揚博', 'Major', None, 'annual_report_prose', '年報供應項目表列「印刷電路板各式藥水」與「(2)代理電子及特用化學品事業」——該表的抬頭是「本公司為國內 IC 晶圓製造、高階封裝及PCB等主流電子產業之設備及材料專業供應商,以下為本公司主要供應項目:」，列的是**代理供應**項目不是自製產品，依 R022 改為 Indirect 且不計比重。★原以此段支撐 Core，係「藥水」二字命中代理供應清單所致，2026-09-03 更正。role 理由：揚博年報自列兩項事業——「PCB 設備製造事業」（自製）與「代理電子及特用化學品事業」（代理），本列屬後者。代理材料即年報第二大營收線「耗材收入」之主體，而 PCB 藥水是供應項目表 PCB 欄的**首項**（「印刷電路板各式藥水」），亦是年報「產業上、中、下游之關聯性」一節描述印刷電路板時具名的代理標的（「自製主要製程機器設備及**代理各種領導品牌的機器設備及材料**」）；它不是設備本業的附帶配套，而是公司兩大事業之一的主要品類，對公司整體經濟具實質重要性，故列 Major。耗材收入占比 37.14% 僅作佐證', 'Indirect', 'Too_Coarse'),
        ('昶昕', 'Core', '特用化學品', 'annual_report_table', '年報營業比重表：特用化學品 10.00%（產品項目：蝕銅液、剝錫液、棕化液、化學銅液、銅錫光澤劑、乾膜顯影劑、剝膜劑，用途為線路成型用化學品）。★原記 56.88%，係 product_revenue 漏掉「回收再生產品 82%」一列後把其餘兩列重新標準化所致，2026-09-03 隨 [mix] 修復更正。Core 理由：R023⑤ 指向 J10（82% vs 10%），但 ⑤ 僅為輔助。① 年報自述「昶昕集團生產特用化學品供應電子業製程使用,亦針對公司提供給客戶的特用化學品,其產製後所產生含金屬之化學廢液,本公司予以回收」——回收線在敘述上是特化的衍生；② 護城河亦在此：「因該化學廢液乃出自我司提供的綠色配方化學品,在充分了解自己產品的情況下,本公司回收再利用的程序」；④ 計畫開發八項以蝕刻液與特化品為主。故 Core 留本節點。★此判定的張力已知：Core 僅占營收 10%，若日後年報改以再生金屬材料自述主業，應依 R023 重裁', 'Direct'),
        ('聯策', 'Minor', '印刷電路板表面處理', 'annual_report_table', '年報營業比重表：印刷電路板表面處理 7.29%；年報載明「在特用材料方面,本公司專注於高階 PCB 與載板之最終表面處理製程。提供適用於硬板、軟板及軟硬結合板之高性能化鎳金藥水」，本節點定義即含表面處理藥水', 'Direct'),
    ],
    "D6-01": [
        ('前鼎', 'Core', '光收發模組', 'annual_report_table', '年報營業比重表「光收發模組 759,735 93.25%」，業務主要內容自述「光纖通訊元件與模組:致力於 CPO(共同封裝光學)、架構之ELSFP(外置雷射光源)、高階光收發模組(Optical Transceiver)、光纖次模組(OSA)及各類相關光通訊產品之設計、研發、生產與銷售」，並自述「前鼎光電係光收發模組及OSA封裝製造廠商」；屬 D6-01 列舉之 Optical Transceiver。', 'Direct'),
        ('兆利', 'Minor', '光纖連接器', 'annual_report_table', '年報營業比重表光纖連接器 6.48%，年報自述「另本公司生產的連接器是使用在光訊號傳輸,屬於光纖連接器,在光電產業中劃分為光被動元件」，屬 D6-01 Passive 所列光纖連接器。', 'Direct'),
        ('統新', 'Core', '光學鍍膜元件', 'annual_report_table', '年報營業比重表：光學鍍膜元件 99.98%。商品項目三項全為光通訊：「(1)光通訊**主動**元件使用之薄膜濾光片(E-PON,G-PON) (2)光通訊**被動**元件使用之薄膜濾光片(CWDM,DWDM) (3)光學與功能性薄膜加工」；計畫開發為 1.6T 光通訊模塊濾光片與共封裝光學(CPO)鍍膜。★2026-09-08 由 G3 成像光學掃描分流至此：品項名為「光學鍍膜元件」，但年報明載其光路服務的是光通訊傳輸，本節點定義已含 Passive', 'Direct'),
        ('東典光電', 'Core', '濾光片', 'annual_report_table', '年報營業比重表：濾光片 75.29%。年報「公司目前之商品項目」全文為「**光通訊**薄膜濾光片」，業務範圍自述「主要從事高階光學薄膜元件產品客製化開發」，計畫開發為雲端機房小型化傳輸模組高階濾片、AI 資料中心傳輸用濾光片、CPO/LPO 元件。★物聯網商品 24.71% 未計入本格，歸屬待判。★同上，由 G3 掃描分流至此', 'Direct'),

        ('訊芯-KY', 'Major', '高速光纖收發模組', 'annual_report_table', '年報營業比重表：高速光纖收發模組 61.55%；為其 SiP 封裝能力的下游產出，Core 在 A5-05（R024）', 'Direct'),
        ('源傑科技', 'Core', '高速光連結模組及主動光纜', 'annual_report_table', '年報營業比重表：高速光連結模組及主動光纜 98.28%', 'Direct'),
        ('光環', 'Major', '光電轉換組件', 'annual_report_table', '年報營業比重表：光電轉換組件 28.00%，業務範圍載明產品含次模組、光引擎/AOC 等型式', 'Direct'),
    ],
    "C11-01": [
        ('寶德', 'Core', '人機介面 + 遊戲控制器', 'annual_report_table', '年報營業比重表兩條合計 91.37%：人機介面 85.63、遊戲控制器 5.74(R020④)。商品項目表分三欄（主要商品類別／重要用途及功能／應用範圍），與本格相關的兩列為：類別「遊戲控制器與電競相關人機介面產品」，用途「能符合各大品牌客戶於專業及休閒遊戲互動的相關電競產品，如 Gaming Mouse／Keyboard／controller／Gaming Headset 等電競週邊產品」，應用範圍「Gaming PC／NB／mobile phone／tablet」；以及類別「Surface Mouse PAD 一般人機介面產品」，用途「適用於辦公室及居家辦公使用」。（分欄照錄，不把應用範圍併進產品清單。）★★**本列由 G2 的 P2 排除產生**(2026-09-08)：本公司在 G2 工業電腦與控制運算的候選掃描中因「人機介面」一詞被命中，讀商品說明後確認其人機介面是**消費性電競與辦公室週邊**，不是工業級人機介面電腦——正是 G2 定義那句「HMI 這個字眼出現在營收品項或產品名稱中，都不足以進入本節點」所擋的情形。本節點的宣告產品即 Keyboard、Mouse、Webcam。Core 理由：無第二候選節點(R019)。★其他 8.63% 不計入：該欄未具名。商品項目表確有第三類「各類伺服器產品」，而計畫開發章把它寫成「儲存伺服器**代理買賣**」「AI 伺服器（雲端運算）**代理**」與「伺服器機櫃設計／安裝及維保服務」——**是代理與服務，不是自製**，依 R022 指向 V3 電腦通路而非製造歸屬。年報未給該業務的營收，無拆分依據，故不計入亦不另建曝險(R020④)。★**更正**：本列原寫「未達 R026 門檻」，那是誤用——R026 定的是曝險建立後 Major／Minor 的分界，不是要不要建曝險的門檻。', 'Direct'),
        ('義隆', 'Major', '筆電輸入裝置模組', 'annual_report_table', '年報營業比重表：筆電輸入裝置模組 69.92%；為其觸控 IC 的下游模組業務，Core 仍在 A4-15（R024）', 'Direct'),
        ('台半', 'Major', None, 'annual_report_table', '年報營運部門銷售表：條碼印表機 64.21%，年報明載「有關條碼印表機部門(鼎翰科技)應揭露事項,鼎翰科技(股票代碼:3611)已自行編製年報」，依 R020③ 列為 Indirect 且不計比重。role 理由：台半合併營收僅條碼印表機與整流器兩條線，條碼印表機即子公司鼎翰(3611)之主要經營業務，且為兩條線中較大者；它不是整流器本業的下游載體或附帶延伸，而是集團的另一個主要事業，對母公司整體經濟具實質重要性，故列 Major。該線營收占比 64.21% 僅作佐證', 'Indirect', 'Too_Coarse'),
    ],
    "F7-01": [
        ('丹立', 'Core', '記憶體產品', 'annual_report_table', '年報營業比重表：記憶體產品 72.28%', 'Direct'),
        ('凌航', 'Core', 'DRAM記憶體產品', 'annual_report_table', '年報營業比重表：DRAM記憶體產品 80.92%', 'Direct'),
        ('創見', 'Core', 'FLASH+DRAM產品', 'annual_report_table', '年報營業比重表：FLASH+DRAM產品 82.47%', 'Direct'),
        ('十銓', 'Core', None, 'annual_report_prose', '年報 item 全歸其他；專業記憶體模組廠。★2026-09-07 更正 evidence_kind 由 company_site 改為 annual_report_prose：本列的分類事實確由年報業務章節（tw.product_narrative）佐證——年報主要商品項目「十銓科技專注於記憶體(DRAM)及儲存產品如固態硬碟(SSD)/隨身碟/記憶卡」，並列 DRAM 70.94%／NAND Flash 之產品別比重——缺的只是營業比重表，不是年報本身。原標 company_site 使 known_date 靠 sync() 的無條件回填借用年報申報日，改正 provenance 後該日期由本列自己的證據支撐，時間軸自然成立★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('品安', 'Core', '記憶體模組相關產品', 'annual_report_table', '年報營業比重表：記憶體模組相關產品 61.71%。Core 理由（2026-09-07 因新增 V7-01 使候選成為二取一而依 R023 重跑）：①年報「公司目前之商品/服務項目」表以**記憶體模組相關產品**列首、代工列次，且該欄的產品說明佔滿整段而代工僅一行；④同向——計畫開發之新商品四項全為 DDR5 模組產品（ECC UDIMM、CUDIMM、RDIMM、RGB UDIMM），無一與代工服務有關，研發資源全數指向自有模組產品。①④同指本節點。⑤同向作為佐證：記憶體模組 61.71% 大於代工 35.91%', 'Direct'),
        ('商丞', 'Core', '儲存事業群', 'annual_report_table', '年報營業比重表：儲存事業群 54.47%', 'Direct'),
        ('威剛', 'Core', 'DRAM產品', 'annual_report_table', '年報營業比重表：DRAM產品 60.73%', 'Direct'),
        ('宇瞻', 'Core', '記憶體模組', 'annual_report_table', '年報營業比重表：記憶體模組 50.48%', 'Direct'),
        ('廣穎', 'Core', 'NANDFLASH快閃記憶體產品', 'annual_report_table', '年報營業比重表：NANDFLASH快閃記憶體產品 68.17%', 'Direct'),
        ('群聯', 'Major', '快閃記憶體模組產品', 'annual_report_table', '年報營業比重表：快閃記憶體模組產品 72.41%。★2026-09-06 由 Core 降為 Major：⑤指向本格（快閃記憶體模組 72.41%），但①②④三項一致指向控制晶片——年報業務範圍首句為「本集團從事**快閃記憶體控制晶片**及其應用產品整合之設計、製造及銷售業務」，產業概況亦稱「本集團目前係**專注於快閃記憶體控制晶片**及週邊應用整合之設計」；②模組營收含外購 NAND 的轉嫁成本，價值創造在控制晶片。Core 依 R023 移至 A4-19', 'Direct'),
    ],
    "V6": [
        ('禾伸堂', 'Major', '其他類', 'annual_report_table', '比重表「其他類」21%，年報載另代理「如風扇、電池及機構元件等等」，不屬 V1～V5 任一產品類別，依 V6 殘差節點定義歸入。', 'Direct'),
        ('榮惠-KY創', 'Core', '印刷電路板', 'annual_report_table', '年報營業比重表：印刷電路板 82.83%。★2026-09-07 由 V7-01 電子製造服務移入：該公司不從事受託製造，方向相反——年報所營業務自述「主係從事各類利基型印刷電路板(PCB)產品之**設計及銷售**，暨工控(IPC)相關電子零組件之銷售業務」，並載明「**委由中國及泰國數十家合作板廠**組成之跨國供應鏈進行生產」，自我定位為「介於專精研發打樣之設計公司及大量生產之板廠間⋯中小量 PCB 產品的技術建議與少量多樣之**採購服務**」。它買製造，不賣製造，故不符 V7 定義；依 R022 亦不得據此建製造型曝險（無自有板廠）。本節點定義的例示產品類別即明列**印刷電路板**，且 PCB 無法歸入 V1～V5 任一者，符合 V6 residual 的兩條前提。Core 理由：印刷電路板 82.83% 為年報壓倒性最大線，工業用零組 5.06% 與其他 12.11% 均遠小於它；①同向——所營業務首句即以 PCB 產品之設計及銷售列首。★年報另有「工業用零組 5.06%」（工控相關電子零組件銷售）未指派，低於 R026 的 15% 門檻', 'Direct'),
        ('偉詮電', 'Major', '電子元件代銷', 'annual_report_table', '年報營業比重表：電子元件代銷 33.00%；IC 設計本業另計，代銷屬其通路業務（R022）', 'Direct'),
        ('陞泰', 'Core', None, 'annual_report_table', '年報「電子材料 81.00%」證明其為電子材料相關通路（官方分類電子通路業），但無法證明 81% 屬電子零件代理此一細分類，故不計比重', 'Direct', 'Too_Coarse'),
        ('敦吉', 'Minor', 'RELAY-繼電器', 'annual_report_table', '年報營業比重表：RELAY-繼電器 5.75%；機電元件，不屬 V1～V5 任一產品類別，依 R031 落殘差節點', 'Direct'),
        ('擎亞', 'Major', '薄膜電體液晶顯示器(SDC', 'annual_report_table', '年報營業比重表：薄膜電晶體液晶顯示器(SDC) 15.54%；商品項目「(2)AMOLED顯示器產品——利用專有 AMOLED 技術提供客戶委託設計、整合及買賣 AMOLED 面板模組產品」。面板模組非半導體亦非被動元件，且非可獨立使用的成品，不落 V1～V5，依 R031 歸殘差節點', 'Direct'),
    ],
    "V7-01": [
        ('品安', 'Major', '代工', 'annual_report_table', '年報營業比重表：代工 35.91%；商品項目表逐字載明「代工收入 **OEM/ODM 訂單之代工收入**」，為受託製造服務而非自有產品銷售，逐字命中本節點定義。★2026-09-07 由 V7 定義補齊後的全表重掃新增', 'Direct'),
        ('瀚宇博', 'Minor', 'EMS', 'annual_report_table', '年報營業比重表：EMS 14.31%；所營業務第 (2) 項即「**電子產品製造服務(Electronic Manufacturing Service, EMS)**包括開模、注塑、塗裝、SMT 貼裝及系統組裝的完整製程架構，以基板組裝加工為主」，服務領域涵蓋個人電腦、儲存設備、通訊、車載等客戶產品。★2026-09-07 由 V7 定義補齊後的全表重掃新增', 'Direct'),
        ('福懋科', 'Major', '模組', 'annual_report_table', '年報營業比重表：(2)模組 26.00%，商品項目為模組代工及測試業務(DIMM)、家電控制器模組測試組裝、網路設備/光通訊模組代工服務。原掛 F7-01 記憶體模組，但該格是模組品牌廠（威剛、宇瞻、十銓），代工廠與品牌廠不同池★2026-09-09 weight_item 由「(2)模組」改為「模組」：品項名沒有變，是**解析器改好了**——prose reader 現在會剝掉年報的列舉編號，`tw.product_revenue` 的品項因此改名，本列的舊參照隨即被 [stale] 檢查抓出來。', 'Direct'),
        ('楠梓電', 'Major', '印刷電路板組裝', 'annual_report_table', '年報營業比重表：印刷電路板組裝 22.92%', 'Direct'),
        ('泰詠', 'Core', '工業電子 + 聲學(聲音) + 車用電子 + IC設計', 'annual_report_table', '年報自述「本公司為 PCB 基板組裝加工廠商,主要業務即提供客戶 PCB 組裝及代工服務」。比重表四列為客戶／應用別而非產品線，全部屬本節點，合計 96.34%(R020④)。★「IC設計 11.56%」不是泰詠在做 IC 設計——年報載明「亦可提供 IC 設計公司在開發板及測試板方面的快速加工服務」，是替 IC 設計客戶做 PCBA，不得據此列名建 A4 曝險。Core 理由：全部營收皆為 PCB 組裝代工(R023①⑤)', 'Direct'),
        ('精成科', 'Major', 'EMS', 'annual_report_table', '年報營業比重表：EMS 24.28%', 'Direct'),
        ('精星', 'Core', 'EMS-PCBA', 'annual_report_table', '年報營業比重表：EMS-PCBA 97.87%', 'Direct'),
    ],
    "A4-25": [
        ('聯發科', 'Core', None, 'annual_report_prose', '產品證據：年報「1.1.3 公司目前之商品項目」A.行動通訊晶片列於首位、D.平板電腦及 Chromebook 晶片；「1.1.1 公司所營業務」列多媒體、電腦週邊、高階消費性電子及其他特殊應用積體電路。營收僅「晶片 98.22%」一條粗線橫跨 A~K 全部商品項目故不計比重(R020②)。Core 理由：現有商品項目以行動通訊晶片列首(R023①)；另依 R023④ 長期策略定位，計畫開發之新商品亦以「A.新一代行動通訊晶片」列首，產業概況首節即為行動運算裝置產業並自述「擁有領先的 5G 技術」「積極參與 6G 標準制定」——此二者僅用於 ④ 策略主軸之判定，不作為產品存在之證據', 'Direct', 'Too_Coarse'),
    ],
    "U5": [
        ('揚智', 'Major', None, 'annual_report_prose', '年報自述「網路相關設備約占 15%」，商品項目為應用交付控制器(ADC)、SSL VPN 遠端存取、網頁應用程式防火牆(WAF)及資料外洩防護解決方案等網路安全與應用產品及服務★2026-09-09 weight_status=Not_Extracted：年報 @214「…網路相關設備約占15%」（與 A4-09 同一句的另一半）。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。★2026-09-09 由 Not_Extracted 改判 Too_Coarse（R020-NE 的重新裁定）：同一句年報的另一半「網路相關設備約占 15%」已成功解析，**但那條線不只有本節點的業務**：本列 evidence 自己列出的商品是「應用交付控制器(ADC)、SSL VPN 遠端存取、網頁應用程式防火牆(WAF)及資料外洩防護」，其中 ADC 是應用遞送／負載平衡設備而不是資安產品，全樹亦無對應節點。★**不得因為四項裡有三項是資安就把 15% 整條給 U5**——「大部分屬於這一格」正是 R020② 禁止的估算。★本公司是「一句 prose 對不同節點得到不同狀態」的第一個實例：同一句話讓 A4-09 取得 85.0 的 weight，讓本格停在 Too_Coarse。', 'Direct', 'Too_Coarse'),
    ],
    "A4-20": [
        ('新唐', 'Major', None, 'annual_report_prose', '年報商品項目 8)可信賴平臺模組晶片(TPM)；計畫開發新產品列新一代 TPM 與開源硬體安全晶片(OpenTitan)', 'Direct', 'Too_Coarse'),
    ],
    "A4-03": [
        ('鈺創', 'Minor', None, 'annual_report_prose', '年報載明集團晶片產品含 NPU SoC(人工智慧處理晶片)，計畫開發新商品列次世代 AI 處理晶片與 AI 及 ISP 晶片平台。role 理由：系統晶片為記憶體以外之次要產品線，年報僅一條 99.89% 粗線無法量化', 'Direct', 'Too_Coarse'),
    ],
    "Q2": [
        ('凱鈺', 'Major', '房地銷售收入', 'annual_report_table', '年報營業比重表：房地銷售收入 35.49%，年報所營業務列 (2)不動產投資開發 (3)自有不動產買賣，且該收入認列於銷貨額而非業外★2026-09-09 補上 weight_item：年報比重表列「房地銷售收入 35.49%」，evidence 本來就引用了這個數字。', 'Direct'),
        ('炎洲', 'Minor', '房產事業', 'annual_report_table', '年報營業比重表：房產事業 8.00%；所營業務「(H)住宅及大樓開發出售與不動產買賣業務」。本節點定義含住宅、商辦、都更、危老', 'Direct'),
    ],
    "I2-01": [
        ('凱鈺', 'Minor', None, 'annual_report_prose', '年報商品表「能源產業」列 10 kW 小型風力發電系統，年報自述為「國內唯一取得 10kW 小風機安規認證的廠商」並於台中港建置國內規模最大小風機案場；能源產業 13.91% 另含 LED 驅動 IC(A4-18)故均不計比重(R020②)。role 理由：凱鈺的主要經營活動為光通訊 IC 與房地銷售，兩者為年報前兩大營收線；小型風力發電系統併於第三大的「能源產業」一欄，該欄另含 LED 驅動 IC，兩者共用一線且公司未給拆分依據，風電相對於通訊 IC 本業與房地事業屬附帶之經濟地位，故列 Minor。該欄合計 13.91% 僅作佐證', 'Direct', 'Too_Coarse'),
    ],
    "L3": [
        ('中探針', 'Minor', '金屬成形加工', 'annual_report_table', '年報營業比重表：金屬成形加工 9.55%', 'Direct'),
    ],
    "U1": [
        ('天擎', 'Minor', 'SaaS/PaaS雲端應用服務及其他', 'annual_report_table', '年報營業比重表：SaaS/PaaS 雲端應用服務及其他 5.67%，產品為中小企業工廠一站式智慧生產管理系統(MES)與宮廟雲端互動影音智慧點燈服務管理系統，均以訂閱服務模式提供。role 理由：天擎的主要經營活動為各類無線監控 SoC（車用、安防、嬰兒監視、工控四條 SoC 線為年報主要營收來源）；SaaS/PaaS 雲端應用服務是這些晶片的下游配套服務，於年報五條營收線中排名第四，相對於 SoC 本業屬配套、非主要之經濟地位，故列 Minor。該線 5.67% 僅作佐證★2026-09-09 補上 weight_item：年報比重表列「SaaS/PaaS雲端應用服務及其他 5.67%」，evidence 本來就引用了這個數字，只是沒寫進 weight_item。★該欄名含「及其他」，比重因此略為高估本格，但年報未給拆分依據，取整條計入優於不計(R020④)。', 'Direct'),
        ('博士旺', 'Major', '系統開發', 'annual_report_table', '年報營業比重表：系統開發 17.74%；商品項目「(3)軟體開發產品:區塊鏈技術軟體應用開發與相關專案委託開發」；正文「本公司的 RWA 與 VASP 平台已於 114 年完成初版建置,並計畫於 115 年積極展開市場推廣」。★若日後確認以「專案委託開發」為主體則應改掛 U2 SI，兩者擇一不得同時計重(R004)★2026-09-15 M5：權重改由年報印出值 17.74% 解析（金額推算 17.7346，差異為公司自身捨入）', 'Direct'),
    ],
    "A2-14": [
        ('捷創科技', 'Core', '設備工程技術服務與維修', 'annual_report_table', '年報營業比重表：設備工程技術服務與維修 77.33%（該線為公司自身揭露的獨立項目）。本列的依據：年報對該線的描述通篇是服務動詞——「本公司銷售與服務方式包含:系統銷售、維護、系統安裝、設定、移機、校驗、維修、到廠服務、系統程式開發及客製化支援與系統整合服務工作站升級…」（印刷頁 p68）；另有「協助原廠進行諸如裝機、移機、維修、校驗、升級等服務…」與「取得來自於原廠的全球單獨授權,提供全球主要半導體製造商原廠除役測試設備之部件更換與維修服務…」（皆印刷頁 p75），逐字對應本節點定義的裝機、移機、改機、驗機、維修與保養。★本列於 2026-09-30 由 A2-13 就地改掛本節點（RULING_B10_C）：該列的經濟內容是對設備本身的現場工程服務，不是後段測試設備的製造；本公司不製造測試機台，所服務的機台屬原廠，而 A2-13 收的是設備整機的製造者。★原 evidence 的 Core 理由兩項已撤除——年報產品項目的揭露先後與本格比重大於另一線，皆為 R023 所禁的判準。Core 為既有 Core row 的節點更正，不是重新選 Core：同一條 77.33% 的線仍是 Core 的依據，只是它所屬的節點得到更正；另一候選 V1 依 R023③ 未勝出。', 'Direct'),
        ('漢測', 'Major', '半導體技術服務', 'annual_report_table', '年報營業比重表：半導體技術服務 42.87%，年報載明為「半導體檢測設備相關裝機、移機、改機、測試、維修及保養等各種專業技術服務」', 'Direct'),
    ],
    "A2-15": [
        ('川寶', 'Core', None, 'annual_report_prose', '年報半導體部門敘述的第一項即「半導體沉積 PECVD 製程設備再生」，集團產品線半導體側第一項為「半導體前段成熟製程再生設備」；業務內容為取得既有機台整新後再售，異於 A2-11 的零件再生服務與 A2-14 的現場工程服務。Core 理由：**⑤只能定到「Core 在半導體側」**——年報比重表是部門別，半導體部門 68.97% 遠大於 PCB 部門 31.03%，但該 68.97% 橫跨本格與 A2-03／A2-09／A2-10 四格且年報無金額拆分，故⑤在四格**之間**不具分辨力。**①③ 才是決定性的**：年報對半導體部門的業務敘述、與集團產品線兩個獨立來源，都把設備再生列為該側業務的首述項目，反映設備再生是本公司在半導體側的核心經濟活動。★「列為第一項」是**支持證據不是裁定規則**：單靠揭露順序不能決定 Core。此處之所以成立，是因為兩個獨立來源一致，且該項本身是可辨識的獨立經濟活動——不是因為它排在前面。★半導體部門 68.97% 一線橫跨 A2-03（PEALD 沉積設備）／A2-09（EFEM）／A2-10（關鍵零組件）／A2-15（設備再生）四格，年報無金額拆分故各格皆不計比重(R020②)；role 依 R026 判，無 weight 的 Direct 曝險以 Major 為預設，不得由 Core 反推其餘各格為 Minor。★2026-09-08 由 A2-03 移入(R023-R)：A2-03 原 Core 理由引用的「該部門具名的第一項」正是設備再生，本節點建立後那一項已不在 A2-03。★本格取得 Core **不使**其餘三格降為 Minor，三者維持 Major。', 'Direct', 'Too_Coarse'),
    ],
    "A1-03": [
        ('漢磊', 'Major', None, 'annual_report_prose', '年報「磊晶及化合物半導體事業」主要業務含矽磊晶片、埋藏層磊晶片、多層矽磊晶片與 Silicon on Insulator(SOI)磊晶，不在 A3-06「GaAs、GaN、SiC、InP」定義內故另立本格。該事業之長期業務發展計劃載明由已獨立上市並自行建檔之子公司嘉晶(3016)經營，依 R020③ 列為 Indirect 且不計比重。role 理由：同 A3-06——矽磊晶與化合物磊晶同屬子公司嘉晶(3016)經營的「磊晶及化合物半導體事業」，該事業是集團最大的一條營收線，為漢磊晶圓製造的上游材料而非其附帶延伸，年報將其列為與元件事業並列的主要事業，對母公司整體經濟具實質重要性，故列 Major。該線營收占比 61.52% 僅作佐證', 'Indirect', 'Too_Coarse'),
    ],
    "A4-27": [
        ('普誠', 'Major', None, 'annual_report_prose', '年報商品項目「其他」列 1.印表機頭驅動IC系列（重要用途：工業用噴墨印表機頭）；年報「開發成功之技術或產品」列「噴墨打印機頭驅動IC」，計畫開發之新商品列「持續開發噴墨打印機頭驅動IC,從工業應用擴充到民生應用」。年報載明「顯示器驅動研發團隊的研發重心已成功轉型,全面聚焦於具備高護城河日本市場獨有的印表機噴墨頭驅動晶片(PHD)」、「憑藉多年累積的高壓製程精密控制技術,我們已與全球領先大廠達成深度的訂製合作」。年報比重僅「積體電路(IC)產品 99.48%」一條粗線故不計比重(R020②)。role 理由：不在年報自述之三項主要營業項目內，但已量產、有專屬研發團隊、且年報點名為顯示器驅動團隊中長期的獲利基石，非商品表單列可比', 'Direct', 'Too_Coarse'),
    ],
    "G2": [
        ('研華', 'Core', '產業電腦及工業控制 + 嵌入式板卡及系統', 'annual_report_table', '年報營業比重表兩條合計 89.00%：產業電腦及工業控制 52.00、嵌入式板卡及系統 37.00(R020④)。所營業務首項為「電腦測試儀器及自動化測試系統之設計裝配組合、製造及買賣」，產業概況章開頭即「工業電腦(Industrial PC, IPC)最初專為工業自動化控制、監控與資料擷取而設計」。★**更正記錄**：本列原記「該年報以敘述行給比重而非表格」並用 evidence_kind 為 annual_report_prose，那是錯的——年報有完整的營業比重表（主要產品／銷售額／％三欄）。tw.product_revenue 的 method 欄位值 lines 描述的是**擷取器**用行解析取得該表，不是年報的揭露形式。Core 理由：無第二候選節點(R019)。★售後服務及其他 11.00% 不計入(R020④)', 'Direct'),
        ('友通', 'Core', '工業用電腦板卡及系統', 'annual_report_table', '年報營業比重表：工業用電腦板卡及系統 54.30%。業務範圍自述「本公司專注於嵌入式主機板(Embedded Motherboard)、邊緣運算系統及工業級觸控電腦(Industrial Panel PC)之研發、製造及銷售」，商品項目十餘項全為工業級運算硬體（工業級主機板、嵌入式電腦模組 SoM、工業級平板電腦、醫療級應用電腦等）。Core 理由：無第二候選節點(R019)。★★**另外三條線年報完全未說明內容，全部不計入**：工業自動化控制 17.81%（**超過 15% 而無內容**）、設備及耗材 12.92%、其他 14.97%。營業比重表只給四個標籤，商品項目章列的全是 IPC 產品，沒有任何一段描述自動化控制業務或設備耗材業務，無從判斷其經濟內容(R020④)。合計 45.70% 屬 evidence coverage gap', 'Direct'),
        ('安勤', 'Core', '工業用電腦 + 嵌入式電腦', 'annual_report_table', '年報營業比重表兩條合計 90.37%：工業用電腦 46.29、嵌入式電腦 44.08(R020④)。商品項目：「(1)嵌入式電腦：包含嵌入式單板電腦、System On Module 模組化電腦、工業級主機板、ARM-Based 單板及系統、工業級無風扇電腦、機架式系統電腦、工業級 All-in-one 觸控電腦及顯示器。(2)產業用電腦：醫規認證床邊電腦、銷售時點情報系統(POS)、車規認證車用電腦」。Core 理由：無第二候選節點(R019)。★其他 9.63% 不計入(R020④)', 'Direct'),
        ('研揚', 'Core', '單板電腦及周邊裝置 + 工業用系統產品', 'annual_report_table', '年報營業比重表兩條合計 94.88%：單板電腦及周邊裝置 49.54、工業用系統產品 45.34(R020④)。商品項目全文為「本公司主要產品與服務涵蓋嵌入式單板電腦、工業級主機板、嵌入式電腦、車載電腦、工業級液晶顯示器、工業級液晶電腦、工業級液晶顯示工作站、PC/104 單板電腦附加卡、插槽式單板電腦與背板、工業級創新開發板、強固型平板電腦、網路設備、人工智慧邊緣運算裝置、人工智慧邊緣運算伺服器、物聯網閘道器，以及智慧應用相關解決方案（如智能販賣機、智慧路燈解決方案），並提供 DMS 設計製造服務等」——**全文照錄，不刪節**：其中網路設備、智慧應用解決方案與 DMS 設計製造服務不是本節點的交付物，但年報未給它們的營收，兩條營收線亦未依此拆分，故不影響本格比重；若日後年報單列其中任一項，本列應重判。產業概況自述「本公司為工業電腦產品之專業設計與製造廠商，產品線涵蓋單板電腦、周邊裝置及工業用系統」——**該欄的周邊裝置是工業電腦的附加卡與背板**，不是消費性電腦週邊(C11-01)。Core 理由：無第二候選節點(R019)。★其他 5.12% 不計入(R020④)', 'Direct'),
        ('維田', 'Core', '工業電腦 + 工業主機', 'annual_report_table', '年報營業比重表兩條合計 78.90%：工業電腦 66.30、工業主機 12.60(R020④)。商品項目九項：擴充式液晶顯示電腦、人機介面電腦、專業特殊應用電腦系統、嵌入式電腦系統、寬溫嵌入式主機板、工業級液晶顯示器、工業級閘道器、工業級邊緣運算電腦、人工智慧運算電腦。★★**工業顯示器不計入本格**（年報印出值 14.05%，重算值 14.0447；2026-09-08 裁定）：本節點收的是「工業級人機介面**電腦**」，而單獨出售的工業顯示器沒有運算單元、不自行提供控制或運算，是 display／HMI 輸出裝置而非控制運算硬體。**這是 G2 定義那句「HMI 這個字眼出現在產品名稱中不足以進入本節點」的正面適用案例**——同一家公司的「人機介面電腦」屬本節點而「工業顯示器」不屬，差別在有沒有交付運算。Core 理由：無第二候選節點(R019)。★其他 7.05% 不計入(R020④)', 'Direct'),
        ('捷波', 'Core', 'IPC產品', 'annual_report_table', '年報營業比重表：IPC產品 78.49%。所營業務自述「工業用電腦主機板及相關系統整合、ARM base 主機板、Panel PC、All-in-One PC、準系統及 AI 智能產品為本公司營運的核心範圍」。Core 理由：無第二候選節點(R019)。★其他 21.51% 不計入：**該欄超過 15% 而年報未說明內容**，屬 evidence coverage gap(R020④)', 'Direct'),
        ('凌華', 'Core', '模組化電腦事業處 + 客製化強固電腦事業處', 'annual_report_table', '年報營業比重表兩條合計 48.00%：模組化電腦事業處 31.00、客製化強固電腦事業處 17.00(R020④)。所營業務之主要內容為「嵌入式電腦及電腦週邊產品、量測及自動化產品、邊緣運算解決方案」；產品線章載明模組化電腦事業處為 COM-HPC／COM Express／SMARC／Qseven／OSM 工業等級嵌入式模組，邊緣運算平台事業處為 Edge AI 推論平台、工業電腦系統及主機板、無風扇嵌入式電腦、3.5吋及 Mini-ITX 嵌入式單板電腦，客製化強固電腦事業處為 CompactPCI、PC104、VPX 工業用模組化強固型電腦與軍規單板電腦。Core 理由：無第二候選節點(R019)。★★**邊緣運算平台事業處 14.00% 整條不計入**(R020②，2026-09-08 裁定)：該事業處的產品列為 Edge AI 推論平台與 GPU 加速系統、工業電腦系統及主機板、無風扇嵌入式電腦、3.5吋及 Mini-ITX 嵌入式單板電腦、**ROS2 開源機器人解決方案**、**移動式機器人控制器**、物聯網閘道器、**工業級固態硬碟**——**排除的理由不是「Edge 不算 G2」**（Edge computing 是本節點已裁定的產品形態之一，見 G2 定義），而是這一條線同時橫跨 G2、G3 機器人（ROS2 與 AMR 控制器）與 F7 記憶體模組（工業級 SSD，正是 G2 定義的 negative test 之一），年報未給拆分依據。本列原提案計入本條而得 62.00%，2026-09-08 依同一標準收緊為 48.00%。★★**顯示運算事業處 20.00% 同樣整條不計入**：該事業處同時包含嵌入式 MXM GPU 模組與繪圖卡、工業級顯示器與觸控電腦、醫療顯示與醫療運算平台、以及**博弈電腦、顯示器、主機板與 I/O 控制器**（博弈屬 taxonomy gap G10）——一條 business-unit 級的營收線橫跨多個 Industry role，年報未給拆分依據。**不得為了讓本格變成 82% 而把其中的「工業級觸控電腦」估出來**。★★**IoT策略解決方案與技術事業處 12.00% 亦不計入且暫不 map**：其內容為軟體定義 EtherCAT 控制系統、AI 視覺系統與影像擷取平台、PXI/PXIe 模組化量測平台、數位影像擷取卡、高階運動控制卡、工業量測與資料擷取卡、IIoT 工業閘道器、AMR 控制系統與 AI 智能相機——其中 EtherCAT 控制系統與工業閘道器可能屬本節點，PXI 量測平台偏儀器、運動控制卡可能落 G4／G12、AMR 控制系統與 G3 有邊界，**同一條線的產品不屬同一個 identity**，現有證據不足以把整條線指向單一節點。★網路通訊暨車載事業處 3.00%（自駕與車載 AI 運算平台、AI-ADAS、智慧座艙控制器）與其他 3.00% 不計入。★★**本公司是 business-unit 級營收線標準的建立案例**：七條營收線全部是「事業處」，四條混合而不計、兩條純本節點而計入、一條屬他支。同一份年報同時提供了正例與反例，故本列的分界不是個案判斷而是標準適用。★**本列的資料前提**：本公司營收表原缺「IoT策略解決方案與技術事業處 12%」一列，庫內分母僅 88%；2026-09-08 重跑 scrapers.annual_report_revenue 補回該列後分母回到 100%，本裁定是在完整分母上做的', 'Direct'),
        ('新代', 'Core', '控制器產品', 'annual_report_table', '年報營業比重表：控制器產品**印出值 48.45%**（金額 6,763,106 仟元）。★2026-09-15 M5：年報印出比重寫入後，本列 weight 由印出值 48.45% 解析（先前依金額推算為 48.4448）；G2 節點定義引用的也是印出值。所營業務具名「研究、設計、開發、製造及銷售下列產品：1.PC-based CNC 控制器(PC-based Computer Numerical Controller)、控制器模組、伺服驅動器、雷射產品及其相關配件與電子材料」；商品項目「主要產品及服務涵蓋機床 CNC 控制器、木工機控制器、產業專機控制器、機械手臂控制器、伺服驅動器與馬達」。★**本公司是 G2 把工業電腦與機器控制器收在同一格的字面證據**：其主力產品自陳為 **PC-based** CNC 控制器——同一個交付物既是工業運算平台也是機器控制器。Core 理由：無第二候選節點(R019)。★★**伺服產品 46.50% 不計入且全樹無節點**：內容為伺服驅動器與馬達，屬 taxonomy gap G12 驅動元件（該 gap 原本只有 2483 百容一家，本公司是第二個且規模遠大於它）。本格 48.45% 與它只差 1.95pp，**但 G12 沒有節點故不構成 Core 候選**——這是候選集合受 taxonomy 限制的一例，記明以便 G12 建格後依 R023 重跑本公司 Core。★智慧製造產品 3.14%、其他 1.91% 不計入', 'Direct'),
        ('寶元數控', 'Core', '控制器 + 控制器周邊及配件', 'annual_report_table', '年報營業比重表兩條合計 100.00%：控制器 86.00、控制器周邊及配件 14.00(R020④)，公司全部營收皆在本格。所營業務首句「本公司主要從事各種精密設備之控制器之研究開發、製造、銷售及服務，以自有品牌寶元數控(LNC)進行全球銷售」；商品項目「產品主要區分為：工具機 CNC 控制器、機器人控制器、產業機械控制器、設備機上盒、工業 AI 系統與控制器周邊及配件」。★**控制器不因其服務的機器類別而分家**：本公司同一條產品線涵蓋工具機、機器人、塑膠機、木工機、產業機械與自動化——依 G2 定義，控制器不因「專門給工具機使用」而歸 G5，本公司是該判準的乾淨案例（一家控制器廠同時服務六類機器）。Core 理由：無第二候選節點(R019)。★年報另述子公司寶元智造提供整機設備與自動化整廠方案規劃，惟營收表無對應線，不建曝險', 'Direct'),
        ('樺漢', 'Core', '工業物聯網事業群', 'annual_report_table', '年報營業比重表：工業物聯網事業群 48.13%（產業概況定義為「邊緣運算、網安通訊、工業與嵌入式系統整合」）。★★**本格 weight 為 NULL，48.13% 不進入本節點的量化比重**(R020②，2026-09-08 裁定)：該線是事業群級的營收線，其自述內容至少橫跨本節點（邊緣運算、工業與嵌入式系統）與網通資安（網安通訊），年報未給拆分依據。**同一標準已於同日適用於 6166 凌華的四條事業處線**。該列因此以 weight_status=Too_Coarse 記錄比重缺席的原因——那是讓 db.fill_weight 跳過該列的機制（R020④ 明文）；evidence_kind 維持 annual_report_table，因為那才是這筆判定所依據的證據類型，weight_item 仍保留原始事業群名稱與 48.13% 這個數字的來源，以免日後有人重新把它解析回填。★★**本列是 discovery precedent：revenue label 不等於 discovery label**（2026-09-08）。本公司三條營收線名為「工業物聯網事業群／智慧工廠與廠物事業群／智慧軟體與方案事業群」，**沒有一條寫得出「工業電腦」四個字**，掃營收品項的 P1 層完全掃不到它。其 G2 identity 來自產業概況的自述：「本公司及子公司為**工業電腦(Industrial PC，IPC；亦可稱產業電腦)**專業智能應用完整解決方案商…公司主要產品為工業嵌入式主機板與模組、嵌入式電腦系統、嵌入式箱式電腦、5G 計算平台與 4K/8K 顯示器，應用於智能工控、智能零售、智能博弈、網路安全…等領域」。與 4562 穎漢恰為鏡像（那裡是欄位名稱指向別的節點，這裡是欄位名稱看不出任何產品），兩者都必須讀商品項目／產業概況章才判得出來。★★**但 Core 不因 weight 為 NULL 而消失**——這兩件事必須分開：量化主張（這條線值多少比重）證據不足，質性主張（這家公司的產業定位是什麼）證據充分。Core 理由：R023① 公司自述之核心產品能力即工業電腦——產業概況章直接把 Industrial PC、工業嵌入式主機板與模組、嵌入式電腦系統、嵌入式箱式電腦、5G 計算平台列為本業與主要產品；④三大發展主軸之首即「工業物聯網」。⑤僅作支持且本輪不採其數字(R019 分支②／R020⑤)。★**與 2399 映泰、5484 慧友不同**：那兩家沒有 Core，是因為它們最大的業務在樹上無節點（R019 分支④）；本公司的最大業務就在本節點，缺的只是可靠的比重。**無數字不等於無 Core evidence**。★★**另兩條線本輪不建曝險，且理由不是它們不重要**：智慧工廠與廠物事業群 36.24%（**營收表的欄位名是「廠物」，產業概況章寫「廠務」，以表格 label 為準以便對帳**）自述為「半導體設備及廠務系統、智能系統整合、高科技材料與設備」，一條線同時橫跨 A7-01 廠務系統、A2 半導體設備與材料，需要自己的 R020② audit；智慧軟體與方案事業群 15.63% 自述為「工業軟體、機器學習、數位雙生、雲地平台系統整合」，指向 U1 軟體，而 U 支尚未做 Definition／evidence audit。**本輪 mapping scope 是 G2，不因為一家公司出現就把另外兩棵樹整個啟動**——兩條線維持 unresolved', 'Direct', 'Too_Coarse'),
        ('映泰', 'Minor', '工業電腦', 'annual_report_table', '年報營業比重表：工業電腦 11.00%。業務範圍自述「本公司主要業務範圍包含電腦主機板、工業電腦、顯示卡及相關週邊軟硬體之開發設計、研究發展、生產製造及銷售」，商品項目三項為(1)個人桌上型電腦主機板 (2)工業電腦 (3)顯示卡。role 依 R026 數值預設：11.00% 低於 15% 故 Minor。★★**本公司無 Core，且這是刻意的**(R019，2026-09-08 裁定)：其兩條最大營收線電腦主機板 49.00% 與顯示卡 16.00%（合計 65%）**全樹皆無節點**——C10-01 個人電腦收的是 Desktop／NB／Mini PC／AIO 整機、C3-01 是伺服器主機板，消費級 PC 主機板與顯示卡沒有格（taxonomy gap，見 db/TAXONOMY_GAPS.md）。若照 R019 分支①「只有一個候選節點故該節點為 Core」，會得到「65% 的業務不在候選集合內，卻由 11% 的線當 Core」這個荒謬結果。**候選集合被 taxonomy 截斷時，R019 不得把唯一可見的候選偽裝成公司的 Core**。★**R026 與 R019 在此分開**：role 可以照既有候選的比重判定（11% → Minor），Core 不能假裝 taxonomy 已經完整。★其他 24.00% 亦未說明內容', 'Direct'),
        ('慧友', 'Major', '工業電腦', 'annual_report_table', '年報營業比重表：工業電腦 26.16%。公司目前的產品服務項目為「(1)嵌入式板卡及系統 (2)車載錄影主機 (3)CCTV 監控產品 (4)工業電腦 (5)售後服務及其他」。role 依 R026 數值預設：26.16% 達 15% 故 Major。★★**本公司無 Core，理由同 2399 映泰**(R019)：其主體業務為影像監控——CCD 電子攝影機 31.06%＋影像處理器 27.43%＋網路音視訊傳輸系統 3.04%，合計 61.53%，**全樹皆無節點**（AA1 是成像光學元件、A4-16 是影像 IC，都不是監控攝影機與錄影系統整機）。年報產業概況整章在談影像監控產業（「影像相關產業已逐步由傳統影像監控設備，朝向智慧化應用與系統整合方向演進」），可見本格不是它的產業定位。候選集合被 taxonomy 截斷，故不給 Core，見 db/TAXONOMY_GAPS.md。★其他 12.30% 未說明內容，不計入', 'Direct'),
    ],
    "G5": [
        ('巨庭', 'Core', '自動鉋木機 + 落地形圓鋸機 + 落地形手動鉋木機', 'annual_report_table', '年報營業比重表三條合計 79.90%：自動鉋木機 38.21、落地形圓鋸機 34.95、落地形手動鉋木機 6.74(R020④)。所營業務之主要內容首項為「鑽床、車床、銑床、自動鉋床等機械及其零件、五金工具製造買賣」；公司目前之商品項目僅三項（a.手動鉋木機 b.自動鉋木機 c.圓鋸機），與三條營收線一一對應；計劃開發之新商品為「更精密之鉋木機及圓鋸機…積極計劃發展其他木工機」。Core 理由：無第二候選節點(R019)。★**木工是加工材料而非 machine identity**：本公司是本節點材料中立條款的 regression case（G5 定義 2026-09-08 增訂）。★本列來源：2026-09-08 G6 audit 已裁定本公司「雖極專用仍屬 G5」（記於 G6 定義的四個實測案例），本輪只是把該裁定落到 exposure，不是新裁定。★其他 20.10% 不計入：年報未作任何說明，無可靠歸屬依據(R020④)', 'Direct'),
        ('建暐', 'Major', '加工設備類', 'annual_report_table', '新列的依據：「本公司近年來持續投入加工設備產品開發,除電腦控制 JG 治具磨床及綜合加工機外,數控立式磨床亦為本公司創造持續成長之價值。」（印刷頁 p35）、「而工具機、治具磨床、數控立式磨床等加工設備產品部分…」（印刷頁 p38）、「JG-640CM 高精度治具研磨機…」（印刷頁 p38）——皆為 G5 逐字具名的機種，且 G5 的判準明文是設備本身是否具有工具機之經濟與產品 identity，而非加工對象或所服務產業。weight 35.000 取自營業比重表的加工設備類一格，為公司自身揭露的獨立營收線。role 依 R026①（Direct 且有 weight，純數值判定）：35.00% 不低於 15%，故為 Major。', 'Direct'),
        ('穎漢', 'Core', '全自動整廠設備 + 工具機', 'annual_report_table', '年報營業比重表兩條合計 72.35%：全自動整廠設備 67.14、工具機 5.21(R020④)。★★**「全自動整廠設備」在本公司是四種機器的統稱，不是整廠整線工程**——營收線名稱指向 G11 而實際內容是工具機，判定依線的實際內容而非欄位名稱。年報兩處字面一致：所營業務首句「本公司所營業務主要為**金屬彎管機、切管機、管端成型機等全自動整廠設備**及工具機等各種自動機械設備之設計製造、安裝與零件買賣業務等」；商品項目「主要生產金屬管件加工成型之全自動整廠設備，包括全自動數值控制彎管機、半自動數值控制彎管機、切管機、管端成型機」。★**裁定的主證據是這條線的內容，不是任何人的分類**：彎管機、切管機、管端成型機都是對既有金屬管件施以塑性成形或切斷的整機，落在本節點首句與其「金屬塑性成形機（成型機、打頭機、輾牙機、鍛壓成形機）」的列舉之內。★**佐證（非決定性）**：年報產業概況轉載了一張工具機分類表，成型工具機一列具名「沖床、剪床、鍛造機、**彎管機**、油壓成型機等類型」，其上方引 ISO／IMTS 定義時亦寫「包含車、銑、磨、**折彎**、剪斷、放電加工、雷射加工等設備皆屬於工具機」。**但該表末行標「資料來源:工研院IEK」，是公司轉載的外部產業分類，不是公司對自身產品的歸類**，故只作佐證。★★**反面證據，明列於此以免被略過**：公司自己的營業比重表把「全自動整廠設備」與「工具機」列為**兩條並列的線**，等於它自己不把彎管機算進它的工具機那一欄。本裁定仍成立，理由是 R016／R023 判的是營收線的**內容與價值鏈角色**而不是公司給欄位取的名字——同一份年報用「全自動整廠設備」指稱四種機器、又用「工具機」單指綜合加工機，那是公司內部的產品線命名，不是 Industry 歸屬。工具機 5.21% 一條為綜合加工機（計畫開發列臥式搪床 YBH3000、臥式銑床 YMH、線軌車銑複合機、動柱型五軸加工中心）。Core 理由：無第二候選節點(R019)。★**整線自動化不建曝險**：計畫開發之新商品(1)全自動整廠設備底下有「擴展機械手臂應用領域」、「優化中控通訊系統與大數據收集功能」、「發展智能化全自動生產線，滿足各種產業客戶無人自動化需求」，指向 taxonomy gap G11 整廠整線自動化。★**反論已考慮**：這三項掛在「(1)全自動整廠設備」這個**現有營收線的標題**之下，且「**優化**中控通訊系統」的用字預設該系統已經存在——這確實指向公司已經在做某種整線工作。仍不建曝險的理由是**證據不足以界定業務範圍**：商品項目章列舉的是四種機器，營收表沒有任何一欄指向整線或系統整合，年報也未給該活動的規模、客戶或交付形式，無法判斷它是獨立業務還是隨機台附帶的產線調試。R026 要求曝險成立前該業務已被確認存在，本案只到 strategy evidence。★若日後年報單列該線，本列應重判，並連帶重跑 Core（R023 taxonomy／候選集合變動條款）。★本列來源同 1539 巨庭：2026-09-08 G6 audit 已裁定本公司先通過 G5，本輪落到 exposure。★其他 27.65% 不計入：**營業比重表未對該欄註記內容**(R020④)。所營業務句提及的「安裝與零件買賣業務」很可能落在該欄，但年報未給拆分依據，亦未揭露其金額，不得據以推定——這是 evidence coverage gap，不是可分配的營收', 'Direct'),
        ('亞崴', 'Core', '龍門型立式綜合加工中心機 + C型立式綜合加工中心機', 'annual_report_table', '年報營業比重表兩條合計 94.20%：龍門型立式綜合加工中心機 50.69、C型立式綜合加工中心機 43.51(R020④)。所營業務之主要內容首句「本公司主要營業項目為電腦數值控制(CNC)工具機之開發設計、生產、製造及銷售業務」，商品項目章自述「係屬產業機械中之金屬切削機械製造業」，年報並自陳工具機「營業比重皆在 95% 以上」——該 95% 的口徑含機台維修與零組件，與本格 94.20% 的分母不同，兩個數字不衝突。Core 理由：全部可歸屬營收線皆為本節點之工具機整機，無第二候選節點(R019)。★其他 5.80% 不計入：年報註記為「機台維修、零組件及外購商品之買賣」，三者混於一欄且含外購轉售(R022)，無可靠拆分依據(R020④)', 'Direct'),
        ('建德工業', 'Core', '磨床及加工中心', 'annual_report_table', '年報營業比重表：磨床及加工中心 96.11%。公司目前之商品八項全為機床：平面磨床、數值控制磨床、龍門式磨床、圓周磨床、圓盤磨床、各式研磨專用機、立式切削中心機、龍門式切削中心機；114 年度計劃開發之新商品為 KGR-600 新代系統磨床。產業概況章整節以台灣工具機出口統計描述本公司所處產業。Core 理由：無第二候選節點(R019)。★零組件 3.04%、其他 0.85% 不計入(R020④)', 'Direct'),
        ('東台', 'Core', '綜合加工中心機 + 電腦數值控制車床 + 專用機', 'annual_report_table', '年報營業比重表三條合計 70.68%：綜合加工中心機 51.43、電腦數值控制車床 17.63、專用機 1.62(R020④)。★★**專用機計入本格**：年報商品說明為「因應零件大量生產功能，以特殊結構、主軸箱及專用機夾治具裝置，可從事鑽孔、攻牙、搪孔、拉孔等功能，主要用於高精密、大批量之零件加工」——是金屬切削機床，依 G5 定義「專用程度不是本節點的入場條件」計入。該裁定同日另有三個實測案例：4544 春日的扣件成型機、1539 巨庭的木工機與 4562 穎漢的彎管／切管／管端成型機（後二者記於 G6 定義，係在 G6 audit 中先通過 G5 而未進 G6，兩家目前尚未 map）。本列是**第一個出現在已建檔公司自身營收表內**的實例。Core 理由：①商品項目章對 CNC 車床與綜合加工中心機的說明均為「主要用於高精密金屬零件加工」，公司自述的產品能力即金屬切削整機；④計畫開發之新商品前三項為臥式 CNC 車床 TG-3800、臥式加工中心 THA-5000P、倒吊立磨機 TMP-2H1276，全為工具機；⑤本格 70.68% 大於 B5-01 印刷電路板加工機 9.84%，僅作支持證據(R023①④⑤)。★維修及整修收入 14.59%、零部件製造收入 3.18% 不計入(R020④)。★其他設備 1.71% 亦不計入：這是一條具名的設備線而非售後服務，年報未說明其內容（商品項目章另述及積層製造設備與雷射加工機，但未指明對應哪一欄），落 G5 或 G6 皆有可能，保守不計——此為明示的判斷，不是 R020④ 的自動排除。★該表以 CNC 車床與綜合加工中心機列於前，屬**揭露順序**，依 R023 只作支持證據，本裁定由 ① 的產品能力與 ④ 的研發方向承擔', 'Direct'),
        ('喬福', 'Core', '電腦車床 + 切削中心機', 'annual_report_table', '年報營業比重表兩條合計 90.00%：電腦車床 47.00、切削中心機 43.00(R020④)。計畫開發之新商品六項全為工具機整機：龍門加工中心機二款、高功能電腦車床、臥式搪銑床二款、超級臥式電腦車床。Core 理由：無第二候選節點(R019)。★其他 10.00% 不計入：年報未對該欄內容作任何說明，無可靠歸屬依據(R020④)', 'Direct'),
        ('榮田', 'Core', '電腦數控車銑複合加工機 + 電腦數控車床', 'annual_report_table', '年報營業比重表兩條合計 95.87%：電腦數控車銑複合加工機 52.35、電腦數控車床 43.52(R020④)。商品項目自述「本公司主係商品包括電腦數控立式車床、電腦數控立式車銑複合加工機及龍門型車銑複合機等，係屬機械產業中之金屬切削機械製造業」。Core 理由：無第二候選節點(R019)。★維修收入及其他 4.13% 不計入(R020④)。★計畫開發之新商品第三項「配合客戶發展**產業專用機**以及**整廠整線自動化**」有兩半：前半指向 G6 專用機械（candidate role，未封版），後半指向整廠整線自動化（taxonomy gap G11，原 G1 已於 2026-09-08 解散）。兩者皆為計畫而無對應營收線，均不建曝險', 'Direct'),
        ('程泰', 'Core', '加工機 + CNC車床', 'annual_report_table', '年報營業比重表兩條合計 100.00%：加工機 57.27、CNC車床 42.73(R020④)，公司全部營收皆在本格。計劃開發之新產品為 GMT-1500 小型 CNC 五軸車銑複合機、GLS-200PLUS 小型臥式 CNC 車銑複合機、SW-42II 走心式 CNC 車床。Core 理由：無第二候選節點(R019)', 'Direct'),
        ('福裕', 'Core', '磨床 + CNC磨床 + CNC綜合加工機 + 龍門磨床 + CNC車床 + 立式車床', 'annual_report_table', '年報營業比重表六條合計 85.81%：磨床 45.78、CNC磨床 21.09、CNC綜合加工機 8.37、龍門磨床 4.83、CNC車床 3.36、立式車床 2.38(R020④)——本列示權重取**可由年報揭露之兩位小數重算**的分項合計。★★**算法必須說清楚**：年報只給金額，百分比由金額計算，未四捨五入之精確和為 85.8158，db.fill_weight 由 pct_effective 逐項加總故庫內存 85.816；兩者與本列的 85.81 只差在四捨五入層級。**不得由 100 減其他 14.18 取得 85.82**——本表七欄恰好合計 100.00，那個數值巧合而算法不同，用剩餘百分比反推本格權重是本專案明文禁止的動作（同 4563 百德列）。另列龍門銑床一項同屬本節點，114 年度營收為 0.00%，不影響合計。★**磨床整機屬本節點而非 G4-03**：G4-03 收的是以切削或磨耗去除材料的工具與磨料（砂輪、磨料、刀片），本公司交付的是研磨整機——同一個「研磨」在兩格分別是工具與機器。Core 理由：無第二候選節點(R019)。★其他 14.18% 不計入：年報註記為「零組件、外購商品之買賣、服務維修收入及代銷收入等」，含外購轉售與代銷(R022)，無可靠拆分依據(R020④)', 'Direct'),
        ('百德', 'Core', '綜合加工機 + 航太加工機', 'annual_report_table', '年報營業比重表兩條合計 61.84%：綜合加工機 35.49、航太加工機 26.35(R020④)。商品項目自述「本集團主要產品為電腦數值控制(CNC)工具機，係屬產業機械中之金屬切削機械製造業」；計畫開發之新商品含高精度立式加工機、新一代五軸加工機、擴增彈性製造產品線。Core 理由：無第二候選節點(R019)。★★**「其他 38.16%」是本公司最大的一條營收線，而年報未揭露其內容**——比重表僅三列，商品項目章只描述 MV／MK 系列加工機，全份年報無任何說明。本格 weight 仍為 61.84：R020④ 的範圍是「該節點的組成線」，本格兩條組成線各自可靠，不因公司別處有無法解析的營收而整筆轉 NULL。★**不得由 61.84 加 38.16 等於 100 反推本格為 100%**，也不得因該欄未知而推定其屬工具機——用 residual percentage 反推 business identity 是本專案一貫禁止的做法。★此欄的性質是 **evidence coverage gap（年報揭露不足）**，既不是 taxonomy gap（樹上缺格）也不是 known_unavailable（那是 provenance／日期的狀態，問的是「知不知道它何時存在」，這裡是「知道有這筆營收但不知道它的經濟內容」）。★**該欄不影響 Core**：即使日後查明它自成一個節點，38.16% 仍小於本格 61.84%，且 R023⑤ 本來就只是支持證據——候選集合被 coverage gap 截斷，但截斷的部分不足以勝出。2026-09-08 裁定：不另開 gap 列、不設 known_unavailable，僅在本 evidence 記明', 'Direct'),
        ('春日', 'Core', '成型機 + 輾牙機 + 打頭機', 'annual_report_table', '年報營業比重表三條合計 88.69%：成型機 77.39、輾牙機 5.79、打頭機 5.51(R020④)。所營業務自述「主要業務為螺絲、螺帽及零件等扣件成型機之開發設計、生產、銷售及維修服務」，並自陳「所生產之扣件成型機為生產扣件(Fastener)必備之**工具機**」。★★**本公司是 G5／G6 分界的決定性案例**：其設備只服務扣件生產，專用程度極高，但設備 identity 是金屬塑性成形機床，故屬 G5——「專用於單一產品」不把工具機推進 G6(G5 定義，2026-09-08)。Core 理由：無第二候選節點(R019)。★其他 11.31% 不計入：年報註記為「含模具、加工、維修收入等」，無可靠拆分依據(R020④)', 'Direct'),
        ('恩德', 'Core', '機械', 'annual_report_table', '年報商品項目逐線載「機械:電腦數控加工中心及各式電腦數控產業機械(適用金屬及非金屬)」與「曲面磨床及相關刀具、零配件」，加工中心與磨床屬本節點；同一「機械」線（73.15%）另含「印刷電路板鑽孔機、成型機與檢測機」（B5-01／B5-02）、刀具（G4-03）與全樹無節點之零配件，年報未拆分，依 R020 ②-MX，含未定組成的混合（MX-OPEN），不計比重。Core 理由：最大營收線「機械」首列產品為工具機整機，產業概況首節為「A.工具機應用於木工產業」，計畫開發含「航太應用角鋁切割多軸加工機開發」(R023①④)。', 'Direct', 'Granularity_Insufficient'),
        ('高鋒', 'Core', '龍門切削中心機 + 立式切削中心機 + 車床 + 搪床 + 臥式切削中心機 + 銑床', 'annual_report_table', '年報營業比重表六條合計 89.82%：龍門切削中心機 43.93、立式切削中心機 38.42、車床 3.32、搪床 3.12、臥式切削中心機 0.71、銑床 0.32(R020④)，皆為本節點定義具名的切削工具機整機。年報「公司目前之產品」逐項為立式加工中心機、臥式切削中心機、龍門加工中心機、車床等系列。★2026-10-07 新裁定：2026-09-08 G4-01 稽核時已認出為工具機，但未列入當日的 G5 backlog。另有其他 10.18% 未掛；所營業務欄另列半導體製程專用真空幫浦、晶圓清洗機等研發項目，無對應營收線', 'Direct'),
    ],
    "B2-03": [
        ('毅嘉', 'Major', None, 'annual_report_prose', '年報計畫開發之新商品第 16 項具名「CCM&OLED 軟硬結合板專案」，為 18 項研發項目中唯一之軟硬結合板品項；其營收含於「電子整合元件產品(含商品+模具+模組) 80.00%」該列之內，該列橫跨 FPC／PCB／SLP 與 SMT 組裝故整列不計比重(R020②)，Core 掛 B2-02 軟板★2026-09-09 weight_status=Too_Coarse：年報確實沿產品／業務軸揭露比重，但其粒度比本樹粗，該線同時涵蓋本節點與其他節點的業務，年報未給拆分依據(R020②)。**不得由人工判斷「大概主要是哪一格」補數字**。', 'Direct', 'Too_Coarse'),
        ('相互', 'Core', '軟硬複合板', 'annual_report_table', '年報營業比重表：軟硬複合板 88.00%，逐字命中本節點定義「同一片同時含硬區與軟區者屬本節點」。★2026-09-07 由 B2-01 移入：原掛硬板格，但年報同時另列「硬板 12.00%」一條，兩條線的區分正是本組的結構軸。Core 理由：①②同指——年報比重表以軟硬複合板列首且為 88.00% 對 12.00% 的壓倒性主線，公司的製程能力（硬質基材與軟性基材壓合為一體）即建立於此；硬板那條是同一產線的附帶產出，故 Core 在本節點', 'Direct'),
        ('華通', 'Major', None, 'annual_report_prose', '年報業務範圍把軟硬板(Rigid-Flex PCB)與 HDI、HLC、FPC 並列為生產製造項目。年報無比重表故不計比重。★本節點目前無 Product Definition；節點名稱「軟硬結合」對 Rigid-Flex PCB 已無歧義', 'Direct', 'Too_Coarse'),
    ],
    "F4-01": [
        ('富驊', 'Major', '電腦、伺服器機殼及其零組件', 'annual_report_table', '營業比重列「電腦、伺服器機殼及其零組件」內含電腦機殼與零件（商品項目「(1)電腦機殼、伺服器機殼及電源供應器。」「(2)電腦週邊零件。」，所營業務含「(2) 其他金屬製品製造業。」），電腦機殼屬 F4-01 定義以金屬加工製造的一般機構件（C4-01 定義亦明訂個人電腦機殼屬 F4-01），年報未拆分，依 R020 ②-MX，完整混合（MX-FULL），本列不計比重。', 'Direct', 'Too_Coarse'),
        ('晟銘電', 'Core', '電腦及伺服器機殼', 'annual_report_table', '營業比重列「電腦及伺服器機殼」內含個人電腦機殼（商品項目「伺服器機殼及個人電腦機殼及液冷機櫃之設計、製造與銷售。」），所營業務為「各種金屬機械沖床鋼模五金電機零件之加工製造進出口及買」，PC 機殼屬 F4-01 定義以金屬沖壓製造的一般機構件（C4-01 定義亦明訂個人電腦機殼屬 F4-01），年報未拆分，依 R020 ②-MX，完整混合（MX-FULL），本列不計比重。Core 理由：所營業務首項為「賣業務(電腦外殼及週邊設備、模具製造)。」，產業概況自述「電腦及伺服器外殼為附屬於電腦產業之週邊產品,係屬資訊」，伺服器機殼則是「並積極開發雲端伺服器機殼與液冷機」的拓展方向；兩列都不計比重，年報自述的定位不足以把 Core 移到伺服器機殼(R023)，維持既有 Core。', 'Direct', 'Too_Coarse'),
        ('濱川', 'Core', '筆電及平板電腦金屬機殼 + 筆電鍵盤金屬支架', 'annual_report_table', '年報營業比重表：筆電及平板電腦金屬機殼 77%、筆電鍵盤金屬支架 15%，合計 92%；目前主要產品為「筆記型電腦及平板電腦金屬機殼」與「筆記型電腦鍵盤之金屬機構板」，製程為「金屬沖壓成型與其外觀處理」，屬自製金屬機構件，符合 F4-01 的金屬沖壓、CNC 定義。', 'Direct'),
        ('聯德控股-KY', 'Core', '汽車零組件 + 電子零組件', 'annual_report_table', '年報自述『主要從事精密金屬沖壓零組件的研發、生產與銷售』，汽車零組件 53.94% 與電子零組件 38.38% 合計 92.32%(R020④)，皆為依客戶訂製的金屬沖壓部品。', 'Direct'),
        ('宏致', 'Major', '金屬沖壓件', 'annual_report_table', '年報營業比重表：金屬沖壓件 16.07%，年報列「金屬沖壓機構件」為提供之產品，F4-01 定義明列金屬沖壓。', 'Direct'),
        ('鉅祥', 'Core', '部品', 'annual_report_table', '年報營業比重表：部品 95.03%；所營業務首項為「模具及沖件之承製買賣」，部品為金屬沖壓與塑膠射出的端子、機構件。', 'Direct'),
        ('毅嘉', 'Major', '機構整合元件產品(含商品+模具+模組+散熱模組)', 'annual_report_table', '年報營業比重表：機構整合元件產品(含商品+模具+模組+散熱模組) 20.00%；商品項目列整合模組「將過往之機構、電子整合元件整合…皆可客製化設計、製造」。該列內含散熱模組但無法拆分，且無其他節點認領本列，故比重歸本節點。★2026-09-07 由 Core 降為 Major：本列 20.00% 遠小於電子整合元件產品 80.00%，Core 依 R023⑤ 移至 B2-02 軟板', 'Direct'),
        ('敦吉', 'Core', '模具成型組裝等', 'annual_report_table', '年報營業比重表：模具成型組裝等 30.40%；年報上中下游圖將「模具加工、沖壓、塑膠射出成型」畫在「中游(本集團)」欄，正文並載明「本集團代理行銷或自行產製之零組件」，所營業務含「(14)CQ01010 模具製造業」「(1)C805050 工業用塑膠製品製造業」。依 R022，年報明確揭露自有製造的業務線建 Direct，不因公司另有通路業務而降級。Core 理由：⑤本線 30.40% 為最大營收線；①公司自述「代理行銷或自行產製」兩者並列，而自製部分具名於上中下游圖的中游欄。★2026-09-04 由 V1 半導體通路移入：該列的證據是「通路」二字誤中產業概況前一句，年報具名的代理品項是變壓器（→V2）與繼電器（→V6），無任何半導體線，V1 已移除。★安規及電磁相容測試驗證 13.80% 為第二大線，屬檢測驗證服務，樹上尚無對應節點，未建檔', 'Direct'),
        ('州巧', 'Core', '顯示器沖壓零組件', 'annual_report_table', '年報營業比重表：顯示器沖壓零組件 62.00%，為最大營收線。年報商品項目載明其內容為「為電視、顯示器及筆電等光電產品生產周邊**金屬結構件**，包含前框(Bezel)、背蓋、燈罩及接地板」。★2026-09-07 E1 面板母體逐家裁定時判出：本公司被「顯示器」三字帶進候選，但產品是金屬沖壓件而非顯示產品，依 value-chain role 歸機構件。另有家居金屬製品 28%、醫療用品 8%（電動輪椅、照護床 OEM/ODM）與 AI 伺服器機櫃零配件，皆為金屬沖壓成形，未逐一另掛', 'Direct'),
    ],
    "B3-04": [
        ('景碩', 'Major', None, 'annual_report_prose', '年報產品項目 (1)PBGA(Plastic Ball Grid Array)基板 (2)多晶模組(MCM)BGA 基板 (3)CSP 晶片尺寸大小型用基板(Mini-BGA)；併於「載板部門 82.11%」一列故不計比重(R020②)。★本節點目前無 Product Definition；節點名稱「FC-CSP Substrate」為產業通用詞，對本公司之 CSP/Mini-BGA 基板已無歧義', 'Direct', 'Too_Coarse'),
    ],
    "Q1": [
        ('擎邦', 'Core', '石化及化工工程', 'annual_report_table', '年報營業比重表：石化及化工工程 55.50%；服務項目第 1 項「石化、化工產業廠房興建之整廠規劃、設計、建造」，第 2 項「公共工程產業之各類工程」含大眾捷運運輸系統、特高壓與電纜隧道工程、公共建築及社會公宅。本節點另涵蓋「捷運、發電廠、空調機電系統及公共工程 42.51%」中的公共工程部分，該列與 A7-01 共用不可拆分，故 55.50% 為下限而非全部。Core 理由：兩條營收線 98.01% 皆為整廠 EPC 與營建統包，高科技無塵室僅為七項服務中的第 3 項且無獨立比重(R023②⑤)', 'Direct'),
    ],
    "P4": [
        ('台達化', 'Minor', '玻璃棉產品(Glasswool', 'annual_report_table', '年報主要產品比重表第 4 列「玻璃棉產品(Glasswool) 3.85%」；所營業務「(5)玻璃棉及其有關產品之製銷」。本節點定義含隔音、防火、綠建材', 'Direct'),
    ],
    "M5": [
        ('達新', 'Major', '成衣纖維製造加工', 'annual_report_table', '年報營業比重表：成衣纖維製造加工 22.32%，主要產品欄為「成衣」；產業趨勢「1.外銷雨衣成衣:本業務主要市場涵蓋歐洲、日本與美國,採行「台灣統籌接單與備料、海外基地生產」之營運模式」「持續投入高級登山用、航海用、高爾夫球用、機車用、自行車用等該類服飾之製造」。本節點定義含運動服、戶外服、機能服', 'Direct'),
    ],
    "M1": [
        ('台化', 'Minor', None, 'annual_report_table', '年報營業比重表：纖維部 2.3%，主要產品欄含耐隆原絲與加工絲，屬本節點定義(尼龍)；耐隆粒屬 J9，同一條線橫跨兩節點故不計比重(R020②)。★台化起家業務即人造纖維：「民國 54 年間,利用闊葉雜木或林木枝梢,製造木漿,生產嫘縈人造纖維,抽絲紡紗,織布及染整之生產銷售」。role 理由：同 J9——台化的主要經營活動為化工與塑膠，纖維部是年報七個部門中倒數第二小者，本節點又僅是該部門三項產品中的兩項（原絲與加工絲）；相對於主要經營活動屬附帶、非主要之經濟地位，故列 Minor。纖維部 2.3% 僅作佐證。★台化起家業務即人造纖維，但那是歷史沿革不是現在的經濟地位，依 R023 的「反否決條件」同精神，role 應反映現況', 'Direct', 'Too_Coarse'),
    ],
    "M2": [
        ('台化', 'Minor', '紡織部', 'annual_report_table', '年報營業比重表：紡織部 0.3%，主要產品欄為「短纖紗」；產品清單「(2)人造纖維產品 生產部門:纖維部、紡織部 產品: 耐隆粒、耐隆原絲、耐隆加工絲 短纖紗」。本節點定義即為棉紗、混紡', 'Direct'),
    ],
    "R2": [
        ('東鹼', 'Major', '船舶運輸', 'annual_report_table', '年報所營業務項目表：船舶運輸 28.19%；「(2)子公司航運事業:船舶租賃買賣業務」「(2)子公司船舶運輸:提供出租輕便型散裝船舶服務,採計時方式營運出租」；產業概況另有完整一節散裝航運分析（BDI 指標與 Capesize/Panamax/Supramax/Handysize 船型）。★年報未載明該航運子公司名稱，無法查證是否另行上市；若日後查出已上市，依 R020③ 應改 Indirect 且比重改為 NULL', 'Direct'),
    ],
    "Y1": [
        ('台灣精銳', 'Major', '客房及餐飲服務', 'annual_report_table', '年報營業比重表：客房及餐飲服務 18.78%，商品說明為「台中日月千禧酒店及旗下餐廳」，屬轉投資經營之飯店業。★依 R020③ 判定為 **Direct 而非 Indirect**：該條的判準是「母公司這一筆曝險會不會與子公司的獨立資料構成同一經濟活動的重複統計」，而日月千禧酒店並非獨立建檔之上市櫃公司(不在 tw.stocks)，市場層級無第二筆計入，故不構成重複。★Y4 的定義載明「飯店附設之餐飲併入 Y1 飯店，不在本節點分列」，故客房與餐飲併為一列。★本公司 Core 在 G4-01 減速機 74.08%', 'Direct'),
    ],
    "Y4": [
        ('興農', 'Minor', None, 'annual_report_table', '年報營業比重表「家用品.團膳 16%」中的團膳（團體膳食供應）部分；與 J14 共用同一條線故不計比重(R020②)。★本節點目前無 Product Definition；節點名稱「餐飲」對團體膳食供應已無歧義。role 理由：興農的主要經營活動為植物保護（農藥）及其通路與家用品事業，三者為年報前三大營收線；團體膳食供應併於「家用品.團膳」一欄之內，是該欄兩項業務中的後者，公司未給拆分依據，年報亦未在任何章節將餐飲列為主要或策略業務——相對於農藥本業屬附帶之經濟地位，故列 Minor。該欄合計 16% 僅作佐證', 'Direct', 'Too_Coarse'),
    ],
    "Q3": [
        ('台肥', 'Major', None, 'annual_report_prose', '年報「台肥公司以三項成熟事業「肥料、化工、不動產」及新興事業「乾淨能源、深層海水、投資」為發展主軸」；「3. 不動產事業:…南港經貿園區「C2旅館及辦公室開發案」辦公大樓,於 110 年第 4 季取得使照後開始啟用營運,招商成果豐碩,已吸引多間國際級科技公司進駐」「台肥第一棟商辦大樓新竹「TFC ONE」出租率達 100%」。已有實際營運中的出租資產故不標 Emerging。年報無營業比重表故不計比重', 'Direct', 'Too_Coarse'),
    ],
    "B2-07": [
        ('達航科技', 'Core', '專業服務', 'annual_report_table', '年報營業比重表：專業服務 75.10%。年報商品項目 (1)「專業服務——本公司憑藉自主研發生產的雷射加工機設備以及自主雷射光源整合之技術所提供之專業雷射加工技術服務」，即對他人板材之受託雷射加工，為本節點定義具名之受託加工。Core 理由：①年報商品項目僅兩類且以專業服務列首，設備產銷列次；⑤同向——本格 75.10% 為 B5-01 設備產銷 21.79% 的三倍以上。①⑤同指本節點。★2026-09-07 由 B5-01 移入：原 Core 把一條**加工服務**收入掛在設備製造節點上，節點與品項不符', 'Direct'),
        ('聯致', 'Core', '代工收入', 'annual_report_table', '年報營業比重表：代工收入 32.00%；銷售產品項目「多種印刷電路板壓合代工」，年報自述「本公司主要產品為玻璃纖維布黏合片、銅箔基板、多層基板壓合代工及化學品生產等…多層基板壓合代工則屬印刷電路板前段製程作業」。★原掛 B6-01 PCB 耗材，但該節點定義是鑽針、銑刀、磷銅球、墊板四項耗材實體品，容不下製程服務(R025)。Core 理由（新增本節點後依 R023 重裁；候選＝B2-07 32%／B1-01 30%／B1-08 13%）：① 年報自述句「玻璃纖維布黏合片、銅箔基板、多層基板壓合代工」是**價值鏈上游到下游的排序**而非主從排序，無區辨力；④ 決定性——年報載明「終止從基板投料到內層至壓合一貫作業之Masslan代工業務。**專注純代工領域,鞏固並擴大特定客戶代工業務**」，策略明確指向代工；⑤ 32% 亦為最大線（與 CCL 30% 接近）', 'Direct'),
        ('凱崴', 'Major', None, 'annual_report_prose', '年報商品表「電路板製程代工 機械鑽孔、機械成型、鐳射鑽孔、電性測試」。★原掛 V7-01 電子製造服務，但該筆 evidence 是年報描述 PCB 產業鏈的段落，且 V7-01 的定義是 EMS／ODM／OEM／Box Build，非製程代工。併於「銅箔基板及鑽孔代工 45%」一列，該線另含代理南亞銅箔基板之銷售（依 R022 屬通路，不得據此建 B1-01 製造型 Direct），橫跨故不計比重(R020②)', 'Direct', 'Too_Coarse'),
    ],
    "B1-08": [
        ('聯致', 'Minor', '玻璃纖維布黏合片', 'annual_report_table', '年報營業比重表：玻璃纖維布黏合片 13.00%；銷售產品項目「軟硬結合板用Low CTE Non-flow PP低熱膨脹不流動粘結片、玻璃纖維布黏合片製造、加工及銷售」。年報載明該產品「除供對外銷售給一般印刷電路板廠商使用外,前者並提供內部生產銅箔基板原料使用」', 'Direct'),
        ('台光電', 'Major', '黏合片', 'annual_report_table', '年報營業比重表：黏合片 42.85%，即浸樹脂而未貼銅箔的 B-stage 膠片，本節點定義具名含「黏合片」。★2026-09-04 由 [core_pending] 的全表重掃補建：B1-08 於本輪新增，而本公司這條 42.85% 的線是全表第二大的膠片營收，先前無節點可掛', 'Direct'),
        ('騰輝電子-KY', 'Major', '黏合片', 'annual_report_table', '年報營業比重表：黏合片 819,822 仟元／19.22%（金額推導），即未貼銅箔的 B-stage 膠片，本節點定義具名含「黏合片」。★2026-09-04 由 [core_pending] 全表重掃補建。★另一條「鋁基板 27.48%」為本公司第二大線且尚未建檔——鋁基覆銅板究竟落 B1 銅箔基板側或 B2 硬板側需查年報，本輪未處置', 'Direct'),
    ],
    "G3-01": [
        ('達明', 'Core', '機器手臂及相關零組件銷售', 'annual_report_table', '年報營業比重表：機器手臂及相關零組件銷售 83.87%。本節點的定義即「以機械本體形式交付之機器人產品的設計、製造與銷售」，本列是該定義的直接適用。Core 理由：本公司已確認的候選集合中，G3-01 是唯一能承接這條核心產品線的節點，屬 R019 分支①，不需要多候選比較。★★**自動化整合方案 15.40% 不建立 Industry exposure**（2026-09-09 裁定）——理由不是它不重要，而是**全樹目前沒有已批准的節點能收它**：原 G1 工業自動化與產線整合已於 2026-09-08 解散（population audit 證明它沒有獨立 territory），而接手的 G11 跨製程工業自動化／整廠整合目前是 `population_pending` 的 gap candidate，不是 approved node。**該活動真實存在，只是暫無 Industry home**；本列記下它，是為了 G11 的 existence test 一旦成立可以直接回頭重判本公司，不必重新發現這條業務(見 db/TAXONOMY_GAPS.md G11)。★★**本列是 G3 的 multi-activity fixture**：同一家公司同時製造機器人本體並提供自動化整合，前者進本節點、後者因無 approved home 暫留 gap——這正是 G3-01 定義裡那句「一家公司可以兩邊都有，但那是兩條線不是一條」的實例。★推論出的通則：**company-level 的 Industry 歸屬不要求每一條營收線都立刻有節點**。把沒有家的活動硬塞進最近的一格，才是真正的錯誤——那會讓 G3-01 重新變成 G1 解散前的樣子（自動化整合被吸進機器人格）。★其他 0.73% 不計入(R020④)', 'Direct'),
    ],
    "G4-01": [
        ('上銀', 'Core', '線性滑軌 + 滾珠螺桿', 'annual_report_prose', '年報兩條合計 82.00%：線性滑軌 62.00、滾珠螺桿 20.00(R020④)。★該年報以敘述句給比重而非表格(method=prose)：「2025年營業額:線性滑軌佔營收62%,滾珠螺桿佔營收20%,機器人佔營收10%,其他8%」。營業主要內容首句為「主要從事**線性滑軌(GW)、滾珠螺桿(BS)**、機器人、Torque Motor迴轉工作台之研究開發、設計、製造、銷售」，滑軌列首。Core 理由：①自述首項即線性滑軌；⑤本格兩條合計 82.00% 遠大於機器人 10.00%。★**機器人 10.00% 不建曝險**：未達 R026 門檻，且該線橫跨關節式與史卡拉機械手臂(G3-01)、晶圓移載系統與晶圓機器手臂(A2-09)、下肢復健機器人(醫療)三處而年報未拆分，R020②不可分割', 'Direct'),
        ('全球傳動', 'Core', '滾珠螺桿 + 線性滑軌 + 滾珠花鍵', 'annual_report_table', '年報營業比重表三條合計 99.38%：滾珠螺桿 48.89、線性滑軌 44.03、滾珠花鍵 6.46(R020④)。所營業務之主要內容全文為「生產及銷售滾珠螺桿、線性滑軌、滾珠花鍵及其他產品」，商品項目另列旋轉系列、單軸機器人、直線軸承、聯軸器及螺桿支撐座，計劃開發為行星式滾柱螺桿、線性執行器與微小型差動式滾柱螺桿。Core 理由：全部營收線皆為本節點宣告之傳動元件，無第二候選節點', 'Direct'),
        ('台灣精銳', 'Core', '減速機', 'annual_report_table', '年報營業比重表：減速機 74.08%。所營業務之主要內容為「主要為**減速機等機械傳動零組件**之製造、研發及銷售，其中減速機以生產行星式減速機為主」，商品說明為「用來降低馬達轉速、增大轉矩及提升輸出扭力的**機械傳動機構**」，計畫開發含高精度齒輪及齒排、伺服用零背隙撓性聯軸器。Core 理由：①自述即機械傳動零組件；⑤ 74.08% 遠大於客房及餐飲服務 18.78%。★另一條營收線為轉投資之台中日月千禧酒店，另掛 Y1', 'Direct'),
        ('瑞穎', 'Major', '自動倉儲使用之軸承 + 車庫門使用之軸承 + 手推車使用之軸承 + 休旅車拉門軸承', 'annual_report_table', '年報營業比重表四條合計 95.00%：自動倉儲使用之軸承 74.15、車庫門使用之軸承 12.56、手推車使用之軸承 7.51、休旅車拉門軸承 0.78(R020④)。四條全部是滾動軸承，屬本節點宣告之軸承。★**不依終端市場拆分 weight**：同一顆軸承賣進自動倉儲、車庫門或手推車仍是軸承，supply-chain role 未變(2026-09-08 G4 零組件 identity 裁定)。年報產業概況自述「本公司所處之軸承產業利基市場，主要涵蓋民生設備、物流輸送設備及自動化設備等應用領域」——終端市場含非工業製造流程者，依上開裁定不影響歸屬。★**Core 待 R023 最終確認**(R019)：本輪取得的是 exposure evidence，尚未完成 Core evidence 檢核', 'Direct'),
        ('鈞興-KY', 'Major', '齒輪箱 + 全鋼傘齒 + 粉末冶金齒輪 + 全鋼直齒', 'annual_report_table', '年報營業比重表四條合計 73.99%：齒輪箱 34.94、全鋼傘齒 16.65、粉末冶金齒輪 14.08、全鋼直齒 8.32(R020④)。所營業務為「主要從事**中小模數齒輪、齒輪箱、精密五金件**之研發、生產及銷售」，齒輪箱品項含行星結構齒輪箱、轉角齒輪箱、渦輪蝸杆齒輪箱與杯型／禮帽型／一體式諧波減速器。★年報自述應用領域為電動工具、園林工具、工業縫紉機、遊艇產業、汽車、醫療機械設備及工業機器人——主力應用非工業製造流程，依 2026-09-08 G4 零組件 identity 裁定不影響歸屬。★**精密五金件 17.59% 未計入本格**：該線含凸輪、行星保持架、傳動軸、打擊塊，其中打擊塊為電動工具衝擊件而非傳動元件，年報未拆分，R020② 不逕行分配。★**Core 待 R023 最終確認**(R019)', 'Direct'),
    ],
    "G4-02": [
        ('捷流閥業', 'Core', '蝶閥', 'annual_report_table', '年報營業比重表：蝶閥 62.36%；年報自述「本公司主要營業項目為工業閥門的研發、設計、製造及銷售,生產閥類產品囊括蝶閥、球閥及控制閥等產品,產品廣泛運用於石油、石化、鋼鐵、造船、能源、水處理」。★另一條「外購閥 33.09%」為外購轉售，依 R022 通路產品線不得認定製造歸屬，不計入本格比重——本格的定義因此不需為了容納它而擴寬到工程服務或通路。★原掛 A7-01 廠務系統，但該節點定義是無塵室／HVAC／Gas／廢水等**廠房系統**，而捷流是通用工業閥門製造商；A2-10 的閥件指半導體設備真空零組件、G4-01 是傳動精密件、G6-01 是專用機械整機，四格皆不含工業閥門本體(R025)。Core 理由：自製蝶閥為最大營收線且為公司自述主營(R023①⑤)。★另一列「外購閥 33.09%」品名自陳外購，依 R022 屬通路性質應為 Indirect，但本節點已有本公司的製造型 Direct 一列，無法在同一節點再記一列，故僅於此載明', 'Direct'),
        ('進典', 'Core', '球閥及蝶閥', 'annual_report_table', '年報營業比重表：球閥及蝶閥 94.48%；年報所列產品即球閥、蝶閥本體。★2026-09-07 由 A7-01 廠務系統移入：A7-01 的定義是無塵室／HVAC／Gas／Chemical／Water／廢水等**廠房系統**，而進典售出的是閥門產品本身，與 4580 捷流閥業同型（捷流已於同一理由自 A7-01 移入本節點）。其閥門大量供應半導體廠務工程，但客戶所屬產業是終端市場不是產品，依 R031 不得據以掛 A7-01；半導體曝險應循題材／生態系表表達。Core 理由：本列 94.48% 為公司近乎全部營收(R023⑤)', 'Direct'),
    ],
    "F8": [
        ('鈺寶-創', 'Major', '模組', 'annual_report_table', '年報營業比重表：模組 58.00%；商品項目「(1) 24 位元高效能長距無線音訊傳輸晶片/模組」等三項皆為「晶片/模組」並列。模組為自家無線音訊晶片的出貨載體，售予品牌客戶而非終端周邊成品，故不掛 C11-01（Keyboard/Mouse/Webcam 成品）、D2-01（Wi-Fi 設備）或 V7-01（EMS 代工）', 'Direct'),
    ],
    "V3": [
        ('茂訊', 'Major', '商用筆記型電腦', 'annual_report_table', '商品項目為『商用筆記型電腦及其週邊商品之銷售』，上游為 Acer、ASUS、Lenovo 等品牌，屬電腦及週邊經銷。', 'Direct'),
        ('青雲', 'Minor', '板卡產品 + 伺服器產品', 'annual_report_table', '年報營業比重表：板卡產品 10.14% 加伺服器產品 2.54%，合計 12.68%(R020④)；商品項目「(2)品牌代理板卡產品」「(3)品牌代理伺服器設備產品」。本節點定義為資訊系統與消費電子成品之代理，兩者皆是', 'Direct'),
        ('聯強', 'Core', None, 'annual_report_prose', '年報業務範圍：3C 產品銷售收入 62.8%，另列半導體產品、資訊商用產品、資訊家用產品、通訊產品；主要客戶為系統整合商、加值商、門市、連鎖大型零售通路商、電商、製造商、企業用戶與個人用戶。本節點定義為資訊系統與消費電子成品之代理／通路。★比重不計：年報無營業比重表（[mix] no_stored_table），62.8% 僅見於敘述且未進 tw.product_revenue。★2026-09-04 由 V1 移入：V1 那列的證據是「通路」二字命中敘述文字，年報並未指出半導體是其主體。Core 理由：①公司自述以 3C 產品銷售為首且占 62.8%；②③價值創造與供應鏈角色是成品通路而非零組件代理，與 V1 的分界依 V3 定義即成品 vs 零組件★2026-09-09 weight_status=Not_Extracted：年報 @32「2025年度集團營業內容及營業比重 3C產品銷售收入 62.8% … 36.6% … 0.6%」，表格欄位交錯，比重無法對應到產品列。★**這一格是工單不是結論**——它說的是「我們沒取到」而不是「公司沒揭露」，來源修好之後 MUST 重新裁定成 weight／Too_Coarse／Not_Disclosed，不得永久停在此值。', 'Direct', 'Not_Extracted'),
    ],
    "V8": [
        ('華立', 'Major', '平面顯示器產業 + 印刷電路板/主機板產業', 'annual_report_table', '年報營業比重表：平面顯示器產業 23.73% 加印刷電路板/主機板產業 12.26%，合計 35.99%(R020④)。年報五大經營範疇之「(3)光電/光通訊/平面顯示器」列 OLED 材料及零組件、系統光電顯示晶片、材料及設備、光通訊用材料；PCB 側列 Dry Film、離型膜、PCB 構裝材料化學品。年報自述本業為「引進全球關鍵或趨勢產業之前瞻材料、元件、系統、設備、氣體、技術及整合服務」，未提及自有製造。★另一條「電子資通訊產業 26.38%」不掛任何格：該事業群同時含 Dry Film／離型膜／PCB 構裝化學品(本節點)、主被動電子元器件(V2／V6)、曝光機(設備)與寬能矽電源器件(半導體，V1)，四格混合且年報未拆(R020②)', 'Direct'),
        ('崇越', 'Minor', 'LED/LCD/電子相關產品', 'annual_report_table', '年報營業比重表：LED/LCD/電子相關產品 3.80%。年報自述「代理銷售半導體、LCD、LED 等產業所需之精密材料、製程設備與零組件」，其中供 LCD／LED 產業者屬本節點，供半導體製程者另計於 V1(84.60%)', 'Direct'),
    ],
    "V4": [
        ('羅昇', 'Core', '自動化控制類 + 工業傳動類', 'annual_report_table', '年報營業比重表：自動化控制類 28.11%、工業傳動類 15.57%，兩條合計 43.68%(R020④)。年報商品項目開頭即載「目前代理之商品大致如下」：自動化控制類列「高效變頻器、節能櫃、交流/直接/線性伺服馬達及驅動器、可程式控制器(PLC)、數控系統(CNC)、人機介面(HMI)、工業電腦(IPC)」等，工業傳動類列「滾珠螺桿、線性滑軌/模組、電動缸、空壓、油壓、聯軸器」等——皆為代理銷售，依 R022 歸通路而非製造節點；傳動與線性運動元件於 2026-10-07 列入本節點定義。★2026-09-08 裁定「羅昇的自動化控制 → V4，因為是代理」，2026-10-07 補寫；工業傳動類於定義修訂後併入。★Core 理由：公司自述「本公司主要為自動化機電組件通路商及專業的自動化技術服務業者」，並稱「本公司從事自動化機電零組件經銷代理,其產品依特性大致可分為工業傳動類及自動化控制類」（印刷頁 p58）——核心產品能力與對外定位都落在本節點(R023①④)；另一候選 V1 的半導體線由 2022 年轉投資之子公司資騰經營，整條線 11.96%；候選 G6-02 為 2024 年併入之子公司怡進的捆包設備，整條線 31.63%，非公司自述之主業；候選 V9 為 2022 年起之綠能布局（子公司達昇、BlueWalker），整條線 12.64%；本節點合計 43.68% 為各節點最大，僅作佐證(⑤)。★設備及耗材類 31.63% 掛 G6-02（判讀為子公司怡進的捆包機線，不計比重，見該列）。★能源管理類產品 12.64% 掛 V9（不計比重，見該列）。五條具名營收線皆已有列。見 TAXONOMY_GAPS', 'Direct'),
        ('和椿', 'Major', '自動化零件', 'annual_report_table', '年報營業比重表：自動化零件 38.11%。商品項目載「提供自動化零組件整合銷售,包含高精度傳動、驅動、感測與控制元件」；重要用途把驅動與傳動組件列在代理銷售項目之下——「代理日本Panasonic伺服馬達、PLC、雷射模組、感測器及視覺模組」，以及 OILES、黑田精工等之「自潤軸承、萬向接頭、直線軸承、滾珠螺桿、直線滑軌」（印刷頁 p104）——屬本節點定義具名的控制產品與傳動元件之代理(R022)。比重不計：同一條線另含自行開發產製的「運動控制卡、多軸運動控制系統」與「機械手臂控制器」（印刷頁 p104），屬 G2 一側，年報未拆分(R020②)。role 依 R026③ 預設 Major。★2026-09-22 已裁定本線為代理轉售、不屬 A2-10；2026-10-07 V4 定義列入傳動元件後掛入。Core 在 B5-01，理由見該列', 'Direct', 'Too_Coarse'),
        ('所羅門', 'Major', '智動化業群', 'annual_report_table', '年報營業比重表：智動化業群（智動化產品）29%。年報載「本公司是美商 Rockwell Automation 在台灣的代理商」（印刷頁 p96），商品項目列自動化事業處「ROCKWELL 全系列產品線」與機器人應用事業處之 Universal Robots、Kawasaki 機械手臂等；重要用途與產製過程表對自動化事業處、機器人應用事業處皆載「代理業務，無產製過程」（印刷頁 p112–p113）——工業自動化與控制產品之代理，屬本節點(R022)。比重不計：同一事業群另含視覺事業處之「自主研發產品」（3D 視覺機器人取放系統等，該表載有產製流程），年報未拆分，且指不出可具名之另一節點，故記 Granularity_Insufficient(R020②-BU、①-S)。role 依 R026③ 預設 Major。★Core Pending(R019 分支 3)，理由見 V9 列；機電事業群 40% 已於 2026-10-07 V9 增設後掛入。★未掛：光電製造業 21%（TN/STN/TFT-LCD 零組件）、電子通路業 8%（IC 及其他通路）、其他電子業 2%。見 TAXONOMY_GAPS', 'Direct', 'Granularity_Insufficient'),
    ],
    "G6-01": [
        ('富強鑫', 'Core', '塑膠射出成型機', 'annual_report_table', '年報營業比重表：塑膠射出成型機 94.61%。所營業務自述「主要經營業務包含智慧射出產線整廠規劃、車用零組件精密射出機、半導體設備專用射出機、永續材料成型之研發設計、生產製造、銷售服務」，商品項目逐一為單射出、多射出各系列射出機——以成型製程指名其機械，屬本節點。★2026-09-08 標為 G6 正面母體，2026-10-07 G6 封版後掛入。另有纜線 2.61%、其他 2.78% 未掛', 'Direct'),
        ('銓寶', 'Core', 'PET吹瓶機', 'annual_report_table', '年報營業比重表：PET吹瓶機 85%。年報載其「吹瓶機設備可使用 100%的回收料(rPET)來生產寶特瓶與容器」，上中下游表以吹瓶機為主要產品、下游為飲料等使用寶特瓶之產業——以成型製程指名其機械，屬本節點。★吹瓶有既有工件（瓶胚），與 G5 的邊界未決，記於 G5；本列依 2026-09-08 裁定歸 G6。★2026-10-07 G6 封版後掛入。另有其他 2% 未掛。★Core 理由：PET 吹瓶機為主業，年報自述「為台灣PET吹瓶機產業的領導者」；精密元件(鑽夾頭、刀桿) 掛 G4-03，為創業初期延續之另一條產品線(R023①④)；85% 僅作佐證', 'Direct'),
    ],
    "G6-02": [
        ('皇將', 'Core', '單機設備 + 整線設備', 'annual_report_table', '年報所營業務自述「製藥、保健食品包裝單機設備及自動化包裝產線規劃整合之專業廠」，商品項目表的整線設備載「除了各式膠囊錠劑、藥液與藥粉等瓶裝生產線外」、單機設備載理瓶、數粒、旋蓋、貼標等包裝單機——以包裝製程指名其機械，屬本節點；兩線都另含軟膠設備（G6-04），單機設備並載為各單機「之生產及代」「理銷售」（原文跨行），代理買賣不屬本節點且全樹沒有承接節點，年報未拆分，依 R020 ②-MX，含未定組成的混合（MX-OPEN），不計比重。Core 理由：所營業務首段自述為包裝設備專業廠並經營自有品牌「CVC」，軟膠囊設備是「2023年4月」併購 Changsung 後才加入的產品線(R023①④)。', 'Direct', 'Granularity_Insufficient'),
        ('羅昇', 'Major', '設備及耗材類', 'annual_report_table', '年報營業比重表：設備及耗材類 31.63%。該線名稱僅見於比重表；依所營業務「提供多種捆包機設備、耗材以及相關的包裝堆棧解決方案」、財報轉投資資訊載子公司怡進主要營業為「捆包設備生產製造及銷售」、2026-05-29 法說簡報稱其為「外銷導向的捆包機領導品牌，75%的產品銷售於歐美市場」，判讀為怡進的捆包機線——捆包屬本節點定義具名的包裝製程。★該對應為判讀（2026-10-07 使用者裁定接受作為安置依據）：三份文件皆未逐字寫明該線即怡進。比重不計：該線另含耗材，內容與金額皆未揭露，指不出可具名之另一節點，故記 Granularity_Insufficient(R020①-S)。role 依 R026③ 預設 Major。怡進非獨立掛牌，屬 Direct(R020③)', 'Direct', 'Granularity_Insufficient'),
    ],
    "G6-03": [
        ('新麥', 'Core', '烤爐 + 攪拌機 + 發酵機 + 分割滾圓整型機 + 壓麵機 + 切片機', 'annual_report_table', '年報營業比重表六條合計 79.24%：烤爐 37.93、攪拌機 18.32、發酵機 11.22、分割滾圓整型機 6.20、壓麵機 3.84、切片機 1.73(R020④)。年報載「本公司主要經營商業用烘焙設備及廚房設備」，自述「本公司係屬食品機械產業之中游製造廠商」；產品說明以麵包製程逐項指名其機械（攪拌、分割滾圓、發酵、烤焙）——屬本節點。★商用烘焙設備屬本節點為 2026-10-07 裁定。★未計入：餐飲設備 5.49、冰箱 1.12、展示櫃 0.67、器具 2.51（非製程設備）；油炸機 0.10、其他機器 1.23（內容未載）；其他零件 9.64（含勞務收入）', 'Direct'),
    ],
    "G6-04": [
        ('皇將', 'Major', '單機設備 + 整線設備', 'annual_report_table', '年報商品項目表的整線設備載「Changsung 公司軟膠囊生產線從明膠烹煮、藥液攪拌混料」，單機設備載「依照客戶需求提供各式軟膠設備包括軟膠囊成形-充填-封合製造機」，並載 Changsung「生產設備研發生製造,是第一大軟膠囊生產線公司」——以軟膠囊劑型製造製程指名其機械，屬本節點；兩線都另含包裝設備（G6-02），單機設備另含代理銷售的未定組成，年報未拆分，依 R020 ②-MX，含未定組成的混合（MX-OPEN），不計比重；role 依 R026 預設 Major。', 'Direct', 'Granularity_Insufficient'),
    ],
    "I8-01": [
        ('科風', 'Core', 'UPS', 'annual_report_table', '年報營業比重表：UPS 98.37%。年報載「科風股份有限公司負責研發、製造與行銷不斷電系統產品」，商品項目為在線式、在線互動式、機能式 UPS 與電源設備配件——屬本節點。★2026-10-07 I8-01 改寫為定義後掛入。另有其他 1.63% 未掛', 'Direct'),
        ('旭隼', 'Core', 'On-LineUPS + Off-LineUPS', 'annual_report_table', '年報營業比重表：On-Line UPS 36.96%、Off-Line UPS 25.79%，兩條合計 62.75%(R020④)。年報載「本公司主要業務為提供不斷電系統(Uninterruptible Power System,簡稱UPS)、太陽能逆變器(PV Inverter)等之專業DMS(Design & Manufacturing Service)設計生產製造服務廠商」，「主要業務為替世界各國品牌客戶進行設計製造」並「堅持不做自有品牌」——自行設計並製造不斷電系統，屬本節點；其以代客設計製造方式出貨，營收線仍依產品別揭露，本列依產品歸屬，未歸 V7-01。★2026-10-07 I8-01 改寫為定義後掛入。另有 Inverter/PV Inverter 28.90%（逆變器歸屬未裁）、其他產品 8.35% 未掛', 'Direct'),
        ('盈正', 'Core', '不斷電系統(簡稱UPS) + 主動式電力濾波器(簡稱APF)', 'annual_report_table', '年報營業比重表：不斷電系統 33.52%、主動式電力濾波器 1.72%，兩條合計 35.24%(R020④)。所營業務首列「不斷電系統設備之製造及銷售」「主動式電力濾波器之製造及銷售」，商品項目載「自行研發、製造及銷售」單相與三相不斷電系統設備及主動式電力濾波器——屬本節點。★Core 理由：所營業務前三項與商品項目皆為自行研發製造，計畫開發之新產品全為自有不斷電、電力品質與電力轉換技術——核心技術與研發指向本節點(R023①④)；另一候選 V9（專案工程 42.86%）係代理他廠大容量不斷電系統以工程方式銷售，營收較大而非自有技術所在，依 R023 不以最大營收線定 Core。★另有太陽能電力轉換器 1.34%（逆變器歸屬未裁）、勞務收入 4.64%、其他 15.92% 未掛', 'Direct'),
        ('勝德', 'Core', '資訊週邊防雷擊電源插座 + 視聽音響電源濾波器', 'annual_report_table', '年報營業比重表：資訊週邊防雷擊電源插座 69.30%、視聽音響電源濾波器 20.34%。自述「本公司係提供各種電子電路、先進、安全、專業之電源保護技術的市場領導廠商」（印刷頁 p64）；重要用途表載兩者皆為使產品「免受自然因素或人為因素所產生之不穩電壓（雷擊突波）或電磁波干擾」之電源保護裝置（印刷頁 p73，該表稱後者為視聽音響電源處理器）；產業鏈載本公司「位居中游的產品研發、製造及買賣廠商」（印刷頁 p66）——自製之插接式電源保護設備，屬本節點（2026-10-08 增訂條款）。單一候選節點(R019 分支 1)。★未掛：物聯網智慧家庭用電安全產品 5.54%（無線監控家用／商用電源設備，未判定）、其他 4.82%。PDU 列於商品項目與計畫開發之新商品，尚非獨立營收線', 'Direct'),
        ('歐格', 'Major', '電源及雲端智慧管理裝置', 'annual_report_table', '年報營業比重表：電源及雲端智慧管理裝置 19.99%。商品項目載該線「係包括電子保護插接器(Surge Protectors)、機櫃電源分配裝置(Rackmount PDU)、旅行用多國插頭轉接器/變壓器/USB充電器(Travel Adapter/Converter/USB Charger)、智能電源管理系統(Smart Power System)及不斷電電源供應器(Uninterruptible Power Supply)等」（印刷頁 p57）；產製過程載本線自備料發料、半成品加工至成品組裝、性能檢測與燒機（印刷頁 p66）——自製之插接式電源保護與配電設備，屬本節點（2026-10-08 增訂條款）。比重不計：同一條線另含旅行轉接頭與 USB 充電器，年報未拆分，且指不出可具名之另一節點，故記 Granularity_Insufficient(R020①-S)。role 依 R026③ 預設 Major。★Core Pending(R019)：較大之博弈機觸控螢幕及周邊設備 46.77% 與電路保護元件 33.24% 皆未判定節點（2026-10-08 裁定博弈機一線不在本次處理），Core 無從決定，不建 Core 列', 'Direct', 'Granularity_Insufficient'),
    ],
    "V9": [
        ('盈正', 'Major', '專案工程', 'annual_report_table', '年報營業比重表：專案工程 42.86%。所營業務第(4)項載「代理大容量不斷電系統以專案工程方式銷售及服務」，商品項目載「代理銷售歐洲SOCOMEC品牌之三相15KVA(含)以上之中大型不斷電系統設備」——電力設備之代理與隨同之專案工程，屬本節點(R022)。該營收線與所營業務第(4)項的對應，係依五項業務與營收線逐一對照所判讀，年報未另說明專案工程之內容。★2026-10-07 V9 增設後掛入；Core 在 I8-01，理由見該列', 'Direct'),
        ('所羅門', 'Major', '機電事業群', 'annual_report_table', '年報營業比重表：機電事業群（不斷電系統）40%。商品項目列 Cummins 發電機、Piller 與伊頓不斷電設備、Siemens 匯流排、Jenbacher 燃氣內燃機發電機組等及其「設計、規劃、整合、安裝工程、零件買賣及維修保養服務」；產製過程表載「(1)代理業務，無產製過程」（印刷頁 p112）——電力設備之代理與隨同之工程維修，屬本節點(R022)。比重不計：同線另含自有品牌發電機組（同表「(2)自有品牌」，由子公司盈門採購引擎、電頭、控制器後組裝出貨，屬 I6-01 發電設備一側），年報未拆分(R020②)。role 依 R026③ 預設 Major。★Core Pending(R019 分支 3，2026-10-07 裁定)：候選為本節點與 V4（智動化業群 29%）。營收最大者為本線；而年報所載研發為「持續深化 AI 視覺與智慧化系統研發」（印刷頁 p2），係智動化業群內之自有產品、不屬任一候選的代理業務；年報無主業自述，無法唯一判定，不建 Core 列。★未掛：光電製造業 21%、電子通路業 8%、其他電子業 2%。見 TAXONOMY_GAPS', 'Direct', 'Too_Coarse'),
        ('羅昇', 'Major', '能源管理類產品', 'annual_report_table', '年報營業比重表：能源管理類產品 12.64%。所營業務載「於歐洲地區以自有品牌「PowerWalker」為個人電子產品、企業 IT 設備、工業製造及資料中心提供不斷電系統為核心之電源保護方案與週邊產品銷售及服務」，財報轉投資資訊載 BlueWalker GmbH 主要營業為「能源管理產品之買賣及服務」，主要原料供應表該類之供應商為 Eaton、Voltronic Power、CyberPower 等（印刷頁 p61）——外購之不斷電系統與電力設備之買賣，屬本節點(R022)。比重不計：同線另含子公司達昇之能源技術服務（節能工程），年報未拆分，該業務無可具名之節點，故記 Granularity_Insufficient(R020①-S)。role 依 R026③ 預設 Major；年報載「羅昇將專注於智動化、綠能、半導體三大領域」，綠能為公司具名的三大發展領域之一，無依據認定其為附屬業務。整條線 12.64% 不作為 role 依據。★2026-10-07 V9 增設後掛入', 'Direct', 'Granularity_Insufficient'),
    ],
    "F3-01": [
        ('宏致', 'Major', '連接線組', 'annual_report_table', '年報營業比重表：連接線組 16.88%，品項為「極細同軸線連接線組」，F3-01 定義明列 Cable、Coaxial。', 'Direct'),
        ('聯穎', 'Core', '訊號傳輸線及線組', 'annual_report_table', '年報商品項目載明「以銷售訊號傳輸線及線組為主」，所營業務含「CC01020 電線及電纜製造業」，占 63.97%。', 'Direct'),
        ('良維', 'Core', '各類電源傳輸線組 + 訊號傳輸線組 + 電源、訊號傳輸及車用線材', 'annual_report_table', '年報營業比重表：各類電源傳輸線組 91.00%、訊號傳輸線組 6.00%、電源、訊號傳輸及車用線材 1.00%，合計 98.00%；年報自述「專注於傳統的 AC 電源傳輸線組、DC 訊號傳輸線組」', 'Direct'),
    ],
    "F6-01": [
        ('新普', 'Core', '鋰電池組', 'annual_report_table', '年報營業比重表「鋰電池組 79,342,442 96.58%」，商品項目為「筆記型電腦用電池組」「企業型伺服器電池組」等各式電池組，屬電池組組裝，同時涵蓋 BBU 與非伺服器用途，不入 C5-01。', 'Direct'),
        ('新盛力', 'Core', '鋰離子電池模組', 'annual_report_table', '年報營業比重表「鋰離子電池模組」114 年占 100%，商品項目為鋰離子電池模組，應用「不斷電電源備援系統;電動手工具;輕型電動車等。」，屬電池組裝，未限定為伺服器電源，不入 C5-01。', 'Direct'),
        ('順達', 'Core', '鋰電池組', 'annual_report_table', '年報自述「本公司屬中游之電池組組裝」，營收 99.8% 為鋰電池組。', 'Direct'),
        ('AES-KY', 'Core', '鋰電池模組', 'annual_report_table', '年報營業比重表：鋰電池模組 99.7%；年報自述「主要從事鋰電池模組之研發、生產和銷售」，產品含輕型電動載具與「中大型工業儲能系統電池模組」。', 'Direct'),
    ],
    "F1-02": [
        ('泰藝', 'Core', '石英振盪器 + 石英振盪子', 'annual_report_table', '年報營業比重表兩條合計 84.85%：石英振盪器 64.12、石英振盪子 20.73(R020④)；商品項目為「石英晶體 (Crystal Unit)」與「一般及精密級石英振盪器 (Oscillator)」等頻率控制元件。', 'Direct'),
        ('安碁', 'Core', 'SMD石英元件', 'annual_report_table', '年報營業比重表：SMD 石英元件 93.64%；所營業務為「石英晶體、石英振盪器等產品及半成品之製造加工及內外銷」，屬頻率元件。比重表另列「減:銷貨退回及折讓」(0.15)%，原表合計 100.00%，93.64% 係以營業收入淨額 611,013 仟元為分母', 'Direct'),
        ('晶技', 'Core', None, 'annual_report_prose', '年報無比重表；產品為石英晶體諧振器與石英振盪器(XO/TCXO/OCXO)，並持續深化「高階石英晶體、晶振與客制化頻率元件」。', 'Direct', 'Not_Disclosed'),
        ('希華', 'Core', '石英元件', 'annual_report_table', '年報營業比重表：石英元件 100.00%；商品包括「石英晶體及石英振盪器」，屬頻率被動元件，非 A1-11 半導體製程用石英材料。', 'Direct'),
    ],
    "I7-01": [
        ('富驊', 'Major', '電源供應器', 'annual_report_table', '年報營業比重表列「電源供應器 2,146,200 52% 2,315,375 42%」（114 年度 42%），商品項目「(1)電腦機殼、伺服器機殼及電源供應器。」，線名即 I7-01 定義所列的一般電源供應器；dossier 內沒有 CRPS、BBU、Power Shelf 等伺服器／資料中心電源產品，也沒有對應到本線的揭露，無 C5-01 歸屬證據，依 I7-01 現行定義維持。', 'Direct'),
        ('迅德', 'Minor', '電源供應器', 'annual_report_table', '年報營業比重表電源供應器 5%（「電源供應器 54,174 5%」），屬 I7-01 定義的一般電源供應器，年報未限定為伺服器電源。', 'Direct'),
        ('博大', 'Core', '電源供應器', 'annual_report_table', '年報營業比重表電源供應器 98.34%，所營業務「各種交直流電源供應器、轉換器、電信器材及其零組件之製造、加工、買賣。」，屬 I7-01 定義的 AC-DC／DC-DC 電源供應器與轉換器。', 'Direct'),
        ('全漢', 'Core', '電源供應器 + 外接式電源 + 基板電源', 'annual_report_table', '年報營業比重表電源供應器 59.75%、外接式電源 22.92%、基板電源 9.60% 合計 92.27%，年報自述「電源供應器製造加工買賣業務。」，三線皆為 I7-01 定義列舉的一般電源／外接式電源／基板電源；年報未將營收線限定為伺服器電源，依規則 10 整條掛 I7-01。', 'Direct'),
        ('新巨', 'Major', '交換式電源供應器', 'annual_report_table', '年報營業比重表交換式電源供應器 46.88%，年報產品敘述含「研發泛用型具易裝配特性之端子台式工業規格交換式電源供應器。」，未限定為伺服器電源，依規則 10 掛 I7-01。', 'Direct'),
    ],
    "C9-01": [
        ('普安', 'Core', None, 'annual_report_prose', '年報商品項目為「SAN儲存系統」、「NAS儲存系統」等儲存系統，所營業務敘述末載明「營業比重100%。」', 'Direct', 'Too_Coarse'),
    ],
    "F4-02": [
        ('富世達', 'Core', '折疊式手機轉軸 + 筆記型電腦轉軸', 'annual_report_table', '年報營業比重表「折疊式手機轉軸 6,759,480 82.56 7,003,932 56.42」與「筆記型電腦轉軸 894,738 10.93 795,699 6.41」，兩線皆為轉軸（樞紐）產品，同屬 F4-02 且角色相同，年報自述兩大核心業務之一為「轉軸」。', 'Direct'),
        ('兆利', 'Core', '樞紐', 'annual_report_table', '年報營業比重表「樞紐 9,280,084 96.89 8,796,124 93.52」，年報自述「本公司係以研發、生產及銷售各類樞紐組件為主」，為單一主力產品線。Core 理由：營收占比最高（93.52%），年報自述「本公司係以研發、生產及銷售各類樞紐組件為主,佔本公司營收比重約達94%」。', 'Direct'),
        ('新日興', 'Core', '樞紐產品', 'annual_report_table', '年報營業比重表「樞紐產品 12,845,711 96% 10,261,917 96%」，年報自述主要業務為各類樞紐製件，應用於筆記型電腦、LCD 顯示器、摺疊手機與穿戴裝置，為單一主力產品線。', 'Direct'),
        ('信錦', 'Core', '底 座 產 品', 'annual_report_table', '年報營業比重表「底 座 產 品 9,380,206 99.75 8,650,088 99.66」，年報自述服務項目為液晶監視器底座、液晶電視底座、AIO 電腦底座之設計與製造，為單一主力產品線。', 'Direct'),
    ],
    "C16-01": [
        ('虹堡', 'Core', '電子金融交易終端機', 'annual_report_table', '營業比重列「電子金融交易終端機」占 83.27%，年報自述「本公司主要營業項目為電子金融交易終端機(POS)及相關產品之設計、開發、」。', 'Direct'),
    ],
    "L5": [
        ('立敦', 'Core', '化成鋁箔 + 電蝕鋁箔', 'annual_report_table', '年報營業比重表「化成鋁箔 2,740,812 65.77」與「電蝕鋁箔 1,315,463 31.57」，兩線皆為電容用鋁箔、同屬 L5 且角色相同，年報自述「立敦科技係以產銷電蝕鋁箔及化成鋁箔為主要業務」。', 'Direct'),
    ],
    "C4-01": [
        ('富驊', 'Core', '電腦、伺服器機殼及其零組件', 'annual_report_table', '營業比重列「電腦、伺服器機殼及其零組件」，線名並列電腦機殼與伺服器機殼；年報自述「本公司及子公司主要業務為電腦及伺服器機殼之產銷」，伺服器機殼系列如「(1) 經濟型伺服器系列機殼(Entry Level Server Chassis)」屬 C4-01 定義的伺服器機殼／機箱及機構配件，年報未拆分，依 R020 ②-MX，完整混合（MX-FULL），本列不計比重。Core 理由：年報自述主業是電腦及伺服器機殼產銷，競爭情形自列「我國伺服器機殼廠商中,以勤誠、晟銘、富驊等為主要參與者」，計劃開發新商品也是伺服器機殼（「(2)AI伺服器機殼及水冷模組固定結構。」），產業定位以伺服器機殼為主。', 'Direct', 'Too_Coarse'),
        ('晟銘電', 'Major', '電腦及伺服器機殼', 'annual_report_table', '營業比重表列「電腦及伺服器機殼 10,309,421 98%」，線名同時列出電腦機殼與伺服器機殼；商品項目「伺服器機殼及個人電腦機殼及液冷機櫃之設計、製造與銷售。」，其中伺服器機殼屬 C4-01 定義的伺服器機殼／機箱，年報未拆分，依 R020 ②-MX，完整混合（MX-FULL），本列不計比重。', 'Direct', 'Too_Coarse'),
        ('營邦', 'Core', '伺服器機箱(ESG)', 'annual_report_table', '營業比重表列「伺服器機箱(ESG)」，商品項目列舉「RMC ( Rackmount Chassis )」、「RSC ( Rackmount Storage Chassis )」與「Chassis Accessory」，明確限定為伺服器／儲存機箱及機構配件，屬 C4-01 定義。', 'Direct'),
    ],
}
