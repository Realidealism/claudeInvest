"""Canonical theme dictionary — T01..T55, the narrative dimension.

Themes are what the market trades on a name for, and they do not line up with
the industry tree: HBM reaches DRAM, packaging, substrate, equipment and
materials at once, which is why tw.company_theme_mapping references this table
and never tw.industry_tree.

The T-codes come from 台股產業分類_Master_V3_7 and are stable identifiers; a
finer name the dictionary has no code for is carried as sub_theme on the
mapping row instead of being given a new code here (CoWoS under T08).

Sync with: python -m db.seed_theme_master
"""

# (theme_code, theme_group, theme_name, sub_theme_coverage, related_industries)
THEMES = [
    ('T01', 'AI 運算 / 基礎設施', 'AI / HPC', 'GPU、CPU、NPU、加速運算、先進製程', 'A、C'),
    ('T02', 'AI 運算 / 基礎設施', 'AI ASIC', 'ASIC Design、Design Service、IP、CoWoS、HBM、SerDes、載板、PCB、電源、散熱、Testing', 'A、B、C（智原/創意/世芯延伸整鏈）'),
    ('T03', 'AI 運算 / 基礎設施', 'AI Server', 'Server ODM、主機板、電源、散熱、連接器、機櫃', 'C'),
    ('T04', 'AI 運算 / 基礎設施', 'AI Data Center', 'Server、Rack、Power、UPS、Transformer、Switchgear、PDU、Cooling、CDU、Thermal、Facility', 'C、I、U'),
    ('T05', 'AI 運算 / 基礎設施', 'AI Power', 'Transformer、Switchgear、UPS、PDU、Power Supply、HVDC、Grid Upgrade、Energy Storage、Power Mgmt', 'I、C'),
    ('T06', 'AI 運算 / 基礎設施', 'Liquid Cooling', 'Cold Plate、CDU、Pump、Manifold、Heat Exchanger、TIM、Immersion Cooling', 'C、F'),
    ('T07', '記憶體 / 封裝 / 互連', 'HBM', 'DRAM、TSV、Wafer Thinning、Die、Interposer、CoWoS、Packaging、Testing、Probe、Thermal、Substrate', 'A'),
    ('T08', '記憶體 / 封裝 / 互連', 'Advanced Packaging', 'CoWoS、SoIC、2.5D/3D、Hybrid Bonding、Fan-Out、RDL、TSV', 'A5'),
    ('T09', '記憶體 / 封裝 / 互連', 'Chiplet / UCIe', 'Chiplet、UCIe、2.5D、3D IC、Hybrid Bonding', 'A4、A5'),
    ('T10', '記憶體 / 封裝 / 互連', 'CXL', 'CXL Memory、Memory Expansion、Memory Pooling、CXL Switch', 'A（Memory Controller）、C'),
    ('T11', '記憶體 / 封裝 / 互連', 'High-Speed Interconnect / SerDes', 'PCIe 5.0/6.0、CXL、SerDes、Retimer、Redriver、112G/224G、NVLink、800G/1.6T', 'A4、D、F（連接器）'),
    ('T12', '記憶體 / 封裝 / 互連', 'CPO / Silicon Photonics', 'Silicon Photonics、EML、VCSEL、Optical Engine、DSP、TIA、Optical Transceiver', 'D6、A'),
    ('T13', '通訊 / 衛星', '5G / 6G', '5G、6G、Private Network、Open RAN、mmWave、RF', 'D3、A（RF）'),
    ('T14', '通訊 / 衛星', 'Satellite / LEO', 'LEO、衛星通訊、Ground Station、RF、Antenna、Phased Array、衛星元件', 'S4、D'),
    ('T15', '通訊 / 衛星', 'Networking', 'Switch、Router、NIC、PHY、乙太網路', 'D1、A（網通IC）'),
    ('T16', '通訊 / 衛星', 'Optical Communication', '光纖、光模組、光收發、EML、DML、VCSEL、DSP、Silicon Photonics', 'D6'),
    ('T17', '機器人 / 國防航太', 'Robotics', '工業/協作機器人、AMR、AGV、Service/Medical Robot；零件 Motor/Servo/Reducer/Encoder/Controller/Sensor/Actuator', 'G3、F'),
    ('T18', '機器人 / 國防航太', 'Humanoid', '人形機器人、Actuator、Reducer、Sensor', 'G3'),
    ('T19', '機器人 / 國防航太', 'Automation', 'PLC、IPC、Motion Control、Servo、Vision', 'G1'),
    ('T20', '機器人 / 國防航太', 'UAV / Drone', 'Drone、軍用/商用無人機、Drone Motor、Flight Controller、Camera、Sensor、Communication、Battery', 'S2、F'),
    ('T21', '機器人 / 國防航太', 'Defense', 'Missile、Radar、電戰(EW)、UAV、Satellite、Avionics、軍用電子', 'S3'),
    ('T22', '機器人 / 國防航太', 'Aerospace', '飛機零件、航太材料、引擎零件', 'S1'),
    ('T23', '電動車 / 電池 / 能源半導體', 'EV', 'Battery、BMS、Inverter、OBC、Motor、ADAS、Charging', 'H'),
    ('T24', '電動車 / 電池 / 能源半導體', 'Battery', 'Cathode、Anode、Electrolyte、Separator、Copper Foil、Cell、Module、Pack、BMS、Recycling', 'H5、F、J'),
    ('T25', '電動車 / 電池 / 能源半導體', 'SiC / GaN (Power Semi)', 'SiC、GaN、MOSFET、IGBT、Power IC（應用另 Tag：EV/Solar/儲能/AI DC/Industrial）', 'A3'),
    ('T26', '電動車 / 電池 / 能源半導體', 'Charging', '充電樁、充電營運、OBC', 'I4、H'),
    ('T27', '電動車 / 電池 / 能源半導體', 'Energy Storage', 'Battery Cell、PCS、EMS、ESS、Container', 'I3'),
    # hint was 'E3' until 2026-09-07, when that node was removed; the theme is
    # where the technology lives now, so it points at the branch instead.
    ('T28', '消費電子 / 顯示 / Edge', 'Micro LED', 'LED Chip、Transfer、Driver IC、Backplane、Display、Repair', 'E'),
    ('T29', '消費電子 / 顯示 / Edge', 'XR', 'AR、VR、MR、Micro OLED、Micro LED、Pancake Lens、Optical Waveguide、Eye Tracking、Motion Sensor', 'E、A（Sensor）'),
    ('T30', '消費電子 / 顯示 / Edge', 'AI Smartphone', 'AI 手機、影像、NPU', 'E、A'),
    ('T31', '消費電子 / 顯示 / Edge', 'AI PC', 'AI PC、NPU', 'C10、A'),
    ('T32', '消費電子 / 顯示 / Edge', 'Edge AI', 'AI PC、AI Smartphone、AI Camera、Industrial AI、Edge Computing、NPU、AIoT', 'A4、C'),
    ('T33', '軟體 / 雲 / 金融科技', 'Cybersecurity', 'Network/Endpoint/Cloud Security、Identity、Zero Trust、OT Security', 'U5'),
    ('T34', '軟體 / 雲 / 金融科技', 'Cloud', 'Cloud Service、Infrastructure、Colocation、IDC', 'U3、U4'),
    ('T35', '軟體 / 雲 / 金融科技', 'AI Software', 'Generative AI、AI Agent、LLM、AI SaaS（熱門名詞歸此，不各自獨立）', 'U6'),
    ('T36', '軟體 / 雲 / 金融科技', 'FinTech', 'Digital Payment、E-payment、Credit Card、Digital Banking、Financial AI', 'W'),
    ('T37', '綠能 / 電網 / 電力設備', 'Renewable Energy', '太陽能、風電、水力', 'I1、I2'),
    ('T38', '綠能 / 電網 / 電力設備', 'Grid', '輸配電、智慧電網、Grid Upgrade', 'I5'),
    ('T39', '綠能 / 電網 / 電力設備', 'HVDC', 'HVDC、DC Power、DC Distribution、Data Center Power', 'I、C'),
    ('T40', '綠能 / 電網 / 電力設備', 'Power Equipment', 'Transformer、Switchgear、電線電纜、重電、發電設備', 'I6'),
    ('T41', '材料 / 散熱 / 循環', 'Advanced Materials', 'Low Dk、Low Loss、Low CTE、High Thermal、High Frequency、High Temp、Lightweight', 'B、A、H'),
    ('T42', '材料 / 散熱 / 循環', 'High Frequency / Low Loss', '高頻高速 CCL、Low Loss 材料、玻纖、樹脂、銅箔', 'B1'),
    ('T43', '材料 / 散熱 / 循環', 'Thermal', '散熱、均熱板、熱管、TIM、Cold Plate', 'C6、F'),
    ('T44', '材料 / 散熱 / 循環', 'Recycling / Circular Economy', '電池回收、資源循環、廢棄物處理', 'Z5、H'),
    ('T45', '供應鏈重組', 'Supply Chain Localization', 'Taiwan / 半導體 / 國防在地化', '跨產業'),
    ('T46', '供應鏈重組', 'China+1', '產能轉移、東南亞 / 印度 / 墨西哥', '跨產業'),
    ('T47', '供應鏈重組', 'US Localization', '美國設廠、在地生產', '跨產業'),
    ('T48', 'Macro / 總經', 'Interest Rate', '升息、降息、利率', 'W、Q'),
    ('T49', 'Macro / 總經', 'FX', '匯率、美元、人民幣、台幣', '跨（出口/進口）'),
    ('T50', 'Macro / 總經', 'Commodity', '原物料價格', 'J、L'),
    ('T51', 'Macro / 總經', 'Oil', '油價、原油、天然氣、煤', 'J、Z、R（油輪）'),
    ('T52', 'Macro / 總經', 'Steel', '鋼價、鐵礦', 'L'),
    ('T53', 'Macro / 總經', 'Copper', '銅價', 'L4、F、I（電網/AI）'),
    ('T54', 'Macro / 總經', 'Freight', 'SCFI、BDI、ECI、運價', 'R'),
    ('T55', 'Macro / 總經', 'US Tariff / Trade War', '美中關稅、貿易戰、地緣政治', '跨產業'),
]
