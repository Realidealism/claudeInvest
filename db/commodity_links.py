"""大宗行情 ⇄ 個股 對照表.

Which listed companies each commodity actually touches, and on which side of
their income statement. The side is the whole point: 熱軋鋼捲 rising is good
for 中鋼 (it sells the stuff) and bad for 燁輝 (it buys the stuff), and no
correlation coefficient can express that — it would give both the same sign.

Roles:
  cost       商品漲 → 利空。這個商品是它買進來的原料
  sell       商品漲 → 利多。這個商品就是它賣出去的東西
  inventory  商品漲 → 利多，但理由是庫存評價利益與轉嫁能力，不是自產。
             台股的記憶體模組廠是典型：結構上 DRAM 是它的成本，實務上它
             是現貨價的多頭代理，標成利空會是錯的。

Deliberately NOT mapped, because the transmission is too indirect to be
anything but noise on a card: 貴金屬、鉛/錫/鋁/鈷、匯率、比特幣、天然氣、
稻米。Absence here is a decision, not an omission.

Stock ids only — names are joined from tw.stocks at export time so a rename
never has to be chased through this file.
"""

from __future__ import annotations

COST      = "cost"
SELL      = "sell"
INVENTORY = "inventory"

ROLE_LABEL = {COST: "成本端", SELL: "售價端", INVENTORY: "庫存端"}

# 商品漲對這檔個股的方向 (+1 利多 / -1 利空)。前端照台股慣例上紅下綠。
ROLE_SIGN = {COST: -1, SELL: 1, INVENTORY: 1}

# DRAM 的三檔報價 (DDR5 / DDR5 eTT / DDR4) 對應同一組公司,
# 定義一次而不是抄三份。
_DRAM = [
    ("2408", SELL,      "自有 DRAM 產能"),
    ("2344", SELL,      "自有 DRAM 產能"),
    ("6770", SELL,      "自有 DRAM 產能"),
    ("2451", INVENTORY, "買顆粒做模組,漲價循環吃庫存評價利益"),
    ("3260", INVENTORY, "買顆粒做模組,漲價循環吃庫存評價利益"),
    ("8271", INVENTORY, "買顆粒做模組,漲價循環吃庫存評價利益"),
    ("4967", INVENTORY, "買顆粒做模組,漲價循環吃庫存評價利益"),
    ("4973", INVENTORY, "買顆粒做模組,漲價循環吃庫存評價利益"),
]

_CRUDE = [
    ("2610", COST, "航空燃油"),
    ("2618", COST, "航空燃油"),
    ("2646", COST, "航空燃油"),
    ("6505", COST, "原油是煉製原料;但急漲時另有庫存利益,方向不純"),
]

_DRY_BULK = [
    ("2606", SELL, "散裝運價"),
    ("2605", SELL, "散裝運價"),
    ("2617", SELL, "散裝運價"),
    ("2637", SELL, "散裝運價"),
]

# symbol (與 market_quote.SYMBOLS / market_html.HTML_SYMBOLS 同鍵)
#   -> [(stock_id, role, 一句話理由)]
LINKS: dict[str, list[tuple[str, str, str]]] = {
    # ---- 鋼鐵 ----
    "iron_ore": [("2002", COST, "高爐煉鋼主原料")],
    "coking_coal": [
        ("2002", COST, "高爐煉鋼主原料"),
        ("1723", COST, "煤化工;原料是中鋼煉焦副產品,非直接採購焦煤"),
    ],
    "hrc": [
        ("2002", SELL, "熱軋是主力產品"),
        ("2023", COST, "冷軋/鍍鋅以熱軋為原料"),
        ("2029", COST, "冷軋/鍍鋅以熱軋為原料"),
        ("2014", COST, "冷軋/鍍鋅以熱軋為原料"),
    ],
    "rebar": [
        ("2015", SELL, "電爐鋼筋;中國螺紋鋼當台灣盤價代理"),
        ("2006", SELL, "電爐鋼筋;中國螺紋鋼當台灣盤價代理"),
    ],
    # ---- 石化上游 ----
    "naphtha": [
        ("6505", COST, "輕油裂解原料"),
        ("1301", COST, "輕油裂解原料"),
        ("1326", COST, "輕油裂解原料"),
        ("1304", COST, "輕油裂解原料"),
        ("1312", COST, "輕油裂解原料"),
    ],
    "benzene": [
        ("1310", COST, "SM 原料(苯+乙烯)"),
        ("1312", COST, "SM 原料(苯+乙烯)"),
    ],
    "propylene": [
        ("1301", COST, "PP 原料"),
        ("1312", COST, "PP/AN 原料"),
    ],
    "xylene": [("1326", COST, "PX→PTA;惟來源可能是混合二甲苯而非 PX")],
    # ---- 塑化 ----
    "styrene": [
        ("1310", SELL, "SM 是主力產品"),
        ("1312", SELL, "SM 是主力產品"),
        ("1309", COST, "PS/ABS 以 SM 為原料"),
    ],
    "pvc": [
        ("1301", SELL, "PVC 主要生產者"),
        ("1305", SELL, "PVC 主要生產者"),
    ],
    "pp": [
        ("1301", SELL, "PP 產品端"),
        ("1326", SELL, "PP 產品端"),
        ("1312", SELL, "PP 產品端"),
    ],
    "hdpe": [("1301", SELL, "HDPE 產品端")],
    "ldpe": [
        ("1304", SELL, "PE 產品端"),
        ("1308", SELL, "PE 產品端"),
    ],
    "lldpe": [
        ("1304", SELL, "PE 產品端"),
        ("1308", SELL, "PE 產品端"),
    ],
    "pta": [
        ("1326", SELL, "PTA 主要生產者"),
        ("1402", SELL, "自產 PTA,但同時是聚酯下游,方向不純"),
        ("1409", COST, "聚酯纖維以 PTA 為原料"),
        ("1444", COST, "聚酯纖維以 PTA 為原料"),
    ],
    "eg": [
        ("1710", SELL, "EG 主要生產者"),
        ("1718", SELL, "EG 主要生產者"),
        ("1402", COST, "聚酯原料"),
        ("1409", COST, "聚酯原料"),
        ("1444", COST, "聚酯原料"),
    ],
    # ---- 橡膠 ----
    "rubber_nat": [
        ("2105", COST, "輪胎主原料"),
        ("2106", COST, "輪胎主原料"),
        ("2101", COST, "輪胎主原料"),
        ("2102", COST, "輪胎主原料"),
    ],
    "sbr": [
        ("2103", SELL, "SBR 主要生產者"),
        ("2105", COST, "輪胎原料"),
        ("2106", COST, "輪胎原料"),
    ],
    # ---- 新能源材料 ----
    "polysilicon": [
        ("6244", COST, "太陽能電池矽料"),
        ("6443", COST, "太陽能電池矽料"),
        ("3576", COST, "太陽能電池矽料"),
    ],
    "lithium": [
        ("4721", COST, "三元前驅體原料"),
        ("4739", COST, "三元前驅體原料"),
    ],
    # ---- 能源 ----
    "brent": _CRUDE,
    "wti": _CRUDE,
    "thermal_coal": [
        ("1101", COST, "水泥燒製燃料"),
        ("1102", COST, "水泥燒製燃料"),
    ],
    # ---- 農產 ----
    "soybean": [
        ("1210", COST, "飼料原料"),
        ("1215", COST, "飼料原料"),
        ("1219", COST, "飼料原料"),
        ("1232", COST, "黃豆壓榨"),
        ("1225", COST, "黃豆壓榨"),
    ],
    "corn": [
        ("1210", COST, "飼料原料"),
        ("1215", COST, "飼料原料"),
        ("1219", COST, "飼料原料"),
    ],
    "wheat": [("1229", COST, "麵粉")],
    "hog": [
        ("1210", SELL, "自有養豬與肉品;豬價高代表下游景氣、飼料需求強"),
        ("1219", SELL, "自有養豬與肉品;豬價高代表下游景氣、飼料需求強"),
    ],
    "urea": [("1722", SELL, "肥料主力產品")],
    # ---- 海運 ----
    "bdi": _DRY_BULK,
    "bdry": _DRY_BULK,
    "fbx": [
        ("2603", SELL, "貨櫃運價"),
        ("2609", SELL, "貨櫃運價"),
        ("2615", SELL, "貨櫃運價"),
    ],
    # ---- 記憶體 / 面板 ----
    "dram_ddr5": _DRAM,
    "dram_ddr5e": _DRAM,
    "dram_ddr4": _DRAM,
    "nand_mlc": [
        ("2337", SELL,      "自有 NAND 產能"),
        ("8299", INVENTORY, "控制 IC+模組,NAND 是採購料但漲價期吃庫存利益"),
        ("2451", INVENTORY, "SSD/記憶卡"),
        ("3260", INVENTORY, "SSD/記憶卡"),
        ("8271", INVENTORY, "SSD/記憶卡"),
        ("4967", INVENTORY, "SSD/記憶卡"),
        ("4973", INVENTORY, "SSD/記憶卡"),
        ("5289", INVENTORY, "工控儲存"),
    ],
    "panel_tv55": [
        ("2409", SELL, "面板產品端"),
        ("3481", SELL, "面板產品端"),
    ],
    # ---- 基本金屬 ----
    "copper": [
        ("1605", COST, "電線電纜主原料"),
        ("1608", COST, "電線電纜主原料"),
    ],
    "nickel": [
        ("2027", COST, "不鏽鋼"),
        ("2034", COST, "不鏽鋼管"),
        ("1605", COST, "不鏽鋼事業"),
    ],
    "zinc": [
        ("2029", COST, "鍍鋅鋼板"),
        ("2023", COST, "鍍鋅鋼板"),
    ],
}
