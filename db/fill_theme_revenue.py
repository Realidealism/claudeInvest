"""
Fill tw.stock_themes.revenue_pct from the annual-report product mix.

Matching is deliberately conservative: a theme is only credited when the report's
own product wording contains one of that theme's keywords. Everything else stays
NULL, because "we could not match it" and "this line is 0%" must not look alike —
柏騰's 碳化矽 0.4% is a real side line, while 台積電's 晶圓 100% is the whole
company, and an unmatched row is neither.

    python -m db.fill_theme_revenue [--dry-run]
"""

import argparse

from db.connection import get_cursor

# theme name -> wordings that appear in 年報「主要商品(服務)之營業比重」.
# Written from the wordings actually observed in the corpus, not from guesses:
# reports say 功率金氧半場效電晶體 rather than PMIC, 晶圓載具品 rather than 設備.
KEYWORDS = {
    # --- IC 設計 ---
    "電源管理IC": ["電源管理", "電源IC", "PMIC", "電源控制", "電源轉換", "功率IC",
                   "功率元件", "功率半導體", "金氧半", "充電管理", "類比IC"],
    "微控制器IC": ["微控制器", "MCU", "單晶片", "微處理器", "微電腦控制"],
    "記憶體IC": ["記憶體IC", "記憶體晶片", "DRAM", "快閃記憶體晶片", "記憶IC"],
    "記憶體控制IC": ["記憶體控制", "控制晶片", "控制IC", "NAND", "Flash控制"],
    "平面顯示器控制IC": ["驅動IC", "顯示驅動", "面板驅動", "時序控制", "TCON",
                        "液晶驅動", "顯示器驅動", "驅動器", "驅動晶片"],
    "觸控IC": ["觸控"],
    "音訊IC": ["音訊", "音效", "語音", "無線音頻"],
    "影像感測IC": ["影像感測", "CMOS影像", "CIS"],
    "影像處理IC": ["影像處理", "ISP", "影像視訊", "智慧影像"],
    "網路通訊IC": ["網通", "乙太網路", "網路晶片", "通訊IC", "高速通訊", "Wi-Fi", "WiFi"],
    "射頻IC": ["射頻", "微波放大", "微波次系統"],
    "光通訊IC": ["光通訊", "光收發", "光連結", "光纜"],
    "LED驅動IC": ["LED驅動"],
    "馬達驅動IC": ["馬達驅動", "馬達控制", "Motor"],
    "感測IC": ["感測器", "感測IC", "霍爾"],
    "多媒體SoC": ["多媒體", "SoC", "系統單晶片", "智慧應用IC"],
    "輸出入介面IC": ["介面IC", "USB", "週邊及控制IC", "周邊及控制IC", "傳輸介面",
                     "高速訊號", "高速通訊", "Retimer", "混合式IC"],
    "IP設計/IC設計代工服務": ["矽智財", "IP授權", "設計服務", "ASIC", "NRE",
                              "技術服務", "權利金", "技術權利"],
    # --- 晶圓製造 ---
    "晶圓代工/IDM": ["晶圓代工", "晶圓製造", "晶圓"],
    "DRAM製造": ["DRAM"],
    "化合物半導體製造": ["砷化鎵", "氮化鎵", "碳化矽", "化合物半導體", "GaAs", "GaN", "SiC",
                       "磊晶晶片", "晶粒", "光電轉換", "微波放大", "微波次系統"],
    "其他IC/二極體製造": ["二極體", "整流", "MOSFET", "TVS", "分立", "離散式",
                          "電晶體", "PPTC"],
    # --- 半導體材料 ---
    "化合物半導體材料": ["磊晶", "碳化矽", "砷化鎵", "GaAs", "GaN", "SiC"],
    "矽晶圓材料": ["矽晶圓", "晶圓材料", "再生晶圓", "長晶", "晶棒", "半導體晶片", "晶錠"],
    "光罩": ["光罩"],
    "化學品": ["化學品", "藥液", "光阻", "特用化學", "氣體", "研磨液", "特氣",
               "電子化學", "電子材料", "溶劑", "精密化學", "錫膏", "錫球"],
    "導線架": ["導線架", "引線框"],
    "基板": ["基板"],
    "封裝材料": ["封裝材料", "銀膠", "導電漿", "封膠"],
    # --- 封裝與測試 ---
    "IC封裝測試": ["封裝", "測試", "封測", "SiP", "混合積體電路", "半導體產品"],
    "測試介面": ["探針卡", "測試座", "測試介面", "測試板"],
    "檢測分析服務": ["分析", "檢測服務", "驗證"],
    # --- 半導體設備 / 廠務 ---
    # Equipment makers are usually pure plays, so their own generic wording
    # ("設備", "機械") is safe here; the same words stay out of the IC-design
    # themes, where "晶片 98%" would wrongly credit one product line with the
    # whole company.
    "前段製程設備": ["製程設備", "半導體設備", "晶圓設備", "真空", "石英",
                     "晶圓載具", "光罩載具", "濕製程", "半導體零組件",
                     "半導體相關產品", "機台設備", "半導體材料", "檢測設備",
                     "AOI", "自動化設備", "定位系統", "運動及控制"],
    "後段封測設備": ["封測設備", "測試設備", "封裝設備", "檢測設備", "半導體測試",
                     "分選機", "半導體製程設備", "自動化設備", "SEMI", "半導體設備",
                     "量測儀器", "光學檢測", "設備工程", "設備銷售", "機台設備",
                     "主製程設備"],
    "設備零組件再生": ["清洗", "再生", "剝鍍", "洗淨", "貴金屬材料"],
    "廠務工程": ["廠務", "無塵室", "機電", "管路", "配管", "工程收入",
                 "供應系統", "整合工程", "球閥", "氟素", "工程合約", "AMC",
                 "污染防治", "濾網"],
    # --- 通路與模組 ---
    "IC模組": ["模組", "DRAM產品", "DRAM記憶體", "Flash產品", "儲存事業", "記憶體產品"],
    "IC通路": ["通路", "代理", "半導體", "元件", "電子材料", "邏輯晶片",
               "類比積體電路", "混合信號", "積體電路"],
    # --- 印刷電路板 ---
    "硬板": ["印刷電路板", "PCB", "多層板", "雙面板", "硬板", "層板",
             # 相互 sells 軟硬複合板 (88%) and 新復興 高頻微波通訊基板 (90.7%);
             # without these the fill lands on their small conventional line
             "軟硬複合板", "高頻微波", "微波通訊基板"],
    "軟板": ["軟性印刷電路板", "軟板", "FPC", "軟性電路板"],
    "IC載板": ["載板", "封裝基板", "ABF", "BT"],
    "COF軟性載板": ["COF", "軟性載板", "捲帶"],
    "特殊材質基板": ["陶瓷基板", "金屬基板", "鋁基板", "特殊基板", "高頻"],
    "基板組裝加工及相關製造": ["PCBA", "EMS", "印刷電路板組裝", "組裝"],
    "銅箔基板": ["銅箔基板", "CCL"],
    "銅箔": ["銅箔"],
    "玻璃纖維/玻纖布": ["玻纖", "玻璃纖維"],
    "環氧與酚醛樹脂": ["環氧", "酚醛", "樹脂"],
    "聚亞醯胺樹脂": ["聚亞醯胺", "PI膜"],
    "軟性銅箔基板與覆蓋膜": ["軟性銅箔基板", "覆蓋膜", "FCCL", "保護膠片"],
    "生產製程及檢測設備": ["PCB設備", "印刷電路板", "AOI", "檢測設備", "鑽孔",
                           "曝光", "自動化設備", "檢測系列", "設備", "機械",
                           "製程設備", "檢測系統", "機台"],
    "製程耗材與加工服務": ["鑽頭", "耗材", "加工服務", "鑽孔代工", "加工收入"],
    "製程藥水": ["藥水", "藥液", "電鍍", "化學品"],
}


def fill(dry_run: bool = False) -> tuple[int, int]:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT st.stock_id, st.theme_id, t.name AS theme, p.item,
                   p.pct_effective AS pct, p.pct_is_derived
            FROM tw.stock_themes st
            JOIN tw.themes t ON t.theme_id = st.theme_id
            -- the view fills in the share for reports that printed amounts only
            JOIN tw.product_revenue_share p ON p.stock_id = st.stock_id
            WHERE p.pct_effective IS NOT NULL
            """)
        rows = [dict(r) for r in cur.fetchall()]

    # Keep the largest matching line per (stock, theme): a company can list the
    # same business twice (自製 / 代理) and the headline share is the bigger one.
    best: dict[tuple, dict] = {}
    for row in rows:
        for keyword in KEYWORDS.get(row["theme"], []):
            if keyword.lower() in row["item"].lower():
                key = (row["stock_id"], row["theme_id"])
                if key not in best or row["pct"] > best[key]["pct"]:
                    best[key] = row
                break

    if dry_run:
        return len(best), len(rows)

    with get_cursor() as cur:
        for (stock_id, theme_id), row in best.items():
            cur.execute(
                "UPDATE tw.stock_themes SET revenue_pct = %s, pct_item = %s, "
                "pct_source = %s WHERE stock_id = %s AND theme_id = %s",
                (row["pct"], row["item"][:60],
                 "annual_report_2025_derived" if row["pct_is_derived"] else "annual_report_2025",
                 stock_id, theme_id))
    return len(best), len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    matched, scanned = fill(args.dry_run)
    print(f"Matched {matched} (stock, theme) pairs from {scanned} product lines"
          + (" [dry run]" if args.dry_run else ""))


if __name__ == "__main__":
    main()
