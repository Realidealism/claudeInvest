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
                   "充電管理", "類比IC"],
    "功率分立元件IC": ["MOSFET", "金氧半", "功率半導體", "功率元件", "電晶體",
                       "離散式", "二極體"],
    "微控制器IC": ["微控制器", "MCU", "單晶片", "微處理器", "微電腦控制"],
    "平面顯示器控制IC": ["驅動IC", "顯示驅動", "面板驅動", "時序控制", "TCON",
                        "液晶驅動", "顯示器驅動", "驅動器", "驅動晶片", "人機介面"],
    "觸控IC": ["觸控"],
    "音訊IC": ["音訊", "音效", "語音", "無線音頻", "麥克風"],
    "影像感測IC": ["影像感測", "CMOS影像", "CIS", "生物辨識"],
    "影像處理IC": ["影像處理", "ISP", "影像視訊", "智慧影像"],
    "網路通訊IC": ["網通", "乙太網路", "網路晶片", "通訊IC", "高速通訊", "Wi-Fi", "WiFi",
                   "高速網路", "寬頻通訊", "通訊晶片"],
    "射頻IC": ["射頻", "微波放大", "微波次系統", "RFIC", "Wi-Fi 產品"],
    "光通訊IC": ["光通訊", "光收發", "光連結", "光纜"],
    "LED驅動IC": ["LED驅動"],
    "馬達驅動IC": ["馬達驅動", "馬達控制", "Motor"],
    "感測IC": ["感測器", "感測IC", "霍爾", "HALL"],
    "光源管理IC": ["感測晶片", "光感測", "環境光", "光源管理"],
    "多媒體SoC": ["多媒體", "SoC", "系統單晶片", "智慧應用IC"],
    "輸出入介面IC": ["介面IC", "USB", "週邊及控制IC", "周邊及控制IC", "傳輸介面",
                     "高速訊號", "高速通訊", "Retimer", "混合式IC", "高速介面", "ESD"],
    # --- 矽智財 ---
    # Split out of the old IP/ASIC cell: 力旺 books 技術權利金 while 創意 books
    # ASIC及晶圓產品, so the two business models never share a wording.
    "矽智財授權": ["矽智財", "IP授權", "智財授權", "權利金", "技術權利", "技術服務"],
    "ASIC設計服務": ["ASIC", "NRE", "委託設計", "設計服務"],
    # --- 晶圓製造 ---
    "晶圓代工": ["晶圓代工", "晶圓製造", "邏輯暨特殊應用", "晶圓"],
    "功率與分立元件IDM": ["二極體", "整流", "MOSFET", "TVS", "分立", "離散式",
                          "電晶體", "PPTC", "功率產品"],
    "影像感測製程": ["彩色濾光片", "微透鏡", "影像感測"],
    "化合物半導體製造": ["砷化鎵", "氮化鎵", "碳化矽", "化合物半導體", "GaAs", "GaN", "SiC",
                       "磊晶晶片", "晶粒", "光電轉換", "微波放大", "微波次系統"],
    # --- 記憶體 ---
    "DRAM製造": ["DRAM", "記憶體產品"],
    "Flash製造": ["Flash", "ROM", "快閃記憶體"],
    "記憶體IC設計": ["記憶體IC", "記憶體晶片", "DRAM", "快閃記憶體晶片", "記憶IC"],
    "記憶體控制IC": ["記憶體控制", "控制晶片", "控制IC", "NAND", "Flash控制"],
    "記憶體模組": ["記憶體模組", "DRAM產品", "DRAM記憶體", "Flash產品", "儲存事業",
                   "記憶體產品", "快閃記憶體", "記憶體事業"],
    # --- 半導體材料 ---
    "化合物半導體材料": ["磊晶", "碳化矽", "砷化鎵", "GaAs", "GaN", "SiC"],
    "矽晶圓材料": ["矽晶圓", "晶圓材料", "再生晶圓", "長晶", "晶棒", "半導體晶片", "晶錠"],
    "光罩": ["光罩"],
    # 年報全庫 2025 沒有任何一列寫「光阻」或「靶材」, so these two cells fill
    # from MANUAL_EVIDENCE, not from here — the words are kept for the day a
    # filing does spell them out.
    "光阻與微影材料": ["光阻", "顯影", "剝離液", "微影"],
    "濺鍍靶材": ["靶材", "濺鍍"],
    "半導體特用氣體": ["特氣", "特殊氣體", "電子級氣體", "製程氣體"],
    "濕製程化學品": ["精密化學", "電子化學", "溶劑", "製程用化學品", "藥液",
                     "特用化學", "電子化學材料"],
    "CMP材料": ["研磨墊", "研磨液", "鑽石碟", "拋光", "研磨"],
    "導線架": ["導線架", "引線框"],
    "陶瓷與封裝基板": ["基板"],
    "封裝材料": ["封裝材料", "銀膠", "導電漿", "封膠", "錫膏", "錫球"],
    # --- 封裝測試 / 先進封裝 ---
    "IC封裝測試": ["封裝", "測試", "封測", "SiP", "混合積體電路", "半導體產品"],
    "晶圓級封裝": ["晶圓級封裝", "晶圓級尺寸封裝", "晶圓封裝", "後護層"],
    "測試介面": ["探針卡", "測試座", "測試介面", "測試板"],
    "檢測分析服務": ["分析", "檢測服務", "驗證"],
    # --- 半導體設備 / 廠務 ---
    # Equipment makers are usually pure plays, so their own generic wording
    # ("設備", "機械") is safe here; the same words stay out of the IC-design
    # themes, where "晶片 98%" would wrongly credit one product line with the
    # whole company.
    "前段製程設備": ["製程設備", "半導體設備", "晶圓設備", "濕製程", "機台設備",
                     "半導體主設備", "影像檢測設備", "半導體相關產品"],
    "後段封測設備": ["封測設備", "測試設備", "封裝設備", "檢測設備", "半導體測試",
                     "分選機", "半導體製程設備", "SEMI", "半導體設備",
                     "量測儀器", "光學檢測", "設備工程", "設備銷售", "機台設備",
                     "鐳射設備", "半導體部門", "半導體類", "半導體光電轉換"],
    "先進封裝設備": ["主製程設備", "氣動與熱能", "半導體設備", "機台設備",
                     "半導體製程設備"],
    "晶圓傳送與自動化": ["晶圓載具", "光罩載具", "載具", "智能自動化", "自動化系統",
                         "半導體自動化", "自動化設備"],
    "設備精密零組件": ["零組件", "石英", "陶瓷", "真空", "精密模具", "定位系統",
                       "運動及控制", "材料類產品", "子系統", "設備技術服務"],
    "設備零組件再生": ["清洗", "再生", "剝鍍", "洗淨"],
    "廠務工程": ["廠務", "無塵室", "機電", "管路", "配管", "工程收入",
                 "供應系統", "整合工程", "球閥", "氟素", "工程合約", "AMC",
                 "污染防治", "濾網"],
    # --- 通路與模組 ---
    "IC模組": ["模組", "混合積體電路"],
    "IC通路": ["通路", "代理", "半導體", "元件", "電子材料", "邏輯晶片",
               "類比積體電路", "混合信號", "積體電路", "佣金"],
    # --- 電子基板 ---
    "硬板": ["印刷電路板", "PCB", "多層板", "雙面板", "硬板", "層板",
             # 相互 sells 軟硬複合板 (88%) and 新復興 高頻微波通訊基板 (90.7%);
             # without these the fill lands on their small conventional line
             "軟硬複合板", "高頻微波", "微波通訊基板"],
    "軟板": ["軟性印刷電路板", "軟板", "FPC", "軟性電路板"],
    "IC載板": ["載板", "封裝基板", "ABF", "BT"],
    "COF軟性載板": ["COF", "軟性載板", "捲帶"],
    "特殊材質基板": ["陶瓷基板", "陶瓷電路板", "金屬基板", "鋁基板", "特殊基板", "高頻"],
    "基板組裝加工": ["PCBA", "EMS", "印刷電路板組裝", "組裝"],
    "PCB設計與整合": ["拼板", "供應鏈整合", "品質管控"],
    "一般銅箔基板": ["銅箔基板", "CCL"],
    "高速高頻銅箔基板": ["銅箔基板", "CCL"],
    "電解與壓延銅箔": ["銅箔"],
    "玻璃纖維與玻纖布": ["玻纖", "玻璃纖維"],
    "環氧與酚醛樹脂": ["環氧", "酚醛", "樹脂"],
    "聚亞醯胺樹脂": ["聚亞醯胺", "PI膜"],
    "軟性銅箔基板與覆蓋膜": ["軟性銅箔基板", "覆蓋膜", "FCCL", "保護膠片"],
    "PCB生產製程及檢測設備": ["PCB設備", "印刷電路板", "AOI", "檢測設備", "鑽孔",
                              "曝光", "自動化設備", "檢測系列", "設備", "機械",
                              "製程設備", "檢測系統", "機台", "專業服務"],
    "PCB製程耗材與加工服務": ["鑽頭", "耗材", "加工服務", "鑽孔代工", "加工收入",
                              "磷銅球", "鑽針", "墊板", "代工收入"],
    "PCB製程藥水": ["藥水", "藥液", "電鍍", "化學品"],
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
        # fill() only ever UPDATEs, so a share left over from a wider version of
        # the dictionary would survive a tightening and no check would see it.
        cur.execute(
            "UPDATE tw.stock_themes SET revenue_pct = NULL, pct_item = NULL, "
            "pct_source = NULL WHERE pct_source LIKE 'annual_report%'")
        for (stock_id, theme_id), row in best.items():
            cur.execute(
                "UPDATE tw.stock_themes SET revenue_pct = %s, pct_item = %s, "
                "pct_source = %s WHERE stock_id = %s AND theme_id = %s",
                (row["pct"], row["item"][:60],
                 "annual_report_2025_derived" if row["pct_is_derived"] else "annual_report_2025",
                 stock_id, theme_id))
    return len(best), len(rows)


def apply_evidence() -> tuple[int, int]:
    """Stamp evidence_kind on every membership.

    A share found by the dictionary above is evidence in itself, so it is
    stamped 'annual_report_table'. Weaker sources come from MANUAL_EVIDENCE,
    where a person wrote down which one was used and why — the seed mirrors the
    file and would otherwise wash that away on the next sync.

    Everything left over is stamped too, rather than left NULL, because "no
    evidence" is a state worth being able to query:

        filing_too_coarse  the 2025 filing exists but splits revenue at a
                           granularity that cannot confirm this theme (IC
                           designers routinely book one line, "IC 收入 99%")
        no_filing          no 營業比重 section in the 2025 report at all

    Both are inherited placements that predate this pass and still owe an
    evidence line. Left as NULL they were invisible to every check.
    """
    from db.themes_data import MANUAL_EVIDENCE

    manual = 0
    with get_cursor() as cur:
        cur.execute("UPDATE tw.stock_themes SET evidence_kind = NULL, note = NULL")
        cur.execute(
            """
            UPDATE tw.stock_themes st SET evidence_kind = CASE WHEN EXISTS (
                       SELECT 1 FROM tw.product_revenue p
                        WHERE p.stock_id = st.stock_id AND p.fiscal_year = 2025)
                   THEN 'filing_too_coarse' ELSE 'no_filing' END
             WHERE st.revenue_pct IS NULL
            """)
        cur.execute(
            "UPDATE tw.stock_themes SET evidence_kind = 'annual_report_table' "
            "WHERE revenue_pct IS NOT NULL")
        table = cur.rowcount
        for path, entries in MANUAL_EVIDENCE.items():
            category, subcategory, theme = path.split(" > ")
            for stock_name, (kind, why) in entries.items():
                cur.execute(
                    """
                    UPDATE tw.stock_themes st
                       SET evidence_kind = %s, note = %s
                      FROM tw.themes t, tw.stocks s
                     WHERE t.theme_id = st.theme_id AND s.stock_id = st.stock_id
                       AND t.category = %s AND t.subcategory = %s AND t.name = %s
                       AND s.name = %s
                    """,
                    (kind, why, category, subcategory, theme, stock_name))
                if cur.rowcount != 1:
                    raise SystemExit(
                        f"MANUAL_EVIDENCE row matched {cur.rowcount} memberships: "
                        f"{stock_name} in {path}")
                manual += 1
    return table, manual


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    matched, scanned = fill(args.dry_run)
    print(f"Matched {matched} (stock, theme) pairs from {scanned} product lines"
          + (" [dry run]" if args.dry_run else ""))
    if not args.dry_run:
        table, manual = apply_evidence()
        print(f"evidence_kind: {table} from the revenue table, {manual} manual")


if __name__ == "__main__":
    main()
