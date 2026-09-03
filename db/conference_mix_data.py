"""Revenue splits read off investor-conference decks, by hand.

Written with company NAMES for the same reason db/industry_data.py is: a
mistyped stock id resolves silently to the wrong company, a name does not
resolve at all.

    MIXES[name] = {
        conf_date   the deck's date, as tw.investor_conferences has it
        axis        product / business_unit / application / customer / region.
                    Only the first two can ever carry an exposure weight (R031);
                    the rest are recorded so a later reader learns the split was
                    by customer without re-opening the PDF.
        periods     (period_label, period_end, [(item, revenue, deck_page), ...])
                    period_label is what the slide printed, verbatim.
    }

AMOUNTS, NOT PERCENTAGES, wherever the deck gives them — the same choice
db.fill_weight already makes for annual reports, and for the same reason: a
printed percentage is rounded and an amount is not, and only amounts can be
added up and checked against something. 辛耘 prints 代理 48% and never prints a
percentage for 晶圓再生 at all, so a percentage-only file could not hold the
line that maps cleanly onto a node.

The lines must be LEAVES of the deck's own hierarchy. 辛耘's slides give 製造業
and then break it into 自製設備 and 晶圓再生; storing all three would double the
total and, worse, let two exposures claim the same revenue. weight_item takes
"A + B", so a parent is never needed.

The seeder checks each period's total against tw.income_statements and refuses
to write if they disagree by more than 1%. That check is the point of the file:
1314 中石化 sat for months with weight_pct derived from a table of TONNES that
came to 2.1% of its revenue, and nothing noticed because percentages that were
renormalised from the wrong base still look like percentages.
"""

MIXES = {
    # 辛耘 files only accounting captions — 銷貨收入 / 勞務收入 / 其他營業收入 —
    # so nothing in the annual report separates the equipment it builds from the
    # equipment it distributes for other people, which is the R022 line that
    # decides how much of it is a distributor. The deck draws exactly that line,
    # and breaks the manufacturing half into equipment and wafer reclaim.
    #
    # Both periods reconcile against the income statement: FY2025's three lines
    # total 11,371,557 against 11,371,368 (0.0017% out) and 1H2026's total
    # 6,115,365 against the deck's own p.30 figure of 6,115 million.
    #
    # That last reconciliation is also what settles the label. "2026/Q2" could
    # mean the quarter or the half; 代理 2,958,074 + 製造業 3,157,291 matches the
    # p.30 half-year revenue, so it is the half. period_end is 06-30 either way,
    # but the shares would have been wrong had it been the quarter.
    "辛耘": {
        "conf_date": "2026-08-27",
        "axis": "business_unit",
        "source_file": "358320260827M001.pdf",
        "periods": [
            ("2025", "2025-12-31", [
                ("自製設備", 3715167, 33),
                ("晶圓再生", 1008562, 34),
                ("代理", 6647828, 36),
            ]),
            ("2026/Q2", "2026-06-30", [
                ("自製設備", 2562617, 33),
                ("晶圓再生", 594674, 34),
                ("代理", 2958074, 36),
            ]),
        ],
    },
}
