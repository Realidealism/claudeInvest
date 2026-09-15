# -*- coding: utf-8 -*-
"""Which document is the annual report is a question the source already answers.

The picker has now been wrong three ways. A whitelist of form codes (F04/FE4)
recorded 4923 力士 and 6425 易發 as having filed nothing, because theirs are
F19 and F18. Replacing it with "take the last filename alphabetically" was not
a rule -- it agreed with reality for the 46 companies that file F04 and then
F11, because a revision both sorts later and is uploaded later, and it was
wrong wherever those came apart.

MOPS names every document on the filing list, and the names say plainly what
the codes only encode:

    F04  股東會年報(尚未適用永續揭露準則)
    F18  股東會年報(適用永續揭露準則)
    F19  股東會年報(僅永續專章)
    F11  股東會年報(股東會後修訂本)
    FE4/FE6/FE7/FE8  英文版-…

Three regimes for the same document, plus a post-AGM revision of any of them.
So the rule is stated in those terms: among the fiscal year's Chinese annual
reports, the latest upload. And where that cannot be established, nothing is
chosen -- 8906 花王 is why. The alphabetical rule took the oldest of its three
editions and parsed it successfully: status ok, four rows, a plausible total.
A stale source leaves no symptom for any downstream check to find.
"""
from datetime import date, datetime

from scrapers.annual_report_revenue import Filing, _pick_report


def _f(day, desc, hour=10):
    stamp = datetime(2026, day // 100, day % 100, hour, 0, 0)
    return Filing(stamp.date(), stamp, desc)


CH = "股東會年報(尚未適用永續揭露準則)"
CH_SUS = "股東會年報(適用永續揭露準則)"
CH_ONLY = "股東會年報(僅永續專章)"
CH_REV = "股東會年報(股東會後修訂本)"
EN = "英文版-股東會年報(尚未適用永續揭露準則)"


def test_a_single_candidate_needs_no_ordering():
    got, note = _pick_report({"2025_1234_20260101F04.pdf": _f(508, CH)}, "1234", 2025)
    assert got == "2025_1234_20260101F04.pdf"
    assert note is None


def test_the_english_edition_is_never_the_choice():
    filings = {"2025_1234_20260101F04.pdf": _f(508, CH),
               "2025_1234_20260101FE4.pdf": _f(509, EN)}
    got, _ = _pick_report(filings, "1234", 2025)
    assert got == "2025_1234_20260101F04.pdf"


def test_the_revision_supersedes_the_original():
    """1102 亞泥, 1802 台玻, 2608 嘉里大榮 and 43 others."""
    filings = {"2025_1102_20260528F04.pdf": _f(508, CH),
               "2025_1102_20260528F11.pdf": _f(903, CH_REV)}
    got, note = _pick_report(filings, "1102", 2025)
    assert got == "2025_1102_20260528F11.pdf"
    assert "latest annual-report filing by upload timestamp" in note


def test_the_later_regime_supersedes_the_earlier_one():
    """2412 中華電 and 2884 玉山金: F19 in February, F18 in May.

    Alphabetically F19 sorts last, so the old rule took the February edition.
    """
    filings = {"2025_2412_20260529F19.pdf": _f(226, CH_ONLY),
               "2025_2412_20260529F18.pdf": _f(508, CH_SUS)}
    got, _ = _pick_report(filings, "2412", 2025)
    assert got == "2025_2412_20260529F18.pdf"


def test_the_oldest_of_three_is_not_the_annual_report():
    """8906 花王: F18 (05-26), F04 (05-28), F11 (08-26).

    The old rule took F18 -- the oldest -- and read it without complaint.
    """
    filings = {"2025_8906_20260611F18.pdf": _f(526, CH_SUS),
               "2025_8906_20260611F04.pdf": _f(528, CH),
               "2025_8906_20260611F11.pdf": _f(826, CH_REV)}
    got, _ = _pick_report(filings, "8906", 2025)
    assert got == "2025_8906_20260611F11.pdf"


def test_a_missing_timestamp_refuses_to_choose():
    filings = {"2025_1234_20260101F04.pdf": _f(508, CH),
               "2025_1234_20260101F11.pdf": Filing(date(2026, 9, 3), None, CH_REV)}
    got, note = _pick_report(filings, "1234", 2025)
    assert got is None
    assert "upload timestamp missing" in note
    assert "F11" in note and "F04" in note


def test_a_tie_at_the_newest_refuses_to_choose():
    filings = {"2025_1234_20260101F04.pdf": _f(508, CH),
               "2025_1234_20260101F18.pdf": _f(508, CH_SUS)}
    got, note = _pick_report(filings, "1234", 2025)
    assert got is None
    assert "tied" in note


def test_a_document_that_is_not_an_annual_report_is_not_a_candidate():
    filings = {"2025_1234_20260101F13.pdf": _f(427, "股東會各項議案參考資料"),
               "2025_1234_20260101F04.pdf": _f(508, CH)}
    got, note = _pick_report(filings, "1234", 2025)
    assert got == "2025_1234_20260101F04.pdf"
    assert note is None


def test_no_annual_report_among_the_filings_says_what_was_there():
    filings = {"2025_1234_20260101F13.pdf": _f(427, "股東會各項議案參考資料")}
    got, note = _pick_report(filings, "1234", 2025)
    assert got is None
    assert "none of the 1 filings" in note and "議案參考資料" in note


def test_without_descriptions_the_weaker_basis_is_recorded():
    """The filename-only fallback must not pass itself off as the real rule."""
    filings = {"2025_1234_20260101F04.pdf": Filing(date(2026, 5, 8), datetime(2026, 5, 8), ""),
               "2025_1234_20260101F11.pdf": Filing(date(2026, 9, 3), datetime(2026, 9, 3), "")}
    got, note = _pick_report(filings, "1234", 2025)
    assert got == "2025_1234_20260101F11.pdf"
    assert "filing list carried no document descriptions" in note


# --------------------------------------------------------------------------
# the description has its own column, and the one next to it is not it
# --------------------------------------------------------------------------

def test_the_revision_note_is_not_the_description():
    """Reading 「檔名前最後一個有值的欄」 read the wrong column.

    MOPS prints 資料細節說明 two cells before the filename and 備註/更正說明
    one cell before it. Taking the last filled cell picked up the latter, so a
    revision that says what changed -- 3071 資通's 「修訂頁碼：4、71、80」, 4989
    榮科's 「修訂永續發展相關表格」 -- stopped looking like an annual report and
    was struck from the candidates, handing the choice back to the edition it
    supersedes. 6550 光聖 prints 「無」 in that column on every row and lost all
    four candidates at once.

    The note is worth keeping in its own right: it is the filing's own statement
    of what the revision changed.
    """
    filings = {
        "2025_3071_20260617F04.pdf": Filing(
            date(2026, 5, 27), datetime(2026, 5, 27, 16, 10, 39), CH, ""),
        "2025_3071_20260617F11.pdf": Filing(
            date(2026, 8, 25), datetime(2026, 8, 25, 10, 55, 54), CH_REV,
            "修訂頁碼：4、71、80"),
    }
    got, note = _pick_report(filings, "3071", 2025)
    assert got == "2025_3071_20260617F11.pdf"
    assert "unable to uniquely select" not in (note or "")


def test_an_english_only_filing_is_refused_for_its_language():
    """5344 立衛 filed only the English edition.

    Refusing it is right -- every heading the readers look for is Chinese -- but
    the note used to say no annual report had been filed, which is false about
    the document and would send the next reader hunting for a filing that is
    sitting on the list.
    """
    filings = {"2025_5344_20260617FE4.pdf":
               Filing(date(2026, 6, 17), datetime(2026, 6, 17), EN)}
    got, note = _pick_report(filings, "5344", 2025)
    assert got is None
    assert "in English" in note
    assert "none of the" not in note


def test_a_filing_that_is_not_an_annual_report_still_says_so():
    filings = {"2025_1234_20260101F13.pdf":
               Filing(date(2026, 4, 27), datetime(2026, 4, 27), "股東會各項議案參考資料")}
    got, note = _pick_report(filings, "1234", 2025)
    assert got is None
    assert "is not an annual report" in note or "is an annual report" in note
    assert "in English" not in note
