# -*- coding: utf-8 -*-
"""A node code written twice in the membership file must stop the loader.

Python accepts a repeated key in a dict literal and keeps the last block. On 2026-09-07 州巧 was added under a
second "F4-01" while the first had been in the file since 2026-09-03; the loader saw a dict that held only the
other block, so there was no name to refuse, and the file-versus-database comparison agreed at one row short.
It was found a month later from the gap log, not by any check.

These cases run on source text, never on a built dict: once the dict exists the earlier block is unrecoverable.
"""
from pathlib import Path

from db import industry_data
from db.seed_industry import repeated_keys


def test_the_membership_file_has_no_repeated_key():
    # the read-only path (db.verify --check-only) never runs the loader, so the file is checked here as well
    source = Path(industry_data.__file__).read_text(encoding="utf-8")
    assert repeated_keys(source) == []


CLEAN = '''
MEMBERS = {
    "F4-01": [
        ('毅嘉', 'Major', None, 'k', 'e', 'Direct'),
    ],
    "F5-01": [
        ('百容', 'Major', None, 'k', 'e', 'Direct'),
    ],
}
'''

TWICE = '''
MEMBERS = {
    "F4-01": [
        ('州巧', 'Core', None, 'k', 'e', 'Direct'),
    ],
    "F5-01": [
        ('百容', 'Major', None, 'k', 'e', 'Direct'),
    ],
    "F4-01": [
        ('毅嘉', 'Major', None, 'k', 'e', 'Direct'),
        ('敦吉', 'Core', None, 'k', 'e', 'Direct'),
    ],
}
'''


def test_a_file_without_repeats_has_no_problem():
    assert repeated_keys(CLEAN) == []


def test_a_repeated_key_is_reported_with_its_lines_and_what_it_drops():
    problems = repeated_keys(TWICE)
    assert len(problems) == 1
    assert problems[0].startswith("F4-01: written 2 times (lines 3, 9)")
    # the earlier block is the one that is lost; the kept block's members are not named as dropped
    assert problems[0].endswith("dropping 州巧")
    assert "毅嘉" not in problems[0] and "敦吉" not in problems[0]


def test_the_built_dict_cannot_show_what_the_source_shows():
    # the reason the check reads text: executing the same source leaves no trace of the first block
    scope = {}
    exec(TWICE, scope)
    assert [m[0] for m in scope["MEMBERS"]["F4-01"]] == ["毅嘉", "敦吉"]
    assert repeated_keys(TWICE) != []


def test_every_earlier_block_is_counted_as_dropped():
    thrice = TWICE.replace('    "F5-01": [', '    "F4-01": [\n        (\'廣宇\', \'Major\', None, \'k\', \'e\', \'Direct\'),\n    ],\n    "F5-01": [')
    problems = repeated_keys(thrice)
    assert len(problems) == 1
    assert "written 3 times" in problems[0]
    assert problems[0].endswith("dropping 州巧、廣宇")


def test_a_literal_the_check_cannot_read_is_a_problem_not_a_pass():
    assert repeated_keys("MEMBERS = dict(a=[])") != []
    assert repeated_keys("OTHER = {}") != []
