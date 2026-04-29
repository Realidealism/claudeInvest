"""Aggregate F1-F8 rule results into a total score and letter grade."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .rules import RuleResult

Grade = Literal["A", "B", "C", "D"]


@dataclass(frozen=True)
class GradeCutoffs:
    a_min: int = 7
    b_min: int = 5
    c_min: int = 3


@dataclass(frozen=True)
class Scoreboard:
    results: list[RuleResult]
    score: int
    unknown_count: int
    grade: Grade

    @property
    def passed_codes(self) -> list[str]:
        return [r.code for r in self.results if r.passed is True]

    @property
    def failed_codes(self) -> list[str]:
        return [r.code for r in self.results if r.passed is False]

    @property
    def unknown_codes(self) -> list[str]:
        return [r.code for r in self.results if r.passed is None]


def score(results: list[RuleResult], cutoffs: GradeCutoffs | None = None) -> Scoreboard:
    c = cutoffs or GradeCutoffs()
    s = sum(1 for r in results if r.passed is True)
    unknown = sum(1 for r in results if r.passed is None)
    grade: Grade
    if s >= c.a_min:
        grade = "A"
    elif s >= c.b_min:
        grade = "B"
    elif s >= c.c_min:
        grade = "C"
    else:
        grade = "D"
    return Scoreboard(results=results, score=s, unknown_count=unknown, grade=grade)
