"""Multiset reconciliation (README measurement rules). Counts, not sets: [a] and [a, a] differ."""

from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True)
class Report:
    missing: Counter
    extra: Counter
    duplicate: Counter
    completeness: float


def compare(source: Counter, table: Counter) -> Report:
    total = sum(source.values())
    if total == 0:
        raise ValueError("source has no keys; refusing to report 100% of nothing")
    missing = source - table
    over = table - source
    duplicate = Counter({k: n for k, n in over.items() if k in source})
    extra = Counter({k: n for k, n in over.items() if k not in source})
    landed = total - sum(missing.values())
    return Report(missing=missing, extra=extra, duplicate=duplicate, completeness=landed / total)
