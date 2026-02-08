"""Utility helpers."""

from __future__ import annotations

from typing import Dict, Iterable

import pandas as pd


def log_table_counts(label: str, tables: Dict[str, pd.DataFrame], order: Iterable[str]) -> None:
    print(f"{label} row counts:")
    for name in order:
        print(f"- {name}: {len(tables[name])}")
