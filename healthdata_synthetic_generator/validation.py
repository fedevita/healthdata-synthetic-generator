"""Validation helpers for synthetic tables."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def assert_fk(child_df: pd.DataFrame, child_fk: str, parent_df: pd.DataFrame, parent_pk: str, rel_name: str) -> None:
    missing = set(child_df[child_fk].dropna().astype(str)) - set(parent_df[parent_pk].dropna().astype(str))
    if missing:
        examples = list(missing)[:5]
        raise ValueError(f"[FK FAIL] {rel_name}: {len(missing)} orphan values. Examples: {examples}")


def validate_synthetic_tables(tables: Dict[str, pd.DataFrame]) -> None:
    assert_fk(tables["admissions"], "id_paziente", tables["patients"], "id_paziente", "admissions->patients")
    assert_fk(tables["admissions"], "id_reparto", tables["wards"], "id_reparto", "admissions->wards")
    assert_fk(tables["diagnoses"], "id_ricovero", tables["admissions"], "id_ricovero", "diagnoses->admissions")
    assert_fk(tables["staff_assignments"], "id_staff", tables["staff"], "id_staff", "staff_assignments->staff")
    assert_fk(tables["staff_assignments"], "id_reparto", tables["wards"], "id_reparto", "staff_assignments->wards")
    assert_fk(tables["devices"], "id_reparto", tables["wards"], "id_reparto", "devices->wards")
    assert_fk(tables["vital_signs"], "id_paziente", tables["patients"], "id_paziente", "vital_signs->patients")
    assert_fk(tables["vital_signs"], "id_dispositivo", tables["devices"], "id_dispositivo", "vital_signs->devices")
