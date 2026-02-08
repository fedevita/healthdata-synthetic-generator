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
    assert_fk(tables["admissions"], "patient_id", tables["patients"], "patient_id", "admissions->patients")
    assert_fk(tables["admissions"], "ward_id", tables["wards"], "ward_id", "admissions->wards")
    assert_fk(tables["diagnoses"], "admission_id", tables["admissions"], "admission_id", "diagnoses->admissions")
    assert_fk(tables["staff_assignments"], "staff_id", tables["staff"], "staff_id", "staff_assignments->staff")
    assert_fk(tables["staff_assignments"], "ward_id", tables["wards"], "ward_id", "staff_assignments->wards")
    assert_fk(tables["devices"], "ward_id", tables["wards"], "ward_id", "devices->wards")
    assert_fk(tables["vital_signs"], "patient_id", tables["patients"], "patient_id", "vital_signs->patients")
    assert_fk(tables["vital_signs"], "device_id", tables["devices"], "device_id", "vital_signs->devices")
