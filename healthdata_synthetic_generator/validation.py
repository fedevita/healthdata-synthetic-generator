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
    assert_fk(tables["ricoveri"], "id_paziente", tables["pazienti"], "id_paziente", "ricoveri->pazienti")
    assert_fk(tables["ricoveri"], "id_reparto", tables["reparti"], "id_reparto", "ricoveri->reparti")
    assert_fk(tables["diagnosi"], "id_ricovero", tables["ricoveri"], "id_ricovero", "diagnosi->ricoveri")
    assert_fk(tables["assegnazioni"], "id_staff", tables["personale"], "id_staff", "assegnazioni->personale")
    assert_fk(tables["assegnazioni"], "id_reparto", tables["reparti"], "id_reparto", "assegnazioni->reparti")
    assert_fk(tables["dispositivi"], "id_reparto", tables["reparti"], "id_reparto", "dispositivi->reparti")
    assert_fk(tables["parametri_vitali"], "id_paziente", tables["pazienti"], "id_paziente", "parametri_vitali->pazienti")
    assert_fk(tables["parametri_vitali"], "id_dispositivo", tables["dispositivi"], "id_dispositivo", "parametri_vitali->dispositivi")
