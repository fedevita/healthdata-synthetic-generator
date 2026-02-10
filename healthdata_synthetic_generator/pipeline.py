"""SDV pipeline orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Dict
import re

import numpy as np
import pandas as pd
from sdv.metadata import Metadata
from sdv.multi_table import HMASynthesizer


def build_metadata(real_tables: Dict[str, pd.DataFrame], metadata_path: Path) -> Metadata:
    if metadata_path.exists():
        metadata_path.unlink()
    metadata = Metadata.detect_from_dataframes(data=real_tables)
    # Force categorical sdtypes for locale-sensitive fields to keep Italian values.
    categorical_fields = {
        "pazienti": [
            "nome",
            "cognome",
            "sesso",
            "citta",
            "indirizzo",
            "cap",
            "paese",
            "email",
            "telefono",
            "codice_fiscale",
            "stato_civile",
            "lingua_primaria",
            "compagnia_assicurativa",
            "piano_assicurativo",
            "id_assicurazione",
            "contatto_emergenza_nome",
            "contatto_emergenza_telefono",
            "gruppo_sanguigno",
        ],
        "personale": [
            "nome",
            "cognome",
            "ruolo",
            "reparto",
            "tipo_impiego",
            "email",
            "telefono",
            "id_licenza",
        ],
        "reparti": ["nome_reparto", "specialita"],
        "assegnazioni": ["turno"],
        "dispositivi": ["tipo_dispositivo", "produttore", "modello", "numero_serie", "stato"],
        "ricoveri": ["tipo_ricovero", "provenienza_ricovero", "esito_dimissione"],
        "diagnosi": ["codice_icd10", "gravita"],
    }
    for table_name, columns in categorical_fields.items():
        for column in columns:
            if column in real_tables.get(table_name, pd.DataFrame()).columns:
                metadata.update_column(column, table_name=table_name, sdtype="categorical")
    metadata.save_to_json(metadata_path)
    return Metadata.load_from_json(metadata_path)


def fit_and_sample(
    real_tables: Dict[str, pd.DataFrame],
    metadata: Metadata,
    scale: float,
) -> Dict[str, pd.DataFrame]:
    synthesizer = HMASynthesizer(metadata)
    synthesizer.fit(real_tables)
    return synthesizer.sample(scale=scale)


def enforce_email_consistency(tables: Dict[str, pd.DataFrame], rng: np.random.Generator) -> None:
    def normalize(value: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z]", "", value or "")
        return cleaned.lower() if cleaned else "utente"

    def build_emails(df: pd.DataFrame, domain: str) -> pd.Series:
        first = df.get("nome", pd.Series(["utente"] * len(df))).astype(str)
        last = df.get("cognome", pd.Series(["utente"] * len(df))).astype(str)
        suffix = rng.integers(1, 10000, size=len(df))
        return [
            f"{normalize(fn)}.{normalize(ln)}{num}@{domain}"
            for fn, ln, num in zip(first, last, suffix)
        ]

    patients = tables.get("pazienti")
    if patients is not None and not patients.empty and "email" in patients.columns:
        patients["email"] = build_emails(patients, "example.it")

    staff = tables.get("personale")
    if staff is not None and not staff.empty and "email" in staff.columns:
        staff["email"] = build_emails(staff, "ospedale.example.it")


def enforce_admission_order(tables: Dict[str, pd.DataFrame], rng: np.random.Generator) -> None:
    admissions = tables.get("ricoveri")
    if admissions is None or admissions.empty:
        return

    admit_ts = pd.to_datetime(admissions["data_ricovero"], errors="coerce")
    discharge_ts = pd.to_datetime(admissions["data_dimissione"], errors="coerce")
    valid_admit = admit_ts.notna()
    los = (discharge_ts - admit_ts).dt.days
    invalid_los = los.isna() | (los < 1) | (los > 30)

    if invalid_los.any():
        offsets = rng.integers(1, 31, size=int(invalid_los.sum()), dtype=np.int64)
        los.loc[invalid_los] = offsets

    if valid_admit.any():
        discharge_ts = admit_ts + pd.to_timedelta(los, unit="D")
        admissions["data_dimissione"] = discharge_ts

    if "durata_degenza_giorni" in admissions.columns:
        admissions["durata_degenza_giorni"] = los.where(valid_admit, admissions["durata_degenza_giorni"])
