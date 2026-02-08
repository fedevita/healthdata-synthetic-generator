#!/usr/bin/env python3
"""
Generate synthetic healthcare datasets using SDV.

This script is designed as a standalone tool to be run from the command line
or in notebook environments (e.g., Google Colab).

High-level flow:
1) Build or load seed tables (real_tables)
2) Detect SDV multi-table metadata + relationships
3) Fit a multi-table synthesizer (HMA)
4) Sample synthetic tables
5) Validate FK integrity
6) Export datasets (CSV or Parquet)

Note: The generation logic will be implemented incrementally.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable

import numpy as np
import pandas as pd

# SDV version for logging
import sdv
# SDV imports (installed via requirements.txt)
from sdv.metadata import Metadata
from sdv.multi_table import HMASynthesizer


# ----------------------------
# Config / CLI
# ----------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic healthcare datasets with SDV.")
    p.add_argument("--out-dir", default="out", help="Output directory (default: out)")
    p.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Export format")
    p.add_argument("--scale", type=float, default=2.0, help="Sampling scale factor (default: 2.0)")
    p.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    return p.parse_args()


# ----------------------------
# Seed / input tables
# ----------------------------
def build_seed_tables(rng: np.random.Generator) -> Dict[str, pd.DataFrame]:
    """
    TODO: Replace this placeholder with one of:
    - generating controlled mock/seed tables (recommended for testing), or
    - loading seed tables from disk (CSV/Parquet), or
    - using a small curated template dataset.

    Must return a dict[str, DataFrame] with at least these tables:
    patients, admissions, diagnoses, wards, staff, staff_assignments, devices, vital_signs

    Each table must include its PK and required FKs:
      admissions.patient_id -> patients.patient_id
      admissions.ward_id -> wards.ward_id
      diagnoses.admission_id -> admissions.admission_id
      staff_assignments.staff_id -> staff.staff_id
      staff_assignments.ward_id -> wards.ward_id
      devices.ward_id -> wards.ward_id
      vital_signs.patient_id -> patients.patient_id
      vital_signs.device_id -> devices.device_id
    """
    # Configuration (row counts)
    n_wards = 10
    n_patients = 200
    n_staff = 60
    n_assignments = 120
    n_devices = 30
    n_admissions = 400
    n_diagnoses = 500
    n_vitals = 2000

    # Date ranges
    admissions_start = pd.Timestamp("2024-01-01")
    admissions_end = pd.Timestamp("2026-12-31")
    vitals_start = pd.Timestamp("2025-01-01")
    vitals_end = pd.Timestamp("2026-12-31")
    birth_start = pd.Timestamp("1950-01-01")
    birth_end = pd.Timestamp("2010-12-31")
    hire_start = pd.Timestamp("2010-01-01")
    hire_end = pd.Timestamp("2024-12-31")

    def make_ids(prefix: str, count: int, width: int) -> list[str]:
        return [f"{prefix}{i:0{width}d}" for i in range(1, count + 1)]

    def random_dates(start: pd.Timestamp, end: pd.Timestamp, count: int) -> pd.Series:
        start_ns = start.value
        end_ns = end.value
        values = rng.integers(start_ns, end_ns, size=count, dtype=np.int64)
        return pd.Series(pd.to_datetime(values))

    # Wards
    ward_ids = make_ids("W", n_wards, 3)
    ward_names = [f"Ward {i:02d}" for i in range(1, n_wards + 1)]
    specialties = rng.choice(
        ["Cardiology", "Neurology", "Oncology", "Pediatrics", "Emergency", "ICU", "Orthopedics"],
        size=n_wards,
        replace=True,
    )
    wards = pd.DataFrame({
        "ward_id": ward_ids,
        "ward_name": ward_names,
        "specialty": specialties,
    })

    # Patients
    patient_ids = make_ids("P", n_patients, 6)
    patients = pd.DataFrame({
        "patient_id": patient_ids,
        "sex": rng.choice(["F", "M"], size=n_patients),
        "birth_date": random_dates(birth_start, birth_end, n_patients).dt.date,
        "city": rng.choice(["Milan", "Rome", "Turin", "Naples", "Bologna", "Florence"], size=n_patients),
    })

    # Staff
    staff_ids = make_ids("S", n_staff, 5)
    staff = pd.DataFrame({
        "staff_id": staff_ids,
        "role": rng.choice(["Nurse", "Doctor", "Technician", "Therapist"], size=n_staff),
        "hire_date": random_dates(hire_start, hire_end, n_staff).dt.date,
    })

    # Staff assignments
    assignment_ids = make_ids("ASG", n_assignments, 6)
    staff_assignments = pd.DataFrame({
        "assignment_id": assignment_ids,
        "staff_id": rng.choice(staff_ids, size=n_assignments, replace=True),
        "ward_id": rng.choice(ward_ids, size=n_assignments, replace=True),
        "shift": rng.choice(["Day", "Night", "Evening"], size=n_assignments),
    })

    # Devices
    device_ids = make_ids("D", n_devices, 5)
    devices = pd.DataFrame({
        "device_id": device_ids,
        "ward_id": rng.choice(ward_ids, size=n_devices, replace=True),
        "device_type": rng.choice(["ECG", "PulseOx", "BP Monitor", "Thermometer"], size=n_devices),
    })

    # Admissions
    admission_ids = make_ids("ADM", n_admissions, 7)
    admit_ts = random_dates(admissions_start, admissions_end, n_admissions)
    length_days = rng.integers(1, 15, size=n_admissions, dtype=np.int64)
    discharge_ts = admit_ts + pd.to_timedelta(length_days, unit="D")
    admissions = pd.DataFrame({
        "admission_id": admission_ids,
        "patient_id": rng.choice(patient_ids, size=n_admissions, replace=True),
        "ward_id": rng.choice(ward_ids, size=n_admissions, replace=True),
        "admit_ts": admit_ts,
        "discharge_ts": discharge_ts,
    })

    # Diagnoses
    diagnosis_ids = make_ids("DX", n_diagnoses, 7)
    diagnoses = pd.DataFrame({
        "diagnosis_id": diagnosis_ids,
        "admission_id": rng.choice(admission_ids, size=n_diagnoses, replace=True),
        "icd10_code": rng.choice(["I10", "E11", "J18", "K21", "M54", "N39"], size=n_diagnoses),
        "severity": rng.choice(["low", "medium", "high"], size=n_diagnoses, p=[0.5, 0.35, 0.15]),
    })

    # Vital signs
    measurement_ids = make_ids("VS", n_vitals, 7)
    vital_signs = pd.DataFrame({
        "measurement_id": measurement_ids,
        "patient_id": rng.choice(patient_ids, size=n_vitals, replace=True),
        "device_id": rng.choice(device_ids, size=n_vitals, replace=True),
        "measured_at": random_dates(vitals_start, vitals_end, n_vitals),
        "heart_rate": rng.integers(50, 120, size=n_vitals),
        "spo2": rng.integers(90, 100, size=n_vitals),
        "systolic_bp": rng.integers(95, 160, size=n_vitals),
        "diastolic_bp": rng.integers(60, 100, size=n_vitals),
    })

    return {
        "wards": wards,
        "patients": patients,
        "staff": staff,
        "staff_assignments": staff_assignments,
        "devices": devices,
        "admissions": admissions,
        "diagnoses": diagnoses,
        "vital_signs": vital_signs,
    }


# ----------------------------
# Validation helpers
# ----------------------------
def assert_fk(child_df: pd.DataFrame, child_fk: str, parent_df: pd.DataFrame, parent_pk: str, rel_name: str) -> None:
    missing = set(child_df[child_fk].dropna().astype(str)) - set(parent_df[parent_pk].dropna().astype(str))
    if missing:
        examples = list(missing)[:5]
        raise ValueError(f"[FK FAIL] {rel_name}: {len(missing)} orphan values. Examples: {examples}")


# ----------------------------
# SDV pipeline
# ----------------------------
def build_metadata(real_tables: Dict[str, pd.DataFrame], metadata_path: Path) -> Metadata:
    """
    Detect metadata from seed tables.
    If you want to force PKs/relationships, do it carefully:
    - detect_from_dataframes can already infer them
    - adding duplicates will raise InvalidMetadataError
    """
    metadata = Metadata.detect_from_dataframes(data=real_tables)
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


def validate_synthetic_tables(tables: Dict[str, pd.DataFrame]) -> None:
    # Minimal FK checks (extend as needed)
    assert_fk(tables["admissions"], "patient_id", tables["patients"], "patient_id", "admissions->patients")
    assert_fk(tables["admissions"], "ward_id", tables["wards"], "ward_id", "admissions->wards")
    assert_fk(tables["diagnoses"], "admission_id", tables["admissions"], "admission_id", "diagnoses->admissions")
    assert_fk(tables["staff_assignments"], "staff_id", tables["staff"], "staff_id", "staff_assignments->staff")
    assert_fk(tables["staff_assignments"], "ward_id", tables["wards"], "ward_id", "staff_assignments->wards")
    assert_fk(tables["devices"], "ward_id", tables["wards"], "ward_id", "devices->wards")
    assert_fk(tables["vital_signs"], "patient_id", tables["patients"], "patient_id", "vital_signs->patients")
    assert_fk(tables["vital_signs"], "device_id", tables["devices"], "device_id", "vital_signs->devices")


# ----------------------------
# Export
# ----------------------------
def export_tables(tables: Dict[str, pd.DataFrame], out_dir: Path, fmt: str) -> None:
    # Create folders (same layout you already use)
    ehr_dir = out_dir / "ehr"
    erp_dir = out_dir / "erp"
    iot_dir = out_dir / "iot"
    ehr_dir.mkdir(parents=True, exist_ok=True)
    erp_dir.mkdir(parents=True, exist_ok=True)
    iot_dir.mkdir(parents=True, exist_ok=True)

    mapping = {
        "patients": ehr_dir / "patients",
        "admissions": ehr_dir / "admissions",
        "diagnoses": ehr_dir / "diagnoses",
        "wards": erp_dir / "wards",
        "staff": erp_dir / "staff",
        "staff_assignments": erp_dir / "staff_assignments",
        "devices": iot_dir / "devices",
        "vital_signs": iot_dir / "vital_signs",
    }

    for table_name, base_path in mapping.items():
        df = tables[table_name]
        if fmt == "csv":
            df.to_csv(f"{base_path}.csv", index=False)
        else:
            # Parquet needs pyarrow installed
            df.to_parquet(f"{base_path}.parquet", index=False)

    print(f"Export completed in: {out_dir.resolve()}")


def log_table_counts(label: str, tables: Dict[str, pd.DataFrame], order: Iterable[str]) -> None:
    print(f"{label} row counts:")
    for name in order:
        print(f"- {name}: {len(tables[name])}")


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"SDV version: {sdv.__version__}")

    table_order = [
        "wards",
        "patients",
        "staff",
        "staff_assignments",
        "devices",
        "admissions",
        "diagnoses",
        "vital_signs",
    ]

    # 1) Seed tables
    real_tables = build_seed_tables(rng)
    log_table_counts("Seed", real_tables, table_order)

    # 2) Metadata (detect)
    metadata_path = out_dir / "metadata.json"
    metadata = build_metadata(real_tables, metadata_path)

    # 3) Fit + sample
    synthetic_tables = fit_and_sample(real_tables, metadata, scale=args.scale)
    log_table_counts("Synthetic", synthetic_tables, table_order)

    # 4) Validate
    validate_synthetic_tables(synthetic_tables)
    print("FK integrity validated.")

    # 5) Export
    export_tables(synthetic_tables, out_dir, fmt=args.format)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
