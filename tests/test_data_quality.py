from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pandera as pa
from pandera import Check

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "out"

TABLE_PATHS: Dict[str, Path] = {
    "patients": OUT_DIR / "ehr" / "patients",
    "admissions": OUT_DIR / "ehr" / "admissions",
    "diagnoses": OUT_DIR / "ehr" / "diagnoses",
    "wards": OUT_DIR / "erp" / "wards",
    "staff": OUT_DIR / "erp" / "staff",
    "staff_assignments": OUT_DIR / "erp" / "staff_assignments",
    "devices": OUT_DIR / "iot" / "devices",
    "vital_signs": OUT_DIR / "iot" / "vital_signs",
}

PKS: Dict[str, str] = {
    "patients": "patient_id",
    "admissions": "admission_id",
    "diagnoses": "diagnosis_id",
    "wards": "ward_id",
    "staff": "staff_id",
    "staff_assignments": "assignment_id",
    "devices": "device_id",
    "vital_signs": "measurement_id",
}

FKS: List[Tuple[str, str, str, str]] = [
    ("admissions", "patient_id", "patients", "patient_id"),
    ("admissions", "ward_id", "wards", "ward_id"),
    ("diagnoses", "admission_id", "admissions", "admission_id"),
    ("staff_assignments", "staff_id", "staff", "staff_id"),
    ("staff_assignments", "ward_id", "wards", "ward_id"),
    ("devices", "ward_id", "wards", "ward_id"),
    ("vital_signs", "patient_id", "patients", "patient_id"),
    ("vital_signs", "device_id", "devices", "device_id"),
]


def load_table(base_path: Path) -> pd.DataFrame:
    parquet_path = base_path.with_suffix(".parquet")
    csv_path = base_path.with_suffix(".csv")

    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)

    raise FileNotFoundError(
        f"Missing dataset file for '{base_path.name}'. Expected {parquet_path.name} or {csv_path.name}."
    )


def load_all_tables() -> Dict[str, pd.DataFrame]:
    if not OUT_DIR.exists():
        raise AssertionError("Missing out/ directory. Run generate_synthetic_data.py first.")

    tables: Dict[str, pd.DataFrame] = {}
    for name, path in TABLE_PATHS.items():
        tables[name] = load_table(path)
    return tables


def assert_pk(df: pd.DataFrame, table_name: str, pk: str) -> None:
    if pk not in df.columns:
        raise AssertionError(f"{table_name}: missing primary key column '{pk}'.")

    null_count = df[pk].isna().sum()
    if null_count:
        raise AssertionError(f"{table_name}: primary key '{pk}' has {null_count} null values.")

    duplicated = df[df[pk].duplicated()][pk]
    if not duplicated.empty:
        examples = duplicated.head(5).tolist()
        raise AssertionError(
            f"{table_name}: primary key '{pk}' has duplicates. Examples: {examples}"
        )


def assert_fk(child: pd.DataFrame, child_fk: str, parent: pd.DataFrame, parent_pk: str, rel: str) -> None:
    missing = set(child[child_fk].dropna().astype(str)) - set(parent[parent_pk].dropna().astype(str))
    if missing:
        examples = list(missing)[:5]
        raise AssertionError(f"FK FAIL {rel}: {len(missing)} orphan values. Examples: {examples}")


def build_schemas() -> Dict[str, pa.DataFrameSchema]:
    date_1950 = pd.Timestamp("1950-01-01")
    date_2010 = pd.Timestamp("2010-12-31")
    date_2010_start = pd.Timestamp("2010-01-01")
    date_2024_end = pd.Timestamp("2024-12-31")
    date_2025_start = pd.Timestamp("2025-01-01")
    date_2026_end = pd.Timestamp("2026-12-31")

    return {
        "wards": pa.DataFrameSchema(
            {
                "ward_id": pa.Column(str),
                "ward_name": pa.Column(str),
                "specialty": pa.Column(str, Check.isin({
                    "Cardiology",
                    "Neurology",
                    "Oncology",
                    "Pediatrics",
                    "Emergency",
                    "ICU",
                    "Orthopedics",
                })),
            }
        ),
        "patients": pa.DataFrameSchema(
            {
                "patient_id": pa.Column(str),
                "sex": pa.Column(str, Check.isin({"F", "M"})),
                "birth_date": pa.Column(
                    pa.DateTime,
                    Check.between(date_1950, date_2010),
                    coerce=True,
                ),
                "city": pa.Column(str),
            }
        ),
        "staff": pa.DataFrameSchema(
            {
                "staff_id": pa.Column(str),
                "role": pa.Column(str),
                "hire_date": pa.Column(
                    pa.DateTime,
                    Check.between(date_2010_start, date_2024_end),
                    coerce=True,
                ),
            }
        ),
        "staff_assignments": pa.DataFrameSchema(
            {
                "assignment_id": pa.Column(str),
                "staff_id": pa.Column(str),
                "ward_id": pa.Column(str),
                "shift": pa.Column(str, Check.isin({"Day", "Night", "Evening"})),
            }
        ),
        "devices": pa.DataFrameSchema(
            {
                "device_id": pa.Column(str),
                "ward_id": pa.Column(str),
                "device_type": pa.Column(str, Check.isin({"ECG", "PulseOx", "BP Monitor", "Thermometer"})),
            }
        ),
        "admissions": pa.DataFrameSchema(
            {
                "admission_id": pa.Column(str),
                "patient_id": pa.Column(str),
                "ward_id": pa.Column(str),
                "admit_ts": pa.Column(pa.DateTime, coerce=True),
                "discharge_ts": pa.Column(pa.DateTime, coerce=True),
            }
        ),
        "diagnoses": pa.DataFrameSchema(
            {
                "diagnosis_id": pa.Column(str),
                "admission_id": pa.Column(str),
                "icd10_code": pa.Column(str, Check.isin({"I10", "E11", "J18", "K21", "M54", "N39"})),
                "severity": pa.Column(str, Check.isin({"low", "medium", "high"})),
            }
        ),
        "vital_signs": pa.DataFrameSchema(
            {
                "measurement_id": pa.Column(str),
                "patient_id": pa.Column(str),
                "device_id": pa.Column(str),
                "measured_at": pa.Column(
                    pa.DateTime,
                    Check.between(date_2025_start, date_2026_end),
                    coerce=True,
                ),
                "heart_rate": pa.Column(int, Check.between(50, 120)),
                "spo2": pa.Column(int, Check.between(90, 100)),
                "systolic_bp": pa.Column(int, Check.between(95, 160)),
                "diastolic_bp": pa.Column(int, Check.between(60, 100)),
            }
        ),
    }


def test_files_exist() -> None:
    if not OUT_DIR.exists():
        raise AssertionError("Missing out/ directory. Run generate_synthetic_data.py first.")

    missing: List[str] = []
    for name, base in TABLE_PATHS.items():
        if not base.with_suffix(".parquet").exists() and not base.with_suffix(".csv").exists():
            missing.append(name)

    if missing:
        raise AssertionError(
            "Missing dataset files for tables: " + ", ".join(missing) + ". Run generate_synthetic_data.py first."
        )


def test_primary_keys() -> None:
    tables = load_all_tables()
    for table_name, pk in PKS.items():
        assert_pk(tables[table_name], table_name, pk)


def test_foreign_keys_integrity() -> None:
    tables = load_all_tables()
    for child, child_fk, parent, parent_pk in FKS:
        assert_fk(tables[child], child_fk, tables[parent], parent_pk, f"{child}.{child_fk}->{parent}.{parent_pk}")


def test_domain_constraints() -> None:
    tables = load_all_tables()
    schemas = build_schemas()

    for table_name, schema in schemas.items():
        try:
            schema.validate(tables[table_name], lazy=True)
        except pa.errors.SchemaErrors as exc:
            failure = exc.failure_cases.head(5)
            raise AssertionError(
                f"{table_name}: domain constraints failed. Examples:\n{failure}"
            ) from exc

    admissions = tables["admissions"].copy()
    admissions["admit_ts"] = pd.to_datetime(admissions["admit_ts"], errors="coerce")
    admissions["discharge_ts"] = pd.to_datetime(admissions["discharge_ts"], errors="coerce")
    invalid_admissions = admissions[admissions["admit_ts"] > admissions["discharge_ts"]]
    if not invalid_admissions.empty:
        examples = invalid_admissions[["admission_id", "admit_ts", "discharge_ts"]].head(5)
        raise AssertionError(
            "admissions: admit_ts must be <= discharge_ts. Examples:\n" + examples.to_string(index=False)
        )
