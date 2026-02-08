"""Data quality checks for generated datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pandera as pa
from pandera import Check

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = PROJECT_ROOT / "out"

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


def get_table_paths(out_dir: Path = DEFAULT_OUT_DIR) -> Dict[str, Path]:
    return {
        "patients": out_dir / "ehr" / "patients",
        "admissions": out_dir / "ehr" / "admissions",
        "diagnoses": out_dir / "ehr" / "diagnoses",
        "wards": out_dir / "erp" / "wards",
        "staff": out_dir / "erp" / "staff",
        "staff_assignments": out_dir / "erp" / "staff_assignments",
        "devices": out_dir / "iot" / "devices",
        "vital_signs": out_dir / "iot" / "vital_signs",
    }


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


def load_all_tables(out_dir: Path = DEFAULT_OUT_DIR) -> Dict[str, pd.DataFrame]:
    if not out_dir.exists():
        raise AssertionError("Missing out/ directory. Run the generator module first.")

    tables: Dict[str, pd.DataFrame] = {}
    for name, path in get_table_paths(out_dir).items():
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
    date_2024_start = pd.Timestamp("2024-01-01")
    date_2024_end = pd.Timestamp("2024-12-31")
    date_2025_start = pd.Timestamp("2025-01-01")
    date_2026_end = pd.Timestamp("2026-12-31")
    date_2018_start = pd.Timestamp("2018-01-01")

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
                "first_name": pa.Column(str),
                "last_name": pa.Column(str),
                "sex": pa.Column(str, Check.isin({"F", "M"})),
                "birth_date": pa.Column(
                    pa.DateTime,
                    Check.between(date_1950, date_2010),
                    coerce=True,
                ),
                "city": pa.Column(str),
                "address": pa.Column(str),
                "postal_code": pa.Column(str, coerce=True),
                "country": pa.Column(str, Check.isin({"Italy"})),
                "email": pa.Column(str),
                "phone": pa.Column(str),
                "national_id": pa.Column(str),
                "marital_status": pa.Column(str, Check.isin({"single", "married", "divorced", "widowed"})),
                "primary_language": pa.Column(str, Check.isin({"it", "en", "es", "fr", "de"})),
                "insurance_provider": pa.Column(str),
                "insurance_plan": pa.Column(str, Check.isin({"basic", "standard", "premium"})),
                "insurance_id": pa.Column(str),
                "emergency_contact_name": pa.Column(str),
                "emergency_contact_phone": pa.Column(str),
                "height_cm": pa.Column(int, Check.between(140, 200)),
                "weight_kg": pa.Column(int, Check.between(45, 120)),
                "blood_type": pa.Column(str, Check.isin({"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"})),
            }
        ),
        "staff": pa.DataFrameSchema(
            {
                "staff_id": pa.Column(str),
                "first_name": pa.Column(str),
                "last_name": pa.Column(str),
                "role": pa.Column(str),
                "department": pa.Column(str),
                "employment_type": pa.Column(str, Check.isin({"Full-time", "Part-time", "Contractor"})),
                "email": pa.Column(str),
                "phone": pa.Column(str),
                "license_id": pa.Column(str),
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
                "manufacturer": pa.Column(str),
                "model": pa.Column(str),
                "serial_number": pa.Column(str),
                "status": pa.Column(str, Check.isin({"Active", "Maintenance", "Retired"})),
                "purchase_date": pa.Column(
                    pa.DateTime,
                    Check.between(date_2018_start, date_2024_end),
                    coerce=True,
                ),
                "last_calibration_date": pa.Column(
                    pa.DateTime,
                    Check.between(date_2024_start, date_2026_end),
                    coerce=True,
                ),
            }
        ),
        "admissions": pa.DataFrameSchema(
            {
                "admission_id": pa.Column(str),
                "patient_id": pa.Column(str),
                "ward_id": pa.Column(str),
                "admit_ts": pa.Column(pa.DateTime, coerce=True),
                "discharge_ts": pa.Column(pa.DateTime, coerce=True),
                "length_of_stay_days": pa.Column(int, Check.between(1, 30)),
                "admission_type": pa.Column(str, Check.isin({"Emergency", "Elective", "Urgent"})),
                "admission_source": pa.Column(str, Check.isin({"ER", "Referral", "Transfer"})),
                "discharge_status": pa.Column(str, Check.isin({"Home", "Transfer", "Rehab", "Deceased"})),
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
                "temperature_c": pa.Column(float, Check.between(35.0, 40.5)),
                "respiratory_rate": pa.Column(int, Check.between(10, 30)),
                "glucose_mg_dl": pa.Column(int, Check.between(70, 180)),
            }
        ),
    }


def validate_files_exist(out_dir: Path = DEFAULT_OUT_DIR) -> None:
    if not out_dir.exists():
        raise AssertionError("Missing out/ directory. Run the generator module first.")

    missing: List[str] = []
    for name, base in get_table_paths(out_dir).items():
        if not base.with_suffix(".parquet").exists() and not base.with_suffix(".csv").exists():
            missing.append(name)

    if missing:
        raise AssertionError(
            "Missing dataset files for tables: " + ", ".join(missing) + ". Run the generator module first."
        )


def validate_primary_keys(tables: Dict[str, pd.DataFrame]) -> None:
    for table_name, pk in PKS.items():
        assert_pk(tables[table_name], table_name, pk)


def validate_foreign_keys(tables: Dict[str, pd.DataFrame]) -> None:
    for child, child_fk, parent, parent_pk in FKS:
        assert_fk(tables[child], child_fk, tables[parent], parent_pk, f"{child}.{child_fk}->{parent}.{parent_pk}")


def validate_domain_constraints(tables: Dict[str, pd.DataFrame]) -> None:
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

    if "length_of_stay_days" in admissions.columns:
        los_days = (admissions["discharge_ts"] - admissions["admit_ts"]).dt.days
        mismatch = admissions["length_of_stay_days"].notna() & los_days.notna() & (admissions["length_of_stay_days"] != los_days)
        if mismatch.any():
            examples = admissions.loc[mismatch, ["admission_id", "length_of_stay_days"]].head(5)
            raise AssertionError(
                "admissions: length_of_stay_days must match discharge-admit in days. Examples:\n"
                + examples.to_string(index=False)
            )

    devices = tables["devices"].copy()
    devices["purchase_date"] = pd.to_datetime(devices["purchase_date"], errors="coerce")
    devices["last_calibration_date"] = pd.to_datetime(devices["last_calibration_date"], errors="coerce")
    invalid_devices = devices[devices["last_calibration_date"] < devices["purchase_date"]]
    if not invalid_devices.empty:
        examples = invalid_devices[["device_id", "purchase_date", "last_calibration_date"]].head(5)
        raise AssertionError(
            "devices: last_calibration_date must be >= purchase_date. Examples:\n"
            + examples.to_string(index=False)
        )
