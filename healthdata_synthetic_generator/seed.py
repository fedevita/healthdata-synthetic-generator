"""Seed table generation."""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def build_seed_tables(rng: np.random.Generator) -> Dict[str, pd.DataFrame]:
    """
    Build seed tables for SDV training.

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
    n_wards = 10
    n_patients = 200
    n_staff = 60
    n_assignments = 120
    n_devices = 30
    n_admissions = 400
    n_diagnoses = 500
    n_vitals = 2000

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

    patient_ids = make_ids("P", n_patients, 6)
    patients = pd.DataFrame({
        "patient_id": patient_ids,
        "sex": rng.choice(["F", "M"], size=n_patients),
        "birth_date": random_dates(birth_start, birth_end, n_patients).dt.date,
        "city": rng.choice(["Milan", "Rome", "Turin", "Naples", "Bologna", "Florence"], size=n_patients),
    })

    staff_ids = make_ids("S", n_staff, 5)
    staff = pd.DataFrame({
        "staff_id": staff_ids,
        "role": rng.choice(["Nurse", "Doctor", "Technician", "Therapist"], size=n_staff),
        "hire_date": random_dates(hire_start, hire_end, n_staff).dt.date,
    })

    assignment_ids = make_ids("ASG", n_assignments, 6)
    staff_assignments = pd.DataFrame({
        "assignment_id": assignment_ids,
        "staff_id": rng.choice(staff_ids, size=n_assignments, replace=True),
        "ward_id": rng.choice(ward_ids, size=n_assignments, replace=True),
        "shift": rng.choice(["Day", "Night", "Evening"], size=n_assignments),
    })

    device_ids = make_ids("D", n_devices, 5)
    devices = pd.DataFrame({
        "device_id": device_ids,
        "ward_id": rng.choice(ward_ids, size=n_devices, replace=True),
        "device_type": rng.choice(["ECG", "PulseOx", "BP Monitor", "Thermometer"], size=n_devices),
    })

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

    diagnosis_ids = make_ids("DX", n_diagnoses, 7)
    diagnoses = pd.DataFrame({
        "diagnosis_id": diagnosis_ids,
        "admission_id": rng.choice(admission_ids, size=n_diagnoses, replace=True),
        "icd10_code": rng.choice(["I10", "E11", "J18", "K21", "M54", "N39"], size=n_diagnoses),
        "severity": rng.choice(["low", "medium", "high"], size=n_diagnoses, p=[0.5, 0.35, 0.15]),
    })

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
