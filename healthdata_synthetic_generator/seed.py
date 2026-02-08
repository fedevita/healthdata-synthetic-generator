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
    purchase_start = pd.Timestamp("2018-01-01")
    purchase_end = pd.Timestamp("2024-12-31")
    calibration_start = pd.Timestamp("2024-01-01")
    calibration_end = pd.Timestamp("2026-12-31")

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
    first_names = ["Luca", "Marco", "Giulia", "Sara", "Anna", "Paolo", "Elena", "Matteo", "Chiara", "Davide"]
    last_names = ["Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Gallo", "Costa", "Fontana", "Greco"]
    languages = ["it", "en", "es", "fr", "de"]
    marital_statuses = ["single", "married", "divorced", "widowed"]
    insurance_providers = ["Aetna", "Allianz", "Generali", "Unisalute", "Uniqa"]
    insurance_plans = ["basic", "standard", "premium"]
    street_names = ["Via Roma", "Corso Italia", "Via Milano", "Via Garibaldi", "Via Dante", "Via Verdi"]
    cities = ["Milan", "Rome", "Turin", "Naples", "Bologna", "Florence"]
    countries = ["Italy"]
    blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    first_name = rng.choice(first_names, size=n_patients)
    last_name = rng.choice(last_names, size=n_patients)
    email_suffix = rng.integers(1, 9999, size=n_patients)
    email = [f"{fn.lower()}.{ln.lower()}{num}@example.org" for fn, ln, num in zip(first_name, last_name, email_suffix)]
    phone = [f"+39 3{rng.integers(10**8, 10**9 - 1):09d}" for _ in range(n_patients)]
    emergency_contact_name = [
        f"{fn} {ln}" for fn, ln in zip(rng.choice(first_names, size=n_patients), rng.choice(last_names, size=n_patients))
    ]
    emergency_contact_phone = [f"+39 3{rng.integers(10**8, 10**9 - 1):09d}" for _ in range(n_patients)]
    patients = pd.DataFrame({
        "patient_id": patient_ids,
        "first_name": first_name,
        "last_name": last_name,
        "sex": rng.choice(["F", "M"], size=n_patients),
        "birth_date": random_dates(birth_start, birth_end, n_patients).dt.date,
        "city": rng.choice(cities, size=n_patients),
        "address": [f"{rng.choice(street_names)} {rng.integers(1, 200)}" for _ in range(n_patients)],
        "postal_code": [f"{rng.integers(10000, 99999):05d}" for _ in range(n_patients)],
        "country": rng.choice(countries, size=n_patients),
        "email": email,
        "phone": phone,
        "national_id": [f"CF{rng.integers(10**9, 10**10 - 1):010d}" for _ in range(n_patients)],
        "marital_status": rng.choice(marital_statuses, size=n_patients),
        "primary_language": rng.choice(languages, size=n_patients),
        "insurance_provider": rng.choice(insurance_providers, size=n_patients),
        "insurance_plan": rng.choice(insurance_plans, size=n_patients),
        "insurance_id": [f"INS{rng.integers(10**7, 10**8 - 1):07d}" for _ in range(n_patients)],
        "emergency_contact_name": emergency_contact_name,
        "emergency_contact_phone": emergency_contact_phone,
        "height_cm": rng.integers(140, 201, size=n_patients),
        "weight_kg": rng.integers(45, 121, size=n_patients),
        "blood_type": rng.choice(blood_types, size=n_patients),
    })

    staff_ids = make_ids("S", n_staff, 5)
    staff_first = rng.choice(first_names, size=n_staff)
    staff_last = rng.choice(last_names, size=n_staff)
    staff_email = [f"{fn.lower()}.{ln.lower()}@hospital.example.org" for fn, ln in zip(staff_first, staff_last)]
    staff = pd.DataFrame({
        "staff_id": staff_ids,
        "first_name": staff_first,
        "last_name": staff_last,
        "role": rng.choice(["Nurse", "Doctor", "Technician", "Therapist"], size=n_staff),
        "department": rng.choice(specialties, size=n_staff),
        "employment_type": rng.choice(["Full-time", "Part-time", "Contractor"], size=n_staff),
        "email": staff_email,
        "phone": [f"+39 3{rng.integers(10**8, 10**9 - 1):09d}" for _ in range(n_staff)],
        "license_id": [f"LIC{rng.integers(10**6, 10**7 - 1):06d}" for _ in range(n_staff)],
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
    manufacturers = ["Medtronic", "Philips", "GE Healthcare", "Siemens", "Mindray"]
    models = ["A1", "B2", "C3", "D4", "E5"]
    purchase_dates = random_dates(purchase_start, purchase_end, n_devices).dt.date
    calibration_dates = []
    for pd_date in purchase_dates:
        pd_ts = pd.Timestamp(pd_date)
        start = pd_ts if pd_ts > calibration_start else calibration_start
        calibration_dates.append(random_dates(start, calibration_end, 1).dt.date.iloc[0])
    devices = pd.DataFrame({
        "device_id": device_ids,
        "ward_id": rng.choice(ward_ids, size=n_devices, replace=True),
        "device_type": rng.choice(["ECG", "PulseOx", "BP Monitor", "Thermometer"], size=n_devices),
        "manufacturer": rng.choice(manufacturers, size=n_devices),
        "model": rng.choice(models, size=n_devices),
        "serial_number": [f"SN{rng.integers(10**9, 10**10 - 1):010d}" for _ in range(n_devices)],
        "status": rng.choice(["Active", "Maintenance", "Retired"], size=n_devices, p=[0.8, 0.15, 0.05]),
        "purchase_date": purchase_dates,
        "last_calibration_date": calibration_dates,
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
        "length_of_stay_days": length_days,
        "admission_type": rng.choice(["Emergency", "Elective", "Urgent"], size=n_admissions),
        "admission_source": rng.choice(["ER", "Referral", "Transfer"], size=n_admissions),
        "discharge_status": rng.choice(["Home", "Transfer", "Rehab", "Deceased"], size=n_admissions, p=[0.8, 0.1, 0.08, 0.02]),
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
        "temperature_c": rng.uniform(35.0, 40.5, size=n_vitals).round(1),
        "respiratory_rate": rng.integers(10, 31, size=n_vitals),
        "glucose_mg_dl": rng.integers(70, 181, size=n_vitals),
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
