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
            admissions.id_paziente -> patients.id_paziente
            admissions.id_reparto -> wards.id_reparto
            diagnoses.id_ricovero -> admissions.id_ricovero
            staff_assignments.id_staff -> staff.id_staff
            staff_assignments.id_reparto -> wards.id_reparto
            devices.id_reparto -> wards.id_reparto
            vital_signs.id_paziente -> patients.id_paziente
            vital_signs.id_dispositivo -> devices.id_dispositivo
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
    ward_names = [f"Reparto {i:02d}" for i in range(1, n_wards + 1)]
    specialties = rng.choice(
        [
            "Cardiologia",
            "Neurologia",
            "Oncologia",
            "Pediatria",
            "Pronto Soccorso",
            "Terapia Intensiva",
            "Ortopedia",
        ],
        size=n_wards,
        replace=True,
    )
    wards = pd.DataFrame({
        "id_reparto": ward_ids,
        "nome_reparto": ward_names,
        "specialita": specialties,
    })

    patient_ids = make_ids("P", n_patients, 6)
    first_names = ["Luca", "Marco", "Giulia", "Sara", "Anna", "Paolo", "Elena", "Matteo", "Chiara", "Davide"]
    last_names = ["Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Gallo", "Costa", "Fontana", "Greco"]
    languages = ["it"]
    marital_statuses = ["celibe/nubile", "sposato/a", "divorziato/a", "vedovo/a"]
    insurance_providers = ["Generali", "Unisalute", "Reale Mutua", "Poste Vita", "Sara Assicurazioni"]
    insurance_plans = ["basic", "standard", "premium"]
    street_names = ["Via Roma", "Corso Italia", "Via Milano", "Via Garibaldi", "Via Dante", "Via Verdi"]
    cities = ["Milano", "Roma", "Torino", "Napoli", "Bologna", "Firenze", "Venezia", "Genova"]
    countries = ["Italia"]
    blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    first_name = rng.choice(first_names, size=n_patients)
    last_name = rng.choice(last_names, size=n_patients)
    email_suffix = rng.integers(1, 9999, size=n_patients)
    email = [f"{fn.lower()}.{ln.lower()}{num}@example.it" for fn, ln, num in zip(first_name, last_name, email_suffix)]
    phone = [f"+39 3{rng.integers(10**8, 10**9 - 1):09d}" for _ in range(n_patients)]
    emergency_contact_name = [
        f"{fn} {ln}" for fn, ln in zip(rng.choice(first_names, size=n_patients), rng.choice(last_names, size=n_patients))
    ]
    emergency_contact_phone = [f"+39 3{rng.integers(10**8, 10**9 - 1):09d}" for _ in range(n_patients)]
    patients = pd.DataFrame({
        "id_paziente": patient_ids,
        "nome": first_name,
        "cognome": last_name,
        "sesso": rng.choice(["F", "M"], size=n_patients),
        "data_nascita": random_dates(birth_start, birth_end, n_patients).dt.date,
        "citta": rng.choice(cities, size=n_patients),
        "indirizzo": [f"{rng.choice(street_names)} {rng.integers(1, 200)}" for _ in range(n_patients)],
        "cap": [f"{rng.integers(10000, 99999):05d}" for _ in range(n_patients)],
        "paese": rng.choice(countries, size=n_patients),
        "email": email,
        "telefono": phone,
        "codice_fiscale": [f"CF{rng.integers(10**9, 10**10 - 1):010d}" for _ in range(n_patients)],
        "stato_civile": rng.choice(marital_statuses, size=n_patients),
        "lingua_primaria": rng.choice(languages, size=n_patients),
        "compagnia_assicurativa": rng.choice(insurance_providers, size=n_patients),
        "piano_assicurativo": rng.choice(insurance_plans, size=n_patients),
        "id_assicurazione": [f"INS{rng.integers(10**7, 10**8 - 1):07d}" for _ in range(n_patients)],
        "contatto_emergenza_nome": emergency_contact_name,
        "contatto_emergenza_telefono": emergency_contact_phone,
        "altezza_cm": rng.integers(140, 201, size=n_patients),
        "peso_kg": rng.integers(45, 121, size=n_patients),
        "gruppo_sanguigno": rng.choice(blood_types, size=n_patients),
    })

    staff_ids = make_ids("S", n_staff, 5)
    staff_first = rng.choice(first_names, size=n_staff)
    staff_last = rng.choice(last_names, size=n_staff)
    staff_email = [f"{fn.lower()}.{ln.lower()}@ospedale.example.it" for fn, ln in zip(staff_first, staff_last)]
    staff = pd.DataFrame({
        "id_staff": staff_ids,
        "nome": staff_first,
        "cognome": staff_last,
        "ruolo": rng.choice(["Infermiere", "Medico", "Tecnico", "Terapista"], size=n_staff),
        "reparto": rng.choice(specialties, size=n_staff),
        "tipo_impiego": rng.choice(["Tempo pieno", "Part-time", "Contratto"], size=n_staff),
        "email": staff_email,
        "telefono": [f"+39 3{rng.integers(10**8, 10**9 - 1):09d}" for _ in range(n_staff)],
        "id_licenza": [f"LIC{rng.integers(10**6, 10**7 - 1):06d}" for _ in range(n_staff)],
        "data_assunzione": random_dates(hire_start, hire_end, n_staff).dt.date,
    })

    assignment_ids = make_ids("ASG", n_assignments, 6)
    staff_assignments = pd.DataFrame({
        "id_assegnazione": assignment_ids,
        "id_staff": rng.choice(staff_ids, size=n_assignments, replace=True),
        "id_reparto": rng.choice(ward_ids, size=n_assignments, replace=True),
        "turno": rng.choice(["Giorno", "Notte", "Sera"], size=n_assignments),
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
        "id_dispositivo": device_ids,
        "id_reparto": rng.choice(ward_ids, size=n_devices, replace=True),
        "tipo_dispositivo": rng.choice(["ECG", "Pulsossimetro", "Sfigmomanometro", "Termometro"], size=n_devices),
        "produttore": rng.choice(manufacturers, size=n_devices),
        "modello": rng.choice(models, size=n_devices),
        "numero_serie": [f"SN{rng.integers(10**9, 10**10 - 1):010d}" for _ in range(n_devices)],
        "stato": rng.choice(["Attivo", "Manutenzione", "Ritirato"], size=n_devices, p=[0.8, 0.15, 0.05]),
        "data_acquisto": purchase_dates,
        "data_ultima_calibrazione": calibration_dates,
    })

    admission_ids = make_ids("ADM", n_admissions, 7)
    admit_ts = random_dates(admissions_start, admissions_end, n_admissions)
    length_days = rng.integers(1, 15, size=n_admissions, dtype=np.int64)
    discharge_ts = admit_ts + pd.to_timedelta(length_days, unit="D")
    admissions = pd.DataFrame({
        "id_ricovero": admission_ids,
        "id_paziente": rng.choice(patient_ids, size=n_admissions, replace=True),
        "id_reparto": rng.choice(ward_ids, size=n_admissions, replace=True),
        "data_ricovero": admit_ts,
        "data_dimissione": discharge_ts,
        "durata_degenza_giorni": length_days,
        "tipo_ricovero": rng.choice(["Emergenza", "Elettivo", "Urgente"], size=n_admissions),
        "provenienza_ricovero": rng.choice(["PS", "Invio", "Trasferimento"], size=n_admissions),
        "esito_dimissione": rng.choice(["Domicilio", "Trasferimento", "Riabilitazione", "Deceduto"], size=n_admissions, p=[0.8, 0.1, 0.08, 0.02]),
    })

    diagnosis_ids = make_ids("DX", n_diagnoses, 7)
    diagnoses = pd.DataFrame({
        "id_diagnosi": diagnosis_ids,
        "id_ricovero": rng.choice(admission_ids, size=n_diagnoses, replace=True),
        "codice_icd10": rng.choice(["I10", "E11", "J18", "K21", "M54", "N39"], size=n_diagnoses),
        "gravita": rng.choice(["bassa", "media", "alta"], size=n_diagnoses, p=[0.5, 0.35, 0.15]),
    })

    measurement_ids = make_ids("VS", n_vitals, 7)
    vital_signs = pd.DataFrame({
        "id_misurazione": measurement_ids,
        "id_paziente": rng.choice(patient_ids, size=n_vitals, replace=True),
        "id_dispositivo": rng.choice(device_ids, size=n_vitals, replace=True),
        "data_misurazione": random_dates(vitals_start, vitals_end, n_vitals),
        "frequenza_cardiaca": rng.integers(50, 120, size=n_vitals),
        "saturazione_ossigeno": rng.integers(90, 100, size=n_vitals),
        "pressione_sistolica": rng.integers(95, 160, size=n_vitals),
        "pressione_diastolica": rng.integers(60, 100, size=n_vitals),
        "temperatura_c": rng.uniform(35.0, 40.5, size=n_vitals).round(1),
        "frequenza_respiratoria": rng.integers(10, 31, size=n_vitals),
        "glicemia_mg_dl": rng.integers(70, 181, size=n_vitals),
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
