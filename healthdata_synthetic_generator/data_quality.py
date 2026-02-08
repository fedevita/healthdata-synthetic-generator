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
    "patients": "id_paziente",
    "admissions": "id_ricovero",
    "diagnoses": "id_diagnosi",
    "wards": "id_reparto",
    "staff": "id_staff",
    "staff_assignments": "id_assegnazione",
    "devices": "id_dispositivo",
    "vital_signs": "id_misurazione",
}

FKS: List[Tuple[str, str, str, str]] = [
    ("admissions", "id_paziente", "patients", "id_paziente"),
    ("admissions", "id_reparto", "wards", "id_reparto"),
    ("diagnoses", "id_ricovero", "admissions", "id_ricovero"),
    ("staff_assignments", "id_staff", "staff", "id_staff"),
    ("staff_assignments", "id_reparto", "wards", "id_reparto"),
    ("devices", "id_reparto", "wards", "id_reparto"),
    ("vital_signs", "id_paziente", "patients", "id_paziente"),
    ("vital_signs", "id_dispositivo", "devices", "id_dispositivo"),
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
                "id_reparto": pa.Column(str),
                "nome_reparto": pa.Column(str),
                "specialita": pa.Column(str, Check.isin({
                    "Cardiologia",
                    "Neurologia",
                    "Oncologia",
                    "Pediatria",
                    "Pronto Soccorso",
                    "Terapia Intensiva",
                    "Ortopedia",
                })),
            }
        ),
        "patients": pa.DataFrameSchema(
            {
                "id_paziente": pa.Column(str),
                "nome": pa.Column(str),
                "cognome": pa.Column(str),
                "sesso": pa.Column(str, Check.isin({"F", "M"})),
                "data_nascita": pa.Column(
                    pa.DateTime,
                    Check.between(date_1950, date_2010),
                    coerce=True,
                ),
                "citta": pa.Column(str),
                "indirizzo": pa.Column(str),
                "cap": pa.Column(str, coerce=True),
                "paese": pa.Column(str, Check.isin({"Italia"})),
                "email": pa.Column(str),
                "telefono": pa.Column(str),
                "codice_fiscale": pa.Column(str),
                "stato_civile": pa.Column(str, Check.isin({"celibe/nubile", "sposato/a", "divorziato/a", "vedovo/a"})),
                "lingua_primaria": pa.Column(str, Check.isin({"it"})),
                "compagnia_assicurativa": pa.Column(str),
                "piano_assicurativo": pa.Column(str, Check.isin({"basic", "standard", "premium"})),
                "id_assicurazione": pa.Column(str),
                "contatto_emergenza_nome": pa.Column(str),
                "contatto_emergenza_telefono": pa.Column(str),
                "altezza_cm": pa.Column(int, Check.between(140, 200)),
                "peso_kg": pa.Column(int, Check.between(45, 120)),
                "gruppo_sanguigno": pa.Column(str, Check.isin({"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"})),
            }
        ),
        "staff": pa.DataFrameSchema(
            {
                "id_staff": pa.Column(str),
                "nome": pa.Column(str),
                "cognome": pa.Column(str),
                "ruolo": pa.Column(str),
                "reparto": pa.Column(str),
                "tipo_impiego": pa.Column(str, Check.isin({"Tempo pieno", "Part-time", "Contratto"})),
                "email": pa.Column(str),
                "telefono": pa.Column(str),
                "id_licenza": pa.Column(str),
                "data_assunzione": pa.Column(
                    pa.DateTime,
                    Check.between(date_2010_start, date_2024_end),
                    coerce=True,
                ),
            }
        ),
        "staff_assignments": pa.DataFrameSchema(
            {
                "id_assegnazione": pa.Column(str),
                "id_staff": pa.Column(str),
                "id_reparto": pa.Column(str),
                "turno": pa.Column(str, Check.isin({"Giorno", "Notte", "Sera"})),
            }
        ),
        "devices": pa.DataFrameSchema(
            {
                "id_dispositivo": pa.Column(str),
                "id_reparto": pa.Column(str),
                "tipo_dispositivo": pa.Column(str, Check.isin({"ECG", "Pulsossimetro", "Sfigmomanometro", "Termometro"})),
                "produttore": pa.Column(str),
                "modello": pa.Column(str),
                "numero_serie": pa.Column(str),
                "stato": pa.Column(str, Check.isin({"Attivo", "Manutenzione", "Ritirato"})),
                "data_acquisto": pa.Column(
                    pa.DateTime,
                    Check.between(date_2018_start, date_2024_end),
                    coerce=True,
                ),
                "data_ultima_calibrazione": pa.Column(
                    pa.DateTime,
                    Check.between(date_2024_start, date_2026_end),
                    coerce=True,
                ),
            }
        ),
        "admissions": pa.DataFrameSchema(
            {
                "id_ricovero": pa.Column(str),
                "id_paziente": pa.Column(str),
                "id_reparto": pa.Column(str),
                "data_ricovero": pa.Column(pa.DateTime, coerce=True),
                "data_dimissione": pa.Column(pa.DateTime, coerce=True),
                "durata_degenza_giorni": pa.Column(int, Check.between(1, 30)),
                "tipo_ricovero": pa.Column(str, Check.isin({"Emergenza", "Elettivo", "Urgente"})),
                "provenienza_ricovero": pa.Column(str, Check.isin({"PS", "Invio", "Trasferimento"})),
                "esito_dimissione": pa.Column(str, Check.isin({"Domicilio", "Trasferimento", "Riabilitazione", "Deceduto"})),
            }
        ),
        "diagnoses": pa.DataFrameSchema(
            {
                "id_diagnosi": pa.Column(str),
                "id_ricovero": pa.Column(str),
                "codice_icd10": pa.Column(str, Check.isin({"I10", "E11", "J18", "K21", "M54", "N39"})),
                "gravita": pa.Column(str, Check.isin({"bassa", "media", "alta"})),
            }
        ),
        "vital_signs": pa.DataFrameSchema(
            {
                "id_misurazione": pa.Column(str),
                "id_paziente": pa.Column(str),
                "id_dispositivo": pa.Column(str),
                "data_misurazione": pa.Column(
                    pa.DateTime,
                    Check.between(date_2025_start, date_2026_end),
                    coerce=True,
                ),
                "frequenza_cardiaca": pa.Column(int, Check.between(50, 120)),
                "saturazione_ossigeno": pa.Column(int, Check.between(90, 100)),
                "pressione_sistolica": pa.Column(int, Check.between(95, 160)),
                "pressione_diastolica": pa.Column(int, Check.between(60, 100)),
                "temperatura_c": pa.Column(float, Check.between(35.0, 40.5)),
                "frequenza_respiratoria": pa.Column(int, Check.between(10, 30)),
                "glicemia_mg_dl": pa.Column(int, Check.between(70, 180)),
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
    admissions["data_ricovero"] = pd.to_datetime(admissions["data_ricovero"], errors="coerce")
    admissions["data_dimissione"] = pd.to_datetime(admissions["data_dimissione"], errors="coerce")
    invalid_admissions = admissions[admissions["data_ricovero"] > admissions["data_dimissione"]]
    if not invalid_admissions.empty:
        examples = invalid_admissions[["id_ricovero", "data_ricovero", "data_dimissione"]].head(5)
        raise AssertionError(
            "admissions: data_ricovero must be <= data_dimissione. Examples:\n" + examples.to_string(index=False)
        )

    if "durata_degenza_giorni" in admissions.columns:
        los_days = (admissions["data_dimissione"] - admissions["data_ricovero"]).dt.days
        mismatch = admissions["durata_degenza_giorni"].notna() & los_days.notna() & (admissions["durata_degenza_giorni"] != los_days)
        if mismatch.any():
            examples = admissions.loc[mismatch, ["id_ricovero", "durata_degenza_giorni"]].head(5)
            raise AssertionError(
                "admissions: durata_degenza_giorni must match data_dimissione-data_ricovero in days. Examples:\n"
                + examples.to_string(index=False)
            )

    devices = tables["devices"].copy()
    devices["data_acquisto"] = pd.to_datetime(devices["data_acquisto"], errors="coerce")
    devices["data_ultima_calibrazione"] = pd.to_datetime(devices["data_ultima_calibrazione"], errors="coerce")
    invalid_devices = devices[devices["data_ultima_calibrazione"] < devices["data_acquisto"]]
    if not invalid_devices.empty:
        examples = invalid_devices[["id_dispositivo", "data_acquisto", "data_ultima_calibrazione"]].head(5)
        raise AssertionError(
            "devices: data_ultima_calibrazione must be >= data_acquisto. Examples:\n"
            + examples.to_string(index=False)
        )
