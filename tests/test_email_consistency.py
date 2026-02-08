from __future__ import annotations

import re

import healthdata_synthetic_generator.data_quality as dq


def _normalize(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z]", "", value or "")
    return cleaned.lower() if cleaned else "utente"


def _email_prefix(nome: str, cognome: str) -> str:
    return f"{_normalize(nome)}.{_normalize(cognome)}"


def _email_local_matches(nome: str, cognome: str, email: str) -> bool:
    local = email.split("@", 1)[0]
    base = _email_prefix(nome, cognome)
    if not local.startswith(base):
        return False
    suffix = local[len(base):]
    return suffix.isdigit() or suffix == ""


def test_patient_emails_match_names() -> None:
    tables = dq.load_all_tables()
    patients = tables["patients"]
    prefixes = patients.apply(
        lambda row: _email_prefix(row["nome"], row["cognome"]), axis=1
    )
    for (email, prefix), nome, cognome in zip(
        zip(patients["email"].astype(str), prefixes),
        patients["nome"].astype(str),
        patients["cognome"].astype(str),
    ):
        assert _email_local_matches(nome, cognome, email), f"Email non coerente: {email}"


def test_staff_emails_match_names() -> None:
    tables = dq.load_all_tables()
    staff = tables["staff"]
    prefixes = staff.apply(
        lambda row: _email_prefix(row["nome"], row["cognome"]), axis=1
    )
    for (email, prefix), nome, cognome in zip(
        zip(staff["email"].astype(str), prefixes),
        staff["nome"].astype(str),
        staff["cognome"].astype(str),
    ):
        assert _email_local_matches(nome, cognome, email), f"Email non coerente: {email}"
