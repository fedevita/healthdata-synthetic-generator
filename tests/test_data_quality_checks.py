from __future__ import annotations

import healthdata_synthetic_generator.data_quality as dq


def test_files_exist() -> None:
    dq.validate_files_exist()


def test_primary_keys() -> None:
    tables = dq.load_all_tables()
    dq.validate_primary_keys(tables)


def test_foreign_keys_integrity() -> None:
    tables = dq.load_all_tables()
    dq.validate_foreign_keys(tables)


def test_domain_constraints() -> None:
    tables = dq.load_all_tables()
    dq.validate_domain_constraints(tables)
