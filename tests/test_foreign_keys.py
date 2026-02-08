from __future__ import annotations

import healthdata_synthetic_generator.data_quality as dq


def test_foreign_keys_integrity() -> None:
    tables = dq.load_all_tables()
    dq.validate_foreign_keys(tables)
