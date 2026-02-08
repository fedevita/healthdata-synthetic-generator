from __future__ import annotations

import healthdata_synthetic_generator.data_quality as dq


def test_primary_keys() -> None:
    tables = dq.load_all_tables()
    dq.validate_primary_keys(tables)
