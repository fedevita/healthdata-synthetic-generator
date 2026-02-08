from __future__ import annotations

import healthdata_synthetic_generator.data_quality as dq


def test_domain_constraints() -> None:
    tables = dq.load_all_tables()
    dq.validate_domain_constraints(tables)
