from __future__ import annotations

import healthdata_synthetic_generator.data_quality as dq


def test_files_exist() -> None:
    dq.validate_files_exist()
