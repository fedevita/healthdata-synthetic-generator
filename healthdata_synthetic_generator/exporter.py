"""Export helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


def export_tables(tables: Dict[str, pd.DataFrame], out_dir: Path, fmt: str) -> None:
    ehr_dir = out_dir / "ehr"
    erp_dir = out_dir / "erp"
    iot_dir = out_dir / "iot"
    ehr_dir.mkdir(parents=True, exist_ok=True)
    erp_dir.mkdir(parents=True, exist_ok=True)
    iot_dir.mkdir(parents=True, exist_ok=True)

    mapping = {
        "patients": ehr_dir / "patients",
        "admissions": ehr_dir / "admissions",
        "diagnoses": ehr_dir / "diagnoses",
        "wards": erp_dir / "wards",
        "staff": erp_dir / "staff",
        "staff_assignments": erp_dir / "staff_assignments",
        "devices": iot_dir / "devices",
        "vital_signs": iot_dir / "vital_signs",
    }

    for table_name, base_path in mapping.items():
        df = tables[table_name]
        if fmt == "csv":
            df.to_csv(f"{base_path}.csv", index=False)
        else:
            df.to_parquet(f"{base_path}.parquet", index=False)

    print(f"Export completed in: {out_dir.resolve()}")
