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
        "pazienti": ehr_dir / "pazienti",
        "ricoveri": ehr_dir / "ricoveri",
        "diagnosi": ehr_dir / "diagnosi",
        "reparti": erp_dir / "reparti",
        "personale": erp_dir / "personale",
        "assegnazioni": erp_dir / "assegnazioni",
        "dispositivi": iot_dir / "dispositivi",
        "parametri_vitali": iot_dir / "parametri_vitali",
    }

    for table_name, base_path in mapping.items():
        df = tables[table_name]
        if fmt == "csv":
            df.to_csv(f"{base_path}.csv", index=False)
        else:
            df.to_parquet(f"{base_path}.parquet", index=False)

    print(f"Export completed in: {out_dir.resolve()}")
