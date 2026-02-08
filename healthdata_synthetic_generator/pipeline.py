"""SDV pipeline orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
from sdv.metadata import Metadata
from sdv.multi_table import HMASynthesizer


def build_metadata(real_tables: Dict[str, pd.DataFrame], metadata_path: Path) -> Metadata:
    if metadata_path.exists():
        metadata_path.unlink()
    metadata = Metadata.detect_from_dataframes(data=real_tables)
    metadata.save_to_json(metadata_path)
    return Metadata.load_from_json(metadata_path)


def fit_and_sample(
    real_tables: Dict[str, pd.DataFrame],
    metadata: Metadata,
    scale: float,
) -> Dict[str, pd.DataFrame]:
    synthesizer = HMASynthesizer(metadata)
    synthesizer.fit(real_tables)
    return synthesizer.sample(scale=scale)


def enforce_admission_order(tables: Dict[str, pd.DataFrame], rng: np.random.Generator) -> None:
    admissions = tables.get("admissions")
    if admissions is None or admissions.empty:
        return

    admit_ts = pd.to_datetime(admissions["admit_ts"], errors="coerce")
    discharge_ts = pd.to_datetime(admissions["discharge_ts"], errors="coerce")
    invalid = admit_ts.notna() & discharge_ts.notna() & (discharge_ts < admit_ts)
    if invalid.any():
        offsets = rng.integers(1, 15, size=int(invalid.sum()), dtype=np.int64)
        discharge_ts.loc[invalid] = admit_ts.loc[invalid] + pd.to_timedelta(offsets, unit="D")
        admissions["discharge_ts"] = discharge_ts
