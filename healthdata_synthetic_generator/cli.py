"""Command-line interface for synthetic dataset generation."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import sdv

from .exporter import export_tables
from .pipeline import build_metadata, enforce_admission_order, fit_and_sample
from .seed import build_seed_tables
from .validation import validate_synthetic_tables
from .utils import log_table_counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic healthcare datasets with SDV.")
    parser.add_argument("--out-dir", default="out", help="Output directory (default: out)")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Export format")
    parser.add_argument("--scale", type=float, default=2.0, help="Sampling scale factor (default: 2.0)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"SDV version: {sdv.__version__}")

    table_order = [
        "wards",
        "patients",
        "staff",
        "staff_assignments",
        "devices",
        "admissions",
        "diagnoses",
        "vital_signs",
    ]

    real_tables = build_seed_tables(rng)
    log_table_counts("Seed", real_tables, table_order)

    metadata_path = out_dir / "metadata.json"
    metadata = build_metadata(real_tables, metadata_path)

    synthetic_tables = fit_and_sample(real_tables, metadata, scale=args.scale)
    enforce_admission_order(synthetic_tables, rng)
    log_table_counts("Synthetic", synthetic_tables, table_order)

    validate_synthetic_tables(synthetic_tables)
    print("FK integrity validated.")

    export_tables(synthetic_tables, out_dir, fmt=args.format)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
