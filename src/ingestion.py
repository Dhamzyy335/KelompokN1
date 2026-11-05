"""PySpark ingestion pipeline for the NBA/WNBA play-by-play & shot-detail dataset.

Pipeline stages, matching the functions below:
    1. inspect_schema   - peek at one sample file per family
    2. ingest_family    - read every file of a family and union them into one DataFrame
    3. sanity_check      - row counts + null counts per column
    4. save_family_parquet - persist a family DataFrame to partitioned Parquet
"""
from __future__ import annotations

import json
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils import FileInfo


def inspect_schema(spark: SparkSession, family: str, files: list[FileInfo], sample_rows: int = 5) -> dict:
    """Load one sample file for `family`, print its schema/sample rows, and
    return a JSON-serializable summary (also used for output/schema_summary.json).
    """
    sample_file = files[0]
    df = spark.read.csv(sample_file.path, header=True, inferSchema=True)

    print(f"\n=== Family: {family} (sample: {sample_file.filename}) ===")
    df.printSchema()
    df.show(sample_rows, truncate=False)
    row_count = df.count()
    print(f"Row count (sample file only): {row_count}")

    return {
        "family": family,
        "sample_file": sample_file.filename,
        "columns": [{"name": name, "type": dtype} for name, dtype in df.dtypes],
        "sample_row_count": row_count,
        "num_files_in_family": len(files),
    }


def save_schema_summary(summary: list[dict], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def ingest_family(spark: SparkSession, family: str, files: list[FileInfo]) -> tuple[DataFrame, dict]:
    """Read every CSV belonging to `family`, tag each row with season/game_type,
    and union everything into a single DataFrame.

    Files are read one by one (not via a single wildcard path) because schemas
    drift across seasons for some families (e.g. cdnnba, datanba) - reading
    individually lets us inferSchema per file and unionByName with
    allowMissingColumns, instead of risking silent column misalignment.
    """
    per_file_schemas: dict[str, list[str]] = {}
    unioned_df: DataFrame | None = None

    for file_info in files:
        df = (
            spark.read.csv(file_info.path, header=True, inferSchema=True)
            .withColumn("season", F.lit(file_info.season))
            .withColumn("game_type", F.lit(file_info.game_type))
        )
        per_file_schemas[file_info.filename] = sorted(df.columns)

        unioned_df = df if unioned_df is None else unioned_df.unionByName(df, allowMissingColumns=True)

    distinct_schemas = {tuple(cols) for cols in per_file_schemas.values()}
    schema_report = {
        "family": family,
        "num_files": len(files),
        "distinct_schema_variants": len(distinct_schemas),
        "schema_consistent": len(distinct_schemas) == 1,
    }

    return unioned_df, schema_report


def ingest_all(spark: SparkSession, families: dict[str, list[FileInfo]]) -> tuple[dict[str, DataFrame], list[dict]]:
    """Run ingest_family for every family. Returns (dataframes, schema_reports)."""
    dataframes = {}
    schema_reports = []

    for family, files in sorted(families.items()):
        df, report = ingest_family(spark, family, files)
        dataframes[family] = df
        schema_reports.append(report)

    return dataframes, schema_reports


def sanity_check(family: str, df: DataFrame) -> dict:
    """Compute total row count and null count per column for one family DataFrame."""
    total_rows = df.count()

    null_counts_row = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).collect()[0]
    null_counts = {c: null_counts_row[c] for c in df.columns}

    return {
        "family": family,
        "total_rows": total_rows,
        "null_counts": null_counts,
    }


def save_family_parquet(df: DataFrame, family: str, output_dir: str) -> str:
    """Write a family DataFrame as Parquet, partitioned by `season`."""
    family_path = os.path.join(output_dir, family)
    df.write.mode("overwrite").partitionBy("season").parquet(family_path)
    return family_path
