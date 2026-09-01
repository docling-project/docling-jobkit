# NOTE: this only works for the databricks classic path and does not work with serverless
# Util script for testing. Seed a Databricks source table from real local files over Spark Connect
#
# Usage:
#   DATABRICKS_HOST=adb-....azuredatabricks.net \
#   DATABRICKS_TOKEN=dapi... \
#   DATABRICKS_CLUSTER_ID=0809-... \
#   uv run dev/spark/seed-databricks-file-helper.py sample_doc1.md sample_doc2.md
# Configs:
#   - You can also set the table name to seed it as by doing TABLE=... since it defaults to
#   jobkit_connector.default.docling_prod_like
# Bare filenames that aren't found are looked up next to this script (dev/spark/),
# where the sample_doc*.md files live.

import os
import sys
from datetime import date
from pathlib import Path

from pydantic import SecretStr

HOST = os.environ.get("DATABRICKS_HOST", "xxx")
PORT = int(os.environ.get("DATABRICKS_PORT", "443"))
TOKEN = os.environ.get("DATABRICKS_TOKEN", "xxx")
CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "xxx")
TABLE = os.environ.get("SEED_TABLE", "jobkit_connector.default.smoke_output1")
INGEST_DATE = os.environ.get("SEED_INGEST_DATE", "2024-01-01")


def _resolve(name: str) -> Path:
    """Use the path as given; fall back to dev/spark/<name> for bare filenames."""
    p = Path(name)
    if p.exists():
        return p
    sibling = Path(__file__).parent / name
    if sibling.exists():
        return sibling
    print(f"Error: file not found: {name}")
    sys.exit(1)


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "Usage: uv run python dev/spark/seed-databricks-file-helper.py "
            "<file> [<file> ...]"
        )
        sys.exit(1)

    from docling_jobkit.connectors.spark import (
        get_spark_session,
        is_spark_authentication_error,
    )
    from docling_jobkit.datamodel.spark_coords import DatabricksAuth, SparkConnection

    ingest_date = date.fromisoformat(INGEST_DATE)

    rows = []
    for arg in sys.argv[1:]:
        path = _resolve(arg)
        rows.append((bytearray(path.read_bytes()), path.name, ingest_date))

    conn = SparkConnection(
        host=HOST,
        port=PORT,
        auth=DatabricksAuth(token=SecretStr(TOKEN), cluster_id=CLUSTER_ID),
    )

    print(f"Connecting to {HOST} (cluster {CLUSTER_ID})...")
    try:
        spark = get_spark_session(conn)

        seed_df = spark.createDataFrame(
            rows, schema="file_bytes binary, source_name string, ingest_date date"
        )
        (seed_df.write.mode("overwrite").partitionBy("ingest_date").saveAsTable(TABLE))

        seeded = spark.table(TABLE)
        print(f"Seeded {seeded.count()} row(s) into {TABLE}:")
        seeded.select("source_name", "ingest_date").show(truncate=False)
    except Exception as exc:
        if is_spark_authentication_error(exc):
            print(
                "Authentication failed: check DATABRICKS_TOKEN / DATABRICKS_CLUSTER_ID."
            )
        else:
            print(f"Seeding failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
