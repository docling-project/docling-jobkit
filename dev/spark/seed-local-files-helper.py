from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Two partitions across three docs.
docs = {
    "sample_doc1.md": "2024-01-01",
    "sample_doc2.md": "2024-01-01",
    "sample_doc3.md": "2024-01-02",
}
rows = []
for name, dt in docs.items():
    with open(name, "rb") as f:
        rows.append((bytearray(f.read()), name, dt))

new_df = spark.createDataFrame(
    rows, schema="content binary, filename string, dt string"
)
new_df.write.mode("overwrite").partitionBy("dt").format("parquet").saveAsTable(
    "docling_docs"
)

print("seeded rows:", spark.table("docling_docs").count())
spark.table("docling_docs").select("filename", "dt").show()
