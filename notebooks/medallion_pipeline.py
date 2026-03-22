# Databricks notebook source
# Cell 1 — Setup using Unity Catalog managed tables

from pyspark.sql.functions import (
    col, trim, upper, when, to_timestamp,
    count, avg, lit, current_timestamp
)

# Update TIMESTAMP with your actual table name
TABLE_NAME = "workspace.default.news_sentiment_20260321_043345"

# Use default schema — Unity Catalog managed tables
CATALOG  = "workspace"
SCHEMA   = "default"
BRONZE   = f"{CATALOG}.{SCHEMA}.news_bronze"
SILVER   = f"{CATALOG}.{SCHEMA}.news_silver"
GOLD_SRC = f"{CATALOG}.{SCHEMA}.news_gold_by_source"
GOLD_DST = f"{CATALOG}.{SCHEMA}.news_gold_sentiment_dist"
GOLD_HC  = f"{CATALOG}.{SCHEMA}.news_gold_high_confidence"

# Quick check — preview the source table
print(f"Source: {TABLE_NAME}")
spark.sql(f"SELECT * FROM {TABLE_NAME} LIMIT 3").show(truncate=50)
print("Setup complete!")

# COMMAND ----------

# Cell 2 — Bronze Layer: raw ingestion as managed table

bronze_df = spark.sql(f"SELECT * FROM {TABLE_NAME}") \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_file", lit(TABLE_NAME))

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(BRONZE)

print(f"Bronze: {spark.table(BRONZE).count()} records written to {BRONZE}")
spark.table(BRONZE).printSchema()

# COMMAND ----------

# Cell 3 — Silver Layer: clean and validate

silver_df = spark.table(BRONZE) \
    .filter(col("title").isNotNull()) \
    .filter(col("sentiment").isNotNull()) \
    .withColumn("title", trim(col("title"))) \
    .withColumn("source", trim(col("source"))) \
    .withColumn("summary", trim(col("summary"))) \
    .withColumn("sentiment", upper(trim(col("sentiment")))) \
    .withColumn("confidence", upper(trim(col("confidence")))) \
    .withColumn("sentiment_score",
        when(col("sentiment") == "POSITIVE", lit(1))
       .when(col("sentiment") == "NEGATIVE", lit(-1))
       .otherwise(lit(0))
    ) \
    .withColumn("published_at", to_timestamp(col("published_at"))) \
    .withColumn("processed_at", current_timestamp()) \
    .drop("ingested_at", "source_file")

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(SILVER)

print(f"Silver: {spark.table(SILVER).count()} records written to {SILVER}")
spark.table(SILVER).show(5, truncate=50)

# COMMAND ----------

# Cell 4 — Gold Layer: analytics aggregations

silver = spark.table(SILVER)

# Gold 1: by source
silver.groupBy("source", "sentiment") \
    .agg(count("*").alias("article_count"),
         avg("sentiment_score").alias("avg_sentiment_score")) \
    .orderBy("source") \
    .write.format("delta").mode("overwrite").saveAsTable(GOLD_SRC)

# Gold 2: sentiment distribution
silver.groupBy("sentiment") \
    .agg(count("*").alias("article_count"),
         avg("sentiment_score").alias("avg_score")) \
    .orderBy("sentiment") \
    .write.format("delta").mode("overwrite").saveAsTable(GOLD_DST)

# Gold 3: high confidence only
silver.filter(col("confidence") == "HIGH") \
    .select("title", "source", "sentiment", "key_topics", "summary", "url") \
    .write.format("delta").mode("overwrite").saveAsTable(GOLD_HC)

print("Gold layer complete! 3 tables written.")
print("\nSentiment distribution:")
spark.table(GOLD_DST).show()
print("\nBy source:")
spark.table(GOLD_SRC).show()

# COMMAND ----------

# Cell 5 — Delta Lake history and time travel

from delta.tables import DeltaTable

# Bronze history
print("=== Bronze History ===")
DeltaTable.forName(spark, BRONZE).history().show(5, truncate=50)

# Silver history
print("=== Silver History ===")
DeltaTable.forName(spark, SILVER).history().show(5, truncate=50)

# Time travel — read Bronze at version 0
print("=== Time Travel — Bronze at version 0 ===")
spark.read \
    .format("delta") \
    .option("versionAsOf", "0") \
    .table(BRONZE) \
    .show(3, truncate=50)

# COMMAND ----------

# Cell 6 — Pipeline summary

print("="*50)
print("  Medallion Pipeline — Summary")
print("="*50)
print(f"  Bronze (raw)            : {spark.table(BRONZE).count()} records")
print(f"  Silver (cleaned)        : {spark.table(SILVER).count()} records")
print(f"  Gold   (by source)      : {spark.table(GOLD_SRC).count()} records")
print(f"  Gold   (sentiment dist) : {spark.table(GOLD_DST).count()} records")
print(f"  Gold   (high confidence): {spark.table(GOLD_HC).count()} records")
print("="*50)
print("  Pipeline complete!")