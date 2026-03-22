# Medallion News Pipeline

A Medallion Architecture (Bronze–Silver–Gold) data pipeline
built on Databricks Community Edition using PySpark and Delta Lake.
Processes AI-enriched news data from the News Sentiment Pipeline project.

---

## Architecture

```
Raw News CSV
    ↓
Bronze Layer  →  Raw ingestion with metadata (Delta Lake)
    ↓
Silver Layer  →  Cleaned, validated, typed data (Delta Lake)
    ↓
Gold Layer    →  Analytics-ready aggregations (Delta Lake)
    ├── news_gold_by_source
    ├── news_gold_sentiment_dist
    └── news_gold_high_confidence
```

---

## Pipeline output

| Layer  | Table                        | Records | Description                  |
|--------|------------------------------|---------|------------------------------|
| Bronze | news_bronze                  | 9       | Raw articles + ingestion meta |
| Silver | news_silver                  | 9       | Cleaned, typed, scored        |
| Gold   | news_gold_by_source          | 8       | Sentiment by news source      |
| Gold   | news_gold_sentiment_dist     | 3       | Overall sentiment breakdown   |
| Gold   | news_gold_high_confidence    | 7       | High confidence articles only |

---

## Key concepts demonstrated

- Medallion Architecture (Bronze → Silver → Gold)
- Delta Lake managed tables via Unity Catalog
- PySpark transformations and aggregations
- Delta Lake time travel and audit history
- Derived feature engineering (sentiment_score)
- Data quality filtering between layers

---

## Tech stack

- Databricks Community Edition
- PySpark
- Delta Lake
- Unity Catalog
- Python

---

## How to run

1. Upload a news sentiment CSV to Databricks (from the
   News Sentiment AI Pipeline project)
2. Import `notebooks/medallion_pipeline.py` into Databricks
3. Attach to a cluster and run all cells top to bottom

---

## Related projects

- [News Sentiment AI Pipeline](https://github.com/facebookym/news-sentiment-ai-pipeline)
  — generates the source data used in this pipeline
- [RAG News Chat](https://github.com/facebookym/rag-news-chat)
  — chat interface over the same news data

---

## Author

**Mark Anthony Phua, CAPM** — AI Data Engineer
phuamarkr@gmail.com | Cebu City, Philippines