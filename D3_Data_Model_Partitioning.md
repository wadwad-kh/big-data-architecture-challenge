# D3. Data Model and Partitioning

## 1. Raw Data in Cold Storage (S3)
*   **Layout & Partition Keys:** Data is partitioned by `year=YYYY/month=MM/day=DD/source_type=SS/`. This optimizes for time-bound historical scans, which is how 5-year data is usually accessed.
*   **File Format:** Apache Parquet with Snappy compression. Parquet is a columnar format that drastically reduces storage size and scan costs for analytic queries.
*   **File Size:** We aim for 128 MB to 256 MB file sizes using Flink's StreamingFileSink rolling policies to avoid the "small files problem" in S3, which ruins read performance.

## 2. Hot Serving Data (ClickHouse)
*   **Layout & Sort Keys:** The primary table uses a `MergeTree` engine. The `ORDER BY` (Sort Key) is `(topic, timestamp, source_id)`. ClickHouse physically sorts data on disk by this key, allowing lightning-fast range scans.
*   **Indexes:** A secondary bloom filter index on `author_id` and `keywords` for quick text/point lookups without scanning whole columns.
*   **Denormalization:** The data is heavily denormalized into a wide table. We do not do JOINs for the live dashboard. Articles, source metadata, and pre-computed sentiment are all in one wide row.

## 3. Justification Example Query
*   **Analyst Query:** *"Show me the count of articles about 'Politics' from Source 'CNN' over the last 7 days."*
*   **Why it's fast:** In ClickHouse (Hot), the engine uses the `ORDER BY (topic, timestamp)` sort key to instantly jump to the 'Politics' section of the disk, and then binary searches the last 7 days of timestamps. It only reads the `article_id` column (columnar format) and ignores all text/body columns, returning the count in milliseconds instead of minutes.
