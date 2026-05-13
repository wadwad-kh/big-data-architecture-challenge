# Bonus Section

## 1. Find a contradiction
*   **The Contradiction:** Storing 5 years of history (2 PB) while enforcing a strict $40,000/month budget cap and maintaining sub-30-second latency for a massive firehose is incredibly tight, especially considering the LLM bill is taking up $18,000 immediately. 2 PB of standard cloud storage alone could easily cost $40,000+.
*   **The Pushback:** I would negotiate with the CEO to drastically reduce the historical data retention requirement (e.g., to 1 year instead of 5), or accept that any data older than 6 months must be moved to the absolute cheapest, slowest archival storage (like AWS S3 Glacier Deep Archive) where query retrieval times are measured in hours, not seconds.

## 2. Multi-region DR (Disaster Recovery)
*   **Data Flow:** To add a second region, we deploy an active-passive setup. We use Kafka's MirrorMaker 2 (or Confluent Cluster Linking) to asynchronously replicate the primary region's Kafka cluster to the DR region. S3 buckets are configured for Cross-Region Replication (CRR).
*   **RPO (Recovery Point Objective):** ~5 minutes (the time it takes for async replication to sync the latest events and S3 objects).
*   **RTO (Recovery Time Objective):** ~30 minutes (the time required to spin up the Flink and ClickHouse compute clusters in the DR region, as keeping them "hot" and running 24/7 would violate the budget).

## 3. Schema evolution
*   **Scenario:** A partner adds a new field next month.
*   **Handling it:** We enforce a Schema Registry (e.g., Confluent Schema Registry using Avro or Protobuf) at the Kafka ingestion layer. We use **Forward Compatibility** rules. The Flink jobs are designed to ignore unknown fields. When the new field arrives, it safely passes through Kafka and is appended as a new column in our Parquet (S3) and ClickHouse tables without requiring any downtime or breaking existing dashboard queries.
