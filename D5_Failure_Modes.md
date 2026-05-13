# D5. Failure Modes

## 1. Ingestion lag spikes (3x normal volume)
*   **What breaks:** Processing latency spikes. Flink might struggle to process windows in real-time, meaning the dashboard updates will fall behind the 30-second target.
*   **What keeps working:** No data is lost. Kafka acts as a massive shock absorber, buffering the 3x volume on disk.
*   **How to recover:** Flink auto-scales (via Kubernetes/YARN) by adding more TaskManagers to burn through the Kafka lag, eventually catching up to real-time.

## 2. One cloud availability zone (AZ) goes down
*   **What breaks:** Temporary loss of a subset of compute nodes. Some Flink tasks will fail, and some Kafka broker connections will drop.
*   **What keeps working:** The system as a whole. Kafka is deployed with a replication factor of 3 across 3 AZs. ClickHouse is also replicated across AZs.
*   **How to recover:** Flink restarts the failed jobs from the last successful checkpoint in the surviving AZs. Traffic is automatically routed to the remaining healthy Kafka brokers.

## 3. A Kafka broker dies
*   **What breaks:** Any producers or consumers directly connected to the leader partitions on that specific broker will experience a momentary timeout.
*   **What keeps working:** The cluster stays up.
*   **How to recover:** Kafka's controller automatically elects new leaders for the partitions that were on the dead broker from the surviving replicas. Clients automatically reconnect to the new leaders within seconds.

## 4. The LLM API rate-limits you for an hour
*   **What breaks:** Live dashboard summaries stop generating.
*   **What keeps working:** Ingestion, data storage, ad-hoc analytics, and basic event dashboarding all work perfectly.
*   **How to recover:** Flink uses a Dead Letter Queue (DLQ) or simply caches the prompts in Kafka for the summarization stream. Once the rate limit lifts, Flink re-processes the backlog. We also degrade the UI gracefully (show "Summary temporarily unavailable").
