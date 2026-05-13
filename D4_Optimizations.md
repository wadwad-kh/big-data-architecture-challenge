# D4. Three Optimization Decisions

## Problem (a) — The Firehose Skew
*1% of accounts produce 60% of messages, crushing specific partitions.*
**What I would do:** I would use "Salting" for the partition key in Kafka. Instead of partitioning purely by `account_id`, I will partition by a composite key: `hash(account_id) + random(1, N)`, where N is the level of spread needed.
**Why:** This forces the massive volume from a single celebrity account to be distributed evenly across multiple Kafka partitions and downstream Flink workers. To ensure account-level analytics don't break, the Flink aggregation step will use a two-phase aggregation: first, it locally aggregates the salted keys (e.g., `account_A_1`, `account_A_2`), and then a second global window merges these partial counts into a final `account_A` count.

## Problem (c) — The LLM Bill
*LLM costs $18,000/month. How to optimize without stopping it.*
**What I would do:** Implement aggressive batching and extractive summarization *before* the LLM. Flink will group all articles for a specific topic within a 5-minute window into a single prompt block, deduplicate exact identical news wires, and strip out HTML/boilerplate text to aggressively reduce the token count. Furthermore, we will implement a semantic cache (like Redis) to check if a highly similar cluster of articles was already summarized recently.
**Why:** LLMs charge per token. Batching 50 similar articles into one prompt using a smaller, cheaper model (like Claude 3 Haiku or GPT-4o-mini) rather than 50 separate API calls reduces prompt overhead tokens. Deduplication and boilerplate removal minimize the input token length, drastically slashing the bill while retaining the product feature.

## Problem (e) — Slow Dashboard
*Dashboard p95 latency is 12s, target is 2s. Charts compute from scratch.*
**What I would do:** Introduce a Materialized View layer in ClickHouse for the exact metrics the dashboard displays.
**Why:** If the dashboard always queries "Top 10 topics in the last 1 hour", it shouldn't scan 50TB of raw data. A ClickHouse Materialized View will pre-aggregate this data incrementally as it arrives from Flink. The dashboard will query this tiny pre-aggregated table, bringing the latency down from 12 seconds to milliseconds. I would measure the 95th percentile query latency before and after the MV implementation to prove the 2s target is met.
