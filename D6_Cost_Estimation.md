# D6. Cost Back-of-Envelope

**Budget Cap:** $40,000 / month

**1. LLM API (Fixed Cost Constraint)**
*   From Problem (c), we know the LLM step costs **$18,000 / month**.
*   Remaining Budget: $22,000 / month.

**2. Hot Storage (ClickHouse on Cloud VMs/EBS)**
*   Data: 50 TB.
*   Assuming standard SSD pricing (e.g., AWS gp3) is around $0.08 / GB / month.
*   50,000 GB * $0.08 = $4,000 / month for EBS storage.
*   Compute for ClickHouse (e.g., 4x large instances with 64 vCPUs + 256GB RAM to support 500 analysts): Approx. 4 * $1,200 = $4,800 / month.
*   *Total Hot Storage & Compute:* **~$8,800 / month**.

**3. Cold Storage (Amazon S3)**
*   Data: 2 PB (2,000,000 GB).
*   Since it is "rarely touched," we can use S3 Glacier Instant Retrieval (approx. $0.004 / GB / month) or S3 Standard-IA ($0.0125). Let's assume standard cold storage average of $0.005 / GB / month.
*   2,000,000 GB * $0.005 = **$10,000 / month**.

**4. Ingestion & Stream Processing (Kafka + Flink)**
*   Kafka Cluster (Compute + EBS for short retention): ~$1,500 / month.
*   Flink Cluster (Compute-heavy for sub-30s processing and 5-min windowing): ~$1,500 / month.
*   *Total Ingestion/Streaming:* **~$3,000 / month**.

**Total Monthly Cost Estimation:**
*   LLM API: $18,000
*   Hot Storage (50TB + Compute): $8,800
*   Cold Storage (2PB): $10,000
*   Ingestion/Streaming Compute: $3,000
*   **Total:** **$39,800 / month**

*Conclusion:* We are under the **$40,000/month** hard cap by efficiently utilizing cheap cold storage (Glacier/S3) for the 2PB archive, allowing us to afford the expensive LLM API and high-performance ClickHouse compute required for the 500 analysts.
