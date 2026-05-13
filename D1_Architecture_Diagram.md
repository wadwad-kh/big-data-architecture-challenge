# D1. Architecture Diagram Instructions

> **Note for you:** You need to actually draw this on Excalidraw or draw.io and export it as a PDF/Image for the final submission. Here is the exact structure you should draw, with boxes and arrows.

## Components to Draw:

### 1. Ingestion Layer (Left Side)
*   **Box 1:** "2,000 RSS Feeds & 5 Partner APIs" (Slow/Steady sources)
*   **Box 2:** "Social Media Firehose" (Peak: 100,000 msgs/sec)
*   **Arrows:** Both boxes point to -> **Apache Kafka (Message Bus)**

### 2. Message Bus
*   **Box 3:** "Apache Kafka" (Central Hub)
    *   *Text inside/near:* "Handles 50k articles/min & 100k peaks. Decouples ingestion from processing."
*   **Arrows:** Kafka points to -> **Apache Flink (Stream Processing)**

### 3. Stream Processing
*   **Box 4:** "Apache Flink" 
    *   *Text inside/near:* "Sub-30s latency processing, 5-min tumbling windows for LLM."
*   **Arrows from Flink:** 
    *   Arrow pointing down to -> **Amazon S3 (Cold Storage)**
    *   Arrow pointing right to -> **ClickHouse (Hot Storage)**
    *   Arrow pointing up to -> **LLM API**

### 4. LLM API (Top)
*   **Box 5:** "LLM API (Batch Prompting)"
    *   *Text inside/near:* "Receives 5-minute batches of articles per topic from Flink. Returns summaries."
*   **Arrows:** LLM API points back to -> **Flink** (which then writes it to ClickHouse) or directly to **ClickHouse**. (Draw arrow from LLM back to Flink).

### 5. Storage Layer
*   **Box 6 (Bottom):** "Amazon S3 (Cold Storage)"
    *   *Text inside/near:* "2 PB, 5 years history. Parquet format."
*   **Box 7 (Right):** "ClickHouse (Hot Storage)"
    *   *Text inside/near:* "50 TB, last 30 days. High-speed columnar analytics."

### 6. Serving Layer & Users (Far Right)
*   **Box 8:** "Grafana / Custom Dashboard"
    *   *Text inside/near:* "Sub-30s live events & LLM summaries."
*   **Box 9:** "500 Concurrent Analysts"
    *   *Text inside/near:* "Ad-hoc SQL queries via BI Tool."
*   **Arrows:** Both Dashboard and Analysts point to -> **ClickHouse**.
