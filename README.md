# Academic Report: Real-Time Stream Ingestion and Distributed Graph Analytics

## 1. Introduction
This report details the implementation of a comprehensive big data processing architecture divided into two primary phases: real-time stream ingestion using Apache Kafka and Apache Flink, and distributed graph analytics leveraging Apache Spark. The development environment utilizes containerization via Docker to simulate a scalable, multi-node cluster, ensuring consistent execution and isolation of distributed processing frameworks.

---

## 2. Phase I: Real-Time Stream Ingestion Architecture

### 2.1 Container Infrastructure Initialization
The foundation of the streaming pipeline relies on deploying Zookeeper, Apache Kafka, and an Apache Flink cluster (comprising a JobManager and TaskManager). These services are orchestrated using a `docker-compose.yml` file within an isolated bridge network to ensure seamless internal communication.
*   **Action:** The cluster was initialized using the `docker compose up -d` command. 
*   **Rationale:** Containerizing the messaging broker and processing engine guarantees environmental consistency and decouples the infrastructure from local operating system constraints.



### 2.2 Kafka Topic Provisioning
Prior to processing, the messaging pathways must be established.
*   **Action:** A shell script (`init-kafka.sh`) was executed to provision the necessary source and sink topics on the Kafka broker.
*   **Rationale:** Pre-defining topics prevents "topic not found" errors during the producer's initial broadcast and ensures that the Apache Flink consumer has an established stream to subscribe to immediately upon deployment.


### 2.3 Data Producer Implementation
*   **Action:** A Python producer script (`producer.py`) was executed to read structural data (`sample_camera_data.json`) and continuously broadcast serialized JSON payloads to the inbound Kafka topic.
*   **Rationale:** Isolating data generation from data processing is a standard architectural practice. It enables the simulation of high-throughput real-time events without bottlenecking the analytics engine.


### 2.4 Flink Stream Processing Deployment
*   **Action:** The streaming logic (`traffic_processor.py`) was deployed to the Flink cluster. 
*   **Rationale:** The Flink engine acts as a continuous consumer, subscribing to the Kafka source topic, applying windowing and transformation logic in real-time, and directing the processed results to an outbound sink. 


---

## 3. Phase II: Distributed Graph Analytics with Apache Spark

The second phase transitions to batch-oriented distributed processing, analyzing the Stanford Web BerkStan directed graph dataset to identify dominant hubs based on inbound connectivity.

### 3.1 Spark Cluster Initialization and Data Distribution
*   **Action:** A standalone Spark cluster (one Master, two Workers) was deployed via Docker. To circumvent virtual volume mounting latencies and ensure strict data locality, the `indegree_job.py` script and the raw `web-BerkStan.txt` dataset were explicitly copied into the `/tmp/` directories of all cluster nodes using the `docker cp` command.
*   **Rationale:** Distributing the data directly to the worker nodes mimics a distributed file system (like HDFS). This ensures that executors do not experience network latency or file-not-found exceptions when attempting to read the raw input.



### 3.2 Lazy Evaluation and Data Parsing
*   **Action:** The dataset was ingested into a functional PySpark DataFrame using Spark's lazy evaluation engine. The `load_edges` function applies filtering to strip out metadata headers (lines starting with `#`), tokenizes the remaining strings using regular expressions (`\s+`), and casts the resulting indices into numerical `long` formats.
*   **Rationale:** Lazy evaluation dictates that these transformations are only executed when a terminal action is called. This principle minimizes the memory footprint, allowing Spark's Catalyst Optimizer to determine the most efficient physical execution plan before consuming cluster resources.



### 3.3 Aggregation of Structural In-Degree Distributions
*   **Action:** The `compute_indegree` function was implemented to group the DataFrame by the destination vertex (`dst`) and aggregate the count of incoming source vertices (`src`).
*   **Rationale:** Grouping and aggregating the edges identifies the structural in-degree of each node, a metric essential for determining the connectivity and hierarchical dominance of web pages within the graph network.

### 3.4 Row Ordering and Explicit Memory Optimization
*   **Action:** The aggregated data was ordered in descending fashion to extract the Top 50 dominant destination nodes using `.orderBy(desc("indegree")).limit(50)`. Crucially, an explicit memory optimization was applied by invoking `.cache()` on the resulting DataFrame.
*   **Rationale:** The application performs multiple terminal actions on the aggregated data (displaying it in the console and writing it to a CSV file). Caching ensures the partitioned results are stored directly in executor RAM after the first computation. This prevents Spark from redundantly re-reading the text file and re-executing the expensive network shuffle operations.


---

## 4. Performance Analysis and Execution Telemetry

Evaluation of the Spark pipeline's efficiency was conducted by extracting execution telemetry from the Spark Web Console.

### 4.1 Stage Run Durations
The operational workload was distributed across multiple logical stages. Reviewing the telemetry indicates that the initial data ingestion, filtering, and parsing stage accounted for the majority of the processing duration (taking roughly 8 to 10 seconds). Subsequent stages for aggregation, sorting, and materialization completed in fractions of a second, highlighting the effectiveness of the `.cache()` optimization.


### 4.2 Directed Acyclic Graph (DAG) Logic Chain
Spark translated the Python instructions into an optimized physical plan. The DAG visualization illustrates this chain:
1.  **FileScan & WholeStageCodegen:** Reading the text file and applying the regex split/cast transformations within optimized Java bytecode.
2.  **Exchange:** A shuffle operation required by the `groupBy` transformation to route identical destination nodes to the same executor.
3.  **HashAggregate:** The final reduction computing the sum of inbound edges before the result is collected.


### 4.3 Shuffle Space Allocation Metrics
Because the graph analytics required aggregating disparate edges, a network shuffle was mandatory. The performance records confirm efficient data movement, with total shuffle reads aligning uniformly with shuffle writes across the 8 configured partitions. Total shuffle volumes were successfully minimized through prior filtering of malformed and metadata rows.


### 4.4 Workload Distribution and Data Skew Analysis
A significant challenge in graph analytics is data skew, where highly connected "hub" nodes disproportionately overload a single worker. Analysis of the Executors tab indicates a highly balanced workload:
*   **Worker 0:** Handled a balanced proportion of the tasks and input payload.
*   **Worker 1:** Handled the remaining proportion of tasks with an identical active execution time.
*   **Conclusion:** The allocation of input data sizes and garbage collection times reveals no evidence of data skew. The cluster efficiently parallelized the processing load without bottlenecking.
