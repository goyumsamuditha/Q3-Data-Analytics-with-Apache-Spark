# Academic Report: Distributed Graph Analytics with Apache Spark

## 1. Introduction
This report details the implementation of a big data processing architecture for distributed graph analytics leveraging Apache Spark. The project analyzes the Stanford Web BerkStan directed graph dataset to identify dominant hubs based on inbound connectivity. The development environment utilizes containerization via Docker to simulate a scalable, multi-node cluster, ensuring consistent execution and isolation of the distributed processing framework.

---

## 2. Distributed Architecture and Initialization

### 2.1 Spark Cluster Initialization
The foundation of this pipeline relies on a standalone Apache Spark cluster (one Master, two Workers) orchestrated via Docker Compose.
*   **Action:** The cluster was initialized using the bridge network `spark-net` to ensure seamless internal communication between nodes.
*   **Rationale:** Containerizing the processing engine guarantees environmental consistency, avoids local operating system constraints, and accurately simulates a distributed compute environment.

### 2.2 Data Distribution
*   **Action:** To circumvent virtual volume mounting latencies and ensure strict data locality, the `indegree_job.py` script and the raw `web-BerkStan.txt` dataset were explicitly copied into the temporary directories of all cluster nodes.
*   **Rationale:** Distributing the data directly to the worker nodes mimics a distributed file system (like HDFS). This ensures that executors do not experience network latency or file-not-found exceptions when attempting to read the raw input.

---

## 3. PySpark Pipeline Implementation

### 3.1 Lazy Evaluation and Data Parsing
*   **Action:** The dataset was ingested into a functional PySpark DataFrame using Spark's lazy evaluation engine. The `load_edges` function applies filtering to strip out metadata headers (lines starting with `#`), tokenizes the remaining strings using regular expressions (`\s+`), and casts the resulting indices into numerical `long` formats.
*   **Rationale:** Lazy evaluation dictates that these transformations are only executed when a terminal action is called. This principle minimizes the memory footprint, allowing Spark's Catalyst Optimizer to determine the most efficient physical execution plan before consuming cluster resources.

### 3.2 Aggregation of Structural In-Degree Distributions
*   **Action:** The `compute_indegree` function was implemented to group the DataFrame by the destination vertex (`dst`) and aggregate the count of incoming source vertices (`src`).
*   **Rationale:** Grouping and aggregating the edges identifies the structural in-degree of each node, a metric essential for determining the connectivity and hierarchical dominance of web pages within the graph network.

### 3.3 Row Ordering and Explicit Memory Optimization
*   **Action:** The aggregated data was ordered in descending fashion to extract the Top 50 dominant destination nodes using `.orderBy(desc("indegree")).limit(50)`. Crucially, an explicit memory optimization was applied by invoking `.cache()` on the resulting DataFrame.
*   **Rationale:** The application performs multiple terminal actions on the aggregated data (displaying it in the console and writing it to a CSV file). Caching ensures the partitioned results are stored directly in executor RAM after the first computation. This prevents Spark from redundantly re-reading the text file and re-executing the expensive network shuffle operations.

---

## 4. Performance Analysis and Execution Telemetry

Evaluation of the Spark pipeline's efficiency was conducted by extracting execution telemetry from the Spark Web Console.

### 4.1 Stage Run Durations
The operational workload was distributed across multiple logical stages. Reviewing the telemetry indicates that the initial data ingestion, filtering, and parsing stage accounted for the majority of the processing duration (taking roughly 8 seconds). Subsequent stages for aggregation, sorting, and materialization completed in fractions of a second, highlighting the effectiveness of the `.cache()` optimization.

### 4.2 Directed Acyclic Graph (DAG) Logic Chain
Spark translated the Python instructions into an optimized physical plan. The DAG visualization illustrates this chain:
1.  **FileScan & WholeStageCodegen:** Reading the text file and applying the regex split/cast transformations within optimized Java bytecode.
2.  **Exchange:** A shuffle operation required by the `groupBy` transformation to route identical destination nodes to the same executor.
3.  **HashAggregate:** The final reduction computing the sum of inbound edges before the result is collected.

### 4.3 Shuffle Space Allocation Metrics
Because the graph analytics required aggregating disparate edges, a network shuffle was mandatory. The performance records confirm efficient data movement, with total shuffle reads aligning uniformly with total shuffle writes (4.3 MiB). Total shuffle volumes were successfully minimized through the prior filtering of malformed and metadata rows.

### 4.4 Workload Distribution and Data Skew Analysis
A significant challenge in graph analytics is data skew, where highly connected "hub" nodes disproportionately overload a single worker. By tuning the cluster to 8 shuffle partitions (`spark.sql.shuffle.partitions`), the workload was highly balanced:
*   **Worker 0:** Handled a balanced proportion of the tasks (12 tasks) and input payload (63.5 MiB).
*   **Worker 1:** Handled the remaining proportion of tasks (18 tasks) and input payload (68.9 MiB).
*   **Conclusion:** The allocation of input data sizes and active execution times (both exactly 21 seconds) reveals no evidence of data skew. The cluster efficiently parallelized the processing load without bottlenecking.

---

## 5. Execution Guide (From Scratch)

This project utilizes a `Makefile` to streamline cluster orchestration and job submission. Follow these exact steps to run the pipeline on a new machine or reset an existing environment.

### Prerequisites (New Laptop Setup)
*   **Docker Desktop:** Ensure it is installed and the Docker daemon is actively running.
*   **Make:** Ensure `make` is installed on your system to utilize the automation commands.
*   **Dataset:** Ensure the raw dataset is downloaded and saved exactly at `data/raw/web-BerkStan.txt`.

### Step 0: Clean Slate Reset (For Already-Run PCs)
If you have previously executed this project, residual containers or networks may cause port conflicts. Purge the previous environment by running:

    make down

### Step 1: Start the Cluster
Spin up the Spark master and worker containers in detached mode:

    make up

*(Verify the cluster is running by opening the Spark Master UI in your browser at http://localhost:8080)*

### Step 2: Distribute the Dataset
To avoid local volume-sync issues across different operating systems, manually push the raw dataset directly into the active containers:

    docker cp data/raw/web-BerkStan.txt spark-master:/data/raw/web-BerkStan.txt
    docker cp data/raw/web-BerkStan.txt spark-worker-1:/data/raw/web-BerkStan.txt
    docker cp data/raw/web-BerkStan.txt spark-worker-2:/data/raw/web-BerkStan.txt

### Step 3: Submit the PySpark Job
Trigger the distributed analytics job using the pre-configured Make command:

    make submit

### Step 4: Monitor Application Telemetry
Once the terminal displays the calculated ASCII table of the Top 50 nodes, the script will automatically pause for 5 minutes. Open the Spark Web Console to monitor the DAG, stages, and execution metrics:

*   **Spark Web UI:** http://localhost:4040
*(Press Ctrl + C in your terminal to exit the pause timer early).*

### Step 5: Extract Results and Teardown
To retrieve the processed results and shut down the cluster environment safely:

    docker cp spark-master:/data/output/top50_indegree/ ./data/output/
    make down