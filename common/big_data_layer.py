"""
big_data_layer.py
------------------
[ASSIGNMENT 2 EXTENSION - Big Data Integration]
[Supports MILESTONE 3 - Distributed Architecture: adds a BATCH LAYER]

Chosen big data technology: Apache Spark (PySpark, local mode).

WHY SPARK (and not e.g. Kafka or Hadoop/HDFS):
  - Runs as a single embedded process (no separate broker/cluster/
    Zookeeper service to install and keep running) -- realistic for a
    student project on a single Windows laptop: `pip install pyspark`
    plus a JDK is the entire setup.
  - It is nonetheless a genuine, industry-standard distributed batch
    processing engine: even in "local[*]" mode, Spark partitions the
    dataset and processes partitions in parallel across CPU cores using
    the same DataFrame/RDD execution model it uses on a real cluster.
  - It directly complements (does not replace) the platform's existing
    live socket-based node system: Spark handles the LARGE, HISTORICAL
    side of the workload; the existing nodes still handle real-time
    task execution. This is a standard "Lambda Architecture" split:

        Batch Layer (Spark, 100,000-row dataset)
                        │
        Speed Layer (live Edge/RAN/Core/Cloud nodes, Milestone 1/2/3)
                        │
        Serving Layer (merged CSV exports)
                        │
        Presentation Layer (Power BI dashboard)

This module owns the Batch + Serving layers. The Speed layer is
unchanged -- it is the exact node platform from common/node.py.
"""

import os
import numpy as np
import pandas as pd

from common.workload_generator import SERVICE_PROFILES, CLASS_WEIGHTS


def generate_large_dataset(n_rows: int = 100_000, seed: int = 2026) -> pd.DataFrame:
    """
    Vectorized (numpy) generation of a large telecom service-request
    dataset with the SAME service-class distribution and per-class
    resource-profile ranges as the small-scale workload_generator.py
    used in Milestones 1/2/3, so results are directly comparable across
    scales. Vectorized rather than a 100,000-iteration Python loop,
    which is itself a small "big data" engineering decision worth
    noting: naive per-row generation would be the first bottleneck.
    """
    rng = np.random.default_rng(seed)
    classes = list(CLASS_WEIGHTS.keys())
    weights = np.array(list(CLASS_WEIGHTS.values()))
    service_class = rng.choice(classes, size=n_rows, p=weights / weights.sum())

    cpu = np.empty(n_rows)
    memory = np.empty(n_rows)
    bandwidth = np.empty(n_rows)
    work_units = np.empty(n_rows)

    for cls in classes:
        mask = service_class == cls
        n = mask.sum()
        prof = SERVICE_PROFILES[cls]
        cpu[mask] = rng.uniform(*prof["cpu"], size=n)
        memory[mask] = rng.uniform(*prof["memory"], size=n)
        bandwidth[mask] = rng.uniform(*prof["bandwidth"], size=n)
        work_units[mask] = rng.uniform(*prof["work_units"], size=n)

    df = pd.DataFrame({
        "task_id": np.arange(n_rows),
        "service_class": service_class,
        "cpu": np.round(cpu, 2),
        "memory": np.round(memory, 1),
        "bandwidth": np.round(bandwidth, 1),
        "work_units": np.round(work_units, 1),
    })
    return df


def get_spark_session():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("5g6g-platform-batch-layer")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def run_batch_layer(n_rows: int = 100_000, seed: int = 2026, results_dir: str = "results/bigdata"):
    """
    [BATCH LAYER] Runs the full Spark pipeline over the large dataset:
      1. Load the dataset into a Spark DataFrame (distributed across partitions).
      2. Simulate the SAME edge-first placement policy used live in
         Milestones 2/3 (common/workload_generator.py::edge_first_placement),
         but applied to all 100,000 rows via Spark's distributed execution
         instead of the Python client submitting each one live.
      3. Aggregate (a) per-service-class demand and (b) per-node simulated
         load, using Spark's distributed groupBy/agg -- this is the actual
         "big data processing" step, executed in parallel across partitions.
      4. Write results as CSVs, ready for Power BI import.

    Returns a dict summary (also written to disk) for use in the report/JSON log.
    """
    from pyspark.sql import functions as F

    os.makedirs(results_dir, exist_ok=True)

    pdf = generate_large_dataset(n_rows=n_rows, seed=seed)
    raw_path = os.path.join(results_dir, "raw_dataset_sample_1000.csv")
    pdf.head(1000).to_csv(raw_path, index=False)  # full 100k kept in Spark only; 1k sample for inspection/Power BI

    spark = get_spark_session()
    try:
        sdf = spark.createDataFrame(pdf)
        # Explicitly partition the dataset so processing is genuinely split and
        # parallelized across partitions/cores, rather than relying on however
        # many cores happen to be free on whatever machine this runs on.
        n_target_partitions = 8
        sdf = sdf.repartition(n_target_partitions)
        sdf = sdf.withColumn("row_id", F.monotonically_increasing_id())

        # --- Simulate the M2/M3 "edge-first" placement policy at scale ---
        # URLLC and mMTC -> round-robin across 2 edge nodes; eMBB -> ran-1
        # (mirrors common/workload_generator.py::edge_first_placement exactly)
        sdf = sdf.withColumn(
            "assigned_node",
            F.when(F.col("service_class") == "eMBB", F.lit("ran-1"))
             .otherwise(F.when(F.col("row_id") % 2 == 0, F.lit("edge-1")).otherwise(F.lit("edge-2")))
        )
        node_type_map = {"edge-1": "EDGE", "edge-2": "EDGE", "ran-1": "RAN"}
        map_expr = F.create_map([F.lit(x) for kv in node_type_map.items() for x in kv])
        sdf = sdf.withColumn("node_type", map_expr[F.col("assigned_node")])

        # --- [Distributed aggregation #1] demand per service class ---
        by_class = (
            sdf.groupBy("service_class")
               .agg(
                   F.count("*").alias("task_count"),
                   F.round(F.avg("cpu"), 3).alias("avg_cpu"),
                   F.round(F.avg("memory"), 2).alias("avg_memory_mb"),
                   F.round(F.avg("bandwidth"), 2).alias("avg_bandwidth_mbps"),
                   F.round(F.avg("work_units"), 2).alias("avg_work_units"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_demand_mbps"),
                   F.round(F.stddev("work_units"), 3).alias("stddev_work_units"),
               )
               .orderBy(F.desc("task_count"))
        )
        by_class_pdf = by_class.toPandas()
        by_class_pdf.to_csv(os.path.join(results_dir, "batch_summary_by_service_class.csv"), index=False)

        # --- [Distributed aggregation #2] simulated load per node ---
        by_node = (
            sdf.groupBy("assigned_node", "node_type")
               .agg(
                   F.count("*").alias("task_count"),
                   F.round(F.sum("cpu"), 1).alias("total_cpu_demand"),
                   F.round(F.sum("memory"), 1).alias("total_memory_demand_mb"),
                   F.round(F.sum("bandwidth"), 1).alias("total_bandwidth_demand_mbps"),
                   F.round(F.avg("work_units"), 2).alias("avg_work_units"),
               )
               .orderBy(F.desc("task_count"))
        )
        by_node_pdf = by_node.toPandas()
        by_node_pdf.to_csv(os.path.join(results_dir, "batch_summary_by_node.csv"), index=False)

        total_rows = sdf.count()
        n_partitions = sdf.rdd.getNumPartitions()

        summary = {
            "technology": "Apache Spark (PySpark, local[*] mode)",
            "total_rows_processed": total_rows,
            "spark_partitions_used": n_partitions,
            "by_service_class": by_class_pdf.to_dict(orient="records"),
            "by_node": by_node_pdf.to_dict(orient="records"),
        }
        return summary
    finally:
        spark.stop()
