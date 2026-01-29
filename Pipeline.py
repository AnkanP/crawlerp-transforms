import os
import uuid
import yaml
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.utils import AnalysisException
import pandas as pd
import plotly.graph_objects as go
import networkx as nx
from jinja2 import Template

# -----------------------------
# 1️⃣ Load YAML Config
# -----------------------------
def load_config(config_file):
    with open(config_file) as f:
        return yaml.safe_load(f)

# -----------------------------
# 2️⃣ Initialize Spark
# -----------------------------
def init_spark(app_name="PipelineApp"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

# -----------------------------
# 3️⃣ Read Source Table / File
# -----------------------------
def read_source(spark, source_conf, runtime_params):
    path = source_conf['path'].format(**runtime_params)
    fmt = source_conf.get('format', 'csv')
    options = source_conf.get('options', {})
    df = spark.read.format(fmt).options(**options).load(path)
    return df

# -----------------------------
# 4️⃣ Apply Select / Transform
# -----------------------------
def apply_select_transform(df, select_conf):
    if select_conf:
        select_exprs = [s if isinstance(s,str) else s for s in select_conf]
        df = df.selectExpr(*select_exprs)
    return df

# -----------------------------
# 5️⃣ Deduplication
# -----------------------------
def apply_dedup(df, dedup_conf):
    if not dedup_conf:
        return df
    method = dedup_conf.get("method","distinct")
    if method=="distinct":
        df = df.distinct()
    elif method=="row_number":
        partition_by = dedup_conf.get("partition_by",[])
        order_by = dedup_conf.get("order_by",[])
        w = Window.partitionBy(*partition_by).orderBy(*[F.expr(c) for c in order_by])
        df = df.withColumn("__rn", F.row_number().over(w)).filter(F.col("__rn")==1).drop("__rn")
    return df

# -----------------------------
# 6️⃣ Write Checkpoint / Temp View
# -----------------------------
def write_checkpoint(df, checkpoint_conf):
    if not checkpoint_conf:
        return
    path = checkpoint_conf['path']
    fmt = checkpoint_conf.get("format","parquet")
    mode = checkpoint_conf.get("mode","overwrite")
    df.write.format(fmt).mode(mode).save(path)
    view = checkpoint_conf.get("register_as")
    if view:
        df.createOrReplaceTempView(view)

# -----------------------------
# 7️⃣ Capture Metrics
# -----------------------------
def capture_metrics(df, stage_name):
    metrics = {
        "stage": stage_name,
        "input_count": df.count(),
        "duplicate_count": df.count() - df.dropDuplicates().count(),
        "output_count": df.count(),
        "null_count": sum([df.filter(F.col(c).isNull()).count() for c in df.columns])
    }
    print(f"[METRICS] {metrics}")
    return metrics

# -----------------------------
# 8️⃣ Build DAG Graph
# -----------------------------
def build_stage_graph(ctes):
    G = nx.DiGraph()
    for cte_name, cte in ctes.items():
        G.add_node(cte_name)
        for dep in cte.get("depends_on", []):
            G.add_edge(dep, cte_name)
    return G

# -----------------------------
# 9️⃣ Generate HTML Dashboard
# -----------------------------
def generate_html_dashboard(ctes, metrics_df, process_date, output_file="pipeline_dashboard.html"):
    graph = build_stage_graph(ctes)
    pos = nx.spring_layout(graph, seed=42)

    # Node positions & colors
    node_x, node_y, node_text, node_color = [], [], [], []
    for node in graph.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        row = metrics_df[metrics_df['stage']==node]
        if not row.empty:
            row = row.iloc[0]
            text = f"{node}<br>Input:{row['input_count']}<br>Duplicates:{row['duplicate_count']}<br>Output:{row['output_count']}<br>Nulls:{row['null_count']}"
            node_text.append(text)
            color = "red" if row['duplicate_count']>0 or row['null_count']>0 else "green"
            node_color.append(color)
        else:
            node_text.append(node)
            node_color.append("gray")

    # Edges
    edge_x, edge_y = [], []
    for edge in graph.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(x=edge_x, y=edge_y, line=dict(width=1,color="#888"), hoverinfo='none', mode='lines')
    node_trace = go.Scatter(x=node_x, y=node_y, mode="markers+text", text=node_text,
                            hoverinfo="text",
                            marker=dict(color=node_color, size=30, line=dict(width=2,color='black')),
                            textposition="bottom center")
    fig = go.Figure(data=[edge_trace,node_trace], layout=go.Layout(title=f"Pipeline DAG - {process_date}", showlegend=False, hovermode='closest'))
    fig.write_html(output_file)
    print(f"[INFO] Dashboard generated: {output_file}")

# -----------------------------
# 10️⃣ Run Pipeline
# -----------------------------
def run_pipeline(config_file, runtime_params):
    config = load_config(config_file)
    spark = init_spark()

    metrics_all = {}
    df_cache = {}

    # Process each CTE
    for cte_name, cte_conf in config.get("ctes", {}).items():
        # Read source or use driving table
        if "s3_source" in cte_conf:
            df = read_source(spark, cte_conf["s3_source"], runtime_params)
        elif "driving_table" in cte_conf:
            drv_table = cte_conf["driving_table"]
            df = spark.table(drv_table)
        else:
            raise ValueError(f"No source found for CTE {cte_name}")

        # Select / Transform
        df = apply_select_transform(df, cte_conf.get("select"))

        # Dedup
        df = apply_dedup(df, cte_conf.get("dedup"))

        # Checkpoint & temp view
        write_checkpoint(df, cte_conf.get("checkpoint"))

        # Capture metrics
        metrics = capture_metrics(df, cte_name)
        metrics_all[cte_name] = metrics
        df_cache[cte_name] = df

    # Final output
    final_conf = config.get("final")
    if not final_conf:
        raise ValueError("Final output config missing")
    final_df = df_cache[final_conf["from"]]
    final_df = apply_select_transform(final_df, final_conf.get("select"))

    return final_df, metrics_all
