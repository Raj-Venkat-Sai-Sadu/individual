
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 🧮 Generic Managed Tables Audit (Databricks) — With Column Mapping & Filters
# MAGIC 
# MAGIC **Purpose**: Compare two managed tables even when **key columns** and **compare/exclude columns** have **different names** in source and target.
# MAGIC 
# MAGIC Features:
# MAGIC - Schema parity (names, types, nullability)
# MAGIC - Row coverage (counts, missing keys in either side)
# MAGIC - Key integrity (duplicate keys on each side)
# MAGIC - Value consistency with **column mappings** and **tolerances** (abs/rel)
# MAGIC - Column statistics (null %, distincts, min/max)
# MAGIC - Canonical row-hash check
# MAGIC - Filters (SQL expressions) and/or partitions
# MAGIC - Standardized audit logging to Delta tables + optional job fail on threshold breaches

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0) Configuration Widgets
# MAGIC Use JSON for mappings where column names differ across source and target. Comma-separated lists are also supported for keys as a fallback.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from functools import reduce
import operator
import json
import time

# Ensure widgets exist (idempotent)
try:
    # Required
    dbutils.widgets.text("source_table", "catalog.schema.source_table")
    dbutils.widgets.text("target_table", "catalog.schema.target_table")

    # Keys (choose one approach)
    dbutils.widgets.text("key_map_json", "{}")  # e.g. {"order_id":"id","tenant_id":"tenant"}
    dbutils.widgets.text("key_columns_source", "")  # fallback, comma-separated
    dbutils.widgets.text("key_columns_target", "")  # fallback, comma-separated

    # Excludes (separate per side)
    dbutils.widgets.text("exclude_columns_source", "_ingest_ts,_load_id")
    dbutils.widgets.text("exclude_columns_target", "_ingest_ts,_load_id")

    # Compare mapping (optional): src_col -> tgt_col when names differ; same-name cols auto-inferred otherwise
    dbutils.widgets.text("compare_map_json", "{}")

    # Filters and partitions
    dbutils.widgets.text("partition_column", "")
    dbutils.widgets.text("partition_values", "")
    dbutils.widgets.text("filter_condition_source", "")
    dbutils.widgets.text("filter_condition_target", "")

    # Tolerances and options
    dbutils.widgets.text("tolerance_map_json", "{}")  # keyed by src or tgt col name
    dbutils.widgets.dropdown("hash_check", "true", ["true", "false"], "hash_check")
    dbutils.widgets.text("audit_database", "audit")
    dbutils.widgets.text("run_id", "")

    # Alerts / thresholds
    dbutils.widgets.text("alert_missing_row_threshold", "0")
    dbutils.widgets.text("alert_mismatch_ratio_threshold", "0.001")
    dbutils.widgets.dropdown("fail_on_threshold_breach", "false", ["true", "false"], "fail_on_threshold_breach")
except NameError:
    pass

# Read widget values
SOURCE_TABLE = dbutils.widgets.get("source_table")
TARGET_TABLE = dbutils.widgets.get("target_table")

KEY_MAP = json.loads(dbutils.widgets.get("key_map_json") or "{}")
KEY_SRC_LIST = [c.strip() for c in (dbutils.widgets.get("key_columns_source") or "").split(",") if c.strip()]
KEY_TGT_LIST = [c.strip() for c in (dbutils.widgets.get("key_columns_target") or "").split(",") if c.strip()]

EXCL_SRC = [c.strip() for c in (dbutils.widgets.get("exclude_columns_source") or "").split(",") if c.strip()]
EXCL_TGT = [c.strip() for c in (dbutils.widgets.get("exclude_columns_target") or "").split(",") if c.strip()]

COMPARE_MAP = json.loads(dbutils.widgets.get("compare_map_json") or "{}")  # src -> tgt

PARTITION_COL = (dbutils.widgets.get("partition_column") or "").strip()
PARTITION_VALUES = [v.strip() for v in (dbutils.widgets.get("partition_values") or "").split(",") if v.strip()]
FILTER_SRC = (dbutils.widgets.get("filter_condition_source") or "").strip()
FILTER_TGT = (dbutils.widgets.get("filter_condition_target") or "").strip()

TOL_MAP = json.loads(dbutils.widgets.get("tolerance_map_json") or "{}")
HASH_CHECK = ((dbutils.widgets.get("hash_check") or "true").lower() == "true")
AUDIT_DB = dbutils.widgets.get("audit_database") or "audit"
RUN_ID = dbutils.widgets.get("run_id") or f"run_{int(time.time())}"
ALERT_MISSING_THRESHOLD = int(float(dbutils.widgets.get("alert_missing_row_threshold") or "0"))
ALERT_MISMATCH_RATIO_THRESHOLD = float(dbutils.widgets.get("alert_mismatch_ratio_threshold") or "0.001")
FAIL_ON_BREACH = ((dbutils.widgets.get("fail_on_threshold_breach") or "false").lower() == "true")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1) Utilities

# COMMAND ----------

def normalize_string(col):
    return F.lower(F.trim(F.col(col).cast("string")))


def apply_filters(df, filter_expr: str):
    return df.filter(F.expr(filter_expr)) if filter_expr else df


def parse_key_map(src_df, tgt_df):
    """Build key mapping source->target from JSON or from paired lists."""
    if KEY_MAP:
        return KEY_MAP
    if KEY_SRC_LIST and KEY_TGT_LIST and len(KEY_SRC_LIST) == len(KEY_TGT_LIST):
        return dict(zip(KEY_SRC_LIST, KEY_TGT_LIST))
    raise ValueError("Key mapping not provided. Set key_map_json or paired key_columns_source/target with equal length.")


def is_numeric_type(dt: T.DataType) -> bool:
    return isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType))


def get_tolerance_for(src_col: str, tgt_col: str):
    # lookup tolerance by src name first, else by target name
    tol = TOL_MAP.get(src_col) or TOL_MAP.get(tgt_col)
    return tol


def create_audit_tables():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {AUDIT_DB}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_DB}.audit_results (
          run_id STRING,
          run_ts TIMESTAMP,
          source_table STRING,
          target_table STRING,
          partition_value STRING,
          filter_source STRING,
          filter_target STRING,
          metric STRING,
          metric_value DOUBLE,
          threshold DOUBLE,
          status STRING,
          notes STRING
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_DB}.audit_diffs (
          run_id STRING,
          diff_type STRING,
          key_src_json STRING,
          key_tgt_json STRING,
          partition_value STRING,
          filter_source STRING,
          filter_target STRING,
          col_name STRING,
          src_value STRING,
          tgt_value STRING
        ) USING DELTA
    """)


def log_summary(rows):
    schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("run_ts", T.TimestampType(), False),
        T.StructField("source_table", T.StringType(), False),
        T.StructField("target_table", T.StringType(), False),
        T.StructField("partition_value", T.StringType(), True),
        T.StructField("filter_source", T.StringType(), True),
        T.StructField("filter_target", T.StringType(), True),
        T.StructField("metric", T.StringType(), False),
        T.StructField("metric_value", T.DoubleType(), True),
        T.StructField("threshold", T.DoubleType(), True),
        T.StructField("status", T.StringType(), False),
        T.StructField("notes", T.StringType(), True),
    ])
    df = spark.createDataFrame(rows, schema)
    df.write.mode("append").saveAsTable(f"{AUDIT_DB}.audit_results")


def log_diffs(df):
    if df is not None:
        df.write.mode("append").saveAsTable(f"{AUDIT_DB}.audit_diffs")


def json_obj_from_cols(prefix: str, cols: list):
    parts = [F.concat(F.lit(f'"{c}":"'), F.col(f"{prefix}{c}").cast("string"), F.lit('"')) for c in cols]
    return F.concat(F.lit("{"), F.concat_ws(",", *parts), F.lit("}")) if parts else F.lit("{}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2) Load, Filter, and Align Columns

# COMMAND ----------
create_audit_tables()

src_df = spark.table(SOURCE_TABLE)
tgt_df = spark.table(TARGET_TABLE)

# Apply optional partition pruning
if PARTITION_COL and PARTITION_VALUES:
    src_df = src_df.filter(F.col(PARTITION_COL).isin(PARTITION_VALUES))
    tgt_df = tgt_df.filter(F.col(PARTITION_COL).isin(PARTITION_VALUES))

# Apply arbitrary filters
src_df = apply_filters(src_df, FILTER_SRC)
tgt_df = apply_filters(tgt_df, FILTER_TGT or FILTER_SRC)

# Derive key mapping
KEY_MAP = parse_key_map(src_df, tgt_df)  # src->tgt
KEY_SRC = list(KEY_MAP.keys())
KEY_TGT = [KEY_MAP[k] for k in KEY_SRC]

# Determine compare pairs (src->tgt)
compare_pairs = []
if COMPARE_MAP:
    for s, t in COMPARE_MAP.items():
        if s in src_df.columns and t in tgt_df.columns and (s not in EXCL_SRC) and (t not in EXCL_TGT):
            compare_pairs.append((s, t))
# Auto-add same-name columns present on both sides, excluding keys & excludes and not already mapped
existing_mapped_src = set(s for s, _ in compare_pairs)
for col in src_df.columns:
    if col in tgt_df.columns        and col not in existing_mapped_src        and col not in KEY_SRC and col not in EXCL_SRC        and col not in KEY_TGT and col not in EXCL_TGT:
        compare_pairs.append((col, col))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3) Schema Comparison

# COMMAND ----------
# Build schema maps
s_map = {f.name: (str(f.dataType), f.nullable) for f in src_df.schema.fields}
t_map = {f.name: (str(f.dataType), f.nullable) for f in tgt_df.schema.fields}
all_cols = sorted(set(s_map.keys()) | set(t_map.keys()))

schema_diff_rows = []
for c in all_cols:
    s = s_map.get(c)
    t = t_map.get(c)
    if s != t:
        schema_diff_rows.append({
            "run_id": RUN_ID,
            "diff_type": "SCHEMA_DIFF",
            "key_src_json": "{}",
            "key_tgt_json": "{}",
            "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
            "filter_source": FILTER_SRC or None,
            "filter_target": FILTER_TGT or (FILTER_SRC or None),
            "col_name": c,
            "src_value": f"type={s[0]}, nullable={s[1]}" if s else None,
            "tgt_value": f"type={t[0]}, nullable={t[1]}" if t else None,
        })

schema_diff_df = spark.createDataFrame(schema_diff_rows, schema=T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("diff_type", T.StringType(), False),
    T.StructField("key_src_json", T.StringType(), False),
    T.StructField("key_tgt_json", T.StringType(), False),
    T.StructField("partition_value", T.StringType(), True),
    T.StructField("filter_source", T.StringType(), True),
    T.StructField("filter_target", T.StringType(), True),
    T.StructField("col_name", T.StringType(), True),
    T.StructField("src_value", T.StringType(), True),
    T.StructField("tgt_value", T.StringType(), True),
])) if schema_diff_rows else None

log_diffs(schema_diff_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4) Row Counts & Duplicate Keys

# COMMAND ----------
row_count_src = src_df.count()
row_count_tgt = tgt_df.count()

src_dups = src_df.groupBy(*[F.col(c) for c in KEY_SRC]).count().filter(F.col("count") > 1).count()
tgt_dups = tgt_df.groupBy(*[F.col(c) for c in KEY_TGT]).count().filter(F.col("count") > 1).count()

summary_rows = []
summary_rows.extend([
    {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE, "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
     "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
     "metric": "row_count_source", "metric_value": float(row_count_src), "threshold": None, "status": "INFO", "notes": None},
    {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE, "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
     "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
     "metric": "row_count_target", "metric_value": float(row_count_tgt), "threshold": None, "status": "INFO", "notes": None},
    {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE, "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
     "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
     "metric": "duplicate_keys_source", "metric_value": float(src_dups), "threshold": 0.0, "status": "FAIL" if src_dups > 0 else "PASS", "notes": None},
    {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE, "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
     "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
     "metric": "duplicate_keys_target", "metric_value": float(tgt_dups), "threshold": 0.0, "status": "FAIL" if tgt_dups > 0 else "PASS", "notes": None},
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5) Missing Rows (Asymmetric Keys)

# COMMAND ----------
# Build aligned key views: alias target keys to source key names
src_keys_df = src_df.select(*[F.col(k).alias(k) for k in KEY_SRC])
tgt_keys_df = tgt_df.select(*[F.col(t).alias(s) for s, t in KEY_MAP.items()])

missing_in_tgt = src_keys_df.join(tgt_keys_df, on=KEY_SRC, how="left_anti")
missing_in_src = tgt_keys_df.join(src_keys_df, on=KEY_SRC, how="left_anti")

missing_tgt_count = missing_in_tgt.count()
missing_src_count = missing_in_src.count()

summary_rows.extend([
    {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE, "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
     "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
     "metric": "missing_in_target", "metric_value": float(missing_tgt_count), "threshold": float(ALERT_MISSING_THRESHOLD),
     "status": "FAIL" if missing_tgt_count > ALERT_MISSING_THRESHOLD else ("WARN" if missing_tgt_count > 0 else "PASS"), "notes": None},
    {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE, "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
     "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
     "metric": "missing_in_source", "metric_value": float(missing_src_count), "threshold": float(ALERT_MISSING_THRESHOLD),
     "status": "FAIL" if missing_src_count > ALERT_MISSING_THRESHOLD else ("WARN" if missing_src_count > 0 else "PASS"), "notes": None},
])

# Prepare missing diffs
missing_in_tgt_diffs = missing_in_tgt.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("MISSING_IN_TARGET").alias("diff_type"),
    json_obj_from_cols("", KEY_SRC).alias("key_src_json"),
    json_obj_from_cols("", KEY_SRC).alias("key_tgt_json"),  # same names here (aliased)
    F.lit(",".join(PARTITION_VALUES) if PARTITION_VALUES else None).alias("partition_value"),
    F.lit(FILTER_SRC or None).alias("filter_source"),
    F.lit(FILTER_TGT or (FILTER_SRC or None)).alias("filter_target"),
    F.lit(None).cast("string").alias("col_name"),
    F.lit(None).cast("string").alias("src_value"),
    F.lit(None).cast("string").alias("tgt_value")
)
missing_in_src_diffs = missing_in_src.select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("MISSING_IN_SOURCE").alias("diff_type"),
    json_obj_from_cols("", KEY_SRC).alias("key_src_json"),
    json_obj_from_cols("", KEY_SRC).alias("key_tgt_json"),
    F.lit(",".join(PARTITION_VALUES) if PARTITION_VALUES else None).alias("partition_value"),
    F.lit(FILTER_SRC or None).alias("filter_source"),
    F.lit(FILTER_TGT or (FILTER_SRC or None)).alias("filter_target"),
    F.lit(None).cast("string").alias("col_name"),
    F.lit(None).cast("string").alias("src_value"),
    F.lit(None).cast("string").alias("tgt_value")
)

log_diffs(missing_in_tgt_diffs)
log_diffs(missing_in_src_diffs)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6) Value Mismatches (With Column Mapping & Tolerances)

# COMMAND ----------
# Build aligned DataFrames for join: alias target keys to source key names
s = src_df.alias("s")
t = tgt_df.select(*[F.col(tgt).alias(src) for src, tgt in KEY_MAP.items()], *[F.col(c).alias(c) for c in tgt_df.columns if c not in KEY_TGT]).alias("t")

joined = s.join(t, on=KEY_SRC, how="inner")

# Per-pair mismatch conditions
mismatch_conditions = []
per_col_diffs = []

# For numeric casting decisions
src_type_map = dict(src_df.dtypes)  # col -> spark string type
tgt_type_map = dict(tgt_df.dtypes)

for src_col, tgt_col in compare_pairs:
    s_col = F.col(f"s.{src_col}")
    t_col = F.col(f"t.{tgt_col if tgt_col in tgt_df.columns else tgt_col}")

    tol = get_tolerance_for(src_col, tgt_col)

    # Decide comparison strategy
    s_dtype = next((f.dataType for f in src_df.schema.fields if f.name == src_col), None)
    t_dtype = next((f.dataType for f in tgt_df.schema.fields if f.name == tgt_col), None)

    if is_numeric_type(s_dtype) and is_numeric_type(t_dtype) and tol:
        if tol.get("type") == "abs":
            cond = F.abs(s_col.cast("double") - t_col.cast("double")) > F.lit(float(tol["value"]))
        elif tol.get("type") == "rel":
            cond = F.abs(s_col.cast("double") - t_col.cast("double")) > F.abs(t_col.cast("double") * F.lit(float(tol["value"])))
        else:
            cond = s_col.cast("double") != t_col.cast("double")
    elif is_numeric_type(s_dtype) and is_numeric_type(t_dtype):
        cond = s_col.cast("double") != t_col.cast("double")
    else:
        # String-like default normalization, but if both are timestamps/dates, compare direct
        if isinstance(s_dtype, (T.TimestampType, T.DateType)) and isinstance(t_dtype, (T.TimestampType, T.DateType)):
            cond = s_col != t_col
        else:
            cond = normalize_string(f"s.{src_col}") != normalize_string(f"t.{tgt_col}")

    mismatch_conditions.append(cond)

    diffs_c = joined.filter(cond).select(
        *[F.col(f"s.{k}").alias(k) for k in KEY_SRC]
    ).select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit("VALUE_MISMATCH").alias("diff_type"),
        json_obj_from_cols("", KEY_SRC).alias("key_src_json"),
        json_obj_from_cols("", KEY_SRC).alias("key_tgt_json"),
        F.lit(",".join(PARTITION_VALUES) if PARTITION_VALUES else None).alias("partition_value"),
        F.lit(FILTER_SRC or None).alias("filter_source"),
        F.lit(FILTER_TGT or (FILTER_SRC or None)).alias("filter_target"),
        F.lit(src_col).alias("col_name"),
        F.col(f"s.{src_col}").cast("string").alias("src_value"),
        F.col(f"t.{tgt_col}").cast("string").alias("tgt_value")
    )
    per_col_diffs.append(diffs_c)

mismatch_df = None
if per_col_diffs:
    mismatch_df = per_col_diffs[0]
    for d in per_col_diffs[1:]:
        mismatch_df = mismatch_df.unionByName(d)

mismatch_rows = joined.filter(reduce(operator.or_, mismatch_conditions)) .select(*[F.col(f"s.{k}") for k in KEY_SRC]).distinct().count() if mismatch_conditions else 0

summary_rows.append({
    "run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
    "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None,
    "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
    "metric": "value_mismatch_rows", "metric_value": float(mismatch_rows),
    "threshold": float(ALERT_MISMATCH_RATIO_THRESHOLD) * float(max(row_count_src, 1)),
    "status": "FAIL" if mismatch_rows > ALERT_MISMATCH_RATIO_THRESHOLD * max(row_count_src, 1) else ("WARN" if mismatch_rows > 0 else "PASS"),
    "notes": f"Per-column diff records: {mismatch_df.count() if mismatch_df is not None else 0}"
})

log_diffs(mismatch_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7) Column Stats Drift (Per-Pair)

# COMMAND ----------
drift_rows = []
for src_col, tgt_col in compare_pairs:
    s_stats = src_df.select(
        F.count(F.col(src_col)).alias("cnt"),
        F.count(F.when(F.col(src_col).isNull(), 1)).alias("nulls"),
        F.countDistinct(F.col(src_col)).alias("distinct"),
        F.min(F.col(src_col)).alias("min"),
        F.max(F.col(src_col)).alias("max")
    ).first()
    t_stats = tgt_df.select(
        F.count(F.col(tgt_col)).alias("cnt"),
        F.count(F.when(F.col(tgt_col).isNull(), 1)).alias("nulls"),
        F.countDistinct(F.col(tgt_col)).alias("distinct"),
        F.min(F.col(tgt_col)).alias("min"),
        F.max(F.col(tgt_col)).alias("max")
    ).first()

    drift_rows.extend([
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{src_col}_null_pct_source", "metric_value": float(s_stats["nulls"]) / max(float(s_stats["cnt"]), 1.0), "threshold": None, "status": "INFO", "notes": None},
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{tgt_col}_null_pct_target", "metric_value": float(t_stats["nulls"]) / max(float(t_stats["cnt"]), 1.0), "threshold": None, "status": "INFO", "notes": None},
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{src_col}_distinct_source", "metric_value": float(int(s_stats["distinct"])), "threshold": None, "status": "INFO", "notes": None},
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{tgt_col}_distinct_target", "metric_value": float(int(t_stats["distinct"])), "threshold": None, "status": "INFO", "notes": None},
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{src_col}_min_source", "metric_value": None, "threshold": None, "status": "INFO", "notes": f"{s_stats['min']}"},
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{tgt_col}_min_target", "metric_value": None, "threshold": None, "status": "INFO", "notes": f"{t_stats['min']}"},
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{src_col}_max_source", "metric_value": None, "threshold": None, "status": "INFO", "notes": f"{s_stats['max']}"},
        {"run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
         "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
         "metric": f"{tgt_col}_max_target", "metric_value": None, "threshold": None, "status": "INFO", "notes": f"{t_stats['max']}"},
    ])

summary_rows.extend(drift_rows)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8) Canonical Row Hash Check (Based on Compare Pairs)

# COMMAND ----------
if HASH_CHECK and compare_pairs:
    # Normalize to string with trimming for strings; keep order stable by compare_pairs
    def normalized_concat_side(df, side: str):
        exprs = []
        for src_col, tgt_col in compare_pairs:
            col_name = src_col if side == 's' else tgt_col
            dt = next((f.dataType for f in (src_df if side == 's' else tgt_df).schema.fields if f.name == col_name), None)
            c = F.col(f"{side}.{col_name}")
            if isinstance(dt, T.StringType):
                c = F.lower(F.trim(c))
            exprs.append(c.cast("string"))
        return F.sha2(F.concat_ws("|", *exprs), 256)

    s_hash = s.select(*[F.col(f"s.{k}").alias(k) for k in KEY_SRC], normalized_concat_side(src_df, 's').alias("row_hash_src"))
    t_hash = t.select(*[F.col(f"t.{KEY_MAP[k]}").alias(k) for k in KEY_SRC], normalized_concat_side(tgt_df, 't').alias("row_hash_tgt"))

    hash_join = s_hash.join(t_hash, on=KEY_SRC, how="inner")
    hash_mismatches = hash_join.filter(F.col("row_hash_src") != F.col("row_hash_tgt")).count()

    summary_rows.append({
        "run_id": RUN_ID, "run_ts": None, "source_table": SOURCE_TABLE, "target_table": TARGET_TABLE,
        "partition_value": ",".join(PARTITION_VALUES) if PARTITION_VALUES else None, "filter_source": FILTER_SRC or None, "filter_target": FILTER_TGT or (FILTER_SRC or None),
        "metric": "row_hash_mismatches", "metric_value": float(hash_mismatches),
        "threshold": float(ALERT_MISMATCH_RATIO_THRESHOLD) * float(max(row_count_src, 1)),
        "status": "FAIL" if hash_mismatches > ALERT_MISMATCH_RATIO_THRESHOLD * max(row_count_src, 1) else ("WARN" if hash_mismatches > 0 else "PASS"),
        "notes": None
    })

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9) Write Summary & Display

# COMMAND ----------
# Add timestamps and persist
summary_rows = [dict(r, run_ts=None) for r in summary_rows]
log_summary(summary_rows)

try:
    display(spark.table(f"{AUDIT_DB}.audit_results").filter(F.col("run_id") == RUN_ID))
except Exception:
    spark.table(f"{AUDIT_DB}.audit_results").filter(F.col("run_id") == RUN_ID).show(100, False)

try:
    display(spark.table(f"{AUDIT_DB}.audit_diffs").filter(F.col("run_id") == RUN_ID).limit(100))
except Exception:
    spark.table(f"{AUDIT_DB}.audit_diffs").filter(F.col("run_id") == RUN_ID).show(100, False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10) Threshold Enforcement (Optional Job Fail)

# COMMAND ----------
latest_summary = spark.table(f"{AUDIT_DB}.audit_results").filter(F.col("run_id") == RUN_ID).collect()
fail_metrics = [r for r in latest_summary if r["status"] == "FAIL"]
warn_metrics = [r for r in latest_summary if r["status"] == "WARN"]

status_msg = f"Run {RUN_ID}: FAIL={len(fail_metrics)}, WARN={len(warn_metrics)}"
print(status_msg)

if FAIL_ON_BREACH and len(fail_metrics) > 0:
    raise Exception(f"Audit thresholds breached: {status_msg}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Notes & Examples
# MAGIC 
# MAGIC **Key mapping (JSON)**
# MAGIC ```json
# MAGIC {"order_id":"id","tenant_id":"tenant"}
# MAGIC ```
# MAGIC or use paired lists (same length):
# MAGIC 
# MAGIC - `key_columns_source = order_id,tenant_id`
# MAGIC - `key_columns_target = id,tenant`
# MAGIC 
# MAGIC **Compare mapping (JSON)** (for differing names):
# MAGIC ```json
# MAGIC {"amount":"amt","status":"state"}
# MAGIC ```
# MAGIC If omitted, same-name columns (excluding keys/excludes) are auto-compared.
# MAGIC 
# MAGIC **Tolerances (JSON)** — key by **source** or **target** column name:
# MAGIC ```json
# MAGIC {"amount": {"type": "abs", "value": 0.01}, "rate": {"type": "rel", "value": 0.001}}
# MAGIC ```
# MAGIC 
# MAGIC **Filters** use Spark SQL expressions:
# MAGIC - `filter_condition_source = status = 'ACTIVE' AND amount > 0 AND order_date >= '2025-12-01'`
# MAGIC - `filter_condition_target =` *(leave blank to reuse source filter)*
# MAGIC 
# MAGIC **Partitions** are applied before filters if provided.
# MAGIC 
# MAGIC **Audit tables**: `{audit_database}.audit_results` and `{audit_database}.audit_diffs`.
