import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from datetime import datetime

COLUMN_RENAME_MAP = {
    'played_at': 'date_played',
}

COLUMN_KEEP = [
    'album_name',
    'artist_name',
    'track_name',
    'play_time',
    'date_played',
    'platform',
    'reason_end',
    'reason_start',
    'shuffle',
    'skipped',
]

COLUMN_TYPES = {
    'date_played': 'timestamp',
}

def process_rows(rows: DataFrame) -> None:
    # read the rows passed in by retrieve_by_run_id or retrieve_by_timestamp
    for old_name, new_name in COLUMN_RENAME_MAP.items():
        if old_name in rows.columns:
            rows = rows.withColumnRenamed(old_name, new_name)

    exprs = [
        F.to_timestamp(F.col(c)).alias(c) if t == 'timestamp' else F.col(c).cast(t).alias(c)
        for c, t in COLUMN_TYPES.items() if c in rows.columns
    ]
    if exprs:
        rows = rows.select(*[F.col(col) for col in rows.columns if col not in COLUMN_TYPES] + exprs)

    keep_cols = [c for c in COLUMN_KEEP if c in rows.columns]
    rows = rows.select(*keep_cols)

    rows = rows.dropDuplicates()
    rows = rows.dropna(subset=["artist_name", "track_name"])

    # some of the columns are not present in the spotify API, there is no other way to presently retrieve them, just leave them as null
    missing_cols = [c for c in COLUMN_KEEP if c not in rows.columns]
    for col in missing_cols:
        rows = rows.withColumn(col, F.lit(None))

    try:
        rows.write.mode("append").saveAsTable("spotify_project.silver.streaming_history")
        print(f"processed {rows.count()} records: successfully appended to silver.streaming_history")
    except Exception as e:
        print(f"failed to write records to silver.streaming_history: {e}")
    
    
# start_timestamp optional, if not supplied then just process everything prior to end_timestamp
def retrieve_by_timestamp(end_timestamp, start_timestamp = None) -> None:
    # Set start_timestamp to the earliest possible timestamp if not provided
    start_ts = start_timestamp if start_timestamp else '1970-01-01 00:00:00'
    rows = spark.read.table("spotify_project.bronze.spotify_ingest") \
        .filter(F.col("played_at").between(start_ts, end_timestamp))
    process_rows(rows)

def retrieve_by_run_id() -> None:
    run_id_df = spark.read.table("spotify_project.metadata.pipeline_watermarks").select("last_success_run_id")
    run_id = run_id_df.collect()[0]["last_success_run_id"]
    rows = spark.read.table("spotify_project.bronze.spotify_ingest").filter(f"run_id = '{run_id}'")
    process_rows(rows)

try:
    retrieve_by_run_id()
except Exception as e:
    print(f"failed to retrieve rows from bronze.spotify_ingest: {e}")



    
