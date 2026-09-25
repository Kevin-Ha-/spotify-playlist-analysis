import importlib
import spotify_tokens
import requests
import json
import uuid
from datetime import datetime, timezone, timedelta

# Reload the module to pick up any changes
importlib.reload(spotify_tokens)
from spotify_tokens import Tokens

def get_recently_played(access_token: str) -> None:
    request_url = 'https://api.spotify.com/v1/me/player/recently-played'
    headers = { 'Authorization': f"Bearer {access_token}" }
    params = { "limit": 50 }

    response = requests.get(request_url, headers=headers, params=params)
    response.raise_for_status()
    construct_data(response.json())

def construct_data(data: dict) -> None:
    run_id = str(uuid.uuid4())
    rows = []
    records_rejected = 0
    start_time = datetime.now(timezone.utc)
    
    # Get latest watermark_value
    watermark_df = spark.table("spotify_project.metadata.pipeline_watermarks")
    most_recent_timestamp = watermark_df.select("watermark_value").collect()[0]["watermark_value"]

    #most_recent_timestamp is timezone-naive, need to make timezone aware in order to do comparison
    if most_recent_timestamp.tzinfo is None:
        most_recent_timestamp = most_recent_timestamp.replace(tzinfo=timezone.utc)

    for item in data["items"]:
        played_at_dt = datetime.fromisoformat(item["played_at"].replace('Z', '+00:00'))
 
        if played_at_dt > most_recent_timestamp:
            try:
                track = item['track']
                album = track['album']

                rows.append({
                    "run_id": run_id,
                    "album_id": album["id"],
                    "album_name": album["name"],
                    "artist_id": track["artists"][0]["id"],
                    "artist_name": track["artists"][0]["name"],
                    "duration_ms": track["duration_ms"],
                    "track_id": track["id"],
                    "track_name": track["name"],
                    "played_at": item["played_at"]
                })
            except Exception as e:
                print(f"error appending row: {e}")
                records_rejected += 1
    
    records_read = len(rows)

    if rows:
        played_at_values = [row["played_at"] for row in rows]
        max_played_at_dt = max([datetime.fromisoformat(played_at.replace('Z', '+00:00')) for played_at in played_at_values])
    else:
        max_played_at_dt = most_recent_timestamp

    # Capture pre-run Delta version of watermark table for potential rollback
    watermark_version = spark.sql("DESCRIBE HISTORY spotify_project.metadata.pipeline_watermarks").select("version").collect()[0]["version"]

    try:
        write_to_ingest_table(rows)
        write_to_pipeline_metadata_table(records_read, records_rejected, run_id, len(rows), start_time, most_recent_timestamp, max_played_at_dt)
        write_to_pipeline_watermark_table(run_id, max_played_at_dt)
        print(f"Pipeline run {run_id} completed successfully - all 3 tables written.")
    except Exception as e:
        print(f"Pipeline run {run_id} failed during table writes: {e}")
        print("Initiating rollback of all table writes...")
        rollback_tables(run_id, watermark_version)
        raise

def write_to_pipeline_metadata_table(read: int, rejected: int, run_id: str, written: int, start_time: datetime, previous_watermark: datetime, after_watermark: datetime) -> None:
    end_time = datetime.now(timezone.utc)
    pipeline_status = 'success'
    if written == 0:
        pipeline_status = 'failed'
    if rejected > 0:
        pipeline_status = 'partial'

    # Add 15 minutes to previous_watermark to counteract earlier subtraction
    previous_watermark = previous_watermark + timedelta(minutes=15)

    audit_row = [{
        "run_id": run_id,
        "start_time": start_time,
        "end_time": end_time,
        "status": pipeline_status,
        "records_read": read,
        "records_rejected": rejected,
        "records_written": written,
        "watermark_before": previous_watermark,
        "watermark_after": after_watermark
    }]

    audit_df = spark.createDataFrame(audit_row)
    audit_df.write.mode('append').saveAsTable("spotify_project.metadata.pipeline_runs")
    print("Processing data -> spotify_project.metadata.pipeline_runs")

def write_to_pipeline_watermark_table(run_id: str, watermark_value: datetime) -> None:
    # set the watermark 15 minutes prior to the current time to account for late arriving data
    watermark_dt = watermark_value - timedelta(minutes=15)
    pipeline_name = 'Spotify API daily pull'
    source = 'spotify_api'
    updated_at = datetime.now(timezone.utc)
    watermark_column = 'played_at'

    row = [{
        "pipeline_name": pipeline_name,
        "source": source,
        "last_success_run_id": run_id,
        "watermark_column": watermark_column,
        "watermark_value": watermark_dt,
        "updated_at": updated_at
    }]

    watermark_df = spark.createDataFrame(row)
    watermark_df.write.mode('overwrite').saveAsTable("spotify_project.metadata.pipeline_watermarks")
    print("Processing data -> spotify_project.metadata.pipeline_watermarks")

def write_to_ingest_table(data: dict) -> None:
    print(f"Processing data -> spotify_project.bronze.spotify_ingest")
    df = spark.createDataFrame(data)
    df.write.mode('append').saveAsTable("spotify_project.bronze.spotify_ingest")


def rollback_tables(run_id: str, watermark_version: int) -> None:
    """Rollback all table writes for a given run_id and restore watermark table to pre-run version."""
    rollback_errors = []

    # Rollback bronze ingest table by run_id
    try:
        spark.sql(f"DELETE FROM spotify_project.bronze.spotify_ingest WHERE run_id = '{run_id}'")
        print(f"Rolled back spotify_project.bronze.spotify_ingest - deleted rows for run_id {run_id}")
    except Exception as e:
        rollback_errors.append(f"spotify_ingest: {e}")
        print(f"Failed to rollback spotify_project.bronze.spotify_ingest: {e}")

    # Rollback pipeline_runs table by run_id
    try:
        spark.sql(f"DELETE FROM spotify_project.metadata.pipeline_runs WHERE run_id = '{run_id}'")
        print(f"Rolled back spotify_project.metadata.pipeline_runs - deleted rows for run_id {run_id}")
    except Exception as e:
        rollback_errors.append(f"pipeline_runs: {e}")
        print(f"Failed to rollback spotify_project.metadata.pipeline_runs: {e}")

    # Restore watermark table to pre-run Delta version
    try:
        spark.sql(f"RESTORE TABLE spotify_project.metadata.pipeline_watermarks TO VERSION {watermark_version}")
        print(f"Restored spotify_project.metadata.pipeline_watermarks to version {watermark_version}")
    except Exception as e:
        rollback_errors.append(f"pipeline_watermarks: {e}")
        print(f"Failed to restore spotify_project.metadata.pipeline_watermarks: {e}")

    if rollback_errors:
        raise RuntimeError(f"Rollback completed with errors: {'; '.join(rollback_errors)}")
    print("All tables rolled back successfully.")

try:
    token = Tokens()
    access_token = token.get_access_token()
    get_recently_played(access_token)
except Exception as e:
    print(f'Error fetching data or invalid access token: {type(e).__name__}: {str(e)}')
    raise