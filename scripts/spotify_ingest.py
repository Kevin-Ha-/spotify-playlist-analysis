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

def construct_data(data:dict) -> None:
    run_id = str(uuid.uuid4())
    rows = []
    records_rejected = 0
    start_time = datetime.now(timezone.utc)
    
    # Get latest watermark_value
    watermark_df = spark.table("spotify_project.metadata.pipeline_watermarks")
    most_recent_timestamp = watermark_df.select("watermark_value").collect()[0]["watermark_value"]

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

    played_at_values = [row["played_at"] for row in rows]
    max_played_at_dt = max([datetime.fromisoformat(played_at.replace('Z', '+00:00')) for played_at in played_at_values])

    # prefereably do atomically...somehow
    write_to_db(rows)
    write_to_pipeline_metadata_table(records_read, records_rejected, run_id, len(rows), start_time)

    # return the most recent timestamp for the watermark
    write_to_pipeline_watermark_table(run_id, max_played_at_dt)

def write_to_pipeline_metadata_table(read: int, rejected: int, run_id: str, written: int, start_time: datetime):
    end_time = datetime.now(timezone.utc)
    pipeline_status = 'success'
    if written == 0:
        pipeline_status = 'failed'
    if rejected > 0:
        pipeline_status = 'partial'

    audit_row = [{
        "run_id": run_id,
        "start_time": start_time,
        "end_time": end_time,
        "status": pipeline_status,
        "records_read": read,
        "records_rejected": rejected,
        "records_written": written
    }]

    try:
        audit_df = spark.createDataFrame(audit_row)
        audit_df.write.mode('append').saveAsTable("spotify_project.metadata.pipeline_runs")
        print("Processing data -> spotify_project.metadata.pipeline_runs")
    except:
        print("error writing to table -> spotify_project.metadata.pipeline_runs")

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

    try:
        watermark_df = spark.createDataFrame(row)
        watermark_df.write.mode('overwrite').saveAsTable("spotify_project.metadata.pipeline_watermarks")
        print("Processing data -> spotify_project.metdata.pipeline_watermarks")
    except:
        print("error writing to table -> spotify_project.metadata.pipeline_watermarks")


def write_to_db(data: dict) -> None:
    try:
        print(f"Processing data -> spotify_project.bronze.spotify_ingest")
        df = spark.createDataFrame(data)
        df = df.write.mode('append').saveAsTable("spotify_project.bronze.spotify_ingest") 
    except:
        print("Error writing to table -> spotify_project.bronze.spotify_ingest")

try:
    token = Tokens()
    access_token = token.get_access_token()
    get_recently_played(access_token)
except Exception as e:
    print(f'Error fetching data or invalid access token: {type(e).__name__}: {str(e)}')