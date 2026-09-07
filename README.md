# Spotify-playlist-analysis

## Overview
Learning project for databricks and spark using spotify artist and track datsets from Kaggle and my own streaming history requested from spotify for plays from 2011-2026
- personal spotify streaming info requested through spotify, data runs from 2011-2026
- current and future streaming info pulled through spotify api (limited to 50 per request) on a job that runs every 1 hour
- artist info csv pulled from Kaggle

  
## Architecture

```mermaid
%%{init: {"flowchart": {"diagramPadding": 150}}}%%
flowchart LR

    %% Source Files
    SHCSV["streaming_history_audio_{year}.json files"]
    SHAPI["spotify api"]
    ArtistCSV["artists.csv"]
    TracksCSV["tracks.csv"]

    %% Bronze Layer
    BronzeSH["bronze.streaming_history_{year}"]
    BronzeAPI["bronze.streaming_api"]
    BronzeArtist["bronze.artist"]
    BronzeTracks["bronze.tracks"]

    %% Silver Layer
    SilverSH["silver.streaming_history"]
    SilverArtist["silver.artist"]
    SilverTracks["silver.tracks"]

    %% Gold Layer
    GoldSH["gold.streaming_history"]
    GoldArtist["gold.artist"]
    GoldTracks["gold.tracks"]

    %% Streaming History
    SHCSV --> BronzeSH
    SHAPI --> BronzeAPI

    BronzeSH --> SilverSH
    BronzeAPI --> SilverSH

    SilverSH --> GoldSH

    %% Artist
    ArtistCSV --> BronzeArtist
    BronzeArtist --> SilverArtist
    SilverArtist --> GoldArtist

    %% Tracks
    TracksCSV --> BronzeTracks
    BronzeTracks --> SilverTracks
    SilverTracks --> GoldTracks

    %% Styling
    classDef source fill:#e8f1f8,stroke:#4a90e2,stroke-width:2px,color:#1a1a1a
    classDef bronze fill:#CD7F32,stroke:#8B5A2B,stroke-width:2px,color:#ffffff
    classDef silver fill:#C0C0C0,stroke:#707070,stroke-width:2px,color:#1a1a1a
    classDef gold fill:#FFD700,stroke:#B8860B,stroke-width:2px,color:#1a1a1a

    class SHCSV,SHAPI,ArtistCSV,TracksCSV source
    class BronzeSH,BronzeAPI,BronzeArtist,BronzeTracks bronze
    class SilverSH,SilverArtist,SilverTracks silver
    class GoldSH,GoldArtist,GoldTracks gold
```

---

