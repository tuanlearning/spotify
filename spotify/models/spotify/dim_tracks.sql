{{
    config(materialized = 'table', unique_key = 'track_id')
}}
select 
  track_id,
  track_name,
  type,
  explicit,
  duration duration_ms,
  preview_url,
  album_id
from {{source('spotify_stg','tracks')}}