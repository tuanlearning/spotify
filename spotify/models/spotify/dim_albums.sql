{{
    config(
        materialized = 'table',
        unique_key = 'album_id'
    )
}}

select distinct
    album_id, 
    album_type, 
    total_tracks, 
    name, 
    cast(release_date as STRING) as release_date
from {{source('spotify_stg','albums')}}