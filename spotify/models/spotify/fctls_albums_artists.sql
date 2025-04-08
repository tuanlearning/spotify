{{
    config(materialized = 'table', unique_key = 'composite_key')
}}
select 
    {{dbt_utils.generate_surrogate_key(['album_id','artist_id'])}} composite_key,
    album_id,
    artist_id
from {{source('spotify_stg','albums')}}