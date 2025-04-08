{{config(materialized = 'table', unique_key = 'composite_key')}}
with preprocess as (
    select
        track_id,
        json_value(artist_elem, '$[0]') as artist_id,
        json_value(artist_elem, '$[1]') as artist_name
    from {{source('spotify_stg','tracks')}},
    unnest(json_extract_array(artists)) as artist_elem
)
select 
    {{dbt_utils.generate_surrogate_key(['track_id','artist_id'])}} composite_key,
    track_id,
    artist_id,
    artist_name
from preprocess