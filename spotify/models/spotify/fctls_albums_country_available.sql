{{config(materialized = 'table', unique_key = 'composite_key')}}
with preprocess as (
  select
      album_id,
      JSON_EXTRACT_ARRAY(available_markets, '$') available_markets
  from spotify_stg.albums
)
select distinct
  {{dbt_utils.generate_surrogate_key(['album_id','available_market'])}} composite_key,
  album_id,
  available_market
from preprocess,
  unnest(available_markets) as available_market