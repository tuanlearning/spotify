{{ config(materialized='table', unique_key = 'id') }}

select 
    id,
    name,
    genres,
    popularity,
    followers
from {{source('spotify_stg','artists')}}