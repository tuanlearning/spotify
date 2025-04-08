import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dotenv import load_dotenv
import altair as alt


# Initialize BigQuery client
@st.cache_resource
def init_bq_client():
    return bigquery.Client(project="august-sandbox-425102-m1")

# Run a query and return a DataFrame
def run_query(client, query, columns):
    result = client.query(query).result()
    data = [list(row) for row in result]
    df = pd.DataFrame(data, columns=columns)
    return df

load_dotenv()
bq_client = init_bq_client()

st.title("🎵 Spotify Dashboard")
st.markdown("Powered by BigQuery + Streamlit")

# Artist details query
artist_query = """
WITH album_count AS (
    SELECT b.artist_id, COUNT(a.album_id) AS album_count
    FROM spotify.dim_albums a
    INNER JOIN spotify.fctls_albums_artists b ON a.album_id = b.album_id
    GROUP BY b.artist_id
),
track_agg AS (
    SELECT b.artist_id,
           COUNT(1) AS tracks_count,
           SUM(CASE WHEN explicit THEN 1 ELSE 0 END) AS explicit_tracks_count,
           AVG(duration_ms) AS average_duration_ms
    FROM spotify.dim_tracks a
    INNER JOIN spotify.fctls_tracks_artists b ON a.track_id = b.track_id
    GROUP BY b.artist_id
)
SELECT a.id,
       a.name,
       a.genres,
       a.popularity,
       a.followers,
       b.album_count,
       c.tracks_count,
       c.explicit_tracks_count,
       ROUND(c.explicit_tracks_count * 100.0 / c.tracks_count, 2) AS explicit_tracks_ratio,
       c.average_duration_ms
FROM spotify.dim_artists a
LEFT JOIN album_count b ON a.id = b.artist_id
LEFT JOIN track_agg c ON a.id = c.artist_id
"""


artist_columns = [
    "id", "name", "genres", "popularity", "followers",
    "album_count", "tracks_count", "explicit_tracks_count",
    "explicit_tracks_ratio", "average_duration_ms"
]

artist_df = run_query(bq_client, artist_query, artist_columns)

# Display artist stats
st.subheader("🎤 Artist Overview")
st.dataframe(artist_df)

# Bar chart: Average duration per artist
st.subheader("⏱️ Avg. Track Duration by Artist (ms)")
duration_chart = alt.Chart(artist_df).mark_bar().encode(
    x=alt.X("name:N", sort="-y", title="Artist"),
    y=alt.Y("average_duration_ms:Q", title="Average Duration (ms)"),
    tooltip=["name", "average_duration_ms"]
).properties(height=400)
st.altair_chart(duration_chart, use_container_width=True)

# Bar chart: Explicit track ratio per artist
st.subheader("🧨 Explicit Track Ratio by Artist (%)")
explicit_chart = alt.Chart(artist_df).mark_bar(color='crimson').encode(
    x=alt.X("name:N", sort="-y", title="Artist"),
    y=alt.Y("explicit_tracks_ratio:Q", title="Explicit Track Ratio (%)"),
    tooltip=["name", "explicit_tracks_ratio"]
).properties(height=400)
st.altair_chart(explicit_chart, use_container_width=True)
