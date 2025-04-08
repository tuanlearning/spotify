from dotenv import load_dotenv
import base64
from requests import post, get
load_dotenv()
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
import pandas as pd
from common.utils import upload_to_gcs, init_bq_client, create_external_table
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from common.utils import upload_to_gcs, init_bq_client, create_external_table, execute_dbt_model

default_args = {
    'start_date': datetime(2025, 3, 6),
    'retries': 0,
}


def get_auth_header():
    CLIENT_ID = os.getenv('CLIENT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')
    auth_string = CLIENT_ID + ":" + CLIENT_SECRET
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        'Authorization': 'Basic ' + auth_base64,
        'Content-Type': "application/x-www-form-urlencoded"
    }
    data = {'grant_type': 'client_credentials'}
    result = post(url, headers= headers, data = data)
    json_result = json.loads(result.content)
    token = json_result['access_token']
    return {"Authorization": "Bearer " + token}

def search_for_artist(artist_name, headers):
    url = "https://api.spotify.com/v1/search"
    headers = headers
    query = f"?q={artist_name}&type=artist&limit=1"
    query_url = url + query
    result = get(url = query_url, headers = headers)
    json_result = json.loads(result.content)
    return json_result

def parse_artist_information(json_result):
    if "error" in json_result:
        logging.error(f"Failed to retrieve the data, status: {json_result['error']['status']}, message: {json_result['error']['message']}")
        return None
    else:
        followers = json_result['artists']['items'][0]['followers']['total']
        genres = ''.join(json_result['artists']['items'][0]['genres'])
        id = json_result['artists']['items'][0]['id']
        popularity = json_result['artists']['items'][0]['popularity']
        name = json_result['artists']['items'][0]['name']
        artist_information = {'id': id, 'name': name, 'genres': genres, 'popularity': popularity, 'followers': followers}
        return artist_information


def search_artist_albums(artist_id, limit, headers):
    headers = headers
    query_url = "https://api.spotify.com/v1/artists/" + artist_id + "/albums" + f"?limit={limit}"
    result = get(url = query_url, headers = headers)
    return json.loads(result.content) if result is not None else None

def parse_albums_information(json_result, artist_id):
    if json_result is None:
        logging.error('Json is null')
        return None
    elif "error" in json_result:
        logging.error(f"Failed to retrieve the data, status: {json_result['error']['status']}, message: {json_result['error']['message']}")
        return None
    elif 'items' not in json_result:
        logging.error(f'Cannot find key: "items" in the json')
        return None
    else:
        album_list = json_result['items']
        albums_information = []
        for album in album_list:
            album_type = album['album_type']
            total_tracks = album['total_tracks']
            available_markets = album['available_markets']
            album_id = album['id']
            name = album['name']
            release_date = album['release_date']
            # artist_id = album['artists'][0]['id']
            albums_information.append \
                                    (
                                        {
                                        'album_id': album_id, \
                                        'album_type': album_type, \
                                        'total_tracks': total_tracks, \
                                        'available_markets': available_markets, \
                                        'name': name, \
                                        'release_date': release_date, \
                                        'artist_id': artist_id
                                        }
                                    )
        return albums_information


def search_album_tracks(album_id, headers, limit):
    headers = headers
    query_url = "https://api.spotify.com/v1/albums/" + album_id + "/tracks" + f"?limit={limit}"
    result = get(url = query_url, headers = headers)
    return json.loads(result.content)

def parse_track(json_result, album_id, tracks):
    if "error" in json_result:
        logging.error(f"Failed to retrieve the data, status: {json_result['error']['status']}, message: {json_result['error']['message']}")
        return None
    elif 'items' not in json_result:
        logging.error(f'Cannot find key: "items" in the json')
        print(json_result)
        return None
    else:
        for item in json_result['items']:
            track_id = item['id']
            track_name = item['name']
            type = item['type']
            explicit = item['explicit']
            duration = item['duration_ms']
            artists = []
            for element in item['artists']:
                artist_id = element['id']
                artist_name = element['name']
                artists.append([artist_id, artist_name])
            preview_url = item['preview_url']
            track =  {
                'track_id': track_id,\
                'track_name': track_name,\
                'type': type,\
                'explicit': explicit,\
                'duration': duration,\
                'artists': artists,\
                'preview_url': preview_url,\
                'album_id': album_id
            }
            tracks.append(track)
    
def create_artist_df(columns, ARTISTS, **kwargs):
    ti = kwargs['ti']
    headers = ti.xcom_pull(task_ids='get_spotify_auth_header')
    artists_df = pd.DataFrame(columns = columns)
    for artist in ARTISTS:
        artist_json_result = search_for_artist(headers = headers, artist_name = artist)
        artist_dict = parse_artist_information(artist_json_result)
        tmp_artist_df = pd.DataFrame([artist_dict], columns = columns)
        artists_df = pd.concat([artists_df, tmp_artist_df])
    artists_df.to_csv('airflow/dags/data/artists.csv', index = False, encoding="utf-8-sig")
    return artists_df

def create_album_df(columns, **kwargs):
    ti = kwargs['ti']
    headers = ti.xcom_pull(task_ids='get_spotify_auth_header')
    artists_df = ti.xcom_pull(task_ids = 'create_artist_df')
    artist_ids = artists_df['id'].drop_duplicates()

    album_df = pd.DataFrame(columns = columns)
    for artist_id in artist_ids:
        album_json_result = search_artist_albums(artist_id=artist_id, limit=50, headers=headers)
        albums_list_dict = parse_albums_information(album_json_result, artist_id)
        for album_dict in albums_list_dict:
            tmp_album_df = pd.DataFrame([album_dict], columns = columns)
            album_df = pd.concat([album_df, tmp_album_df])
            
    album_df.to_csv('airflow/dags/data/albums.csv', index = False, encoding="utf-8-sig")
    return album_df

def create_track_df(TRACK_COLUMNS, **kwargs):
    ti = kwargs['ti']
    headers = ti.xcom_pull(task_ids='get_spotify_auth_header')
    albums_df = ti.xcom_pull(task_ids = 'create_album_df')
    album_ids = albums_df['album_id'].drop_duplicates()
    tracks = []
    for album_id in album_ids:
        json_result = search_album_tracks(album_id, headers, 50)
        parse_track(json_result, album_id, tracks)
    tracks_df = pd.DataFrame(tracks, columns = TRACK_COLUMNS)
    # for track in tracks:
    #     tmp_track_df = pd.DataFrame(columns = TRACK_COLUMNS, data = [track])
    #     tracks_df = pd.concat([tracks_df, tmp_track_df])
    tracks_df.to_csv('airflow/dags/data/tracks.csv', index = False, encoding="utf-8-sig")
    return tracks_df

# def dfs_to_csv(**kwargs): # cannot use this since ti.xcom_pull modify the pandas frame
#     ti = kwargs['ti']
#     artists_df = ti.xcom_pull(task_ids = 'create_artist_df')
#     albums_df = ti.xcom_pull(task_ids = 'create_album_df')
#     tracks_df = ti.xcom_pull(task_ids = 'create_track_df')
#     logging.info(f'Row count artists: {len(artists_df)}, Row count albums: {len(albums_df)}, Row count tracks: {len(tracks_df)}')
#     dfs = [artists_df, albums_df, tracks_df]

#     local_paths = ['airflow/dags/data/artists.csv', 'airflow/dags/data/albums.csv', 'airflow/dags/data/tracks.csv']
#     data_directory = os.path.join(os.getcwd(), 'airflow', 'dags', 'data')
#     logging.info(f"Current working directory: {os.getcwd()}, data directory: {data_directory}")
#     for df, local_path in zip(dfs, local_paths):
#         try:
#             df.to_csv(local_path, index = False, encoding="utf-8-sig")
#             logging.info(f"Write df to {local_path}")
#         except Exception as e:
#             logging.error(f'Cannot write df to {local_path}: {e}')

def upload_csv_to_gcs():
    local_paths = ['airflow/dags/data/artists.csv', 'airflow/dags/data/albums.csv', 'airflow/dags/data/tracks.csv']
    destination_blob_names = ["artists/artists.csv", "albums/albums.csv", "tracks/tracks.csv"]

    for local_path, destination_blob_name in zip(local_paths, destination_blob_names):
        upload_to_gcs(
            bucket_name="spotify_tuanlg",
            source_file_name=local_path,
            destination_blob_name=destination_blob_name
        )
    
def create_external_tables():
    bq_client = init_bq_client()
    create_external_table(bq_client, 'august-sandbox-425102-m1', 'spotify_stg', 'artists', 'gs://spotify_tuanlg/artists/artists.csv')
    create_external_table(bq_client, 'august-sandbox-425102-m1', 'spotify_stg', 'albums', 'gs://spotify_tuanlg/albums/albums.csv')
    create_external_table(bq_client, 'august-sandbox-425102-m1', 'spotify_stg', 'tracks', 'gs://spotify_tuanlg/tracks/tracks.csv')

with DAG('spotify',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:
    get_spotify_auth_header = PythonOperator(
        task_id = 'get_spotify_auth_header',
        python_callable = get_auth_header,
        provide_context = True
    )
    create_artist_df = PythonOperator(
        task_id = 'create_artist_df',
        python_callable = create_artist_df,
        op_args = [
            ['id','name','genres','popularity','followers'], 
            ['RPT MCK','WXRDIE','TLINH','GreyD','Wren Evans',\
             'Dangrangto','Binz','Soobin Hoang Son','Hieu Thu Hai','Obito','Tage',\
             'Son Tung MTP', 'Erik', 'Quang Anh Rhyder', 'Vu', 'Chillies', 'Amee', \
            'Ha Anh Tuan','Justatee','Duc Phuc','Wean','Madihu']
        ],
        provide_context = True
    )
    create_album_df = PythonOperator(
        task_id = 'create_album_df',
        python_callable = create_album_df,
        op_args = [
            ['album_id','album_type','total_tracks','available_markets','name','release_date','artist_id']
        ],
        provide_context = True
    )
    create_track_df = PythonOperator(
        task_id = 'create_track_df',
        python_callable = create_track_df,
        op_args = [
            ['track_id','track_name','type','explicit','duration','artists','preview_url','album_id']
        ],
        provide_context = True
    )
    # dfs_to_csv = PythonOperator(
    #     task_id = 'dfs_to_csv',
    #     python_callable = dfs_to_csv
    # )
    upload_csv_to_gcs = PythonOperator(
        task_id = 'upload_csv_to_gcs',
        python_callable = upload_csv_to_gcs
    )

    create_external_tables_from_gcs = PythonOperator(
        task_id = 'create_external_tables_from_gcs',
        python_callable = create_external_tables
    )

    run_dbt_artists = execute_dbt_model('dim_artists')
    run_dbt_albums = execute_dbt_model('dim_albums')
    run_dbt_tracks = execute_dbt_model('dim_tracks')
    run_dbt_fctls_albums_country_available = execute_dbt_model('fctls_albums_country_available')
    run_dbt_fctls_albums_artists = execute_dbt_model('fctls_albums_artists')
    run_dbt_fctls_tracks_artists = execute_dbt_model('fctls_tracks_artists')

    get_spotify_auth_header >> create_artist_df >> create_album_df >> create_track_df >> upload_csv_to_gcs >> create_external_tables_from_gcs
    create_external_tables_from_gcs >> run_dbt_artists
    create_external_tables_from_gcs >> run_dbt_albums >> [run_dbt_fctls_albums_artists, run_dbt_fctls_albums_country_available] 
    create_external_tables_from_gcs >> run_dbt_tracks >> run_dbt_fctls_tracks_artists