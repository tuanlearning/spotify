from dotenv import load_dotenv
import os
import base64
from requests import post, get
load_dotenv()
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
import pandas as pd
from common.utils import upload_to_gcs, init_bq_client, create_external_table
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

def get_token(CLIENT_ID, CLIENT_SECRET):
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
    return token

def get_auth_header(token):
    print(token)
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
    query_url = f"https://api.spotify.com/v1/artists/{artist_id}/albums?limit={limit}"
    result = get(url=query_url, headers=headers)
    
    # Check if the response status code is 200 (OK)
    if result.status_code == 200:
        try:
            # Attempt to parse the response JSON
            return result.json()  # Automatically handles JSON decoding
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON response from Spotify API: {result.content}")
            return None
    else:
        logging.error(f"API request failed with status code {result.status_code}: {result.content}")
        return None
    
def parse_albums_information(json_result, artist_id):
    if json_result is None:
        logging.error('Json is null')
    if "error" in json_result:
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
    return json.loads(result.content) if result is not None else None

def parse_track(json_result, album_id, tracks):
    if json_result is None:
        logging.error(f'json result is null')
        return None
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
    
def create_artist_df(columns, headers, ARTISTS):
    artists_df = pd.DataFrame(columns = columns)
    for artist in ARTISTS:
        artist_json_result = search_for_artist(headers = headers, artist_name = artist)
        artist_dict = parse_artist_information(artist_json_result)
        tmp_artist_df = pd.DataFrame([artist_dict], columns = columns)
        artists_df = pd.concat([artists_df, tmp_artist_df])
    return artists_df

def create_album_df(artist_ids, columns, headers):
    album_df = pd.DataFrame(columns = columns)
    for artist_id in artist_ids:
        album_json_result = search_artist_albums(artist_id=artist_id, limit=50, headers=headers)
        albums_list_dict = parse_albums_information(album_json_result, artist_id)
        for album_dict in albums_list_dict:
            tmp_album_df = pd.DataFrame([album_dict], columns = columns)
            album_df = pd.concat([album_df, tmp_album_df])
    return album_df

def create_track_df(album_ids, headers, TRACK_COLUMNS):
    tracks = []
    for album_id in album_ids:
        json_result = search_album_tracks(album_id, headers, 50)
        parse_track(json_result, album_id, tracks)
    tracks_df = pd.DataFrame(columns = TRACK_COLUMNS)
    for track in tracks:
        tmp_track_df = pd.DataFrame(columns = TRACK_COLUMNS, data = [track])
        tracks_df = pd.concat([tracks_df, tmp_track_df])
    return tracks_df

def get_dfs(ARTISTS):
    token = get_token(CLIENT_ID, CLIENT_SECRET)
    headers = get_auth_header(token)

    ARTIST_COLUMNS = ['id','name','genres','popularity','followers']
    artists_df = create_artist_df(columns = ARTIST_COLUMNS, headers=headers, ARTISTS = ARTISTS)
    
    artist_ids = artists_df['id'].drop_duplicates()

    ALBUM_COLUMNS = ['album_id','album_type','total_tracks','available_markets','name','release_date','artist_id']
    albums_df = create_album_df(artist_ids = artist_ids, headers=headers, columns = ALBUM_COLUMNS)

    TRACK_COLUMNS = ['track_id','track_name','type','explicit','duration','artists','preview_url','album_id']
    album_ids = albums_df['album_id'].drop_duplicates()
    tracks_df = create_track_df(album_ids, headers, TRACK_COLUMNS)

    return artists_df, albums_df, tracks_df

def main():
    ARTISTS = ['RPT MCK','WXRDIE','TLINH','GreyD','Wren Evans','Dangrangto','Binz','Soobin Hoang Son']
    artists_df, albums_df, tracks_df = get_dfs(ARTISTS)

    dfs = [artists_df, albums_df, tracks_df]
    local_paths = ['data/artists.csv', 'data/albums.csv', 'data/tracks.csv']
    destination_blob_names = ["artists/artists.csv", "albums/albums.csv", "tracks/tracks.csv"]
    for df, local_path in zip(dfs, local_paths):
        df.to_csv(local_path, index = False, encoding="utf-8-sig")

    for local_path, destination_blob_name in zip(local_paths, destination_blob_names):
        upload_to_gcs(
            bucket_name="spotify_tuanlg",
            source_file_name=local_path,
            destination_blob_name=destination_blob_name
        )
    
    bq_client = init_bq_client()
    
    create_external_table(bq_client, 'august-sandbox-425102-m1', 'spotify_stg', 'artists', 'gs://spotify_tuanlg/artists/artists.csv')
    create_external_table(bq_client, 'august-sandbox-425102-m1', 'spotify_stg', 'albums', 'gs://spotify_tuanlg/albums/albums.csv')
    create_external_table(bq_client, 'august-sandbox-425102-m1', 'spotify_stg', 'tracks', 'gs://spotify_tuanlg/tracks/tracks.csv')


if __name__ == '__main__':
    main()