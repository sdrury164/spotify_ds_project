import numpy as np
import pandas as pd
import sqlite3 as sql
import spotipy
import SpotipyTools as spt
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from requests.exceptions import ReadTimeout
import spotipy.cache_handler as ch
import os
import pprint
from datetime import datetime
from requests.exceptions import ReadTimeout
import spotify_credentials as sp_cred

def get_album_track_ids(album_name, spy):
    album_results = spy.search(q='album:'+album_name, type='album')
    try:
        album_id = album_results['albums']['items'][0]['id']
        album = spy.album(album_id)

        album_track_ids = []

        for i in range(len(album['tracks']['items'])):
            album_track_ids.append(album['tracks']['items'][i]['id'])

        return album_track_ids
    except:
        return None

def get_album_track_features(album_name, spy):
    track_ids = get_album_track_ids(album_name, spy)
    if track_ids:
        track_features = [spy.audio_features(x)[0] for x in track_ids]
        for features in track_features:
            try:
                features['name'] = spy.track(features['id'])['name']
            except:
                features['name'] = None
            try:
                features['album_name'] = album_name
            except:
                features['album_name'] = None
            try:
                features['artist'] = spy.track(features['id'])['artists'][0]['name']
            except:
                features['artist'] = None
        return track_features
    else:
        return None

# Get detailed features of each track
# track_id = id given to track by spotify
# spy = spotify.Spotify() class instance
# def get_track_featured(track_id, spy):
#     track_features = spy.track(features['id'])['name']
#
#     pitch_class = {
#         0: 'C',
#         1: 'C#/Db',
#         2: 'D',
#         3: 'D#/Eb',
#         4: 'E',
#         5: 'F',
#         6: 'F#/Gb',
#         7: 'G',
#         8: 'G#/Ab',
#         9: 'A',
#         10: 'A#/Bb',
#         11: 'B'
#     }

def get_album_info(album_name, spy):
    album_results = spy.search(q='album:'+str(album_name), type='album')
    try:
        album_id = album_results['albums']['items'][0]['id']
        album = spy.album(album_id)
        return album
    except:
        return None

def get_artist_info(artist_name, spy):
    artist_results = spy.search(q='artist:'+artist_name, type='artist')
    try:
        artist_id = artist_results['artists']['items'][0]['id']
        artist = spy.artist(artist_id)
        return artist
    except:
        return None

# Get track IDs for all tracks on specified album.
# Note that this function searches by album name and
# selects the first album returned by query.
def get_album_track_ids(album_name, spy):
    album_results = spy.search(q='album:'+album_name, type='album')
    try:
        album_id = album_results['albums']['items'][0]['id']
        album = spy.album(album_id)

        album_track_ids = []

        for i in range(len(album['tracks']['items'])):
            album_track_ids.append(album['tracks']['items'][i]['id'])

        return album_track_ids
    except:
        return None

def get_df_from_db(data, cols):
    dict_list = []

    for item in data:
        item_dict = dict()
        for i in range(len(item)):
            item_dict[cols[i]] = item[i]
        dict_list.append(item_dict)

    return pd.DataFrame(dict_list)

def get_db_table(db_conn, table_name):
    cursor = conn.execute('SELECT * FROM ' + table_name)
    data = cursor.fetchall()
    return data

def get_df_from_table_name(db_conn, table_name, cols):
    data = get_db_table(db_conn, table_name)
    df = get_df_from_db(data, cols)
    return df

conn = sql.connect('p4k_review_features.db')

scope = 'user-library-read'

# Pick which credentials to use to prevent timeout problems
use_credential_set = 1

if use_credential_set == 1:
    os.environ['SPOTIPY_CLIENT_ID'] = sp_cred.client_id
    os.environ['SPOTIPY_CLIENT_SECRET'] = sp_cred.client_secret
    os.environ['SPOTIPY_REDIRECT_URI'] = sp_cred.redirect_url
elif use_credential_set == 2:
    os.environ['SPOTIPY_CLIENT_ID'] = sp_cred.client_id2
    os.environ['SPOTIPY_CLIENT_SECRET'] = sp_cred.client_secret2
    os.environ['SPOTIPY_REDIRECT_URI'] = sp_cred.redirect_url2
elif use_credential_set == 3:
    os.environ['SPOTIPY_CLIENT_ID'] = sp_cred.client_id3
    os.environ['SPOTIPY_CLIENT_SECRET'] = sp_cred.client_secret3
    os.environ['SPOTIPY_REDIRECT_URI'] = sp_cred.redirect_url3

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

sp.trace = False

reviews_cols = [
    'album_name',
    'album_id',
    'album_artist',
    'p4k_score',
    'p4k_genre',
    'p4k_date',
    'p4k_author',
    'p4k_author_role',
    'p4k_review_text',
    'p4k_is_bnm',
    'p4k_link',
    'p4k_label',
    'p4k_release_year'
]

p4k = pd.read_csv('p4k_filledna.csv')

current_review_df = get_df_from_table_name(conn, 'REVIEWS', reviews_cols)
current_review_links = current_review_df['p4k_link']
current_review_count = len(current_review_df)
reviews_left = len(p4k) - current_review_count

p4k_to_add = p4k[~p4k['link'].isin(current_review_links)]

print(str(len(p4k)) + ' total reviews in Pitchfork dataset.')
print(str(reviews_left) + ' reviews to get Spotify data for remaining.\n')

pitch_class = {
    0: 'C',
    1: 'C#/Db',
    2: 'D',
    3: 'D#/Eb',
    4: 'E',
    5: 'F',
    6: 'F#/Gb',
    7: 'G',
    8: 'G#/Ab',
    9: 'A',
    10: 'A#/Bb',
    11: 'B'
}

# Load in album IDs of albums already in db
album_cursor = conn.execute('SELECT * FROM ALBUMS')
fetched_albums = album_cursor.fetchall()

ids_in_db = []
for album in fetched_albums:
    ids_in_db.append(album[1])
ids_in_db = set(ids_in_db)

for i in range(len(p4k_to_add)):
    current_time = datetime.now().strftime('%d/%m/%Y %H:%M:%S')

    if i % 50 == 0:
        print('Adding album ' + str(i) + ' out of ' + str(len(p4k_to_add)) + ' albums.')
        print('Current time is ' + current_time + '.\n')
    if i == 2:
        print('The first couple of queries have been processed!\n')

    found_album = False
    while found_album == False:
        found_album = True
        try:
            album_p_row = p4k_to_add.iloc[i]
            review_date = album_p_row['date']
            album_name = album_p_row['album'].replace(' EP', '')
            album_s_row = get_album_info(album_name, sp)

            # artist = str(album_p_row['artist'])
            # artist_s_row = get_artist_info(artist, sp)
            # try:
            #     artist_id = artist_s_row['id']
            # except:
            #     artist_id = '%NO_ID%'

            try:
                artist_id = album_s_row['artists'][0]['id']
                artist = album_s_row['artists'][0]['name']
            except:
                artist_id = '%NO_ID%'
                artist = '%NO_NAME%'

            if album_s_row:
                album_id = album_s_row['id']
                if album_id not in ids_in_db:
                    album_release_date = album_s_row['release_date']
                    album_release_date_precision = album_s_row['release_date_precision']
                    try:
                        track_ids = get_album_track_ids(album_name, sp)
                        track_list = sp.tracks(track_ids)['tracks']
                        features_list = sp.audio_features(track_ids)
                        track_info_list = []

                        for i in range(len(track_ids)):
                            track = track_list[i]
                            track_info = features_list[i]

                            track_info['track_name'] = track['name']
                            track_info['track_id'] = track['id']
                            track_info['release_date'] = album_release_date
                            track_info['release_date_precision'] = album_release_date_precision
                            track_info['album_name'] = album_name
                            track_info['album_id'] = album_id
                            track_info['datetime_accessed'] = current_time
                            track_info['key_num'] = track_info['key']
                            track_info['key'] = pitch_class[track_info['key_num']]

                            track_items = [
                                'album',
                                'artists',
                                'duration_ms',
                                'explicit',
                                'track_number',
                                'popularity'
                            ]

                            for item in track_items:
                                track_info[item] = track[item]

                            conn.execute('''INSERT INTO TRACKS
                                            (track_name, track_id, duration_ms,
                                            explicit, track_number, album_name,
                                            album_id, release_date, release_date_precision,
                                            datetime_accessed, danceability, energy,
                                            key, key_num, loudness,
                                            mode, speechiness, acousticness,
                                            instrumentalness, liveness, valence,
                                            tempo, time_signature)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''',
                                            (track_info['track_name'],
                                            track_info['track_id'],
                                            track_info['duration_ms'],
                                            track_info['explicit'],
                                            track_info['track_number'],
                                            track_info['album_name'],
                                            track_info['album_id'],
                                            track_info['release_date'],
                                            track_info['release_date_precision'],
                                            track_info['datetime_accessed'],
                                            track_info['danceability'],
                                            track_info['energy'],
                                            track_info['key'],
                                            track_info['key_num'],
                                            track_info['loudness'],
                                            track_info['mode'],
                                            track_info['speechiness'],
                                            track_info['acousticness'],
                                            track_info['instrumentalness'],
                                            track_info['liveness'],
                                            track_info['valence'],
                                            track_info['tempo'],
                                            track_info['time_signature']))

                            conn.execute('''INSERT INTO ARTIST_ON_TRACK
                                            (track_id, artist_id)
                                            VALUES (?, ?);''',
                                            (track_info['track_id'], artist_id))

                            track_info_list.append(track_info)

                            track_info_df = pd.DataFrame(track_info_list)

                        image1 = album_s_row['images'][0]['url']
                        image2 = album_s_row['images'][1]['url']
                        image3 = album_s_row['images'][2]['url']

                        conn.execute('''INSERT INTO ALBUMS
                                        (album_name, album_id, type,
                                        url, image1, image2,
                                        image3, label, release_date,
                                        release_date_precision, track_count, popularity,
                                        datetime_accessed, avg_danceability, sd_danceability,
                                        avg_energy, sd_energy, avg_loudness,
                                        sd_loudness, avg_speechiness, sd_speechiness,
                                        avg_acousticness, sd_acousticness, avg_instrumentalness,
                                        sd_instrumentalness, avg_liveness, sd_liveness,
                                        avg_valence, sd_valence, avg_tempo,
                                        sd_tempo)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''',
                                        (album_name, album_s_row['id'], album_s_row['type'],
                                        album_s_row['external_urls']['spotify'], image1, image2,
                                        image3, album_s_row['label'], album_s_row['release_date'],
                                        album_s_row['release_date_precision'], album_s_row['total_tracks'], album_s_row['popularity'],
                                        current_time, track_info_df['danceability'].mean(), track_info_df['danceability'].std(),
                                        track_info_df['energy'].mean(), track_info_df['energy'].std(), track_info_df['loudness'].mean(),
                                        track_info_df['loudness'].std(), track_info_df['speechiness'].mean(), track_info_df['speechiness'].std(),
                                        track_info_df['acousticness'].mean(), track_info_df['acousticness'].std(), track_info_df['instrumentalness'].mean(),
                                        track_info_df['instrumentalness'].std(), track_info_df['liveness'].mean(), track_info_df['liveness'].std(),
                                        track_info_df['valence'].mean(), track_info_df['valence'].std(), track_info_df['tempo'].mean(),
                                        track_info_df['tempo'].std()))

                        conn.execute('''INSERT INTO REVIEWS
                                        (album_name, album_id, album_artist, p4k_score,
                                        p4k_genre, p4k_date, p4k_author,
                                        p4k_author_role, p4k_review_text, p4k_is_bnm,
                                        p4k_link, p4k_label, p4k_release_year)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''',
                                        (album_name, album_id, artist, float(album_p_row['score']),
                                        album_p_row['genre'], album_p_row['date'], album_p_row['author'],
                                        album_p_row['role'], album_p_row['review'], int(album_p_row['bnm']),
                                        album_p_row['link'], album_p_row['label'], album_p_row['release_year']))

                        conn.execute('''INSERT INTO ARTIST_ON_ALBUM
                                        (album_id, artist_id)
                                        VALUES (?, ?);''',
                                        (album_id, artist_id))

                        for genre in album_s_row['genres']:
                            conn.execute('''INSERT INTO ALBUM_IS_GENRE
                                            (album_id, genre)
                                            VALUES (?, ?);''',
                                            (album_id, genre))

                        conn.commit()

                        found_album = True
                    except Exception as e:
                        print('Could not retrieve trck IDs for album ' + str(album_name) + '.')
                        print('Album number is ' + str(i) + ' out of ' + str(len(p4k_to_add)) + ' albums.')
                        print('Exception: \n' + str(e))
                        conn.execute('''INSERT INTO ALBUM_NOT_FOUND
                                        (album_name, album_artist, review_date,
                                        error_msg, current_time)
                                        VALUES (?, ?, ?, ?, ?);''',
                                        (str(album_name), artist, review_date,
                                        '%IDs_NOT_FOUND%' + str(e), current_time))
                        conn.commit()
                        found_album = True
            else:
                print('No album row for album ' + str(album_name) + '.')
                conn.execute('''INSERT INTO ALBUM_NOT_FOUND
                                (album_name, album_artist, review_date,
                                error_msg, current_time)
                                VALUES (?, ?, ?, ?, ?);''',
                                (str(album_name), artist, review_date,
                                'NO ALBUM ROW', current_time))
                conn.commit()
                found_album = True
        except Exception as e:
            print('Could not retrieve album ' + str(album_name) + '.')
            print('Album number is ' + str(i) + ' out of ' + str(len(p4k_to_add)) + ' albums.')
            print('Exception: \n' + str(e))
            conn.execute('''INSERT INTO ALBUM_NOT_FOUND
                            (album_name, album_artist, review_date,
                            error_msg, current_time)
                            VALUES (?, ?, ?, ?, ?);''',
                            (str(album_name), artist, review_date,
                            str(e), current_time))
            conn.commit()
            found_album = True
        except ReadTimeout:
            print('Spotify timed out - we\'ll try again...')
            found_album = False
