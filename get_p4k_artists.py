import numpy as np
import pandas as pd
import sqlite3 as sql

import os
import sys

import spotipy
import SpotipyTools as spt
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from requests.exceptions import ReadTimeout
import spotipy.cache_handler as ch

import p4k_db_functions as db_func

auth_manager = SpotifyClientCredentials(
    '58caee4dd952410191cad03bb1a4755f',
    '09177d69ef5b43e4b3b70e2bdc8a7c93',
    cache_handler = ch.CacheFileHandler()
)

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

conn = sql.connect('p4k_review_features.db')

col_dict = db_func.get_col_dict()

artist_on_album = db_func.get_df_from_table_name(conn, 'ARTIST_ON_ALBUM', col_dict['artist_on_album'])

artist_ids = artist_on_album['artist_id'].unique()

artists_in_table = db_func.get_df_from_table_name(conn, 'ARTISTS', col_dict['artists'])

if len(artists_in_table) > 0:
    print('\n' + '-'*50)
    print('artists_in_table.columns:')
    print(artists_in_table.columns)
    print('-'*50 + '\n')

    ids_in_table = artists_in_table['artist_id']
    ids_to_get = artist_on_album[~artist_on_album['artist_id'].isin(ids_in_table)]['artist_id'].copy()
else:
    ids_to_get = artist_ids.copy()

print('\n' + '-'*50)
print('Number of IDs to fetch: ' + str(len(ids_to_get)))
print('-'*50 + '\n')

i = 0

for id in ids_to_get:
    i += 1

    if i % 50 == 0:
        print('\nFetching artist ID #' + str(i) + '\n')

    try:
        artist = sp.artist(id)
        artist_name = artist['name']
        popularity = artist['popularity']
        if len(artist['images']) > 0:
            img_url = artist['images'][0]['url']
        else:
            img_url = ''

        conn.execute('''INSERT INTO ARTISTS
                        (artist_name, artist_id, popularity,
                        img_url)
                        VALUES (?, ?, ?, ?);''',
                        (artist_name, id, popularity,
                        img_url))

        conn.commit()
    except Exception as e:
        print('\n' + '-'*50)
        print('Error on ID "' + str(id) + '"')
        print('i = ' + str(i))
        print('Exception:\n' + str(e))
        print('-'*50 + '\n')
