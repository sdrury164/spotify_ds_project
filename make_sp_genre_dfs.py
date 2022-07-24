# -*- coding: utf-8 -*-
"""
Created on Mon Jul  4 08:51:53 2022

@author: Steve
"""

import numpy as np
import pandas as pd
import re
import sqlite3 as sql

import matplotlib.pyplot as plt

from pprint import pprint
from datetime import datetime

import p4k_db_functions as db_func

conn = sql.connect('p4k_review_features.db')

col_dict = db_func.get_col_dict()

def get_spotify_genre_df():
    sp_genres = db_func.get_df_from_table_name(conn, 'ARTIST_IS_GENRE', col_dict['artist_is_genre'])
    sp_genres['last_word'] = [genre.split(' ')[-1] for genre in sp_genres['genre']]
    sp_genres['genre_length'] = [len(genre.split(' ')) for genre in sp_genres['genre']]
    return sp_genres

def get_col_per_genre_df(sp_genres):
    artist_df = sp_genres[['artist_name', 'artist_id']].drop_duplicates().copy()
    artist_sp_genres = sp_genres.groupby('artist_id')['genre'].apply(list)
    artist_sp_genres = artist_sp_genres.reset_index()
    print('artist_sp_genres.columns:')
    pprint(artist_sp_genres.columns)
    artist_sp_genres['genre_list'] = artist_sp_genres['genre'].copy()
    artist_sp_genres = artist_sp_genres.drop('genre', axis=1)
    artist_sp_genres = artist_sp_genres.merge(artist_df, on='artist_id')
    for genre in sp_genres['genre'].unique():
        artist_sp_genres[genre] = [1 if genre in genre_list else 0 for genre_list in artist_sp_genres['genre_list']]
    return artist_sp_genres

sp_genres = get_spotify_genre_df()

# artist_sp_genres = get_col_per_genre_df(sp_genres)
artist_sp_genres = pd.read_csv('artist_spotify_genre_le.csv')

albums = db_func.get_df_from_table_name(conn, 'ALBUMS', col_dict['albums'])
artist_on_album = db_func.get_df_from_table_name(conn, 'ARTIST_ON_ALBUM', col_dict['artist_on_album'])
albums_bare_df = albums[['album_name', 'album_id']].copy()
album_genres_sparse = albums_bare_df.merge(artist_on_album, on='album_id')
album_genres_sparse = album_genres_sparse.merge(artist_sp_genres, on='artist_id')
album_genres_sparse = album_genres_sparse.drop('genre_list', axis=1)

album_genres_sparse.to_csv('album_spotify_genre_le.csv')
