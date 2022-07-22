"""
Various functions and variable definitions to make it easier
to interact with the SQL database that contains information
about albums, tracks, reviews, and more.
"""

import sqlite3 as sql
import pandas as pd

conn = sql.connect('p4k_review_features.db')

"""
Input data from get_db_table() function (which is in the form
of a list of lists) and the column names for the data and return
a pandas DataFrame
"""
def get_df_from_db(data, cols):
    dict_list = []

    for item in data:
        item_dict = dict()
        for i in range(len(item)):
            item_dict[cols[i]] = item[i]
        dict_list.append(item_dict)

    return pd.DataFrame(dict_list)

"""
Retrieve a table from SQL database as list of lists
"""
def get_db_table(db_conn, table_name):
    cursor = conn.execute('SELECT * FROM ' + table_name)
    data = cursor.fetchall()
    return data

"""
This is the function to use to actually load in a table from
the DB. It makes use of the above two functions (get_df_from_db()
and get_db_table()).
Input: connection to SQL table object from sqlite3
"""
def get_df_from_table_name(db_conn, table_name, cols):
    data = get_db_table(db_conn, table_name)
    df = get_df_from_db(data, cols)
    return df

album_cols = [
    'album_name',
    'album_id',
    'type',
    'url',
    'image1',
    'image2',
    'image3',
    'label',
    'release_date',
    'release_date_precision',
    'track_count',
    'popularity',
    'datetime_accessed',
    'avg_danceability',
    'sd_danceability',
    'avg_energy',
    'sd_energy',
    'avg_loudness',
    'sd_loudness',
    'avg_speechiness',
    'sd_speechiness',
    'avg_acousticness',
    'sd_acousticness',
    'avg_instrumentalness',
    'sd_instrumentalness',
    'avg_liveness',
    'sd_liveness',
    'avg_valence',
    'sd_valence',
    'avg_tempo',
    'sd_tempo'
]

error_cols = [
    'name',
    'artist',
    'review_date',
    'error_msg',
    'accessed'
]

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

album_not_found_cols = [
    'album_name',
    'album_artist',
    'review_date',
    'error_msg',
    'current_time'
]

artist_on_album_cols = [
    'album_id',
    'artist_id'
]

album_is_genre_cols = [
    'album_id',
    'genre'
]

tracks_cols = [
    'track_name',
    'track_id',
    'duration_ms',
    'explicit',
    'track_number',
    'album_name',
    'album_id',
    'release_date',
    'release_date_precision',
    'datetime_accessed',
    'danceability',
    'energy',
    'key',
    'key_num',
    'loudness',
    'mode',
    'speechiness',
    'acousticness',
    'instrumentalness',
    'liveness',
    'valence',
    'tempo',
    'time_signature'
]

artist_on_track_cols = [
    'track_id',
    'artist_id'
]

table_names = [
    'ALBUMS',
    'TRACKS',
    'REVIEWS',
    'ALBUM_IS_GENRE',
    'ARTIST_ON_ALBUM',
    'ARTIST_ON_TRACK',
    'ALBUM_NOT_FOUND'
]

col_list = [
    album_cols,
    tracks_cols,
    reviews_cols,
    album_is_genre_cols,
    artist_on_album_cols,
    artist_on_track_cols,
    album_not_found_cols
]

def get_col_dict():
    album_cols = [
        'album_name',
        'album_id',
        'type',
        'url',
        'image1',
        'image2',
        'image3',
        'label',
        'release_date',
        'release_date_precision',
        'track_count',
        'popularity',
        'datetime_accessed',
        'avg_danceability',
        'sd_danceability',
        'avg_energy',
        'sd_energy',
        'avg_loudness',
        'sd_loudness',
        'avg_speechiness',
        'sd_speechiness',
        'avg_acousticness',
        'sd_acousticness',
        'avg_instrumentalness',
        'sd_instrumentalness',
        'avg_liveness',
        'sd_liveness',
        'avg_valence',
        'sd_valence',
        'avg_tempo',
        'sd_tempo'
    ]

    error_cols = [
        'name',
        'artist',
        'review_date',
        'error_msg',
        'accessed'
    ]

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

    album_not_found_cols = [
        'album_name',
        'album_artist',
        'review_date',
        'error_msg',
        'current_time'
    ]

    artist_on_album_cols = [
        'album_id',
        'artist_id'
    ]

    album_is_genre_cols = [
        'album_id',
        'genre'
    ]

    tracks_cols = [
        'track_name',
        'track_id',
        'duration_ms',
        'explicit',
        'track_number',
        'album_name',
        'album_id',
        'release_date',
        'release_date_precision',
        'datetime_accessed',
        'danceability',
        'energy',
        'key',
        'key_num',
        'loudness',
        'mode',
        'speechiness',
        'acousticness',
        'instrumentalness',
        'liveness',
        'valence',
        'tempo',
        'time_signature'
    ]

    artist_on_track_cols = [
        'track_id',
        'artist_id'
    ]

    lyrics_cols = [
        'track_name',
        'album_name',
        'album_id',
        'rough_lyrics',
        'lyrics'
    ]

    bad_lyrics_cols = [
        'album_name',
        'album_id',
        'error_msg'
    ]

    track_sentiment_cols = [
        'track_name',
        'album_name',
        'album_id',
        'pos_sentiment',
        'neu_sentiment',
        'neg_sentiment'
    ]

    artist_cols = [
        'artist_name',
        'artist_id',
        'popularity',
        'img_url'
    ]

    artist_is_genre_cols = [
        'artist_name',
        'artist_id',
        'genre'
    ]

    lda_coherences_cols = [
        'filename',
        'coherence_type',
        'num_topics',
        'chunksize',
        'passes',
        'iterations',
        'coherence_score'
    ]

    table_names = [
        'ALBUMS',
        'TRACKS',
        'REVIEWS',
        'ALBUM_IS_GENRE',
        'ARTIST_ON_ALBUM',
        'ARTIST_ON_TRACK',
        'ALBUM_NOT_FOUND',
        'LYRICS',
        'BAD_LYRICS',
        'TRACK_SENTIMENTS',
        'ARTISTS',
        'ARTIST_IS_GENRE',
        'LDA_COHERENCES'
    ]

    # col_list = [
    #     album_cols,
    #     tracks_cols,
    #     reviews_cols,
    #     album_is_genre_cols,
    #     artist_on_album_cols,
    #     artist_on_track_cols,
    #     album_not_found_cols
    # ]

    col_dict = {
        'albums': album_cols,
        'tracks': tracks_cols,
        'reviews': reviews_cols,
        'album_is_genre': album_is_genre_cols,
        'artist_on_album': artist_on_album_cols,
        'artist_on_track': artist_on_track_cols,
        'album_not_found': album_not_found_cols,
        'lyrics': lyrics_cols,
        'bad_lyrics': bad_lyrics_cols,
        'track_sentiments': track_sentiment_cols,
        'artists': artist_cols,
        'artist_is_genre': artist_is_genre_cols,
        'lda_coherences': lda_coherences_cols
    }

    return col_dict

"""
Quick way to check what tables are in the SQL database.
"""
def list_tables(return_list=False):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_list = cursor.fetchall()
    print(table_list)
    if return_list == True:
        return table_list

"""
Read in Pitchfork review genres dataframe. This returns the REVIEWS table combined
with one-hot encoded genre information as a dataframe. Note that Pitchfork genre
labels are not neccesarily the same as Spotify genre labels.
"""
def get_review_genres_df():
    review_genres = pd.read_excel('review_genres.xlsx')
    col_dict = get_col_dict()
    reviews = get_df_from_table_name(conn, 'REVIEWS', col_dict['reviews'])

    reviews['p4k_genre'] = [x if x else '' for x in reviews['p4k_genre']]
    reviews['genre_list'] = [x.split(',') for x in reviews['p4k_genre']]
    reviews['genre_count'] = [len(x) for x in reviews['genre_list']]

    # genre_counts = album_genre_df.groupby('genre').count()
    # genre_list = list(genre_counts.index)

    reviews['is_rock'] = ['Rock' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_electronic'] = ['Electronic' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_rap'] = ['Rap' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_experimental'] = ['Experimental' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_pop_rb'] = ['Pop/R&B' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_folk_country'] = ['Folk/Country' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_metal'] = ['Metal' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_jazz'] = ['Jazz' in genre_list for genre_list in reviews['genre_list']]
    reviews['is_global'] = ['Global' in genre_list for genre_list in reviews['genre_list']]

    rock = reviews[reviews['is_rock']].copy()
    rock['genre'] = ['Rock' for x in rock['album_name']]
    electronic = reviews[reviews['is_electronic']].copy()
    electronic['genre'] = ['Electronic' for x in electronic['album_name']]
    rap = reviews[reviews['is_rap']].copy()
    rap['genre'] = ['Rap' for x in rap['album_name']]
    experimental = reviews[reviews['is_experimental']].copy()
    experimental['genre'] = ['Experimental' for x in experimental['album_name']]
    pop_rb = reviews[reviews['is_pop_rb']].copy()
    pop_rb['genre'] = ['Pop/R&B' for x in pop_rb['album_name']]
    folk_country = reviews[reviews['is_folk_country']].copy()
    folk_country['genre'] = ['Folk/Country' for x in folk_country['album_name']]
    metal = reviews[reviews['is_metal']].copy()
    metal['genre'] = ['Metal' for x in metal['album_name']]
    jazz = reviews[reviews['is_jazz']].copy()
    jazz['genre'] = ['Jazz' for x in jazz['album_name']]
    global_df = reviews[reviews['is_global']].copy()
    global_df['genre'] = ['Global' for x in global_df['album_name']]
    no_genre = reviews[reviews['p4k_genre']==''].copy()
    no_genre['genre'] = ['None' for x in no_genre['album_name']]

    album_genre_df = pd.concat([rock, electronic, rap, experimental, pop_rb, folk_country, metal, jazz, global_df, no_genre])

    for col in album_genre_df.columns:
        if col[:2] == 'is':
            album_genre_df = album_genre_df.drop(col, axis=1)

    return album_genre_df

"""
Quickly retrieve ALBUMS table joined with ARTISTS table via IDs in the
ARTIST_ON_ALBUM table as a dataframe. This function doesn't do anything
that can't be done with other functions, but this dataframe is needed
often enough that it helps to have a function just to generate it.
"""
def get_albums_w_artists():
    col_dict = get_col_dict()
    albums = get_df_from_table_name(conn, 'ALBUMS', col_dict['albums'])
    artist_on_album = get_df_from_table_name(conn, 'ARTIST_ON_ALBUM', col_dict['artist_on_album'])
    artists = get_df_from_table_name(conn, 'ARTISTS', col_dict['artists'])
    albums_w_artists = albums.merge(artist_on_album, on='album_id').merge(artists, on='artist_id')
    return albums_w_artists
