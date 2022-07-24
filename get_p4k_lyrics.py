import numpy as np
import pandas as pd
import sqlite3 as sql
import p4k_db_functions as db_func
import lyricsgenius
import re
import genius_credentials as gen_cred

def clean_lyrics(lyrics, title):
    # Get rid of stuff like "[Chorus]" and "[Verse 1]"
    lyrics = re.sub("[\(\[].*?[\)\]]", "", lyrics)
    # Replace line breaks with spaces
    lyrics = lyrics.replace('\n', ' ')
    # Get rid of title at beginning of string
    lyrics = lyrics.replace(title, '')
    # Get rid of "Lyrics" at beginning of string
    lyrics = lyrics.replace('Lyrics', '')
    # Get rid of "##Embed"
    lyrics = re.sub('..Embed', '', lyrics)
    # Get rid of whitespace at beginning of string
    lyrics = lyrics.lstrip()

    return lyrics

client_access_token = gen_cred.genius_access_token
client_secret = gen_cred.genius_secret

genius = lyricsgenius.Genius(client_access_token)

conn = sql.connect('p4k_review_features.db')
col_dict = db_func.get_col_dict()
lyrics_df = db_func.get_df_from_table_name(conn, 'LYRICS', col_dict['lyrics'])
bad_lyrics_df = db_func.get_df_from_table_name(conn, 'BAD_LYRICS', col_dict['bad_lyrics'])

if len(lyrics_df.columns) > 0:
    print(lyrics_df.columns)
    print(bad_lyrics_df.columns)

    processed_album_ids = pd.concat([lyrics_df['album_id'], bad_lyrics_df['album_id']])
    reviews_df = db_func.get_df_from_table_name(conn, 'REVIEWS', col_dict['reviews'])
    albums_to_get = reviews_df[~reviews_df['album_id'].isin(processed_album_ids)]
else:
    albums_to_get = db_func.get_df_from_table_name(conn, 'REVIEWS', col_dict['reviews'])

for i in range(len(albums_to_get)):
    album_row = albums_to_get.iloc[i]
    album_name = album_row['album_name']
    album_artist = album_row['album_artist']
    album_id = album_row['album_id']
    try:
        genius_album = genius.search_album(album_name, album_artist).to_dict()
        genius_tracks = genius_album['tracks']
        print('Lyrics found for album ' + album_name + '!\n')
        for track_dict in genius_tracks:
            track_name = track_dict['song']['title']
            rough_lyrics = track_dict['song']['lyrics']
            lyrics = clean_lyrics(rough_lyrics, track_name)
            conn.execute('''INSERT INTO LYRICS
                            (track_name, album_name, album_id,
                            rough_lyrics, lyrics)
                            VALUES (?, ?, ?, ?, ?);''',
                            (track_name, album_name, album_id,
                            rough_lyrics, lyrics))
            conn.commit()
        # track_dict = dict()
        # for track in genius_album['tracks']:
        #     entry = {
        #         'album': album_name,
        #         'artist': album_artist,
        #         'lyrics': track['song']['lyrics']
        #     }
        #     track_dict[track['song']['title']] = entry
        # album_dict[album_name] = track_dict
    except Exception as e:
        print('No lyrics found for ' + album_name)
        error_msg = str(e)
        print('\nError message:')
        print(error_msg + '\n')
        conn.execute('''INSERT INTO BAD_LYRICS
                        (album_name, album_id, error_msg)
                        VALUES (?, ?, ?);''',
                        (album_name, album_id, error_msg))
        conn.commit()
