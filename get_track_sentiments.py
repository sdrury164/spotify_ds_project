import numpy as np
import pandas as pd
import re
import sqlite3 as sql

from pprint import pprint
from datetime import datetime

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import p4k_db_functions as db_func

def get_vader_sentiment(full_tweet_text):
    sid_obj = SentimentIntensityAnalyzer()
    sentiment_dict = sid_obj.polarity_scores(full_tweet_text)
    return sentiment_dict

conn = sql.connect('p4k_review_features.db')

col_dict = db_func.get_col_dict()
t_sentiments = db_func.get_df_from_table_name(conn, 'TRACK_SENTIMENTS', col_dict['track_sentiments'])
lyrics = db_func.get_df_from_table_name(conn, 'LYRICS', col_dict['lyrics'])

ids_w_sentiments = t_sentiments['album_id']

lyrics_to_process = lyrics[~lyrics['album_id'].isin(ids_w_sentiments)]

for i in range(len(lyrics_to_process)):
    if i % 20 == 0:
        print('\n\nProcessed ' + str(i) + ' (' + str(len(ids_w_sentiments) + i) + ') out of ' + str(len(lyrics_to_process)) + ' (' + str(len(lyrics)) + ') tracks.\n\n')
    lyrics_row = lyrics_to_process.iloc[i]
    track_name = lyrics_row['track_name']
    album_name = lyrics_row['album_name']
    album_id = lyrics_row['album_id']

    sentiment_dict = get_vader_sentiment(lyrics_row['lyrics'])

    conn.execute('''INSERT INTO TRACK_SENTIMENTS
                    (track_name, album_name, album_id,
                    pos_sentiment, neu_sentiment, neg_sentiment)
                    VALUES (?, ?, ?, ?, ?, ?);''',
                    (track_name, album_name, album_id,
                    sentiment_dict['pos'], sentiment_dict['neu'], sentiment_dict['neg']))

    if i % 50 == 0:
        conn.commit()

conn.close()
