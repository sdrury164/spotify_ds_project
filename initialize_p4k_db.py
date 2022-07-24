import sqlite3 as sql

conn = sql.connect('p4k_review_features2.db')

conn.execute('''CREATE TABLE TRACKS
                (track_name TEXT NOT NULL,
                track_id TEXT NOT NULL,
                duration_ms FLOAT NOT NULL,
                explicit TEXT,
                track_number INT,
                album_name TEXT NOT NULL,
                album_id TEXT NOT NULL,
                release_date TEXT,
                release_date_precision TEXT,
                datetime_accessed TEXT NOT NULL,
                danceability FLOAT,
                energy FLOAT,
                key TEXT,
                key_num INT,
                loudness FLOAT,
                mode INT,
                speechiness FLOAT,
                acousticness FLOAT,
                instrumentalness FLOAT,
                liveness FLOAT,
                valence FLOAT,
                tempo FLOAT,
                time_signature INT);''')

conn.execute('''CREATE TABLE ALBUMS
                (album_name TEXT NOT NULL,
                album_id TEXT NOT NULL,
                type TEXT,
                url TEXT,
                image1 TEXT,
                image2 TEXT,
                image3 TEXT,
                label TEXT,
                release_date TEXT,
                release_date_precision TEXT,
                track_count INT,
                popularity INT,
                datetime_accessed TEXT NOT NULL,
                avg_danceability FLOAT,
                sd_danceability FLOAT,
                avg_energy FLOAT,
                sd_energy FLOAT,
                avg_loudness FLOAT,
                sd_loudness FLOAT,
                avg_speechiness FLOAT,
                sd_speechiness FLOAT,
                avg_acousticness FLOAT,
                sd_acousticness FLOAT,
                avg_instrumentalness FLOAT,
                sd_instrumentalness FLOAT,
                avg_liveness FLOAT,
                sd_liveness FLOAT,
                avg_valence FLOAT,
                sd_valence FLOAT,
                avg_tempo FLOAT,
                sd_tempo FLOAT);''')

conn.execute('''CREATE TABLE REVIEWS
                (album_name TEXT NOT NULL,
                album_id TEXT NOT NULL,
                album_artist TEXT NOT NULL,
                p4k_score FLOAT,
                p4k_genre TEXT,
                p4k_date TEXT,
                p4k_author TEXT,
                p4k_author_role TEXT,
                p4k_review_text TEXT,
                p4k_is_bnm INT,
                p4k_link TEXT,
                p4k_label TEXT,
                p4k_release_year FLOAT);''')

conn.execute('''CREATE TABLE ARTIST_ON_ALBUM
                (album_id TEXT NOT NULL,
                artist_id TEXT NOT NULL);''')

conn.execute('''CREATE TABLE ARTIST_ON_TRACK
                (track_id TEXT NOT NULL,
                artist_id TEXT NOT NULL);''')

conn.execute('''CREATE TABLE ALBUM_IS_GENRE
                (album_id TEXT NOT NULL,
                genre TEXT);''')

conn.execute('''CREATE TABLE ALBUM_NOT_FOUND
                (album_name TEXT NOT NULL,
                album_artist TEXT NOT NULL,
                review_date TEXT,
                error_msg TEXT,
                current_time TEXT);''')

conn.execute('''CREATE TABLE IF NOT EXISTS LYRICS
                (track_name TEXT NOT NULL,
                album_name TEXT NOT NULL,
                album_id TEXT NOT NULL,
                rough_lyrics TEXT NOT NULL,
                lyrics TEXT NOT NULL);''')

conn.execute('''CREATE TABLE BAD_LYRICS
                (album_name TEXT NOT NULL,
                album_id TEXT NOT NULL,
                error_msg TEXT NOT NULL);''')

conn.execute('''CREATE TABLE IF NOT EXISTS LDA_COHERENCES
                (filename TEXT NOT NULL,
                coherence_type TEXT NOT NULL,
                num_topics INT,
                chunksize INT,
                passes INT,
                iterations INT,
                coherence_score FLOAT NOT NULL);''')

conn.execute('''CREATE TABLE IF NOT EXISTS ARTIST_IS_GENRE
                (artist_name TEXT NOT NULL,
                 artist_id TEXT NOT NULL,
                 genre TEXT NOT NULL);''')

conn.execute('''CREATE TABLE IF NOT EXISTS ARTISTS
                (artist_name TEXT NOT NULL,
                artist_id TEXT NOT NULL,
                popularity INT NOT NULL,
                img_url TEXT);''')
