import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                 artist TEXT,
                                 auth TEXT,
                                 firstName TEXT,
                                 gender TEXT,
                                 itemInSession int,
                                 lastName TEXT,
                                 length NUMERIC,
                                 level VARCHAR,
                                 location VARCHAR,
                                 method TEXT,
                                 page TEXT,
                                 registration NUMERIC,
                                 sessionId VARCHAR,
                                 song TEXT,
                                 status int,
                                 ts BIGINT,
                                 userAgent VARCHAR,
                                 userId int);
                                 """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                 num_songs int,
                                 artist_id VARCHAR,
                                 artist_latitude DOUBLE PRECISION,
                                 artist_longitude DOUBLE PRECISION,
                                 artist_location VARCHAR,
                                 artist_name TEXT,
                                 song_id VARCHAR,
                                 title VARCHAR,
                                 duration NUMERIC,
                                 year int);
                                 """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id int IDENTITY(0,1) PRIMARY KEY,
                            start_time BIGINT NOT NULL,
                            user_id INTEGER NOT NULL, 
                            level VARCHAR,
                            song_id VARCHAR, 
                            artist_id VARCHAR, 
                            session_id VARCHAR, 
                            location VARCHAR, 
                            user_agent VARCHAR);
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id int PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT,
                        gender TEXT,
                        level TEXT);
                        """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR PRIMARY KEY,
                        title VARCHAR NOT NULL,
                        artist_id VARCHAR NOT NULL,
                        year INTEGER NOT NULL,
                        duration NUMERIC NOT NULL);
                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                          artist_id VARCHAR PRIMARY KEY,
                          name TEXT,
                          location VARCHAR,
                          latitude DOUBLE PRECISION,
                          longitude DOUBLE PRECISION);
                          """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time BIGINT PRIMARY KEY,
                        hour INTEGER,
                        day INTEGER,
                        week INTEGER,
                        month INTEGER,
                        year INTEGER,
                        weekday INTEGER);
                        """)

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
                          from{}
                          IAM_ROLE{}
                          json {}
                          region 'us-east-1'
                          """).format(config ['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs
                         from{}
                         IAM_ROLE{}
                         json 'auto'
                         region 'us-east-1'
                         """).format(config ['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays start_time,
                                                  user_id,
                                                  level,
                                                  song_id,
                                                  artist_id,
                                                  session_id,
                                                  location,
                                                  user_agent
                            SELECT se.ts AS start_time,
                                   se.userId AS user_id,
                                   se.level AS level,
                                   ss.song_id AS song_id,
                                   ss.artist_id AS artist_id,
                                   se.sessionId AS session_id,
                                   se.location AS location,
                                   se.userAgent AS user_agent
                            FROM staging_events AS se
                            INNER JOIN staging_songs AS ss
                            ON (se.song = ss.title
                                AND se.artist = ss.artist_name)
                            WHERE se.page = 'NextSong' AND se.userId IS NOT NULL
                            """)

user_table_insert = ("""INSERT INTO users user_id,
                                          first_name,
                                          last_name,
                                          gender,
                                          level
                        SELECT se.userId AS user_id,
                               se.firstName AS first_name,
                               se.lastName AS last_name,
                               se.gender AS gender,
                               se.level AS level
                        FROM staging_events AS se
                        """)

song_table_insert = ("""INSERT INTO songs song_id,
                                          title,
                                          artist_id,
                                          year, 
                                          duration
                        SELECT ss.song_id AS song_id,
                               ss.title AS title,
                               ss.artist_id AS artist_id,
                               ss.year AS year,
                               ss.duration AS duration
                        FROM staging_songs AS ss                        
                        """)

artist_table_insert = ("""INSERT INTO artists artist_id,
                                              name,
                                              location,
                                              latitude,
                                              longitude
                          SELECT ss.artist_id AS artist_id,
                                 ss.artist_name AS name,
                                 ss.artist_latitude AS latitude,
                                 ss.artist_longitude AS longitude
                          FROM staging_songs AS ss 
                          """)

time_table_insert = ("""INSERT INTO time start_time,
                                         hour,
                                         day,
                                         week,
                                         month,
                                         year,
                                         weekday
                         SELECT se.ts AS start_time, extract(HOUR FROM se.ts) AS hour,
                                extract(day FROM se.ts) AS day, extract(week FROM se.ts) AS week,
                                extract(week FROM se.ts) AS week,
                                extract(month FROM se.ts) AS month,
                                ss.year AS year, extract(weekday FROM se.ts) AS weekday
                         FROM staging_events AS se
                            INNER JOIN staging_songs AS ss
                            ON (se.song = ss.title
                                AND se.artist = ss.artist_name)
                         WHERE se.userId IS NOT NULL
                         """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
