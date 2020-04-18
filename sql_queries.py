import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS events_staging (event_id     INT IDENTITY(0,1),
                                                                           artist        varchar,
                                                                           auth          varchar,
                                                                           firstName     varchar,
                                                                           gender        varchar,
                                                                           itemInSession int,
                                                                           lastName      varchar,
                                                                           length        numeric,
                                                                           level         varchar,
                                                                           location      varchar,
                                                                           method        varchar,
                                                                           page          varchar,
                                                                           registration  numeric,
                                                                           sessionId     int,
                                                                           song          varchar,
                                                                           status        int,
                                                                           ts            bigint,
                                                                           userAgent     varchar,
                                                                           userId        int)
                                """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS songs_staging (num_songs        int,
                                                                           artist_id        varchar, 
                                                                           artist_latitude  numeric, 
                                                                           artist_longitude numeric, 
                                                                           artist_location  varchar, 
                                                                           artist_name      varchar, 
                                                                           song_id          varchar, 
                                                                           title            varchar, 
                                                                           duration         numeric, 
                                                                           year             int)
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id      int SORTKEY PRIMARY KEY, 
                                                          first_name   varchar, 
                                                          last_name    varchar, 
                                                          gender       varchar, 
                                                          level        varchar)
                        DISTSTYLE ALL                                  
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id      varchar SORTKEY PRIMARY KEY, 
                                                          title        varchar, 
                                                          artist_id    varchar, 
                                                          year         int, 
                                                          duration     numeric)
                        DISTSTYLE ALL
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id   varchar SORTKEY PRIMARY KEY, 
                                                              name        varchar, 
                                                              location    varchar, 
                                                              latitude    numeric, 
                                                              longitude   numeric)
                          DISTSTYLE ALL
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time   timestamp SORTKEY PRIMARY KEY, 
                                                         hour         int, 
                                                         day          int, 
                                                         week         int,
                                                         month        int, 
                                                         year         int,
                                                         weekday      int)
                       DISTSTYLE ALL 
                    """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id    INT IDENTITY(0,1) DISTKEY PRIMARY KEY, 
                                                                  start_time     timestamp NOT NULL REFERENCES time(start_time), 
                                                                  user_id        int NOT NULL REFERENCES users(user_id), 
                                                                  level          varchar, 
                                                                  song_id        varchar REFERENCES songs(song_id), 
                                                                  artist_id      varchar REFERENCES artists(artist_id), 
                                                                  session_id     int, 
                                                                  location       varchar, 
                                                                  user_agent     varchar)
                         """)

# STAGING TABLES

staging_events_copy = (""" COPY events_staging FROM {}
                           CREDENTIALS 'aws_iam_role={}'
                           REGION 'us-west-2'
                           JSON {}
                      """).format(config.get('S3', 'LOG_DATA'),
                                  config.get('IAM_ROLE', 'ARN'),
                                  config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" COPY songs_staging FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          JSON 'auto'
                      """).format(config.get('S3', 'SONG_DATA'),
                                  config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songs (start_time, user_id, level, song_id, 
                                               artist_id, session_id, location, user_agent)
                            SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
                                   e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
                              FROM events_staging e, 
                         LEFT JOIN songs_staging s ON (s.title = e.song AND s.duration = e.duration AND s.artist_name = e.artist)  
                             WHERE e.page ='NextSong' 
                        """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                          FROM events_staging
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                          FROM songs_staging
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latittute, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latittute, artist_longitude
                            FROM songs_staging
                      """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                               EXTRACT(hr from start_time) AS hour,
                               EXTRACT(d from start_time) AS day,
                               EXTRACT(w from start_time) AS week,
                               EXTRACT(mon from start_time) AS month,
                               EXTRACT(yr from start_time) AS year,
                               EXTRACT(weekday from start_time) AS weekday
                          FROM events_staging
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
