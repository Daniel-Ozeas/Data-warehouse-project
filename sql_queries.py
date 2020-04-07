import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY          = config.get('AWS', 'KEY')
SECRET       = config.get('AWS', 'SECRET')

IAM_ROLE     = config.get('IAM_ROLE','ARN')

LOG_DATA     = config.get('S3','LOG_DATA')
SONG_DATA    = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS song;"
artist_table_drop         = "DROP TABLE IF EXISTS artist;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              NUMERIC,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        NUMERIC,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  BIGINT,
        userAgent           VARCHAR,
        userId              INTEGER 
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER PRIMARY KEY,
        artist_id           VARCHAR,
        artist_latitude     NUMERIC,
        artist_longitude    NUMERIC,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            NUMERIC,
        year                INTEGER
        
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id INTEGER IDENTITY(0,1) SORTKEY, 
        start_time  TIMESTAMP NOT NULL, 
        user_id     VARCHAR NOT NULL, 
        level       VARCHAR, 
        song_id     VARCHAR, 
        artist_id   VARCHAR, 
        session_id  INTEGER NOT NULL, 
        location    VARCHAR, 
        user_agent  VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id     INTEGER PRIMARY KEY, 
        first_name  TEXT NOT NULL, 
        last_name   TEXT NOT NULL, 
        gender      TEXT, 
        level       TEXT NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE song(
        song_id    VARCHAR PRIMARY KEY, 
        title      TEXT    NOT NULL SORTKEY, 
        artist_id  VARCHAR NOT NULL DISTKEY, 
        year       INT     NOT NULL, 
        duration   NUMERIC NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(
        artist_id  VARCHAR PRIMARY KEY, 
        name       TEXT NOT NULL, 
        location   TEXT, 
        latitude   NUMERIC, 
        longitude  NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE  IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY, 
        hour       INT     NOT NULL, 
        day        INT     NOT NULL, 
        week       INT     NOT NULL, 
        month      TEXT    NOT NULL, 
        year       INT     NOT NULL, 
        weekday    TEXT    NOT NULL)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    COMPUPDATE OFF
    JSON {}
    timeformat as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role '{}'
    region 'us-west-2'
    JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay ( 
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent)
    SELECT 
        e.start_time,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        s.artist_location,
        e.userAgent
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, * 
            FROM staging_events 
            WHERE page = 'NextSong') e
    JOIN staging_songs s
    ON (e.artist = s.artist_name) AND (e.song = s.title) AND (e.length = s.duration)
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT DISTINCT
        e.userId,
        e.firstName,
        e.lastName,
        e.gender,
        e.level
    FROM staging_events e
    JOIN staging_songs s
    ON (e.artist = s.artist_name) AND (e.song = s.title)
""")

song_table_insert = ("""
    INSERT INTO song(
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs s
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artist (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
    WHERE ss.artist_name IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,
        EXTRACT(HOUR    FROM start_time),
        EXTRACT(DAY     FROM start_time),
        EXTRACT(WEEK    FROM start_time),
        EXTRACT(MONTH   FROM start_time),
        EXTRACT(YEAR    FROM start_time),
        EXTRACT(WEEKDAY FROM start_time)
    FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
