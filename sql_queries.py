import configparser

# CONFIG

# config = configparser.ConfigParser()
# config.read('dwh.cfg')
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
ROLE_ARN = config.get("DWH", "ROLE_ARN")

# DROP TABLES

staging_events_table_drop = "drop table IF EXISTS staging_events_table CASCADE"
staging_songs_table_drop = "drop table IF EXISTS staging_songs_table CASCADE"
songplay_table_drop = "drop table IF EXISTS songplay_tbl CASCADE"
user_table_drop = "drop table IF EXISTS users_tbl CASCADE"
song_table_drop = "drop table IF EXISTS songs_tbl CASCADE"
artist_table_drop = "drop table IF EXISTS artists_tbl CASCADE"
time_table_drop = "drop table IF EXISTS time_tbl CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE if not exists staging_events_table (
    artist varchar(255),
    auth varchar(10),
    firstname varchar(100),
    gender char,
    iteminsession int,
    lastname varchar(255),
    length float,
    level varchar(10),
    location varchar(255),
    method varchar(10),
    page varchar(50),
    registration float,
    sessionid int,
    song varchar(255),
    status int,
    ts timestamp,
    useragent varchar(255),
    userid int);
""")

staging_songs_table_create = ("""
    CREATE TABLE if not exists staging_songs_table (
    num_songs int,
    artist_id varchar(50),
    artist_latitude float,
    artist_longitude float,
    artist_location varchar(255),
    artist_name varchar(255),
    song_id varchar(50),
    title varchar(255),
    duration float,
    year int);
""")

songplay_table_create = ("""create table if not exists songplay_tbl (
    songplay_id int IDENTITY(1,1),
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar(10),
    song_id varchar(50) NOT NULL,
    artist_id varchar(50),
    session_id int,
    location varchar(255),
    user_agent varchar(255),
    PRIMARY KEY (songplay_id));
""")

user_table_create = ("""create table if not exists user_tbl (
    userid int NOT NULL,
    firstname varchar(100),
    lastname varchar(255),
    gender char,
    level varchar(10),
    PRIMARY KEY(userid));
""")

song_table_create = ("""create table if not exists song_tbl (
    song_id varchar(50) NOT NULL,
    title varchar(255),
    artist_id varchar(50) NOT NULL,
    year int,
    duration float,
    PRIMARY KEY(song_id));
""")

artist_table_create = ("""create table if not exists artist_tbl (
    artist_id varchar(50) NOT NULL,
    name varchar(255),
    location varchar(255),
    latitude float,
    longitude float,
    PRIMARY KEY(artist_id));
""")

time_table_create = ("""create table if not exists time_tbl (
    start_time timestamp NOT NULL,
    hour int,
    day int,
    week int,
    month varchar(15),
    year int,
    weekday varchar(15),
    PRIMARY KEY(start_time));
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    format as json 'auto'
    TIMEFORMAT 'epochmillisecs'
""").format(config.get("S3", "LOG_DATA"), config.get("DWH", "ROLE_ARN"))

staging_songs_copy = ("""
    copy staging_songs_table from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    format as json 'auto'
""").format(config.get("S3", "SONG_DATA"), config.get("DWH", "ROLE_ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT into songplay_tbl (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
    ts, userid, level, song_id, artist_id, sessionid, location, useragent
    FROM
    (SELECT e.ts, e.userid, e.level, a.song_id, a.artist_id, e.sessionid, e.location, e.useragent
    FROM 
    staging_events_table e
    JOIN
    (select song.song_id, artist.artist_id, song.title, artist.name, song.duration
    FROM 
    song_tbl song
    JOIN 
    artist_tbl artist
    ON 
    song.artist_id = artist.artist_id) AS a
    ON
    (a.title = e.song
    AND a.name = e.artist
    AND a.duration = e.length)
    WHERE e.page = 'nextsong'
    AND userid is not null)
""")

user_table_insert = ("""INSERT into user_tbl (userid, firstname, lastname, gender, level) 
    SELECT
    userid, firstname, lastname, gender, level
    FROM
    staging_events_table e
    WHERE e.page = 'nextsong'
    AND e.userid is not null
""")

song_table_insert = ("""INSERT into song_tbl (song_id, title, artist_id, year, duration) 
    SELECT
    song_id, title, artist_id, year, duration
    FROM
    staging_songs_table e
    WHERE e.song_id is not null
""")

artist_table_insert = ("""INSERT into artist_tbl (artist_id, name, location, latitude, longitude) 
    SELECT
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM
    staging_songs_table e
    WHERE e.artist_id is not null
""")

time_table_insert = ("""INSERT into time_tbl (start_time, hour, day, week, month, year, weekday) 
    SELECT
    start_time,
        EXTRACT(hr from start_time),
        EXTRACT(d from start_time),
        EXTRACT(w from start_time),
        EXTRACT(mon from start_time),
        EXTRACT(yr from start_time),
        EXTRACT(weekday from start_time)
    FROM
    (SELECT DISTINCT ts as start_time
        FROM staging_events_table)
""")

#select * from stl_load_errors order by starttime desc limit 5

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]