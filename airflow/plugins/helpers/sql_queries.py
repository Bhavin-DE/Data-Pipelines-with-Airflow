class SqlQueries:

    """
    Insert query for SONGPLAY table
    """
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                     *
              FROM staging_events
              WHERE page = 'NextSong') events
              LEFT JOIN staging_songs songs
              ON events.song = songs.title
              AND events.artist = songs.artist_name
              AND events.length = songs.duration
        """)

    """
    Insert query for USERS table
    """
    users_table_insert = ("""
        INSERT INTO users (userid, 
                           firstname, 
                           lastname, 
                           gender, 
                           level) 
                           (SELECT distinct userid, 
                                            firstname, 
                                            lastname, 
                                            gender, 
                                            level
                            FROM staging_events
                            WHERE page='NextSong')
        """)

    """
    Insert query for SONGS table
    """
    songs_table_insert = ("""
        INSERT INTO songs (song_id, 
                           title, 
                           artist_id, 
                           year, 
                           duration) 
                          (SELECT distinct song_id, 
                                           title, 
                                           artist_id, 
                                           year, 
                                           duration
                           FROM staging_songs)
        """)
    
    """
    Insert query for ARTISTS table
    """
    artists_table_insert = ("""
        INSERT INTO artists (artist_id, 
                             artist_name, 
                             artist_location, 
                             artist_latitude, 
                             artist_longitude) 
                            (SELECT distinct artist_id, 
                                             artist_name, 
                                             artist_location, 
                                             artist_latitude, 
                                             artist_longitude
                             FROM staging_songs)
        """)
    """
    Insert query for TIME table
    """
    time_table_insert = ("""
        INSERT INTO TIME (start_time, 
                          hour, 
                          day, 
                          week, 
                          month,
                          year, 
                          weekday )
                         (SELECT start_time, 
                                 extract(hour from start_time), 
                                 extract(day from start_time), 
                                 extract(week from start_time), 
                                 extract(month from start_time), 
                                 extract(year from start_time), 
                                 extract(dayofweek from start_time)
                          FROM songplays)
        """)