Due to tremendous log files that collected for SPARKIFY company to analyze the user's trend and having a prediction analysis; 
the Dataware house with traditional processing becomes insufficient and not compatible with growing of their data volume Park processing with the guaranteed fast and accurate processing of their data.

The benefits that provided by AWS to pay as you need regarding using the compute machines on the fly "scale in / scale out" with the attached storage gives a SPARKIFY extra benefit which impacts their plans, therefore, touches increasing in their revenue...

I extracted data from their repository on S3 SONG-DATA nad LOG-DATA

then processing these data as data frames then store them as parquet files that correspond to each table in their dimension model


Dimension model:

the star schema has been created it includes the following tables.

Fact Table

    songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

Dimension Tables

    users (user_id, first_name, last_name, gender, level)

    songs (song_id, title, artist_id, year, duration)

    artists (artist_id, name, location, latitude, longitude)

    time - timestamps of records in songplays broken down into specific units

    (start_time, hour, day, week, month, year, weekday)
