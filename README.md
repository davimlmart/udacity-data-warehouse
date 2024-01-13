# Sparkify Data Warehouse

**This project is a data warehouse and ETL built on AWS Redshift.**

**A database was created to allow better analytics studies on user's listening data.**

## Table of Contents

- [Prerequisites](#prerequisites)
- [Running the Files](#running-the-files)
- [Thought Process](#thought-process)
- [Possible Analyses](#possible-analysis)

## Prerequisites

1. The code runs in Python, and uses the following libraries:
    - configparser
    - pandas
    - json
    - time
    - psycopg2
    - boto3

2. Also you need to fill the files:
    - cluster.cfg -> desired cluster info, your AWS credentials
    - dwh.cfg -> desired data warehouse variables, cluster endpoint, role ARN, paths of input files

3. Files sql_queries.py and IAC.py must be in the same folder.

## Running the files

1. **File 1 (create_tables.py):**
   The first file to be executed must be create_tables.py, to create the cluster (if necessary), 
   and create all the tables that will be used on the database and ETL process.
   You can run the file in a jupyter notebook on the same folder using %run create_tables.py,
   in your IDE of choice, or in the terminal using python create_tables.py.
   Check the outputs for any possible error, and wait for the message: Cluster available and tables created.
   This file has a dependency on sql_queries.py, IAC.py, cluster.cfg, and dwh.cfg.

2. **File 2 (etl.py):**
   The second file is etl.py, to run the copy statements for the staging tables, insert data from staging tables 
   into the star schema database, and run a few quality checks about the data.
   You can run the file in a similar manner as create_tables.py.
   This file has a dependency on sql_queries.py and dwh.cfg.

## Thought process

1. Datawarehouse and database structure
    The datawarehouse was created thinking so that other ETLs or databases could also be created in it.
    Redshift and AWS infrastructure allow flexibility for modifying anything necessary.
    The origin data is alocated on the same database as the refined data, this is so that the users can look at them if they have the need to add data or modify the tables and make new analysis.
    This analytics database and schema was designed as a star schema to create a fact table and four dimmensions. This structure allows analysts to study songplays, sessions, starting dates, users, songs and artists informations and listening frequencies. Joining them with the dimmension tables, there is more data about duration, about which specific date part each session happend, each users and artists locations, song duration and more.

2. Cluster creation and specifics
    I included the cluster creation process as an optional part of the process, so it can be easily created  
    or modified using infrastructure as a code. 
    The structure for the creation begins on create_tables.py, checking if the cluster is available and if the IAM user exists. If so, the process keeps running, if not, IAC.py runs to create the cluster, wait for it to become available and save the caracteristics.
    If the endpoint is different then the one specified on dwh.cfg, the new endpoint and role ARN will be saved on the file.
    I first started as a dc2.large with 4 nodes, but the process was to slow to load up the full songs dataset, so one option to take less time is using the more expensive 8 nodes.

3. Create tables
    The staging tables were created using the data types assumed from the .json files that were inputed. The songs staging table had errors in the copy statement using VARCHAR for some
    columns, so they wew updated to VARCHAR(512) to receive all content from the original file.
    Since the datasets were big and taking some time to copy, I didn't use distkey or sortkey in any column.
    The songplays table was created considering the same date formats as the input data from the
    sources. There was the addition of the songplay_id column as a primary key using IDENTITY structure from Redshift. start_time was creating converting the ts column from the staging table.
    Users, songs and artists tables were created with the same datatypes, considering they were only slightly adapted from the staging tables. The columns with the table's names were each ones primary ids.
    Time table were created considering a conversion for the ts data on the staging table.

4. Copy syntax
    The copy statement were my main challange. I tried some different things but the final result was some simpler statements. 
    The events I only specified the JSON columns as it was on the input log, and specified the region to us-west-2 to speed up the data transfer. The data was efficiently coppied with this query.
    The songs was more challanging, I first thougth about using a manifest to load the data from different folder, but ended up using Redhsift's auto option to make the loading process. Reading the AWS docs and seeing answers on Udacity's Knowledge plataform it seemed to achieve the goals ot this project. The region was also specified to speed up the process. 

5. Insert data into star schema tables
    Having the staging tables, the insert queries used an SELECT to input data. 
    On songplays a JOIN was necessary to pull the song and artist ids from the songs_dataset, I've used the song title and artist name for this join, assuming that an artist never uses the same names for more than one song. Also the ts from events was translated into a more readable timespamp format to create the start time. A filter of page was used to select only songplays from the full events dataset.
    On users, the only table used was events, gathering each user information and grouping them so we have individual records.
    On songs and artists only the songs_dataset was used, in a similar manner as users, grouping them to avoid duplicates.
    For the time table, I used a cte to format the timestamp column and select the distinct ones. Then I used EXTRACT for each column to describe the timestamp. I choose not to use only NextSong pages to create a more complete time table.

6. Quality checks
    In the end, there are some quality checks to count total entries of data, in the staging tables and in star schema tables. In the other tables there are some checks of null values that I observed in the datasets, so the analytics user knows it comes from the source data.

## Possible analysis

    Here are some exampels of queries than can be used to learn more about this dataset:

    20 top listeners and its locations
    SELECT user_id, location, COUNT(songplay_id) AS number_of_plays
    FROM songplays
    GROUP BY 1, 2
    ORDER BY 3, 2, 1
    LIMIT 20;

    100 top locations of songplays
    SELECT location, COUNT(songplay_id) AS number_of_plays
    FROM songplays
    GROUP BY 1
    ORDER BY 2, 1
    LIMIT 100;

    50 top songs
    SELECT title, artist, COUNT(songplay_id) number_of_plays
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    JOIN artists a ON sp.artist_id = a.artist_id
    GROUP BY 1, 2
    ORDER BY 3, 2, 1
    LIMIT 50;

    Top songs per year
    SELECT 
        COALESCE(t.year, 'Total') AS year,
        s.title AS song_title, 
        COUNT(sp.songplay_id) AS number_of_plays
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    JOIN time t ON sp.start_time = t.start_time
    GROUP BY ROLLUP(t.year, s.title)
    ORDER BY 1, 3 DESC;

    Top artists per year
    SELECT 
        COALESCE(t.year, 'Total') AS year,
        a.name AS artist, 
        COUNT(sp.songplay_id) AS number_of_plays
    FROM songplays sp
    JOIN artists a ON sp.artist_id = a.artist_id
    JOIN time t ON sp.start_time = t.start_time
    GROUP BY ROLLUP(t.year, a.name)
    ORDER BY 1, 3 DESC;

    Songs per duration
    SELECT 
        CASE
            WHEN duration < 60 THEN '0-59 Seconds'
            WHEN duration < 120 THEN '60-119 Seconds'
            WHEN duration < 180 THEN '120-179 Seconds'
            WHEN duration < 240 THEN '180-239 Seconds'
            WHEN duration < 300 THEN '240-299 Seconds'
            ELSE '300+ Seconds'
        END AS duration_range,
        COUNT(*) AS songs
    FROM songs
    GROUP BY 1
    ORDER BY 1;
