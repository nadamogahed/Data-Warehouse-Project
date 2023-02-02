# Project description: 
 - A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
 
--------------------------
### Database design:
#### Fact Table: (the tables that has all the foreign keys of the dimension tables)

#### songplays:

 - records in log data associated with song plays i.e. records with page NextSong
- with columns:
songplay_id as a primary key and type of serial, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables:

#### 1)users:
- users in the app
- With columns:
user_id as a primary key and type of bigint, first_name, last_name, gender, level


#### 2)songs: 
- songs in music database
- With columns:
song_id as a primary key and type varchar, title, artist_id, year, duration

#### 3)artists:
- artists in music database
- With columns:
artist_id as a primary key and type of varchar, name, location, latitude, longitude

#### 4)time:
- timestamps of records in songplays broken down into specific units
- With columns:
start_time as a primary key and type of bigint, hour, day, week, month, year, weekday
---------------------------

### ETL Process: 
- In function (load_staging_tables):
Create the staging_tables for events and songs ,then loaded the json files in them.
- In function (insert_tables):
Insert the data from staging_tables to other fact table, and Dimension Tables.

---------------------------

### Project Repository files: 
- 2)create_tables.py: 
drops and creates tables. this file to reset tables before each time to run ETL scripts.

- 3)dwh.cfg:
config informations anbout database name and password, and the location of json files.

- 4)etl.py:
reads and processes files from song_data and log_data and loads them into tables.

- 5)sql_queries.py:
contains all your sql queries, and is imported into the last three files above.
-------------------------

### How To Run the Project:
First run python sql_queries.py, then run python create_tables.py to create tables and connect to the database, at least run python etl.py to successfully insert values on the tables.
