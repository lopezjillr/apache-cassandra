# Project 2: Data Modeling with Apache Cassandra

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of CSV logs on user activity on the app.

This project uses data modeling with Apache Cassandra and an ETL pipeline in Python that parses event data in CSV and inserts the data into tables using Python which can use queries to answer the Sparkify's questions. 

## Project File Description
- `queries.py` is a file containing scripts to create, drop, insert, select tables
- `queries_result.py` is a script that runs the queries that were requested by Sparkify team
- `etl.py` is a data pipeline that extracts data from the datasets provided in the event_data folder and inserts them into a table using data modeling


## Datasets

### Event Data Dataset

Here is a list of the columns in the event_data csv files.

- artist
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

## Description of Queries

#### Query 1: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

```(cql)
    CREATE TABLE IF NOT EXISTS session_library 
    (
        session_id text, 
        item_in_session text, 
        artist text, 
        song_title text, 
        song_length float, 
        PRIMARY KEY (session_id, item_in_session)
    )
```

For this query, sessionID and itemInSession are considered as the composite key because both fields are needed to uniquely identify the correct data. The data is modeled in this way so that the query returns the artist, the song title, and the song length that was listened to during `session_id=338` and `item_in_session=4`.

#### Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

```(cql)
    CREATE TABLE IF NOT EXISTS user_library 
    (   user_id text, 
        session_id text, 
        item_in_session text, 
        artist text, 
        song_title text,  
        user_first_name text, 
        user_last_name text, 
        PRIMARY KEY ((user_id, session_id), item_in_session)
    ) 
    WITH CLUSTERING ORDER BY (session_id ASC, item_in_session ASC)
```

For this query, `user_id` and `session_id` is used as a composite key and `item_in_session` is used as a clustering column so that the data is sorted by itemInSession. The data is modeled so that the data returned is for a given `userId` and `sessionId` and includes the name of the artist, song, and user name, sorted by the itemInSession.

#### Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own

```(cql)
    CREATE TABLE IF NOT EXISTS song_library 
    (
        song_title text, 
        user_id text, 
        user_first_name text, 
        user_last_name text, 
        PRIMARY KEY (song_title, user_id)
    )
```

For this query, `song_title` and `user_id` is used as a composite key. Since there could be several users that could have listened to the same song, the combination of `song_title` and `user_id` for the composite key will create a new row for that event.


## How to run:

1. Open a new terminal window
2. Run command `python etl.py` to create tables, parse through event_data csv files, and insert data to tables
3. Run command `python query_result.py` to run queries provided






