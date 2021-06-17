###
#  file containing queries to drop and create tables, insert data into tables, and create the workspace
###

# DROP TABLES

drop_session_library = "DROP TABLE IF EXISTS session_library"
drop_user_library = "DROP TABLE IF EXISTS user_library"
drop_song_library = "DROP TABLE IF EXISTS song_library"

# CREATE TABLES

create_session_library = ("""
    CREATE TABLE IF NOT EXISTS session_library 
    (
        session_id text, 
        item_in_session text, 
        artist text, 
        song_title text, 
        song_length float, 
        PRIMARY KEY (session_id, item_in_session)
    )
""")

create_user_library = ("""
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
    WITH CLUSTERING ORDER BY (item_in_session ASC)
""")

create_song_library = ("""
    CREATE TABLE IF NOT EXISTS song_library 
    (
        song_title text, 
        user_id text, 
        user_first_name text, 
        user_last_name text, 
        PRIMARY KEY (song_title, user_id)
    ) 
""")


# INSERT EVENTS

insert_session_library = ("""
        INSERT INTO session_library (session_id, item_in_session, artist, song_title, song_length)
        VALUES (%s, %s, %s, %s, %s)
""")

insert_user_library = ("""
        INSERT INTO user_library (user_id, session_id, item_in_session, artist, song_title, user_first_name, user_last_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

insert_song_library = ("""
        INSERT INTO song_library (song_title, user_id, user_first_name, user_last_name)
        VALUES (%s, %s, %s, %s)
""")



# SELECT 

select_session_library = ("SELECT * FROM session_library LIMIT 5")

select_user_library = ("SELECT * FROM user_library LIMIT 5")

select_song_library = (" SELECT * FROM song_library LIMIT 5")


# Create workspace

create_workspace = ("""
        CREATE KEYSPACE IF NOT EXISTS {0}
        WITH REPLICATION = 
        {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}
""")



# QUERY LISTS

create_table_queries = [create_session_library, create_user_library, create_song_library]
drop_table_queries = [drop_session_library, drop_user_library, drop_song_library]