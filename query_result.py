import cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
import os
import glob
import csv
import time

from queries import create_workspace, create_table_queries, drop_table_queries, insert_session_library, insert_user_library, insert_song_library

def connect_and_get_session():
    """
    creates a local connection to Cassandra instance in your local machine
    and returns information regarding the cluster and current session
    """
    try: 
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
    except Exception as e:
        print(e)

    session = cluster.connect()
    print("Session connected!")
    
    return cluster, session

def create_keyspace(session, keyspace):
    """
    arguments:
    - session: current session
    - keyspace: keyspace we're connectiong to
    
    creates a keyspace
    """
    try:
        session.execute(create_workspace.format(keyspace))
        
    except Exception as e:
        print(e)


def run_query_one(session):
    """
    arguments:
    - session: current session
    
    returns the result of a query
    """

    print("Query #1: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4")
    print("")
    
    try:
        rows = session.execute("SELECT session_id, item_in_session, artist, song_title, song_length FROM session_library WHERE session_id='338' AND item_in_session ='4'")
        for row in rows:
            print(row)
    except Exception as e:
        print(e)

def run_query_two(session):
    """
    arguments:
    - session: current session
    
    returns the result of a query
    """
    
    print("Query #2: Give me only the following: name of artist, song (sorted by itemInSession) and user (firs and last name) for userid = 10, sessionid = 182")
    print("")
    
    try:
        rows = session.execute("SELECT artist, song_title, user_id, session_id, item_in_session \
                                FROM user_library \
                                WHERE user_id='10' AND session_id='182'")
        for row in rows:
            print(row)
    except Exception as e:
        print(e)
        
def run_query_three(session):
    """
    arguments:
    - session: current session
    
    returns the result of a query
    """
    
    print("Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'")
    print("")
    
    try:
        rows = session.execute("SELECT song_title, user_id, user_first_name, user_last_name \
                                FROM song_library \
                                WHERE song_title='All Hands Against His Own'")
        for row in rows:
            print(row)
    except Exception as e:
        print(e)

def main():
    cluster, session = connect_and_get_session()
    print("")
    
    try:
        print("Setting up keyspace...")
        create_keyspace(session, 'sparkify')
        session.set_keyspace('sparkify')
        print("")
        print("-------")
        
        # print query reseults
        
        run_query_one(session)
        
        print("")
        print("-------")
        
        run_query_two(session)
        
        print("")
        print("-------")
        
        run_query_three(session)

    except Exception as e:
        print(e)
    finally:
        if cluster:
            cluster.shutdown()
        if session:
            session.shutdown()

if __name__ == '__main__':
    main()