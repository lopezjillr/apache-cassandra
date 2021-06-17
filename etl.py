import cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
import os
import glob
import csv
import time

from queries import create_workspace, create_table_queries, drop_table_queries, insert_session_library, insert_user_library, insert_song_library, select_session_library, select_user_library, select_song_library

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
        
def create_filepaths(): 
    """
    creates a file path for each file to be processed
    and returns a list of file paths
    """
    
    filepath = os.getcwd() + '/event_data'
    
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))
        
    return file_path_list
    
def process_files(file_path_list):
    """
    arguments:
    - file_path_list: takes a list of file path names created in create_filepaths()
    
    opens each file from file_path_list, extracts data row by row, and writes them into a 
    new file called 'event_datafile_new.csv' with the correct columns
    """
    
    full_data_rows_list = [] 
    
    print("Processing files...")
    
    for f in file_path_list:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
            
            for line in csvreader:
                full_data_rows_list.append(line) 

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
        

    with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
        print("Number of rows added to event_datafile_new.csv for process:")
        print(sum(1 for line in f))


def create_tables(session):
    """
    arguments:
    - session: current session
    
    creates tables using the create_tables_queries from queries.py
    """
    for table in create_table_queries:
        try:
            session.execute(table)
        except Exception as e:
            print(e)

def drop_tables(session):
    """
    arguments:
    - session: current session
    
    drops tables using the drop_tables_queries from queries.py
    """
    for table in drop_table_queries:
        try:
            session.execute(table)
        
        except Exception as e:
            print(e)


def insert_data(session):
    """
    arguments:
    - session: current session
    
    reads data from 'event_datafile.new.csv' file and batch inserts them into corresponding
    data tables
    """
    
    file = 'event_datafile_new.csv'

    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    batch_count = 0;
    
    
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header

        for line in csvreader:
            batch.add(insert_session_library, (line[8], line[3], line[0], line[9], float(line[5])))
            batch.add(insert_user_library, (line[10], line[8], line[3], line[0], line[9], line[1], line[4]))
            batch.add(insert_song_library, (line[9], line[10], line[1], line[4]))
            batch_count += 3
            
            if(batch_count >= 300):
                session.execute(batch)
                batch.clear()
                batch_count = 0
    
                
def verify_session_library(session):
    """
    arguments:
    - session: current session
    
    verifies that tables were created and data was inserted
    """
    
    print("session_library")
    try:
        rows = session.execute(select_session_library)
        for row in rows:
            print(row)
    except Exception as e:
        print(e)
    print("")

def verify_user_library(session):
    """
    arguments:
    - session: current session
    
    verifies that tables were created and data was inserted
    """
    
    print("user_library")
    try:
        rows = session.execute(select_user_library)
        for row in rows:
            print(row)
    except Exception as e:
        print(e)
    
def verify_song_library(session):
    """
    arguments:
    - session: current session
    
    verifies that tables were created and data was inserted
    """
    
    print("")
    print("song_library")
    try:
        rows = session.execute(select_song_library)
        for row in rows:
            print(row)
    except Exception as e:
        print(e)

    
def main():
    cluster, session = connect_and_get_session()
    print("")
    
    try:
        print("Creating new keyspace...")
        print("")
        create_keyspace(session, 'sparkify')
        session.set_keyspace('sparkify')
        file_path_list = create_filepaths()
        process_files(file_path_list)
        print("")
        
        #drop tables
        drop_tables(session)
        
        #create tables
        create_tables(session)
        
        #insert data from csv file
        print("Inserting data....")
        insert_data(session)
        print("")
        
        #verify data was inserted\
        print("Verifying data...")
        verify_session_library(session)
        verify_user_library(session)
        verify_song_library(session)
        print("")
        print("Data inserted!")

    except Exception as e:
        print(e)
    finally:
        if cluster:
            cluster.shutdown()
        if session:
            session.shutdown()

if __name__ == '__main__':
    main()