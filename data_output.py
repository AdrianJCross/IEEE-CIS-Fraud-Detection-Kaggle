import pandas as pd
import sqlite3
import os.path
import error_codes as error

def getTables(conn):
    cursor = conn.cursor()
    cmd = "SELECT name FROM sqlite_master WHERE type='table'"
    cursor.execute(cmd)
    names = [row[0] for row in cursor.fetchall()]
    return names

def isTable(conn, nameTbl):
    return (nameTbl in getTables(conn))



def csv_to_sql(conn, filename, tablename, chunk_size, debug, data_path = '/home/across/fraud_detection_project/data/'):
    chunk_no=0
    for chunk in pd.read_csv(data_path+filename, chunksize=chunk_size):
        if debug == True:
            chunk_no+=1
            print("Processing chunk number ", chunk_no)
            chunk.info(verbose=True)
        pandas_to_sql(data_chunk = chunk, table_name = tablename, conn = conn)




def pandas_to_sql(data_chunk,table_name,conn): #outputs chunk of pandas to an sql database
    
    data_chunk.to_sql(table_name,conn,if_exists='append',index=False)

def create_sql_db(filename, remove, debug):
    
    if os.path.isfile(filename) and remove == True:
        os.remove(filename)   
        if debug == True:
            print('File ', filename, ' erased and replaced')
        return sqlite3.connect(filename)  
    
    elif os.path.isfile(filename) and remove == False:
        if debug == True:
            print('Error in creating db file ', filename, ' already exists... change db file name or set to remove existing file')
        return error.db_creation()
        
    else:
        return sqlite3.connect(filename)
        
        