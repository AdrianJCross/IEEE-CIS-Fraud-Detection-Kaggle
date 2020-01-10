#converts each csv file to sql format in fraud_detection.db
import evaluate as eval
import pandas as pd
import numpy as np
import sys
import sqlite3
import data_output as do
data_path = '/home/across/fraud_detection_project/data/'
output_path = '/home/across/fraud_detection_project/output/'

def create_db(filename,debug):
    conn = do.create_sql_db(filename,remove = True, debug = debug)
    c = conn.cursor()
    conn.commit()
    return conn

def main():

    conn = create_db(filename = 'fraud_detection.db', debug = False)
   
    do.csv_to_sql(conn = conn, filename = "train_identity.csv", tablename = 'raw_train_identity', chunk_size = 10**5, debug = False)
    print('train identity inputted')
    do.csv_to_sql(conn = conn, filename = "test_identity.csv", tablename = 'raw_test_identity', chunk_size = 10**5, debug = False)
    print('test identity inputted')
    do.csv_to_sql(conn = conn, filename = "train_transaction.csv", tablename = 'raw_train_transaction', chunk_size = 10**5, debug = False)
    print('train transaction inputted')
    do.csv_to_sql(conn = conn, filename = "test_transaction.csv", tablename = 'raw_test_transaction', chunk_size = 10**5, debug = False)
    print('test transaction inputted')
    print('raw data has been input into .db file')
    
if __name__ == "__main__":
    main()