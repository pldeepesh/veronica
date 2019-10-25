import pandas as pd
import psycopg2 as con
import json
from io import StringIO
from datetime import datetime
from sqlalchemy import create_engine, schema

creds = json.load(open('config.json','rb'))
DB_HOST = creds['postgres']['host']
DB_PORT = creds['postgres']['port']
DB_USER = creds['postgres']['user']
DB_PASS = creds['postgres']['password']

def get_engine(database):
    engine = create_engine('postgresql+psycopg2://' + DB_USER + ':' + DB_PASS + '@' + DB_HOST + ':' + DB_PORT + '/' + database)
    return engine

def get_conn_cursor(database):
    conn = con.connect(user = DB_USER, port = DB_PORT,host = DB_HOST, password = DB_PASS, database=database)
    cursor = conn.cursor()

    return conn,cursor

def execute_query(query,database):
    conn,cursor = get_conn_cursor(database)
    data = ''
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        conn.commit()
    except:
        conn.rollback()
    
    conn.close()
    return data

def insert_data_from_df(database,table,dataframe,upsert=None,schema=None,if_exists='append'):
    engine = get_engine(database)
    #insert into the database
    dataframe.to_sql(table,engine,schema=schema,if_exists=if_exists,index=False)