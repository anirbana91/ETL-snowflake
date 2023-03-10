from datetime import  date
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine import URL
import pandas as pd
import json
import io
import boto3
import os
import pyodbc
# Get API Keys
content = open('aws_cred.json')
config = json.load(content)
access_key = config['access_key']
secret_access_key = config['secret_access_key']

#
def extract(database_name,load_type,source_table='all_tables'):
    conn_str = "DRIVER={SQL Server};PORT=1433;SERVER=DESKTOP-I2P6GLV\SQLEXPRESS02;DATABASE=%s;Trusted_Connection=yes;" % (
        database_name)

    cnxn = pyodbc.connect(conn_str)
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})

    engine = create_engine(connection_url)
    session = scoped_session(sessionmaker(bind=engine))
    s = session()

    # LOADING INCR DATA FOR SPECIFIC TABLE
    if load_type == "incr":
        # FOLDER CREATION IN S3 BUCKET
        ts_col = "current_date_" + source_table
        BUCKET_NAME = "om-test-sql"
        INCR_PATH = "etl_project/incr/" + source_table + "/"
        HIST_PATH = "etl_project/hist/" + source_table + "/"
        filename_INCR = INCR_PATH + source_table + ".csv"
        filename_HIST = HIST_PATH + source_table + " " + str(datetime.now()) + ".csv"

        # FETCHING INCR RECORDS OF SPECIFIC TABLE
        fetch_latest_record = "select * from {} where {} = (select max({}) from {} )".format(source_table, ts_col, ts_col, source_table)
        print(fetch_latest_record)
        flr = pd.read_sql_query(fetch_latest_record, con=engine)
        print(flr, source_table)
        s3_client = boto3.client('s3', aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_access_key,
                                 region_name='ap-south-1')
    # elif: FOR ALL INCR TABLES

        with io.StringIO() as csv_buffer:

        # loading into incremental folder
            flr.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket=BUCKET_NAME, Key=filename_INCR, Body=csv_buffer.getvalue())
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            print("Data Imported Successful")

            # loading into historical folder
            flr.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(Bucket=BUCKET_NAME, Key=filename_HIST, Body=csv_buffer.getvalue())
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")

    elif load_type == "full_load":
        # SELECTING ALL TABLES FOR FULL LOAD
        src_tables = s.execute("""select name as table_name from sys.tables""")
        if source_table == 'all_tables':
            for tbl in src_tables:
                df = pd.read_sql_query(f'select * from {tbl[0]}', engine)
                print("df is created")
                load(df, tbl[0])
                print("load is run")

        # SELECTING SPECIFIC TABLES FOR FULL LOAD
        else:
            df = pd.read_sql_query(f'select * from {source_table}', engine)
            print(df)
            load(df, source_table)
            print("load is run")

def load(df, tbl):
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}...for table {tbl}')
        # save to s3
        upload_file_bucket = "om-test-sql"
        upload_file_key = 'etl_project/full_load/' + str(tbl) + f"/{str(tbl)}"
        filepath = upload_file_key + "_" + str(date.today()) + ".csv"
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,
                                 region_name='ap-south-1')
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(df)
            print("Data Imported Successful")
    except Exception as e:
        print("Data load error: " + str(e))

extract('ABDB','full_load','customers')


# full_load - all_tables + full_Load - working
#             spec_table + full_load - working
#
# incr_load - all_tables + incr  - pending
#             spec_table + incr  - working