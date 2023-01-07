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
content = open('config.json')
config = json.load(content)
access_key = config['access_key']
secret_access_key = config['secret_access_key']
# get password from environment var
# connection_string = "DRIVER={SQL Server Native Client 15};SERVER=LAPTOP-MUFDTM9A;DATABASE=practice;UID=demo;PWD=test321"
# connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
#built connection
conn_str = (
    r'DRIVER={SQL Server};'
    r'PORT=1433;'
    r'SERVER=DESKTOP-I2P6GLV\SQLEXPRESS02;'
    r'DATABASE=ABDB;'
    r'Trusted_Connection=yes;'
)
cnxn = pyodbc.connect(conn_str)
# engine = create_engine(cnxn)
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})



def extract():
    try:
        engine = create_engine(connection_url)
        session = scoped_session(sessionmaker(bind=engine))
        s = session()

        #execute query
        src_tables = s.execute("""select t.name as table_name from sys.tables t where t.name in ('Item_History_data','order_history_data','customer_History_data')""")

        for tbl in src_tables:
            print(tbl)
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * from {tbl[0]}',engine)
            load(df,tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
#load data
def load(df,tbl):
    try:
        rows_imported = 0
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}...for table {tbl}')
        #save to s3
        upload_file_bucket = 'om-test-06'
        upload_file_key = 'public/' + str(tbl) + f"/{str(tbl)}"
        filepath = upload_file_key + ".csv"
        s3_client = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_access_key,region_name='ap-south-1')
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)

            response = s3_client.put_object(
                Bucket = upload_file_bucket, Key= filepath, Body=csv_buffer.getvalue()
            )
            status = response.get("ResponseMetadata",{}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported +=len(df)
            print("Data Imported Successful")
    except Exception as e:
        print("Data load error: " + str(e))


try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))