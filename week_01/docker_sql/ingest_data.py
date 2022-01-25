import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    # download csv
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # conn = engine.connect()


    # print(pd.io.sql.get_schema(df, name = 'yellow_taxi_data', con=engine))


    df_iter = pd.read_csv(csv_name, iterator = True, chunksize = 100000)

    df = next(df_iter)


    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists = 'replace')

    df.to_sql(name=table_name, con=engine, if_exists = 'append')


    while True:
        t_start = time()
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists = 'append')
        
        t_end = time()
        
        print('inserted another chunk, took %.3f seconds' %(t_end-t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Inest CSV file to postgres')

    # user, password, host, port, db name, table name
    # url of the csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password  for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='target table')
    parser.add_argument('--url', help='url for the source data')

    args = parser.parse_args()

    main(args)