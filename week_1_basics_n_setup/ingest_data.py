import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os

def run(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_file = 'output.csv.gz'
    else:
        csv_file = 'output.csv'

    os.system(f"rm {csv_file}")

    os.system(f"wget {url} -O {csv_file}")

    print('downloaded the file '+csv_file)

    os.system(f"ls")
    
    print('opening the db connection to add records')

    df = pd.read_csv(csv_file, nrows = 5)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    datetime_cols_fun = lambda columns: [col for col in columns if col.endswith('_datetime')]
    datetime_cols = datetime_cols_fun(df.columns)
    print('list of date time columns ', datetime_cols)

    try:
        #insert the first batch
        df_iter = pd.read_csv(csv_file, iterator = True, chunksize=100000, parse_dates=datetime_cols)
        df = next(df_iter)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

        #insert rest of the data
        while True:
            start_t = time()
            df = next(df_iter)
            df.to_sql(name=table_name, con=engine, if_exists ='append') 
            end_t = time()
            print('inserted a chunk of data,  time taken: %.3f second'%(end_t-start_t))
    except StopIteration:
        print('finished inserting all data')


if __name__=='__main__':
    parser = argparse.ArgumentParser('Input arguments data ingestion pipeline')
    parser.add_argument('--user', required=True, type=str, help = 'user id for db')
    parser.add_argument('--password', required=True, type=str, help = 'password for db')
    parser.add_argument('--host', required=True, type=str, help = 'host for db')
    parser.add_argument('--port', required=True, type=str, help = 'port for db')
    parser.add_argument('--db', required=True, type=str, help = 'database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    run(parser.parse_args())