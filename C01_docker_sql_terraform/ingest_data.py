import pandas as pd
import argparse, os, sys
from sqlalchemy import create_engine
# from prefect import flow, task

# @task(log_prints=True, retries=3)
def ingest_data(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    print("URL Passed: ", url)
    
    csv_name = "output.csv"
    if url.split(".")[-1] == "csv":
        os.system(f"wget {url} -O {csv_name}")
    elif url.split(".")[-1] == "gz":
        os.system(f'wget {url} && gunzip {url.split("/")[-1]} && mv {(url.split("/")[-1]).replace(".gz", "")} output.csv')

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    print(df.columns)
    try:
        df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
        df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
    except Exception as e:
        pass
    
    # Insert column headers
    df.head(n=0).to_sql(name=f"{table_name}", con=engine, if_exists='replace')

    # Insert batch-wise data
    df.to_sql(name=f"{table_name}", con=engine, if_exists='append')
    while True:
            try:
                df = next(df_iter)
                print(f"{len(df)} rows ingested")
            except:
                print("SUCCESS: Ingestion successfully completed.")
                break
            else:
                df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
                df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
                df.to_sql(name=f"{table_name}", con=engine, if_exists='append')


# @flow(name="Ingestion")
def main():
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database for postgres')
    parser.add_argument('--table_name', help='name of the table to write results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    ingest_data(args)

if __name__ == "__main__":
    main()

