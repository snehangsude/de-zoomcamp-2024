import dlt, datetime
import pandas as pd


YM_PAIRS= [(year, month) for year in [2019, 2020] for month in range(1, 13)]

@dlt.source
def ny_taxi_data_source():
    return [fhv_taxi_data, green_taxi_data1920, yellow_taxi_data1920]

@dlt.resource(table_name="yellow_taxi_rides",write_disposition="append")
def yellow_taxi_data1920():
    
    for year, months in YM_PAIRS:
        URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{months:02d}.parquet"
        df = pd.read_parquet(URL)
        if "airport_fee" in df.columns:
            df.drop(['airport_fee'], inplace=True, axis=1)
        print(URL, df.shape)
        yield df 



@dlt.resource(table_name="green_taxi_rides",write_disposition="append")
def green_taxi_data1920():
    
    for year, months in YM_PAIRS:
        URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{months:02d}.parquet"
        df = pd.read_parquet(URL)
        print(URL, df.shape)
        yield df 



@dlt.resource(table_name="fhv_taxi_rides",write_disposition="append")
def fhv_taxi_data():
    
    for year, months in YM_PAIRS:
        URL = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{months:02d}.csv.gz"
        df =  pd.read_csv(URL, compression="gzip", encoding='windows-1252')
        try:
            df[['PUlocationID','DOlocationID']] = df[['PUlocationID','DOlocationID']].astype(float)
        except KeyError:
            df[['PULocationID','DOLocationID']] = df[['PULocationID','DOLocationID']].astype(float)
            df.rename(columns={"DOLocationID": "DOlocationID", "PULocationID": "PUlocationID", "dropoff_datetime": "dropOff_datetime"}, inplace=True)
        print(URL, df.shape)
        yield df


if __name__ == "__main__":
    # configure the pipeline with your destination details
    time1 = datetime.datetime.now()
    pipeline = dlt.pipeline(
        pipeline_name='ny_taxi_data', destination='bigquery', dataset_name='ny_taxi_parq'
    )
    load_info = pipeline.run(ny_taxi_data_source())
    print(load_info)
    time2 = datetime.datetime.now()
    print(f"\n\nTotal dlt pipeline: {time2-time1}")

