from itertools import count
import pandas as pd
import pyarrow
from sqlalchemy import URL
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine
import click

## Database connection setup
url_object = URL.create(
    "postgresql+pg8000",
    username="koosha",
    password="admin", 
    host="localhost",
    database="NYC_Taxi",
)
engine = create_engine(url_object)


## Function to load taxi data from the specified URL
def load_taxi_data(year,month,taxi_type):
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    month = str(month).zfill(2)
    url = f"{prefix}{taxi_type}_tripdata_{year}-{month}.parquet"
    df = pd.read_parquet(url, engine='pyarrow')
    table_name = f"{taxi_type}_tripdata_{year}_{month}"
    return df,table_name

## Function to split the DataFrame into chunks

def split_dataframe(df, chunk_size):
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i + chunk_size]

## Main function to load the data into the database in chunks

@click.command()
@click.option('--year', type=int, prompt=True, help='The year of the taxi data to load')
@click.option('--month', type=int, prompt=True, help='The month of the taxi data to load')
@click.option('--taxi_type', type=str, prompt=True, help='The type of taxi data to load')

def main(year, month, taxi_type):
    df, table_name = load_taxi_data(year, month, taxi_type)

    df.head(0).to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False
    )

    chunk_size = 10000
    for i, chunk in enumerate(tqdm(split_dataframe(df,chunk_size), desc = "Loading chunks")):
        chunk.to_sql(
            table_name,
            engine,
            if_exists = "append",
            index=False,
            method="multi",
            chunksize=1000
        )
        print(f"chunk {i+1} loaded | rows in chunk: {len(chunk)}")



if __name__ == '__main__':
    main()

