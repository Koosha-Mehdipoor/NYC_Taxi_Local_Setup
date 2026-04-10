
import pandas as pd
import pyarrow
from sqlalchemy import URL
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine
import io



def load_taxi_data(year,month,taxi_type):
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    month = str(month).zfill(2)
    url = f"{prefix}{taxi_type}_tripdata_{year}-{month}.parquet"
    df = pd.read_parquet(url, engine='pyarrow')
    table_name = f"{taxi_type}_tripdata_{year}_{month}"
    return df,table_name



df, table_name = load_taxi_data(2025,10,'green')

buffer = io.StringIO()
df.to_csv(buffer, index=False, header=False)
buffer.seek(0)


conn = psycopg2.connect(
    host="localhost",   # or "postgres" in docker
    database="NYC_Taxi",
    user="koosha",
    password="admin"
)
cursor = conn.cursor()


if "green" in table_name:
    cursor.execute(f"""
    DROP TABLE IF EXISTS public.{table_name};
    CREATE TABLE  public.{table_name} (
        "VendorID" integer,
        lpep_pickup_datetime timestamp without time zone,
        lpep_dropoff_datetime timestamp without time zone,
        store_and_fwd_flag text COLLATE pg_catalog."default",
        "RatecodeID" double precision,
        "PULocationID" integer,
        "DOLocationID" integer,
        passenger_count double precision,
        trip_distance double precision,
        fare_amount double precision,
        extra double precision,
        mta_tax double precision,
        tip_amount double precision,
        tolls_amount double precision,
        ehail_fee double precision,
        improvement_surcharge double precision,
        total_amount double precision,
        payment_type double precision,
        trip_type double precision,
        congestion_surcharge double precision,
        cbd_congestion_fee double precision
    );
""")
    conn.commit()
    cursor.copy_expert(
        f"COPY {table_name} FROM STDIN WITH CSV",
        buffer
    )

    conn.commit()
    cursor.close()
    conn.close()
elif "yellow" in table_name:
    cursor.execute(f"""
    DROP TABLE IF EXISTS public.{table_name};
    CREATE TABLE  public.{table_name} (
        "VendorID" integer,
        tpep_pickup_datetime timestamp without time zone,
        tpep_dropoff_datetime timestamp without time zone,
        passenger_count double precision,
        trip_distance double precision,
        "RatecodeID" double precision,
        store_and_fwd_flag text COLLATE pg_catalog."default",
        "PULocationID" integer,
        "DOLocationID" integer,
        payment_type bigint,
        fare_amount double precision,
        extra double precision,
        mta_tax double precision,
        tip_amount double precision,
        tolls_amount double precision,
        improvement_surcharge double precision,
        total_amount double precision,
        congestion_surcharge double precision,
        "Airport_fee" double precision,
        cbd_congestion_fee double precision
    );
""")
    conn.commit()
    cursor.copy_expert(
        f"COPY {table_name} FROM STDIN WITH CSV",
        buffer
    )

    conn.commit()
    cursor.close()
    conn.close()

else:
    print("Input is wrong")




