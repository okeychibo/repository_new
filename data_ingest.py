import pandas as pd
import wget
from sqlalchemy import create_engine


final_df = []
month = ['01']

for data in month:
    result = wget.download(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-{data}.parquet")
    final_df.append(result)
    df = pd.read_parquet(result)                                                                           

# change date from object to datetime
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

engine = create_engine('postgresql://postgres:root@localhost:5432/ayo_db')

df_iter = df.iloc[:100000,:]

df_iter.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# gggg