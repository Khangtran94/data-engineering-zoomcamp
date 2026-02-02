#!/usr/bin/env python
# coding: utf-8

# In[73]:


import pandas as pd
# Read a sample of the data
year = 2025
month = 11

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz')

# Display first rows
df.head()


# In[74]:


df.dtypes


# In[75]:


df.shape


# In[76]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    # nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)
df


# In[77]:


from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg://root:root@localhost:5433/ny_taxi')
engine


# In[78]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[79]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[80]:


df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000)
df_iter


# In[81]:


from tqdm.auto import tqdm
for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

