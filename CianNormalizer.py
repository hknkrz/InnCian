from datetime import datetime
import pandas as pd

CIAN_DATASET_PATH = 'cian.csv'

df = pd.read_csv(CIAN_DATASET_PATH)


df = df.rename(columns={"rooms_count": "rooms", "offer_date":"date"})
try:
    df['district'] = df['district'].str.split(',').str.get(0)
except:
    pass

df = df[['cost', 'total_area', 'floor', 'rooms', 'region', 'district', 'metro', 'metro_distance', 'floors_number',
         'description', 'longitude', 'latitude', 'date', 'url']]


df = df.reset_index(drop=True)
df = df.dropna()
df.to_csv('normalized_cian_v2.csv',index=False)


df = df.drop_duplicates(subset=['cost', 'total_area', 'floor', 'rooms', 'region', 'district', 'metro', 'floors_number'])




