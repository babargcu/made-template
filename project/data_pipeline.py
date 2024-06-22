import os
import pandas as pd
import requests
from sqlalchemy import create_engine
import io

# URLs for the datasets
summer_url = 'https://dati.comune.milano.it/dataset/d9b6975d-7b28-413a-ac7f-cd05432824c1/resource/62de6bfa-1d15-4498-b69a-71f90dbf1018/download/ds1561_stagione_termica_estiva.csv'
winter_url = 'https://dati.comune.milano.it/dataset/ef94c475-cb1a-4432-bd90-9cb3a739bd71/resource/b5a63c19-4a34-4ba0-8b49-04696372d8d2/download/ds1560_stagione_termica_invernale.csv'

def download_csv(url: str) -> pd.DataFrame:
    """Download CSV data from a given URL."""
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful
    csv_data = response.content.decode('utf-8')
    df = pd.read_csv(io.StringIO(csv_data), delimiter=';')
    return df

def extract() -> tuple:
    """Extract data from the provided URLs."""
    df_summer = download_csv(summer_url)
    df_winter = download_csv(winter_url)
    return df_summer, df_winter

def transform(df_summer: pd.DataFrame, df_winter: pd.DataFrame) -> pd.DataFrame:
    """Transform and merge data sets."""
    # Rename columns for consistency
    df_summer.columns = ["Metric", "Milano Bicocca", "Milano Bocconi", "Milano Bovisa", "Milano Centro", "Milano Citta' Studi", "Milano San Siro", "Milano Sud"]
    df_winter.columns = ["Metric", "Milano Bicocca", "Milano Bocconi", "Milano Bovisa", "Milano Centro", "Milano Citta' Studi", "Milano San Siro", "Milano Sud"]
    
    # Unpivot the dataframes to have a long format
    df_summer = df_summer.melt(id_vars=["Metric"], var_name="Station", value_name="Summer_Value")
    df_winter = df_winter.melt(id_vars=["Metric"], var_name="Station", value_name="Winter_Value")
    
    # Merge the dataframes on Metric and Station
    df = pd.merge(df_summer, df_winter, on=["Metric", "Station"], how="outer")
    
    # Pivot the merged dataframe to have separate columns for each metric
    df = df.pivot_table(index="Station", columns="Metric", values=["Summer_Value", "Winter_Value"]).reset_index()
    df.columns = [' '.join(col).strip() for col in df.columns.values]
    
    return df

def load(df):
    db_path = os.path.join(os.getcwd(), '../data', 'milan_climate.sqlite')  # Use absolute path
    os.makedirs(os.path.dirname(db_path), exist_ok=True)  # Ensure directory exists
    disk_engine = create_engine(f'sqlite:///{db_path}')
    df.to_sql('climate_data', disk_engine, if_exists='replace', index=False)

# Run the ETL process
df_summer, df_winter = extract()
df = transform(df_summer, df_winter)
load(df)

print("ETL completed successfully.")
