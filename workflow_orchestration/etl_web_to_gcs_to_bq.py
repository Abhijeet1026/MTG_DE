import os
import requests
import pandas as pd
import datetime as dt
import warnings
from pathlib import Path
from prefect import task, flow
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import json
import ast
from prefect_dbt.cli import DbtCoreOperation, DbtCliProfile
warnings.filterwarnings('ignore')
import pandas_gbq

@task(log_prints = True, name = "Fetch_data", retries = 3)
def fetch_data(url:str) -> pd.DataFrame:
    "Fuction to fetch the data using the url"
    response = requests.get(url, timeout = 5)
    if response.status_code == 200:
        json_file, recent_date  = response.json()["download_uri"], response.json()["updated_at"][:10]
        df = pd.read_json(json_file)
        print("Completed Successfully")
        return df, recent_date
    else:
        print(f"Request_failed")

@task(log_prints = True, name = "write_to_gcs")
def write_to_gcs(api_data: pd.DataFrame, recent_date: str, type_of_cards: str, gcp_cloud: object ) -> None :
    "Writing Parquet format file to  GCP cloud storage"
#https://prefecthq.github.io/prefect-gcp/cloud_storage/#prefect_gcp.cloud_storage.GcsBucket.read_path
    gcp_cloud.upload_from_dataframe(
        df = api_data.astype(str),
        to_path = f"MTG/{type_of_cards}_{recent_date}.parquet",
        serialization_format="parquet_snappy",
    )
    return

@task(log_prints = True, name = "Retrive Data from GCS")
def read_from_gcs(recent_date: str, type_of_cards: str, gcp_cloud:object) -> pd.DataFrame:
    "Reading the file from GCS bucket as dataframe"
    df = pd.read_parquet(f"gs://dataengineering1-433721_data_lake_card/MTG/{type_of_cards}_{recent_date}.snappy.parquet")
    # print(df.head(2))
    return df

def string_concatination(string: list) -> str:
    """Concatenation of string""" 
    joined_string = ','.join(ast.literal_eval(string))
    return(joined_string)

def get_prices(a: str, currency):
    """Extraction of card prices"""
    card_prices = []
  
    for i in a:
        i = ast.literal_eval(i)
        card_prices.append(i[currency])
    card_prices = pd.to_numeric(card_prices)
    return card_prices

@task(log_prints = True, name = "Minor transformation to the data")
def transform(data_df, recent_date: str ) -> pd.DataFrame:
    "Performing minor transformation to the data"
    data_df["id"] = data_df["id"].astype("string")
    data_df["name"] = data_df["name"].astype("string")
    data_df["released_at"] = pd.to_datetime(data_df["released_at"])
    data_df["color_identity"] = data_df["color_identity"].apply(string_concatination)
    data_df["set_name"] = data_df["set_name"].astype("string")
    data_df["artist"] = data_df["artist"].astype("string")
    usd_prices = get_prices(data_df["prices"], currency = "usd")
    euro_prices = get_prices(data_df["prices"], currency = "eur")
    year, month, day = int(recent_date[0:4]), int(recent_date[5:7]), int(recent_date[8:10])
    date = dt.datetime(year, month, day)
    
    data_df["usd_prices"] = usd_prices
    data_df["euro_prices"] = euro_prices
    data_df["DataExtraction_date"] = date

    data_df = data_df[
        [
            "id",
            "name",
            "released_at",
            "color_identity",
            "set_name",
            "artist",
            "usd_prices",
            "euro_prices",
            "DataExtraction_date",
        ]
    ]

    print(data_df["id"][1])
    print(data_df["name"][1])
    print(data_df["released_at"][1])
    print(data_df["color_identity"][1])
    print(data_df["set_name"][1])
    print(data_df["artist"][1])
    print(data_df["usd_prices"][1])
    print(data_df["euro_prices"][1])
    print(data_df["DataExtraction_date"][1])
    return data_df

@task(log_prints = True, name = "Write data to BQ") 
def write_data_to_BQ(df :pd.DataFrame)-> None: 
    print(df.shape)
    
    gcp_credentials_block = GcpCredentials.load("gcp-creds")



    df.to_gbq(
        destination_table= "mtg_card_data_raw.default_cards_data",
        project_id= "dataengineering1-433721",
        credentials=  gcp_credentials_block.get_credentials_from_service_account(),
        chunksize= 20000,
        if_exists= "append",

    )

    print("Appended data successfullty to BQ Table")

    return


        
@flow(log_prints = True, name = "Card data API to Big Query")
def data_api(type_of_cards:str) -> None:
#https://scryfall.com/docs/api/bulk-data/all
    gcp_cloud = GcsBucket.load("de-gcs")
    url = f"https://api.scryfall.com/bulk-data/{type_of_cards}"
    api_data_df, recent_date = fetch_data(url)
    write_to_gcs(api_data_df,recent_date, type_of_cards, gcp_cloud)
    data_df = read_from_gcs(recent_date, type_of_cards, gcp_cloud)
    
    data_transformation = transform(data_df, recent_date)
    print(data_transformation.shape)
    write_data_to_BQ(data_transformation)




if __name__ == "__main__":
    card_type = ["default_cards", "oracle_cards"]
    data_api(card_type[0])