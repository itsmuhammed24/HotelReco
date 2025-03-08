
import os
import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted(file_name1, file_name2):
    SOURCE1_PATH = DATALAKE_ROOT_FOLDER + "raw/source1/" + file_name1
    FORMATTED_SOURCE1_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/source1/"
    if not os.path.exists(FORMATTED_SOURCE1_FOLDER):
        os.makedirs(FORMATTED_SOURCE1_FOLDER)
    df1 = pd.read_csv(SOURCE1_PATH)
    parquet_file_name1 = file_name1.replace(".csv", ".snappy.parquet")
    df1.to_parquet(FORMATTED_SOURCE1_FOLDER + parquet_file_name1, compression='snappy')

    SOURCE2_PATH = DATALAKE_ROOT_FOLDER + "raw/source2/" + file_name2
    FORMATTED_SOURCE2_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/source2/"
    if not os.path.exists(FORMATTED_SOURCE2_FOLDER):
        os.makedirs(FORMATTED_SOURCE2_FOLDER)
    df2 = pd.read_csv(SOURCE2_PATH)
    parquet_file_name2 = file_name2.replace(".csv", ".snappy.parquet")
    df2.to_parquet(FORMATTED_SOURCE2_FOLDER + parquet_file_name2, compression='snappy')


convert_raw_to_formatted("Hotel_details.csv", "hotels_RoomPrice.csv")
