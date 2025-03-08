from pyspark.sql import SparkSession
import os
from elasticsearch import Elasticsearch


HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/hotelAnalysis/HotelStatistics/"


def index_to_elasticsearch(current_day):

    # Chemins des fichiers JSON
    json_files = [
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/top_10_hotels.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/hotel_distribution.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/repaired_sources.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/min_max_prices.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/top_10_popular_hotels.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/popular_room_types.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/room_details.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/top_hotels_by_roomtype.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/top_3_rooms_with_name.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/top_common_amenities.json",
        USAGE_OUTPUT_FOLDER_STATS + current_day + "/clustered_hotels.json"

    ]

    # Création de la session Spark
    spark = SparkSession.builder.appName("IndexToElasticsearch").getOrCreate()

    # Configuration Elasticsearch
    es_host = "localhost"  # Adresse de l'hôte Elasticsearch
    es_port = "9200"  # Port Elasticsearch
    es_index = "hotel_recommandation1"  # Nom de l'index Elasticsearch

    # Création d'une instance Elasticsearch
    es = Elasticsearch([{'host': es_host, 'port': es_port}])

    # Fonction pour indexer un DataFrame dans Elasticsearch
    def index_dataframe_to_es(dataframe, index_name):
        for row in dataframe.rdd.collect():
            data = row.asDict()
            es.index(index=index_name, body=data)
            print(data)

    # Lecture des fichiers JSON et indexation dans Elasticsearch
    for file_path in json_files:
        df = spark.read.json(file_path)
        file_name = os.path.basename(file_path)
        index_name = es_index
        index_dataframe_to_es(df, index_name)

    # Fermeture de la session Spark
    spark.stop()


# Exemple d'utilisation

index_to_elasticsearch("2023-06-11")
