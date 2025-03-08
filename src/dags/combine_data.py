import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, countDistinct, first, min, max
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler


HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def combine_data(current_day, country, city=None):
    HOTEL_DETAILS_PATH = DATALAKE_ROOT_FOLDER + "formatted/source1/Hotel_details.snappy.parquet"
    HOTEL_ROOM_PRICE_PATH = DATALAKE_ROOT_FOLDER + "formatted/source2/hotels_RoomPrice.snappy.parquet"
    USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/hotelAnalysis/HotelStatistics/" + current_day + "/"
    USAGE_OUTPUT_FOLDER_BEST = DATALAKE_ROOT_FOLDER + "usage/hotelAnalysis/HotelTop10/" + current_day + "/"
    if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
        os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
    if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
        os.makedirs(USAGE_OUTPUT_FOLDER_BEST)

    spark = SparkSession.builder.appName("CombineData").getOrCreate()

    # Read the parquet files using Spark DataFrame API
    df_hotel_details = spark.read.parquet(HOTEL_DETAILS_PATH)
    df_hotel_room_price = spark.read.parquet(HOTEL_ROOM_PRICE_PATH)

    # Filter by country and city
    df_filtered = df_hotel_details.filter(df_hotel_details["country"] == country)
    if city:
        df_filtered = df_filtered.filter(df_hotel_details["city"] == city)

    # Top 10 hotels with highest average starrating
    top_10_hotels = df_filtered.groupBy("hotelid", "hotelname").agg(avg("starrating").alias("average_starrating")) \
        .orderBy(desc("average_starrating")).limit(10)
    top_10_hotels.show()

    # Geographical distribution of hotels in the city
    hotel_distribution = df_filtered.select("latitude", "longitude")
    hotel_distribution.show()

    # Repair sources for each hotel
    repaired_sources = df_filtered.select("hotelid", "Source").distinct()
    repaired_sources.show()

    # Minimum and maximum room prices for each hotel
    min_max_prices = df_hotel_room_price.groupBy("hotelcode").agg(
        min("onsiterate").alias("minimum_price"),
        max("onsiterate").alias("maximum_price")
    ).join(df_hotel_details, df_hotel_room_price.hotelcode == df_hotel_details.hotelid, "left") \
        .select(df_hotel_details.hotelname, df_hotel_room_price.hotelcode, "minimum_price", "maximum_price")

    min_max_prices.show()

    # Top 10 hotels with most countries
    top_10_popular_hotels = df_hotel_details.groupBy("hotelid", "hotelname").agg(
        countDistinct("country").alias("country_count"),
        first("city").alias("city"),
        first("country").alias("country")
    ).orderBy(desc("country_count")).limit(10)

    top_10_popular_hotels.show()

    # Room types for each hotel
    popular_room_types = df_hotel_room_price.join(df_hotel_details,
                                                  df_hotel_room_price.hotelcode == df_hotel_details.hotelid, "left") \
        .groupBy(df_hotel_room_price.hotelcode, df_hotel_room_price.roomtype) \
        .agg(count(df_hotel_room_price.roomtype).alias("roomtype_count"),
             first(df_hotel_details.hotelname).alias("hotelname")) \
        .orderBy(desc("roomtype_count"))

    popular_room_types.show()

    # Table with price, rate description, and room amenities for each room type
    room_details = df_hotel_room_price.select("roomtype", "onsiterate", "ratedescription", "roomamenities")
    room_details.show()

    # Filter by roomtype and display hotels with the most occurrences and best ratings
    selected_roomtype = "Vacation Home"
    filtered_hotels = df_hotel_room_price.filter(df_hotel_room_price["roomtype"] == selected_roomtype)

    top_hotels_by_roomtype = filtered_hotels.join(df_hotel_details,
                                                  filtered_hotels.hotelcode == df_hotel_details.hotelid, "left") \
        .groupBy(df_hotel_details.hotelname) \
        .agg(count(filtered_hotels.roomtype).alias("occurrences"),
             first(filtered_hotels.ratedescription).alias("best_rating")) \
        .orderBy(desc("occurrences"), desc("best_rating")).limit(10)

    top_hotels_by_roomtype.show()

    # Total number of hotels
    total_hotels = df_hotel_details.select("hotelname").distinct().count()
    print("Total number of hotels:", total_hotels)

    # Average star rating of hotels in the given country
    avg_star_rating = df_filtered.agg(avg("starrating")).first()[0]
    print("Average star rating:", avg_star_rating)

    # Number of unique room types
    unique_room_types = df_hotel_room_price.select("roomtype").distinct().count()
    print("Number of unique room types:", unique_room_types)

    # Top 3 most popular rooms for each hotel with prices
    top_3_rooms = df_hotel_room_price.groupBy("hotelcode", "roomtype").agg(avg("onsiterate").alias("average_price")) \
        .orderBy(col("average_price")).limit(3)

    # Join with hotel details to get hotel name
    top_3_rooms_with_name = top_3_rooms.join(df_hotel_details, top_3_rooms.hotelcode == df_hotel_details.hotelid,
                                             "left") \
        .select(df_hotel_details.hotelname, top_3_rooms.roomtype, top_3_rooms.average_price)

    top_3_rooms_with_name.show()

    # Top 5 most common room amenities
    top_common_amenities = df_hotel_room_price.select("roomamenities") \
        .groupBy("roomamenities").count().orderBy(desc("count")).limit(5)
    top_common_amenities.show()

    # Perform clustering on hotel distribution using K-means
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    data = assembler.transform(hotel_distribution)

    kmeans = KMeans(k=3, seed=0)
    model = kmeans.fit(data)

    transformed_data = model.transform(data)
    transformed_data.show()

    # Save the results to JSON files
    top_10_hotels.write.json(USAGE_OUTPUT_FOLDER_STATS + "top_10_hotels.json")
    hotel_distribution.write.json(USAGE_OUTPUT_FOLDER_STATS + "hotel_distribution.json")
    repaired_sources.write.json(USAGE_OUTPUT_FOLDER_STATS + "repaired_sources.json")
    min_max_prices.write.json(USAGE_OUTPUT_FOLDER_STATS + "min_max_prices.json")
    top_10_popular_hotels.write.json(USAGE_OUTPUT_FOLDER_STATS + "top_10_popular_hotels.json")
    popular_room_types.write.json(USAGE_OUTPUT_FOLDER_STATS + "popular_room_types.json")
    room_details.write.json(USAGE_OUTPUT_FOLDER_STATS + "room_details.json")
    top_hotels_by_roomtype.write.json(USAGE_OUTPUT_FOLDER_STATS + "top_hotels_by_roomtype.json")
    top_3_rooms_with_name.write.json(USAGE_OUTPUT_FOLDER_STATS + "top_3_rooms_with_name.json")
    top_common_amenities.write.json(USAGE_OUTPUT_FOLDER_STATS + "top_common_amenities.json")
    transformed_data.write.json(USAGE_OUTPUT_FOLDER_STATS + "clustered_hotels.json")


combine_data("2023-06-11", "France", "Paris")
