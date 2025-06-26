from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, avg, count

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("Movie Ratings Analysis") \
    .getOrCreate()

# Step 2: Load CSV files
movies_df = spark.read.csv("movies.csv", header=True, inferSchema=True)
ratings_df = spark.read.csv("ratings.csv", header=True, inferSchema=True)

# Step 3: Join movies with ratings
combined_df = ratings_df.join(movies_df, on="movieId")

# Step 4: Top-rated movies (with more than 50 ratings)
top_rated_movies = combined_df.groupBy("title") \
    .agg(
        avg("rating").alias("average_rating"),
        count("rating").alias("rating_count")
    ) \
    .filter("rating_count >= 50") \
    .orderBy("average_rating", ascending=False)

print("🎬 Top Rated Movies (with at least 50 ratings):")
top_rated_movies.show(10, truncate=False)

# Step 5: Most popular movies (by total ratings)
most_popular_movies = combined_df.groupBy("title") \
    .count() \
    .withColumnRenamed("count", "rating_count") \
    .orderBy("rating_count", ascending=False)

print("🔥 Most Popular Movies (by rating count):")
most_popular_movies.show(10, truncate=False)

# Step 6: Average rating per genre
# Split and explode genres
genre_df = movies_df.withColumn("genre", explode(split("genres", "\\|")))

# Join with ratings
ratings_genre_df = ratings_df.join(genre_df, on="movieId")

# Group by genre and compute average rating
genre_ratings = ratings_genre_df.groupBy("genre") \
    .agg(avg("rating").alias("average_rating")) \
    .orderBy("average_rating", ascending=False)

print("🎭 Average Rating by Genre:")
genre_ratings.show(truncate=False)

# Optional: stop Spark session
spark.stop()
