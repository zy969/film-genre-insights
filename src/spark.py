from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, explode, split, year, sum, count, stddev, avg, lower, trim, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Movie Genre Analysis") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0") \
    .getOrCreate()
project_id = "thermal-formula-416221"
bucket_name = "movie-zy969"
dataset_name = "movie"
table_name = "movie"

# Load data from BigQuery
movies_df = spark.read \
    .format("bigquery") \
    .option("project", project_id) \
    .option("table", f"{project_id}:{dataset_name}.{table_name}") \
    .load()

# Deduplicate
movies_df = movies_df.dropDuplicates(["id"])

# Filter out records with missing release_date or genres
movies_df = movies_df.filter(col("release_date").isNotNull() & col("genres").isNotNull())


# Filter out movies released in 2024 and after
movies_df = movies_df.withColumn("year", year(col("release_date")))
movies_df = movies_df.filter(col("year") <= 2023)

# Fill missing values with mean
columns_to_replace = ["runtime", "budget", "revenue", "vote_count", "vote_average"]
for column in columns_to_replace:
    movies_df = movies_df.withColumn(column, when(col(column) == 0, None).otherwise(col(column)))
mean_values = movies_df.select([mean(col(c)).alias(c) for c in columns_to_replace]).collect()[0]
movies_df = movies_df.na.fill({
    "runtime": mean_values["runtime"],
    "budget": mean_values["budget"],
    "revenue": mean_values["revenue"],
    "vote_count": mean_values["vote_count"],
    "vote_average": mean_values["vote_average"] 
})

# Outlier handling
stats = movies_df.select(
    [mean(col("budget")).alias("mean_budget"), stddev(col("budget")).alias("stddev_budget"),
     mean(col("revenue")).alias("mean_revenue"), stddev(col("revenue")).alias("stddev_revenue"),
     mean(col("vote_count")).alias("mean_vote_count"), stddev(col("vote_count")).alias("stddev_vote_count")]).collect()[0]

budget_bounds = (stats.mean_budget - (3 * stats.stddev_budget), stats.mean_budget + (3 * stats.stddev_budget))
revenue_bounds = (stats.mean_revenue - (3 * stats.stddev_revenue), stats.mean_revenue + (3 * stats.stddev_revenue))
vote_count_bounds = (stats.mean_vote_count - (3 * stats.stddev_vote_count), stats.mean_vote_count + (3 * stats.stddev_vote_count))

movies_df = movies_df.filter(
    (col("budget").between(*budget_bounds)) & 
    (col("revenue").between(*revenue_bounds)) &
    (col("vote_count").between(*vote_count_bounds))
)

# Preprocess genres for consistency
movies_df = movies_df.withColumn("genres", lower(trim(col("genres"))))
movies_df = movies_df.withColumn("genre", explode(split(movies_df.genres, ",\s*")))

# Analyze genre distribution
genre_distribution = movies_df.groupBy("genre").agg(count("id").alias("movie_count"),
                                                     mean("vote_average").alias("average_rating"),
                                                     sum("revenue").alias("total_revenue"))

# Analyze trends over time
movies_df = movies_df.withColumn("year", year(col("release_date")))
time_trend_analysis = movies_df.groupBy("year", "genre").agg(count("id").alias("movie_count"),
                                                              mean("vote_average").alias("average_rating"),
                                                              sum("revenue").alias("total_revenue"))

# Budget and revenue analysis
budget_revenue_analysis = movies_df.select("budget", "revenue",
                                           ((col("revenue") - col("budget")) / col("budget")).alias("ROI"), "genre")
budget_revenue_analysis = budget_revenue_analysis.filter(
    (col("ROI") >= -2) & (col("ROI") <= 20)
)

# Group by genre and calculate average ROI
average_roi_by_genre = budget_revenue_analysis.groupBy("genre").agg(avg("ROI").alias("average_ROI"))

# Save results to BigQuery
average_roi_by_genre.write.format("bigquery").option("project", project_id).option("table", f"{project_id}:{dataset_name}.average_roi_by_genre").option("temporaryGcsBucket", bucket_name).mode('overwrite').save()
genre_distribution.write.format("bigquery").option("project", project_id).option("table", f"{project_id}:{dataset_name}.genre_distribution").option("temporaryGcsBucket", bucket_name).mode('overwrite').save()

# Filter time trend analysis data for the years 2000-2023
time_trend_analysis = time_trend_analysis.filter((col("year") >= 2000) & (col("year") <= 2023))
time_trend_analysis.write.format("bigquery").option("project", project_id).option("table", f"{project_id}:{dataset_name}.time_trend_analysis").option("temporaryGcsBucket", bucket_name).mode('overwrite').save()

spark.stop()
