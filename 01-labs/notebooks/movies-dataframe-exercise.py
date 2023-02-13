# Databricks notebook source
# MAGIC %md
# MAGIC ## CSC8101 - Practical 07 Feb 2023
# MAGIC 
# MAGIC #### Exercise 2 - Movies Dataset - Take home
# MAGIC 
# MAGIC Two input [datasets](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset):
# MAGIC 
# MAGIC - `ratings` dataset: Movie ratings from 270,000 users for all 45,000 movies. Ratings are on a scale of 1-5 and have been obtained from the official GroupLens website.
# MAGIC - `movies` dataset: The main Movies Metadata file. Contains information on 45,000 movies featured in the Full MovieLens dataset.
# MAGIC 
# MAGIC Each of these datasets is read into a DataFrame below.
# MAGIC 
# MAGIC ##### Task 1
# MAGIC 
# MAGIC 1. How many partitions has each dataset?
# MAGIC 2. How big is each dataset? (Report number of rows)
# MAGIC 3. Repartition the `ratings` dataset by key `movieID` across `100` partitions.
# MAGIC 4. Verify that the `ratings` dataset now has `100` partitions.
# MAGIC 
# MAGIC Docs:
# MAGIC - [Repartition](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)
# MAGIC 
# MAGIC ##### Task 2
# MAGIC 
# MAGIC Write a data pipeline function that takes as input the two datasets above and outputs the `N` most popular films (by rating), for a given `genre`, for a given `decade` (specified by its start year, e.g. `1980` for 80s and `2000` for 2000s).
# MAGIC 
# MAGIC > Example function run: `pipeline(N = 10, genre = "comedy", decade = 2010)`
# MAGIC 
# MAGIC Run your function for the following parameter inputs and report your findings. Set `N = 10` throughout:
# MAGIC 
# MAGIC - `genre = "Thriller"`, `decade = 1980`
# MAGIC - `genre = "Drama"`, `decade = 2000`
# MAGIC - `genre = "Comedy"`, `decade = 2010`
# MAGIC 
# MAGIC Helpful docs:
# MAGIC 
# MAGIC - [DataFrame quickstart](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html?highlight=select)
# MAGIC - [withColumn](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn)
# MAGIC - [select](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select)
# MAGIC - [orderBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html?highlight=orderby#pyspark.sql.DataFrame.orderBy)
# MAGIC - [join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join)
# MAGIC - [filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html?highlight=filter#pyspark.sql.DataFrame.filter)

# COMMAND ----------

import pyspark.sql.functions as FN
import pyspark.sql.types as TP

# Task 1

# File location and type
ratings_file_location = "/FileStore/tables/movies/ratings.csv"
movies_file_location = "/FileStore/tables/movies/movies_metadata.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","



# The applied options are for CSV files. For other file types, these will be ignored.
ratings = spark.read.format(file_type) \
          .option("inferSchema", infer_schema) \
          .option("header", first_row_is_header) \
          .option("sep", delimiter) \
          .load(ratings_file_location)

movies_metadata = spark.read.format(file_type) \
                  .option("inferSchema", infer_schema) \
                  .option("header", first_row_is_header) \
                  .option("sep", delimiter) \
                  .load(movies_file_location)

# COMMAND ----------

display(ratings.take(5))

# COMMAND ----------

display(movies_metadata.printSchema())

# COMMAND ----------

movies_metadata[['genres']].take(1)

# COMMAND ----------

# Initial pre-processing

## Select relevant columns
movies = movies_metadata[['id','original_title','genres','release_date']]

## sample data
display(movies.take(5))

# COMMAND ----------

# Notice that genres is a string but that's not very useful - so we need to make it a structure that can be 
# In this case that is an array of dicts with key set ('id', 'name')


# schema for 'genres' column in movies metadata dataset
genres_schema = TP.ArrayType(
    TP.StructType([
        TP.StructField("id", TP.IntegerType()),
        TP.StructField("name", TP.StringType())
    ])
)

# Now we overwrite columns 'genres' to parse the string into a data structure that we can manipulate
movies = movies.withColumn("genres", FN.from_json(movies.genres, genres_schema))


# COMMAND ----------

display(movies.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Partition `ratings` dataset
# MAGIC 
# MAGIC 1. How many partitions has each dataset?
# MAGIC 2. How big is each dataset? (Report number of rows)
# MAGIC 3. Repartition the `ratings` dataset by key `movieID` across `100` partitions.
# MAGIC 4. Verify that the `ratings` dataset now has `100` partitions.

# COMMAND ----------

# write your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Pipeline
# MAGIC 
# MAGIC Write a data pipeline function that takes as input the two datasets above and outputs the `N` most popular films (by rating), for a given `genre`, for a given `decade` (specified by its start year, e.g. `1980` for 80s and `2000` for 2000s).

# COMMAND ----------

def movies_pipeline(movies_df, ratings_df, N = 10, genre = "Comedy", decade = 1980):
    # write your solution here
    # develop your solution in separate cells before implementing this function    
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2a. Begin by further pre-processing the movies dataframe
# MAGIC 
# MAGIC Save the output of this sequence of operations into a new variable.
# MAGIC 
# MAGIC - Extract the name of each genre in column `genres`
# MAGIC - Convert date string to datetime structure in column `release_date`
# MAGIC - Remove movies with null date values
# MAGIC - Create new column `year` from `release_date`
# MAGIC - Drop the `release_date` column
# MAGIC 
# MAGIC Docs:
# MAGIC 
# MAGIC - [Operations on columns - Pyspark functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) (used within `withColumn` or `select`)
# MAGIC - [Operations on DataFrames](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
# MAGIC - [withColumn](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn)
# MAGIC - [filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html?highlight=filter#pyspark.sql.DataFrame.filter)
# MAGIC - [drop](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html#pyspark.sql.DataFrame.drop)

# COMMAND ----------

# write your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2b. Filter the processed movies dataset based on parameters `genre` and `decade`
# MAGIC 
# MAGIC Save the output of this sequence of operations into a new variable.
# MAGIC 
# MAGIC Docs:
# MAGIC 
# MAGIC - [Operations on columns - Pyspark functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) (used within `withColumn` or `select`)
# MAGIC - [filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html?highlight=filter#pyspark.sql.DataFrame.filter)
# MAGIC 
# MAGIC Hints:
# MAGIC 
# MAGIC - Each film can have multiple genres. What is the pyspark function that allows you to find whether an array contains an element?
# MAGIC - How can we calculate that a given year is part of a decade? There's a simple mathematical formula..

# COMMAND ----------

# write your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2c. Calculate the average rating of each film in the ratings dataset
# MAGIC 
# MAGIC Save the output of this opepration into a new variable.
# MAGIC 
# MAGIC Docs:
# MAGIC 
# MAGIC - [groupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html#pyspark.sql.DataFrame.groupBy)
# MAGIC - [agg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.agg.html#pyspark.sql.DataFrame.agg)

# COMMAND ----------

# write your solution here

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2d. Join the result of 2b with 2c, order by avg rating (desceding order) and select top N
# MAGIC 
# MAGIC Docs:
# MAGIC 
# MAGIC - [join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join)
# MAGIC - [orderBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html?highlight=orderby#pyspark.sql.DataFrame.orderBy)
# MAGIC - [select](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select)

# COMMAND ----------

N = 10

result = subset.join(avg_ratings, subset.id == avg_ratings.movieID, how = "inner")\
               .orderBy(FN.col('avg_rating').desc())\
               .select(['original_title', 'year', 'avg_rating'])\
               .take(N)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2e. Put everything together in a function

# COMMAND ----------

# copy the function signature above and write your solution here

# COMMAND ----------

display(
    movies_pipeline(movies, ratings, genre = "Thriller", decade = 1980, N = 10)
)
