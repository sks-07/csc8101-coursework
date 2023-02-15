# Databricks notebook source
# MAGIC %md
# MAGIC # [CSC8101] Big Data Analytics - 2022 Spark Coursework

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coursework overview
# MAGIC 
# MAGIC ### Inputs
# MAGIC 
# MAGIC - **NYC Taxi Trips dataset** - list of recorded taxi trips, each with several characteristics, namely: distance, number of passengers, origin zone, destination zone and trip cost (total amount charged to customer).
# MAGIC - **NYC Zones dataset** - list of zones wherein trips can originate/terminate.
# MAGIC 
# MAGIC ### Tasks
# MAGIC 
# MAGIC 1. Data cleaning
# MAGIC   1. Remove "0 distance" and 'no passengers' records.
# MAGIC   2. Remove outlier records. 
# MAGIC 2. Add new columns
# MAGIC   1. Join with zones dataset
# MAGIC   2. Compute the unit profitability of each trip
# MAGIC 3. Zone summarisation and ranking
# MAGIC   1. Summarise trip data per zone
# MAGIC   2. Obtain the top 10 ranks according to:
# MAGIC     1. The total trip volume
# MAGIC     2. Their average profitabilitiy
# MAGIC     3. The total passenger volume
# MAGIC 4. Record the total and task-specific execution times for each dataset size and format.
# MAGIC 
# MAGIC ### How to
# MAGIC 
# MAGIC ###### Code structure and implementation
# MAGIC 
# MAGIC - You must implement your solution to each task in the provided function code skeleton.
# MAGIC - The task-specific functions are combined together to form the full pipeline code, executed last (do not modify this code).
# MAGIC - Before implementing the specified function skeleton, you should develop and test your solution on separate code cells (create and destroy cells as needed).
# MAGIC 
# MAGIC ###### Development
# MAGIC 
# MAGIC - Develop an initial working solution for the 'S' dataset and only then optimise it for larger dataset sizes.
# MAGIC - To perform vectorised operations on a DataFrame:
# MAGIC   - use the API docs to look for existing vectorised functions in: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions
# MAGIC   - if a customised function is required (e.g. to add a new column based on a linear combination of other columns), implement your own User Defined Function (UDF). See:  https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html
# MAGIC - Use only the `pyspark.sql` API - documentation link below - (note that searching through the docs returns results from the `pyspark.sql` API together with the `pyspark.pandas` API):
# MAGIC   - https://spark.apache.org/docs/3.2.0/api/python/reference/pyspark.sql.html
# MAGIC - Periodically download your notebook to your computer as backup and safety measure against accidental file deletion.
# MAGIC  
# MAGIC ###### Execution time measurement
# MAGIC 
# MAGIC - Execution time is calculated and returned by the Spark Engine and shown in the output region of the cell.
# MAGIC - To measure the execution time of a task you must perform a `collect` or similar operation (e.g. `take`) on the returned DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 0 - Read data
# MAGIC 
# MAGIC The code below is ready to run. **Do not modify this code**. It does the following:
# MAGIC 
# MAGIC - Reads the 'zones' dataset into variable 'zone_names'
# MAGIC - Defines the `init_trips` function that allows you to read the 'trips' dataset (from the DBFS FileStore) given the dataset size ('S' to 'XXL') and format ('parquet' or 'delta') as function arguments
# MAGIC - Defines the `pipeline` function, called in Task 4 to measure the execution time of the entire data processing pipeline
# MAGIC - Shows you how to call the `init_trips` function and display dataset characteristics (number of rows, schema)

# COMMAND ----------

## global imports
import pyspark.sql as ps
import pyspark.sql.functions as pf
import pandas as pd

# Load zone names dataset - (much faster to read small file from git than dbfs)
zones_file_url = 'https://raw.githubusercontent.com/NewcastleComputingScience/csc8101-coursework/main/02-assignment-spark/taxi_zone_names.csv'
zone_names = spark.createDataFrame(pd.read_csv(zones_file_url))

# Function to load trips dataset by selected dataset size
def init_trips(size = 'S', data_format = "parquet", taxi_folder = "/FileStore/tables/taxi"):     
    
    files = {
        'S'  : ['2021_07'],
        'M'  : ['2021'],
        'L'  : ['2020_21'],
        'XL' : ['1_6_2019', '7_12_2019'],
        'XXL': ['1_6_2019', '7_12_2019', '2020_21']
    }
    
    # validate input dataset size
    if size not in files.keys():
        print("Invalid input dataset size. Must be one of {}".format(list(files.keys())))
        return None               
    
    if data_format == "parquet":
        filenames = list(map(lambda s: f'{taxi_folder}/parquet/tripdata_{s}.parquet', files[size]))
        trips_df = spark.read.parquet(filenames[0])
        
        for name in filenames[1:]:
            trips_df = trips_df.union(spark.read.parquet(name))
            
    elif data_format == "delta":
        filenames = f"{taxi_folder}/delta/taxi-{size}-delta/"
        trips_df = spark.read.format("delta").load(filenames)
    
    else:
        print("Invalid data format. Must be one of {}".format(['parquet', 'delta']))
        return None
        
    print(
    """
    Trips dataset loaded!
    ---
      Size: {s}
      Format: {f}
      Tables loaded: {ds}
      Number of trips (dataset rows): {tc:,}
    """.format(s = size, f = data_format, ds = filenames, tc = trips_df.count()))
    
    return trips_df

# helper function to print dataset row count
def print_count(df):
    print("Row count: {t:,}".format(t = df.count()))

def pipeline(trips_df, with_task_12 = False, zones_df = zone_names):
    # Do not edit
    #---

    ## Task 1.1
    _trips_11 = t11_remove_zeros(trips_df)

    ## Task 1.2
    if with_task_12:
        _trips_12 = t12_remove_outliers(_trips_11)
    else:
        _trips_12 = _trips_11

    ## Task 2.1
    _trips_21 = t21_join_zones(_trips_12, zones_df = zone_names)

    ## Task 2.2
    _trips_22 = t22_calc_profit(_trips_21)

    ## Task 3.1
    _graph = t31_summarise_trips(_trips_22)

    ## Task 3.2
    _zones = t32_summarise_zones_pairs(_graph)

    _top10_trips     = t32_top10_trips(_zones)
    _top10_profit    = t32_top10_profit(_zones)
    _top10_passenger = t32_top10_passenger(_zones)
    
    return([_top10_trips, _top10_profit, _top10_passenger])

# COMMAND ----------

# CHANGE the value of argument 'size' to record the pipeline execution times for increasing dataset sizes
SIZE = 'S'
DATA_FORMAT = 'parquet'

# Load trips dataset
trips = init_trips(SIZE, DATA_FORMAT)

# uncomment line only for small datasets
# trips.take(1)

# COMMAND ----------

print_count(trips)

# COMMAND ----------

# dataset schemas
trips.printSchema()

# COMMAND ----------

display(trips[['PULocationID', 'DOLocationID', 'trip_distance', 'passenger_count', 'total_amount']].take(5))

# COMMAND ----------

zone_names.printSchema()

# COMMAND ----------

display(zone_names.take(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Filter rows
# MAGIC 
# MAGIC **Input:** trips dataset
# MAGIC 
# MAGIC ### Task 1.1 - Remove "0 distance" and 'no passengers' records
# MAGIC 
# MAGIC Remove dataset rows that represent invalid trips:
# MAGIC 
# MAGIC - Trips where `trip_distance == 0` (no distance travelled)
# MAGIC - Trips where `passenger_count == 0` and `total_amount == 0` (we want to retain records where `total_amount` > 0 - these may be significant as the taxi may have carried some parcel, for example)
# MAGIC 
# MAGIC Altogether, a record is removed if it satisfies the following conditions:
# MAGIC 
# MAGIC `trip_distance == 0` or `(passenger_count == 0` and `total_amount == 0)`.
# MAGIC 
# MAGIC **Recommended:** Select only the relevant dataset columns for this and subsequent tasks: `['PULocationID', 'DOLocationID', 'trip_distance', 'passenger_count', 'total_amount')]`
# MAGIC 
# MAGIC ### Task 1.2 - Remove outliers using the modified z-score
# MAGIC 
# MAGIC Despite having removed spurious "zero passengers" trips in task 1.1, columns `total_amount` and `trip_distance` contain additional outlier values that must be identified and removed.
# MAGIC 
# MAGIC To identify and remove outliers, you will use the modified [z-score](https://en.wikipedia.org/wiki/Standard_score) method.
# MAGIC The modified z-score uses the median and [Median Absolute Deviation](https://en.wikipedia.org/wiki/Median_absolute_deviation) (MAD), instead of the mean and standard deviation, to determine how far an observation (indexed by i) is from the mean:
# MAGIC 
# MAGIC $$z_i = \frac{x_i - \mathit{median}(\mathbf{x})}{\mathbf{MAD}},$$
# MAGIC 
# MAGIC where x represents the input vector, xi is an element of x and zi is its corresponding z-score. In turn, the MAD formula is:
# MAGIC 
# MAGIC $$\mathbf{MAD} = 1.483 * \mathit{median}(\big\lvert x_i - \mathit{median}(\mathbf{x})\big\rvert).$$
# MAGIC 
# MAGIC Observations with **high** (absolute) z-score are considered outlier observations. A score is considered **high** if its __absolute z-score__ is larger than a threshold T = 3.5:
# MAGIC 
# MAGIC $$\big\lvert z_i \big\rvert > 3.5.$$
# MAGIC 
# MAGIC where T represents the number of unit standard deviations beyond which a score is considered an outlier ([wiki](https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule)).
# MAGIC 
# MAGIC This process is repeated twice, once for each of the columns `total_amount` and `trip_distance` (in any order).
# MAGIC 
# MAGIC **Important:** Use the surrogate function [`percentile_approx`](https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.functions.percentile_approx.html?highlight=percentile#pyspark.sql.functions.percentile_approx) to estimate the median (calculating the median values for a column is expensive as it cannot be parallelised efficiently).

# COMMAND ----------

# develop your solution here (create/destroy cells as needed) and then implement it in the functions below

# COMMAND ----------

# Your solution implementation to task 1.1 goes HERE
def t11_remove_zeros(df):
    # input: trips dataset
    return df

# COMMAND ----------

# execute task 1.1
trips_11 = t11_remove_zeros(trips)

print_count(trips_11)

## uncomment only for smaller datasets
# display(trips_11.take(10))

# COMMAND ----------

# Your solution implementation to task 1.2 goes HERE
def t12_remove_outliers(df):
    return df

# COMMAND ----------

# execute task 1.2
trips_12 = t12_remove_outliers(trips_11)

print_count(trips_12)
# display(trips_12.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Compute new columns
# MAGIC 
# MAGIC ### Task 2.1 - Zone names
# MAGIC 
# MAGIC Obtain the **start** and **end** zone names of each trip by joining the `trips` and `zone_names` datasets (i.e. by using the `zone_names` dataset as lookup table).
# MAGIC 
# MAGIC **Note:** The columns containing the start and end zone ids of each trip are named `PULocationID` and `DOLocationID`, respectively.
# MAGIC 
# MAGIC ### Task 2.2 - Unit profitability
# MAGIC 
# MAGIC Compute the column `unit_profitabilty = total_amount / trip_distance`.

# COMMAND ----------

# develop your solution here (create/destroy cells as needed) and then implement it in the functions below

# COMMAND ----------

# Your solution implementation to task 2.1 goes HERE
def t21_join_zones(df, zones_df = zone_names):
    # input: output of task 1.2 and zone_names dataset
    return df

# COMMAND ----------

# execute task 2.1
trips_21 = t21_join_zones(trips_12, zones_df = zone_names)

print_count(trips_21)
# display(trips_21.take(10))

# COMMAND ----------

# Your solution implementation to task 2.2 goes HERE
def t22_calc_profit(df):
    # input: output of task 2.1
    return df

# COMMAND ----------

# execute task 2.2
trips_22 = t22_calc_profit(trips_21)

print_count(trips_22)
# display(trips_22.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Rank zones by traffic, passenger volume and profitability
# MAGIC 
# MAGIC ### 3.1 - Summarise interzonal travel
# MAGIC 
# MAGIC Build a graph data structure of zone-to-zone traffic, representing aggregated data about trips between any two zones. The graph will have one node for each zone and one edge connecting each pair of zones. In addition, edges contain aggregate information about all trips between those zones. 
# MAGIC 
# MAGIC For example, zones Z1 and Z2 are connected by *two* edges: edge Z1 --> Z2 carries aggregate data about all trips that originated in Z1 and ended in Z2, and edge Z2 --> Z2 carries aggregate data about all trips that originated in Z2 and ended in Z1.
# MAGIC 
# MAGIC The aggregate information of interzonal travel must include the following data:
# MAGIC 
# MAGIC - `average_unit_profit` - the average unit profitability (calculated as `mean(unit_profitabilty)`).
# MAGIC - `trips_count` -- the total number of recorded trips.
# MAGIC - `total_passengers` -- the total number of passenger across all trips (sum of `passenger_count`).
# MAGIC 
# MAGIC This graph can be represented as a new dataframe, with schema:
# MAGIC 
# MAGIC \[`PULocationID`, `DOLocationID`, `average_unit_profit`, `trips_count`, `total_passengers` \]
# MAGIC 
# MAGIC __hint__: the `groupby()` operator produces a `pyspark.sql.GroupedData` structure. You can then calculate multiple aggregations from this using `pyspark.sql.GroupedData.agg()`: 
# MAGIC - https://spark.apache.org/docs/3.2.0/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.groupby.html
# MAGIC - https://spark.apache.org/docs/3.2.0/api/python/reference/api/pyspark.sql.GroupedData.agg.html
# MAGIC 
# MAGIC ### Task 3.2 - Obtain top-10 zones
# MAGIC 
# MAGIC For each of the following measures, report the top-10 zones _using their plain names you dereferenced in the previous step, not the codes_. Note that this requires ranking the nodes in different orders. Specifically, you need to calculate the following further aggregations:
# MAGIC 
# MAGIC - the **total** number of trips originating from Z. This is simply the sum of `trips_count` over all outgoing edges for Z, i.e., edges of the form Z -> \*
# MAGIC - the **average** profitability of a zone. This is the average of all `average_unit_profit` over all *outgoing* edges from Z.
# MAGIC - The **total** passenger volume measured as the **sum** of `total_passengers` carried in trips that originate from Z

# COMMAND ----------

# develop your solution here (create/destroy cells as needed) and then implement it in the functions below

# COMMAND ----------

## Your solution to task 3.1 goes HERE
def t31_summarise_trips(df):
    # input: output of task 2.2
    return df

# COMMAND ----------

# execute task 3.1
graph = t31_summarise_trips(trips_22)

print_count(graph)
# display(graph.take(10))

# COMMAND ----------

# Your solution to task 3.2 goes HERE (implement each of the functions below)
def t32_summarise_zones_pairs(df, zones_df = zone_names):
    return df

# Top 10 ranked zones by traffic (trip volume)
def t32_top10_trips(df_zones):
    # input: output of task 3.2
    return None

# Top 10 ranked zones by profit
def t32_top10_profit(df_zones):
    # input: output of task 3.2
    return None

# Top 10 ranked zones by passenger volume
def t32_top10_passenger(df_zones):
    # input: output of task 3.2
    return None

# COMMAND ----------

# execute task 3.2
zones = t32_summarise_zones_pairs(graph)

top10_trips     = t32_top10_trips(zones)
top10_profit    = t32_top10_profit(zones)
top10_passenger = t32_top10_passenger(zones)

# COMMAND ----------

# use 'display()' or return a pandas DataFrame for 'pretty' output
top10_trips

# COMMAND ----------

# use 'display()' return a pandas DataFrame for 'pretty' output
top10_profit

# COMMAND ----------

# use 'display()' or return a pandas DataFrame for 'pretty' output
top10_passenger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - Record the pipeline's execution time
# MAGIC 
# MAGIC Record the execution time of:
# MAGIC 
# MAGIC 1. the whole pipeline
# MAGIC 2. the whole pipeline except task 1.2
# MAGIC 
# MAGIC on the two tables below, for all dataset sizes: `'S'`, `'M'`, `'L'`, `'XL'`, `'XXL'`, and data formats: `parquet` and `delta`.
# MAGIC 
# MAGIC Analyse the resulting execution times and comment on the effect of dataset size, dataset format and task complexity (with and without task 1.2) on pipeline performance.

# COMMAND ----------

# after developing your solution, it may be convenient to combine all your functions in a single cell (at the start or end of the notebook)

# CHANGE the value of the following arguments to record the pipeline execution times for increasing dataset sizes
SIZE = 'S'
DATA_FORMAT = 'parquet'
WITH_TASK_12 = True

# Load trips dataset
trips = init_trips(SIZE, DATA_FORMAT)

# COMMAND ----------

# run and record the resulting execution time shown by databricks (on the cell footer)

# IMPORTANT: this function calls all task functions in order of occurrence. For this code to run without errors, you have to load into memory all of the previous task-specific functions, even if you haven't implemented these yet.
pipeline(trips, with_task_12 = WITH_TASK_12)

# COMMAND ----------

# MAGIC %md
# MAGIC _Table 1. Pipeline performance for `parquet` format._
# MAGIC 
# MAGIC | metric                      | S    | M    | L    | XL   | XXL  |
# MAGIC |-----------------------------|------|------|------|------|------|
# MAGIC | rows (M)                    |  000 |  000 |  000 |  000 |  000 |
# MAGIC | execution time   (w/o 1.2)  | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
# MAGIC | execution time              | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
# MAGIC | sec / 1M records (w/o 1.2)  | 0.0  | 0.0  | 0.0  | 0.0  | 0.0  |
# MAGIC | sec / 1M records            | 0.0  | 0.0  | 0.0  | 0.0  | 0.0  |

# COMMAND ----------

# MAGIC %md
# MAGIC _Table 2. Pipeline performance for `delta` format._
# MAGIC 
# MAGIC | metric                      | S    | M    | L    | XL   | XXL  |
# MAGIC |-----------------------------|------|------|------|------|------|
# MAGIC | rows (M)                    |  000 |  000 |  000 |  000 |  000 |
# MAGIC | execution time   (w/o 1.2)  | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
# MAGIC | execution time              | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
# MAGIC | sec / 1M records (w/o 1.2)  | 0.0  | 0.0  | 0.0  | 0.0  | 0.0  |
# MAGIC | sec / 1M records            | 0.0  | 0.0  | 0.0  | 0.0  | 0.0  |
