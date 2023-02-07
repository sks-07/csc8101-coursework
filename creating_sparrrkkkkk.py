# Databricks notebook source
"""
for python the spark content is a built in variable.
"""
sc 

# COMMAND ----------

dir(sc)

# COMMAND ----------

"""

"""

# COMMAND ----------

!ls


# COMMAND ----------

!cd 

# COMMAND ----------

df = spark.read.parquet("/FileStore/tables/my_taxis/yellow_tripdata_2022_01.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

# checking the number of partitions in rdd
print(df.rdd.getNumPartitions())


# COMMAND ----------

#repartitions of rdd
new_df = df.repartition(3)
print(new_df.rdd.getNumPartitions())

# COMMAND ----------

"""
select a particular column sql.dataframe object have colRegex method
"""
col1=df.select(df.colRegex("tpep_pickup_datetime"))
display(col1)

# COMMAND ----------

df.collect()

# COMMAND ----------

type(df)

# COMMAND ----------

dir(df)

# COMMAND ----------

"""
parallelize create multiple partions of an iterable
"""
data = sc.parallelize(list(range(10)),5)
print(data.glom().collect())

# COMMAND ----------

help(sc.parallelize)

# COMMAND ----------

print(type(data))

# COMMAND ----------

print(data.getNumPartitions())

# COMMAND ----------

data.collect()

# COMMAND ----------

text_df = sc.textFile('/FileStore/tables/Dante_Inferno.txt')

# COMMAND ----------

text_df.getNumPartitions()

# COMMAND ----------

def len_check(dff):
    print(f'total len: {len(dff.collect())}')
    for i,k in enumerate(dff.glom().collect()):
        print(f'length parition {i}: {len(k)}')

        
len_check(text_df)
#changing the paritions 
repart_df = text_df.repartition(5)
len_check(repart_df)

# print(len(text_df.collect()))
# print(f'first partition :{len(text_df.glom().collect()[0])}')
# print(f'first partition :{len(text_df.glom().collect()[1])}')

# COMMAND ----------

lines_1  = text_df.filter(lambda x: "inferno" in x)
lines_2  = repart_df.filter(lambda x: "inferno" in x)


# COMMAND ----------

print(lines_1.count()) #91 ms

# COMMAND ----------

print(lines_2.count()) #66 ms 

# COMMAND ----------

lines_1.collect()

# COMMAND ----------

repart_df = text_df.repartition(6)
len_check(repart_df)


# COMMAND ----------

lines_3 =repart_df.filter(lambda x: "inferno" in x)
print(lines_3.count())#56 ms with 6 paritions

# COMMAND ----------


