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

lines_1.take(20)

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

uppercase = lines_1.map(lambda x : x.upper())

# COMMAND ----------

uppercase.take(5)

# COMMAND ----------

lines_1.first()

# COMMAND ----------

lines_1.filter(lambda x: "inferno" in x).map(lambda x: x.upper()).take(5)

# COMMAND ----------

lines_1.filter(lambda x: "inferno" in x).map(lambda x: x.replace('\x92','\'')).take(5)

# COMMAND ----------

uppercase.count()

# COMMAND ----------

movie = sc.textFile('/FileStore/tables/sample_movielens_movies.txt')

# COMMAND ----------

movie.take(5)

# COMMAND ----------

thriller = movie.filter(lambda x : "Thriller" in x)
comedy  = movie.filter(lambda x : "Comedy" in x)

# COMMAND ----------

#union of two rdd
thriller_and_comedy = thriller.union(comedy)
thriller_and_comedy.take(5)

# COMMAND ----------

thriller_and_comedy.count()

# COMMAND ----------

comedy.count()

# COMMAND ----------

x = comedy.map(lambda x: x.split("::"))


# COMMAND ----------

comdeyGenres = x.map(lambda x: x[2])
comdeyGenres.take(5)

# COMMAND ----------

comdeyGenres.distinct().count()

# COMMAND ----------

comdeyGenres.count()

# COMMAND ----------

thrillerGenres = thriller.map(lambda x:x.split("::")).map(lambda l:l[2])
thrillerGenres.distinct().collect()

# COMMAND ----------

genre_intersection = thrillerGenres.intersection(comdeyGenres)

# COMMAND ----------

genre_intersection.count()

# COMMAND ----------

#computing average
num = sc.parallelize([2,3,4,5])
pairs = num.map(lambda x: (x,1))


# COMMAND ----------

pairs.collect()

# COMMAND ----------

sum,coun = pairs.reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]))

# COMMAND ----------

sum/float(coun)

# COMMAND ----------

z = sc.parallelize([1,2,3,4,5,6], 2)

# COMMAND ----------

y = z.aggregate(0,(lambda acc,value:max(acc,value)),(lambda acc1,acc2:acc1+acc2))

# COMMAND ----------

y

# COMMAND ----------


