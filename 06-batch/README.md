## Question 1: Install Spark and PySpark

```
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').getOrCreate()
spark.version
```

result '4.1.1'

## Question 2: Yellow November 2025

```
partitioned_df = df.partition(4)
partitioned_df.write.parquet(path)
```
this generates 4 files each with 24 MB
option b

## Question 3: Count records

```
from pyspark.sql import functions as F
df.filter(F.to_date("tpep_pickup_datetime") == "2025-11-15").count()
```

result: 162604 
option c

## Question 4: Longest trip

```
df = df.withColumn("trip_length", df.tpep_dropoff_datetime - df.tpep_pickup_datetime)
df.agg(F.max("trip_length")).show()
```
This shows interval in days INTERVAL 3 18:38 which equals 90 hour
option c

## Question 5: User Interface
by default it runs on port 4040 but since my port 4040 was full it automatically started on 4041
option c

## Question 6: Least frequent pickup location zone

```
zone_df = spark.read.csv(path, header=True)
joined_df = df.join(zone_df, df.PULocationID == zone_df.LocationID)
joined_df.groupBy(Zone).count().sort('count').show()
```

This shows 3 pickup zones with only one time use
+--------------------+-----+
|                Zone|count|
+--------------------+-----+
|Governor's Island...|    1|
|Eltingville/Annad...|    1|
|       Arden Heights|    1|
+--------------------+-----+
options a and b are correct
