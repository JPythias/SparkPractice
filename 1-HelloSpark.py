# JawnPythias
# date:02/03/2024

# 1.拿到Spark入口对象 SparkSession
# local: 在本地内存运行
from pyspark.sql import SparkSession

# master是session的入口，可以是本地、yarn或者mesos
spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

# 2.提交大数据分析任务
rdd = spark.sparkContext.parallelize([('tom', 20), ('jack', 40)])
df = rdd.toDF(['name', 'age'])

print(df.count())