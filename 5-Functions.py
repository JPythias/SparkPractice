# JawnPythias
# date:03/03/2024

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import Row

spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

# 1.map
# RDD里的函数

rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
result = rdd.map(lambda x : x * x)

# collect rdd -> list
print(result.collect())

# 2.flatMap
# Hello World 单词计数

# 3.filter
rdd = spark.sparkContext.parallelize([('tom', 20), ('jack', 18)])
df = rdd.toDF(['name', 'age'])

df.filter(col('age') < 20).show()
df.where(col('age') < 18).show()

# 4.randomSplit
# randomSplit 会按照传入的权重随机将一个 Dataset 分为多个 Dataset, 传入 randomSplit 的数组有多少个权重, 最终就会生成多少个 Dataset, 这些权重的加倍和应该为 1, 否则将被标准化
rdd = spark.sparkContext.parallelize([(i, 'fake') for i in range(1, 100)])

print(rdd.collect())
df = rdd.toDF(['num', 'data'])

dfs = df.randomSplit([0.7, 0.3])
print(dfs[0].count())
print(dfs[1].count())

# 5.sample
# sample会在dataset中随机抽样

# 6.orderBy
rdd = spark.sparkContext.parallelize([("jayChou", 41), ("burukeyou", 23)])
df = spark.createDataFrame(rdd.map(lambda row: Row(name=row[0], age=row[1])))

df.orderBy(col('name').desc()).show()
df.orderBy(col('name').asc()).show()

df.sort(col('name').desc()).show()

# 7.去重
rdd = spark.sparkContext.parallelize([("jayChou", 41), ("burukeyou", 23), ("burukeyouClone", 23)])
df = spark.createDataFrame(rdd.map(lambda row: Row(name=row[0], age=row[1])))
df.dropDuplicates(['age']).show()
df.dropDuplicates(['name', 'age']).show()

# distinct
rdd = spark.sparkContext.parallelize([('tom', 20), ('jack', 18), ('tom', 21)])
df = rdd.toDF(['name', 'age'])
# df.select('tom').distinct().show()

# limit 限制结果集数量
rdd = spark.sparkContext.parallelize([('tom', 20), ('jack', 18), ('tom', 21)])
df = rdd.toDF(['name', 'age'])
df.limit(1).show()

