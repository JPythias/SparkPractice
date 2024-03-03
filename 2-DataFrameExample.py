# JawnPythias
# date:02/03/2024

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

# 创建RDD sparkSession : Spark SQL    sparkContext : RDD的入口
# 1.通过内存创建dataframe
# 一般构建测试数据
rdd = spark.sparkContext.parallelize([('tom', 2), ('jerry', 1)])
df = rdd.toDF(['name', 'age'])

df.printSchema()# 打印schema表格
df.show()# 展示数据（头20条数据）

# 2.读数据创建，数据不一定全在内存（Hadoop/Hive/MySQL）
df = spark.read\
    .option('header', True)\
    .csv('dataset/BeijingPM20100101_20151231.csv')
# read中有多种不同的读取数据格式
# 设置读取表头的部分为真

df.printSchema()# 打印schema表格
df.show()# 展示数据（头20条数据）

# 3. select
result = df.select('No', 'year', 'month', 'day', 'PM_Dongsi')

# 4.group-by
result = df.select('year', 'month', 'PM_Dongsi')\
    .where(col('PM_Dongsi') != 'NA')\
    .groupby('year')\
    .count()

result.show()