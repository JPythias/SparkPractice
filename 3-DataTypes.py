# JawnPythias
# date:02/03/2024

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

schema = StructType([
    StructField('Id', IntegerType(), True),
    StructField('First', StringType(), True),
    StructField('Last', StringType(), True),
    StructField('Url', StringType(), True),
    StructField('Published', StringType(), True),
    StructField('Hits', LongType(), True),
    StructField('Campaigns', ArrayType(StringType()), True),
])

# 手写方式定义
df = spark.read\
    .schema(schema)\
    .json('dataset/blogs.txt')

df.printSchema()
df.show()

# 自动推断类型
# 绝大多数时间不会使用
df = spark.read\
    .option('infoSchema', True)\
    .json('dataset/blogs.txt')

df.printSchema()