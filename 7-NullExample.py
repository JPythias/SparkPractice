# JawnPythias
# date:03/03/2024

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

spark.read\
    .option('header', True)\
    .csv('dataset/beijingpm_with_nan.csv')

# 1.drop
