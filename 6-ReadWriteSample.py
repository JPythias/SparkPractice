# coding=UTF8

# JawnPythias
# date:03/03/2024



from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName("HelloSpark")\
    .master("local")\
    .getOrCreate()

# pure text
# referring to album

# json: JSON data
df = spark.read\
    .option('header', True)\
    .csv('dataset/BeijingPM20100101_20151231.csv')

df = spark.read\
    .option('header', True)\
    .format('csv')\
    .load('dataset/BeijingPM20100101_20151231.csv')
print('a')

# jdbc：database
# referring to album