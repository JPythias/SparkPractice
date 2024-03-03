# JawnPythias
# date:03/03/2024

# python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

# Driver
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('HelloSpark') \
    .getOrCreate()

fire_schema = StructType([StructField("CallNumber", IntegerType(), True),
                          StructField("UnitID", StringType(), True),
                          StructField("IncidentNumber", IntegerType(), True),
                          StructField("CallType", StringType(), True),
                          StructField("CallDate", StringType(), True),
                          StructField("WatchDate", StringType(), True),
                          StructField("CallFinalDisposition", StringType(), True),
                          StructField("AvailableDtTm", StringType(), True),
                          StructField("Address", StringType(), True),
                          StructField("City", StringType(), True),
                          StructField("Zipcode", IntegerType(), True),
                          StructField("Battalion", StringType(), True),
                          StructField("StationArea", StringType(), True),
                          StructField("Box", StringType(), True),
                          StructField("OriginalPriority", StringType(), True),
                          StructField("Priority", StringType(), True),
                          StructField("FinalPriority", IntegerType(), True),
                          StructField("ALSUnit", BooleanType(), True),
                          StructField("CallTypeGroup", StringType(), True),
                          StructField("NumAlarms", IntegerType(), True),
                          StructField("UnitType", StringType(), True),
                          StructField("UnitSequenceInCallDispatch", IntegerType(), True),
                          StructField("FirePreventionDistrict", StringType(), True),
                          StructField("SupervisorDistrict", StringType(), True),
                          StructField("Neighborhood", StringType(), True),
                          StructField("Location", StringType(), True),
                          StructField("RowID", StringType(), True),
                          StructField("Delay", FloatType(), True)
                          ]
                         )

df = spark.read.option('header', True).schema(fire_schema).csv('dataset/sf-fire-calls.txt')

# 1. 过滤CallType == 'Medical Incident', 并只打印"IncidentNumber", "AvailableDtTm", "CallType" 三个字段
df.select('IncidentNumber', 'AvailableDtTm', 'CallType') \
    .where("CallType == 'Medical Incident'") \
    .show(truncate=False)

# 2. 过滤掉CallType为空的数据，并统计唯一CallType的个数
df.select('CallType') \
    .where(col('CallType').isNotNull()) \
    .agg(countDistinct('CallType')) \
    .show(truncate=False)

# 3.过滤掉CallType为空的数据，显示所有的CallType并去重
df.select('CallType') \
    .where(col('CallType').isNotNull()) \
    .distinct().show(truncate=False)

# 4.重命名Delay为ResponseDelayedinMins，并过滤出延误大于5分钟的记录，只打印ResponseDelayedinMins字段
df.withColumnRenamed('Delay', 'ResponseDelayedinMins')\
    .filter(col('ResponseDelayedinMins') > 5)\
    .select('ResponseDelayedinMins').show()

# 5.转换IncidentDate、OnWatchDate、AvailableDtTS为日期格式，并删除掉行AvailableDtTm
clean_df = df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yy'))\
    .drop('CallDate')

# 6.显示所有有事故的年份，并按照年份从小到到排序
clean_df.withColumn('year', year('IncidentDate'))\
    .select('year')\
    .distinct()\
    .orderBy(col('year').desc())\
    .show()

# 7.过滤CallType为空的记录，并统计每种CallType的类型总数并按照顺序倒排
df.where(col('CallType').isNotNull())\
    .groupby('CallType')\
    .agg(count('CallType').alias('count'))\
    .orderBy(col('count').desc()).show(truncate=False)
