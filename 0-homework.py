# JawnPythias
# date:03/03/2024

# python
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

# 1.打印2018年份所有的CallType，并去重
# df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yy')) \
#     .drop('CallDate') \
#     .withColumn('year', year('IncidentDate')) \
#     .select('year', 'CallType') \
#     .where("year == '2018'") \
#     .distinct() \
#     .select('CallType').show()

# 2.2018年的哪个月份有最高的火警
# df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yy')) \
#         .withColumn('year', year('IncidentDate'))\
#         .withColumn('month', month("IncidentDate"))\
#         .select('year', 'month')\
#         .where("year == '2018'")\
#         .groupby('month')\
#         .agg(count('month').alias('count'))\
#         .orderBy(col('count').desc()).show(truncate=False)

# 3.San Francisco的哪个neighborhood在2018年发生的火灾次数最多？
# df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yy')) \
#     .withColumn('year', year('IncidentDate'))\
#     .select('year', 'City', 'Neighborhood')\
#     .where("year == '2018'")\
#     .where("City == 'San Francisco'")\
#     .groupby('Neighborhood')\
#     .agg(count('Neighborhood').alias('count'))\
#     .orderBy(col('count').desc()).show(truncate=False)

# 4.San Francisco的哪个neighborhood在2018年响应最慢？
# df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yy')) \
#     .withColumn('year', year('IncidentDate'))\
#     .select('year', 'City', 'Neighborhood', 'Delay')\
#     .where("year == '2018'")\
#     .where("City == 'San Francisco'")\
#     .groupby('Neighborhood')\
#     .agg(sum('Delay').alias('SumDelay'))\
#     .orderBy(col('SumDelay').desc()).show(truncate=False)

# 5.2018年的哪一周的火警次数最多
# df.withColumn('IncidentDate', to_timestamp('CallDate', 'MM/dd/yy')) \
#     .withColumn('year', year('IncidentDate'))\
#     .where("year == '2018'")\
#     .withColumn('WeekOfYear', weekofyear('IncidentDate'))\
#     .select('year', 'WeekOfYear')\
#     .groupby('WeekOfYear')\
#     .agg(count('WeekOfYear').alias('count'))\
#     .orderBy(col('count').desc()).show(truncate=False)

# 6.数据集中任意值之间有关联（correlation）吗？
# cor_df = df.select('CallNumber', 'IncidentNumber', 'Zipcode', 'FinalPriority',
#                    'NumAlarms', 'UnitSequenceInCallDispatch', 'Delay')
# cor_list = list()
# cor_list.append(cor_df.stat.corr('UnitSequenceInCallDispatch', 'Delay'))
# cor_list.append(cor_df.stat.corr('FinalPriority', 'Delay'))
# cor_list.append(cor_df.stat.corr('UnitSequenceInCallDispatch', 'NumAlarms'))
# cor_list.append(cor_df.stat.corr('IncidentNumber', 'Delay'))
# cor_list.append(cor_df.stat.corr('IncidentNumber', 'Zipcode'))
# print(cor_list)

# 7.实现使用parquest存储并读取
text_df = spark.read.text("dataset/sf-fire-calls.txt")
processed_df = text_df.selectExpr("split(value, ',') as data").select("data")
processed_df.write.parquet("dataset/output.parquest")
spark.stop()