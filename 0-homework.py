# JawnPythias
# date:03/03/2024

# �𾯺��з���
'''
2018����ĸ��·�����ߵĻ�
San Francisco���ĸ�neighborhood��2018�귢���Ļ��ִ�����ࣿ
San Francisco���ĸ�neighborhood��2018����Ӧ������
2018�����һ�ܵĻ𾯴������
���ݼ�������ֵ֮���й�����correlation����
ʵ��ʹ��parquest�洢����ȡ
'''

# 1.�������ݼ�
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