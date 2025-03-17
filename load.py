'''
=================================================
This program is made to load data from data that has been extracted and transformed previously to mongoDB.
=================================================
'''

from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

client = MongoClient("yourMongo_URI")
client.list_database_names()

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('/opt/airflow/dags/cleaned', header=True, inferSchema=True)

df = df.toPandas()

document_list = df.to_dict(orient='records')
client['yourDatabaseName']['YourCollectionName'].insert_many(document_list)