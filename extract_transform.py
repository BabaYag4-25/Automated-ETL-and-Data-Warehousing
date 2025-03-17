'''
=================================================
This program is created to extract the raw dataset and feed it into the transform function to clean the dataset according to the needs.
=================================================
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

file_path = '/opt/airflow/dags/P2M3_Ahmad-Ghifari_data_raw.csv'

def extract(file_path):
    
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    return data

'''
  This function is intended to retrieve raw data for further data cleaning.

  Parameters:
 file_path: string - raw data location
 spark: spark session creation
 data: read csv raw data

 Return
 data: list of data in csv raw data
'''

data = extract(file_path)

def transform(data):
    for x in data.columns:
        data = data.withColumnRenamed(x, x.strip())

    data = data.withColumn("Date",to_timestamp(col("Date"), "dd/MM/yyyy"))
    
    numerik_df = data[['Units Sold', 'Manufacturing Price', 'Sale Price', 'Gross Sales', 'Discounts','Sales', 'COGS', 'Profit']]
    for y in numerik_df.columns:
        data = data.withColumn(y, regexp_replace(col(y), r'\$', ''))
        data = data.withColumn(y, regexp_replace(col(y), r'-', '0'))
        data = data.withColumn(y, regexp_replace(col(y), r',', ''))
        data = data.withColumn(y, regexp_replace(col(y), r'[\(\)]', ''))
        data = data.withColumn(y, col(y).cast("float"))
    
    for z in data.columns:
        nama_baru = z.lower().replace(" ", "_")
        data = data.withColumnRenamed(z, nama_baru)
        
    return data

'''
  This function is intended to perform Data Cleaning.

  Explanation:
 data: string - calls def extract
 looping x: removes whitespaces in the names of all columns
 to_timestamp: changes the data type of column 'date' from object to timestamp
 numeric_df: makes it easy to pick up any column you want to clean
 looping y: clean up the columns in 'numeric_df' by removing '$', '-', ',', '()' and changing the data type to 'float'
 looping z: change all column names to lowercase and change spaces to underscore('_')

 Return
 data: list of cleaned data
'''

transform(data).write.csv('/opt/airflow/dags/cleaned', header=True, mode='overwrite')
