# Databricks notebook source
import os
from pyspark.sql.functions import max

# COMMAND ----------

print('asdfas')

# COMMAND ----------

def get_max_id(table_path_list):
    # Initialize the max id to -1
    max_id = -1

    for table_path in table_path_list:
        try:
            df = spark.read.format('delta').option('header', 'true').load(table_path)
        except:
            print(f'{table_path} is not a delta table! This file is skipped.')
            continue

        max_id_temp = df.select(max("id")).first()[0]

        if max_id_temp > max_id:
            max_id = max_id_temp
    
    return max_id

# COMMAND ----------

table_path_list = [CustomisedPath('CI/data_lakehouse/bronze/crowdsource data 2021_2022/CSMD_MNSI_May2021_May2022/'),
                   CustomisedPath('CI/data_lakehouse/bronze/crowdsource data 2023/CSMD_MNSI_Oct2022_Dec2023/')]

# Initialize the max id to -1
max_id = -1
for table_path in table_path_list:
    try:
        df = spark.read.format('delta').option('header', 'true').load(table_path)
    except:
        print(f'{table_path} is not a delta table! This file is skipped.')
        continue

    max_id_temp = df.select(max("id")).first()[0]

    print(max_id_temp)
    if max_id_temp > max_id:
        max_id = max_id_temp
    
    
max_id

# COMMAND ----------

'a' if 3>2 else 'c'

# COMMAND ----------

table = spark.read.option('recursiveFileLookup', 'true').load(CustomisedPath("CI/data_lakehouse/bronze/crowdsource data 2021_2022/"))

# COMMAND ----------

CustomisedPath("CI/data_lakehouse/bronze/crowdsource data 2021_2022/")

# COMMAND ----------


