# Databricks notebook source
# MAGIC %md
# MAGIC Run config notebooks

# COMMAND ----------

# MAGIC %run ../form_recognizer/utils/adls_utils

# COMMAND ----------

# create Adls object for connection and interaction with Azure Data Lake Storage

adls = Adls('avainternalformreco', 'de-form-recognizer')
adls.configure_connection()


# COMMAND ----------

# load all csv files

df = adls.load_extracted_csv()

# COMMAND ----------

# create CSV files for most popular airports and airlines

#airlines
df_airline = (df
              .groupBy('airline').count()
              .sort(desc("count"))
             )

adls.write_aggregated_csv(df_airline, 'airlines')

#most popular destination
df_airport = (df
              .groupBy('to').count()
              .sort(desc("count"))
              .withColumnRenamed('to', 'airport')
             )

adls.write_aggregated_csv(df_airport, 'airports')

# COMMAND ----------


