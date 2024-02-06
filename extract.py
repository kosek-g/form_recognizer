# Databricks notebook source
# Imports

import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Run config notebooks

# COMMAND ----------

# MAGIC %run ../form_recognizer/utils/adls_utils

# COMMAND ----------

# MAGIC %run ../form_recognizer/utils/form_rec_utils

# COMMAND ----------

# create Adls object for connection and interaction with Azure Data Lake Storage

adls = Adls('avainternalformreco', 'de-form-recognizer')
adls.configure_connection()

#get list of files to process
raw_files = adls.get_unprocessed_files()

# COMMAND ----------

# create FormRecognizer object for connection and interaction with Form Recognizer and its trained model

form_recognizer = FormRecognizer()

# COMMAND ----------

# create list of images urls for Form Recognizer

images_urls = images_urls = [f'{adls.dir_unstructured}{file}{adls.token}' for file in raw_files]


# COMMAND ----------

#Get data from the Form Recognizer, prepere them and load into DataFrame

documents_data = []

for image in images_urls:
    image_data = form_recognizer.extract_data(image)

    row ={}

    for idx, document in enumerate(image_data.documents):
        for name, field in document.fields.items():
            field_value = field.value if field.value else field.content
            if name == 'date' and isinstance(field_value, datetime.date) == False:
                data_dict = {name: None}
                row.update(data_dict)
            else:
                data_dict = {name: field_value}
                row.update(data_dict)

    row = dict(sorted(row.items()))
    documents_data.append(row)

# create DataFrame
try:
    df = spark.createDataFrame(data=documents_data)
except ValueError:
    print('No files to process in "unstructured" directory')

# COMMAND ----------

# Create separate DFs for each Airline and save them in proper folders in 'Extracted' dir

df_airlines = df.select('airline').distinct()
airlines_list = df_airlines.rdd.flatMap(lambda x: x).collect()

for airline in airlines_list:
    df_filter = df.filter(
                           col('airline') == airline
                           )
    #reorder columns
    df_filter = df_filter.select('airline', 'name', 'from', 'to', 'date')

    #write to proper folder
    adls.extracted_write_to_csv(df_filter, airline)

# COMMAND ----------

# move processed files to 'processed' dir

adls.move_raw_files_to_processed_dir(raw_files)
