# Databricks notebook source
from azure.storage.blob import ContainerClient
from pyspark.sql import DataFrame
from datetime import date
from typing import List
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

class Adls:
    '''
    Abstraction for interacting with ADLS
    '''
    TODAY = date.today()

    def __init__(self, storage_name: str, form_reco_container: str):
        self.token = os.environ.get("TOKEN")
        self.storage_name = storage_name
        self.form_reco_container = form_reco_container
        self.abfss_path = f'abfss://{self.form_reco_container}@{self.storage_name}.dfs.core.windows.net'
        self.dir_unstructured = f'https://{self.storage_name}.blob.core.windows.net/{self.form_reco_container}/unstructured/'
        self.dir_unstructured_abfss = f'{self.abfss_path}/unstructured/'
        self.dir_extracted = f'{self.abfss_path}/extracted/'
        self.dir_aggregated = f'{self.abfss_path}/aggregated/'
        self.dir_processed = f'{self.abfss_path}/processed'


    def configure_connection(self):
        """Creates connection to ADLS """
        spark.conf.set(f"fs.azure.account.auth.type.{self.storage_name}.dfs.core.windows.net", "SAS")
        spark.conf.set(f"fs.azure.sas.token.provider.type.{self.storage_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
        spark.conf.set(f"fs.azure.sas.fixed.token.{self.storage_name}.dfs.core.windows.net", self.token)

        #disable Delta format
        spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")


    def get_unprocessed_files(self):
        """Returns lists of unprocessed tickets from 'unstructered' dir """
        container_url = f'https://{self.storage_name}.blob.core.windows.net/{self.form_reco_container}{self.token}'

        container = ContainerClient.from_container_url(container_url)

        # create a list with relevant file names
        tickets_blobs = []

        for blob in container.list_blobs():
            ticket = blob.name[-11:]
            if blob.name[0:13] == 'unstructured/':
                tickets_blobs.append(ticket)

        return tickets_blobs


    def extracted_write_to_csv(self, df: DataFrame, airline: str):
        '''Saves sererate csv files for each airline in extracted/year/month/day '''
        folder_location = f'{self.dir_extracted}/{Adls.TODAY.year}/{Adls.TODAY.month}/{Adls.TODAY.day}/{airline}'
        df.repartition(1).write.save(path= folder_location, format='csv', mode='overwrite', header = 'True')


    def move_raw_files_to_processed_dir(self, list_of_files: List):
        '''Moves raw files from unstructered dir to "processed" '''
        for file in list_of_files:
            dbutils.fs.mv(f'{self.dir_unstructured_abfss}/{file}',
                          f'{self.dir_processed}/{Adls.TODAY.year}/{Adls.TODAY.month}/{Adls.TODAY.day}/{file}')


    def load_extracted_csv(self):
        '''Loads multiple CSV files from extracted directory to one DataFrame'''
        path = f'abfss://{self.form_reco_container}@{self.storage_name}.dfs.core.windows.net/extracted/'

        schema = StructType([ \
                    StructField("airline",StringType(),True), \
                    StructField("name",StringType(),True), \
                    StructField("from",StringType(),True), \
                    StructField("to", StringType(), True), \
                    StructField("date", DateType(), True)  \
                  ])

        df = (
            spark.read
            .schema(schema)
            .option("header", "true")
            .option("recursiveFileLookup","true")
            .csv(path)
        )

        if df.count() == 0:
            print('No records found')
        else:
            print(f'{df.count()} records loaded.')

        return df


    def write_aggregated_csv(self, df:DataFrame, report_type: str):
        '''Writes csv files in relevant folder (airports/airlines) '''
        if report_type == 'airports':
            folder_location = f'{self.dir_aggregated}/airports/{Adls.TODAY.year}/{Adls.TODAY.month}/{Adls.TODAY.day}/'
        elif report_type == 'airlines':
            folder_location = f'{self.dir_aggregated}/airlines/{Adls.TODAY.year}/{Adls.TODAY.month}/{Adls.TODAY.day}/'
        else:
            print('Report type not specyfied or is not correct')
            pass

        df.repartition(1).write.save(path= folder_location, format='csv', mode='overwrite', header = 'True')




# COMMAND ----------


