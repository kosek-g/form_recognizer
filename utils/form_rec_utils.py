# Databricks notebook source
import os
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient

class FormRecognizer:

    '''
    Abstraction for interacting with Form Recognizer and its trained model
    '''

    def __init__(self):
        self.endpoint = os.environ.get("FORM_RECOGNIZER_ENDPOINT")
        self.key = os.environ.get("API_KEY")
        self.model_id = "airline_ticket"


    def extract_data(self, image_url: str):
        document_analysis_client = DocumentAnalysisClient(
                                    endpoint=self.endpoint, credential=AzureKeyCredential(self.key)
                                    )


        poller = document_analysis_client.begin_analyze_document_from_url(self.model_id, image_url)
        result = poller.result()

        return result

# COMMAND ----------


