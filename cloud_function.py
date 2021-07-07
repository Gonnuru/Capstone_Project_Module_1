# -*- coding: utf-8 -*-
"""
Created on Sun Jul  4 23:49:51 2021

@author: Gonnuru
"""
import os
from base64 import b64decode
import logging
from  pandas import DataFrame
from json import loads
from google.cloud.storage import Client

# take environment variables from .env.
BUCKET_NAME = os.getenv('BUCKET_NAME')

class LoadToStorage:
    def __init__(self,event,context):
        self.context = context
        self.event = event
        self.bucket_name = BUCKET_NAME
    
    def get_message_data(self) -> str:  
        # Gets message from the PubSub topic

        logging.info(
                f"This message was triggered by messageId {self.context.event.id} published at {self.context.timestamp}"
                f"to {self.context.resource['name']}")
        
        if "data" in self.event:
            pubsub_message = b64decode(self.event["data"]).decode('utf-8')
            logging.info(pubsub_message)
            return pubsub_message

        else:
            logging.error("Incorrect Format found")
            return ""
        
    def dataframe_payload_structure(self, message: str)-> DataFrame:
        # Transforms data to support CSV format

        try:
            df = DataFrame(loads(message))
            if not df.empty:
                logging.info(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
                return df
            else:
                logging.warning(f"Created empty DataFrame")
                return df
        
        except Exception as e:
            logging.error(f"Error found while creation of DataFrame {str(e)}")
            raise
        
    def upload_to_bucket(self, df: DataFrame, file_name: str = "payload") -> None:
        # Uploads data stored in CSV files to the storage bucket
        storage_client = Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{file_name}.csv")

        blob.upload_from_string(data=df.to_csv(index=False), content_type = "text/csv")
        
        logging.info(f"File {file_name}.csv uploaded to {self.bucket_name}")
        
   # Main Function
    def process(event, context):
        """
        Triggered from a message on a Cloud Pub/Sub topic.
            Args:
                - event (dict): Event payload.
                - context (google.cloud.functions.Context): Metadata for the event.
        """
        
        root = logging.getLogger()
        root.setLevel(logging.INFO)

        svc = LoadToStorage(event, context)

        message = svc.get_message_data()
        upload_df = svc.dataframe_payload_structure(message)
        payload_timestamp = upload_df["price_timestamp"].unique().tolist()[0]
        
        svc.upload_to_bucket(upload_df,"crypto_dataset"+payload_timestamp))