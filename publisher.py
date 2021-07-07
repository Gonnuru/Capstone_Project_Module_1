# -*- coding: utf-8 -*-
"""
Created on Tue Jul 6 07:51:24 2021

@author: Gonnuru
"""

from requests import Session
import os
from os import environ
from time import sleep
import logging
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futues import Future




GOOGLE_CREDENTIALS = os.getenv('JSON_CREDENTIAL_FILE')
PROJECT_ID = os.getenv('PROJECT_ID')
TOPIC_ID = os.getenv('TOPIC_ID')

# setting Google service account key to the environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/app/keys/" + GOOGLE_CREDENTIALS

class PublishToPubsub:
    def __init__(self):
       

        self.project_id = PROJECT_ID
        self.topic_id = TOPIC_ID
        self.publisher_client = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []
        
        
    def get_crypto_ticker_data(self) -> str:
        # Get real time data from selected crypto tickers
        url="https://api.nomics.com/v1/currencies/ticker"
        CRYPTO_CONFIG = {"tickers":"BTC,ETH","currency":"USD"}

        params = {
                "key": environ.get("API_TOKEN",""),

                "convert": CRYPTO_CONFIG["currency"],
                "interval": "id",
                "per-page": "100",
                "page": "1",
                }
        

        # start session       
        ses = Session()
        res = ses.get(url, params=params, stream=True)
        
        # response
        if 200 <= res.status_code < 400:
            logging.info(f"Response - {res.status_code}: {res.text}")
            return res.text
        else:
            logging.error(f"Failed to fetch API data - {res.status_code}: {res.text}")
            raise Exception(f"Failed to fetch API data - {res.status_code}: {res.text}")
            
    def get_callback(self,publish_future: Future, data: str) -> callable:

        # Callback function which handles publish failures

            def callback(publish_future):
                try:
                    # wait 60 seconds for the publish call to succeed.
                    logging.info(publish_future.result(timeout = 60))
                    print(publish_future)

                except futures.TimeoutError:
                        print(data)
                        logging.error(f"Publishing {data} timed out.")
            
            return callback
        
    def publish_message_to_topic(self,message: str) -> None:
        """Publish message to a PubSub topic with an error handler"""

        # client returns a future when a message is published
        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))
        
        # publish failures are handles in the callback function
        publish_future.add_done_callback(self.get_callback(publish_future,message))
        self.publish_futures.append(publish_future)
        
        # waits for all the publish futures to resolve before exiting.
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        
        logging.info(f"Published messages with error handler to {self.topic_path}." )
        
if __name__ == "__main__":
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    
    svc = PublishToPubsub()

    # 24 hours in a day
    for i in range(24):
        message = svc.get_crypto_ticker_data()
        svc.publish_message_to_topic(message)

         # collects data every 1 hour (3600 seconds)
        sleep(3600)