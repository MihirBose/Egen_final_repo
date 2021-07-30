# import requests

# #response = requests.get("api.openweathermap.org/data/2.5/weather?q=Delhi&appid=523a55d456ef8408f644d6e92977fea4")

# #response = requests.get("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Springfield,IL/2021-07-28?unitGroup=us&key=AHBS12DVFHFFSHF764DGDB")

# response = requests.get("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/buffalo/today?unitGroup=us&key=5L93K2E42ABNWMGNHSZ83PCRX&include=hours%2Ccurrent")
# resp = response.json()
# #print (type(resp))
# print (resp['currentConditions'])
# #print(response.json())

from requests import Session
import requests
import os
from os import environ
from time import sleep
import logging
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
from google.cloud import secretmanager


guardian_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/buffalo/today"
os.environ["GOOGLE_CLOUD_PROJECT"] = "egen-project-1"

class PublishToPubsub:
    def __init__(self):
        self.project_id = "egen-project-1"
        self.topic_id = "guardian_stream"
        self.secret_id = "api-key"
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []

    def access_secret_version(self, version_id="latest"):
        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version.
        name = f"projects/{self.project_id}/secrets/{self.secret_id}/versions/{version_id}"

        # Access the secret version.
        response = client.access_secret_version(name=name)

        # Return the decoded payload.
        return response.payload.data.decode('UTF-8')

    def get_guardian_data(self) -> str:

        params = {
            self.secret_id : self.access_secret_version()
        }
        ses = Session()
        res = ses.get(guardian_url, params=params, stream=True)

        if 200 <= res.status_code <= 400:
            logging.info(f"Response - {res.status_code}:{res.text}")
            print (f"SUCCESS!!!")
            return res.text
        else:
            raise Exception(f"failed to fetch API data - {res.status_code}:{res.text}")

    def get_callback(self, publish_future: Future, data: str) -> callable:
        def callback(publish_future):
            try:
                # Wait for 60 seconds for the publish call to succeed.
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error(f"Publishing {data} timed out.")
        return callback

    def publish_message_to_topic(self, message: str) -> None:
        """publish message to a pubsub topic with an error handler"""

        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))

        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)

        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info(f"Published messages with error handler to {self.topic_path}.")

if __name__ == "__main__":

    print (f"hello!!!")
    svc = PublishToPubsub()
    message = svc.get_guardian_data()
    svc.publish_message_to_topic(message)
    sleep(5)
