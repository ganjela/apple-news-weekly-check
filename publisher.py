from google.cloud import pubsub_v1
from utils.settings import SETTINGS
import json


class Publisher:
    def __init__(self):
        self.client = pubsub_v1.PublisherClient()
        self.topic_path = self.client.topic_path(SETTINGS.PROJECT_ID, SETTINGS.TOPIC_ID)

    def publish(self, data):
        if not data:
            return
        message = json.dumps(data).encode('utf-8')
        self.client.publish(self.topic_path, message).result()