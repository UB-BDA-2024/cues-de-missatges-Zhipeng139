import json
import os
import sys
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.subscriber import Subscriber
from shared.redis_client import RedisClient
from shared.message import MessageStrcuture

class RedisConsumer(Subscriber):
    def __init__(self, config):
        super().__init__(config)

    def consume(self):
        def callback(ch, method, properties, body: MessageStrcuture):
            database = RedisClient(host="redis")
            message = json.loads(body)
            action = message.get("action")
            data = message.get("data")

            logging.info(f"Redis: Received message of action: {action}")
            logging.info(f"Redis: Data: {data}, type: {type(data)}")

            if action == "set_data":
                logging.info(f"Redis: Setting data {data.get('data')} with key {data.get('sensor_id')}")
                database.set(
                    key=str(data.get("sensor_id")),
                    value=json.dumps(data.get("data"))
                )
            else:
                logging.error(f"Redis: Action {action} not supported")
            database.close()

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
            logging.info("Redis: Started consuming messages")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Error during consumption: {e}")
            raise e
        
    def close(self):
        super().close()    