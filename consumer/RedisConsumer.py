import json
import os
import sys
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.subscriber import Subscriber
from shared.redis_client import RedisClient
from shared.message import MessageStrcuture

# Dependency to get redis client
def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()

class RedisConsumer(Subscriber):
    def __init__(self, config):
        super().__init__(config)

    def consume(self):
        def callback(ch, method, properties, body: MessageStrcuture):
            message = json.loads(body)
            action = message.get("action")
            data = message.get("data")
            database = RedisClient(host="redis")

            logging.info(f"Redis: Received message of action: {action}")
            logging.info(f"Redis: Data: {data}")

            if action == "set_data":
                database.set(
                    key=str(data.get("key")),
                    value=data.get("data")
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
        self.redis.close()
    