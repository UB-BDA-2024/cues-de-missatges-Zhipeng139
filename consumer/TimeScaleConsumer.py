import json
import os
import sys
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.subscriber import Subscriber
from shared.timescale import Timescale
from shared.message import MessageStrcuture


class TimeScaleConsumer(Subscriber):
    def __init__(self, config):
        super().__init__(config)

    def consume(self):
        def callback(ch, method, properties, body: MessageStrcuture):
            database = Timescale()
            message = json.loads(body)
            action = message.get("action")
            data = message.get("data")

            logging.info(f"Timescale: Received message of action: {action}")
            logging.info(f"Timescale: Data: {data}, type: {type(data)}")

            if action == "insert_data":
                logging.info(f"Timescale: Inserting data {data} with key {data.get('sensor_id')}")
                database.insert_data(
                    sensor_id=data.get("sensor_id"),
                    velocity=data.get("velocity"),
                    temperature=data.get("temperature"),
                    humidity=data.get("humidity"),
                    battery_level=data.get("battery_level"),
                    last_seen=data.get("last_seen")
                )
            else:
                logging.error(f"Timescale: Action {action} not supported")
            database.close()
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
            logging.info("Timescale: Started consuming messages")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Error during consumption: {e}")
            raise e
        
    def close(self):
        super().close()
    