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
        self.database = Timescale()

    def consume(self):
        def callback(ch, method, properties, body: MessageStrcuture):
            logging.info(f"Timescale: Received message: {body}")

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
            logging.info("Timescale: Started consuming messages")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Error during consumption: {e}")
            raise e
        
    def close(self):
        super().close()
    