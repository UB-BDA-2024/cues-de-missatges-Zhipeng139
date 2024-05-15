import pika
import logging
import json
import time

logging.basicConfig(level=logging.INFO)

QUEUE_NAME = 'test'

class Publisher:
    def __init__(self):
        self.credentials = pika.PlainCredentials('guest', 'guest')
        self.parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', self.credentials)
        self.conn = None
        self.channel = None
        self.connect()

    def connect(self):
        retries = 3
        for attempt in range(retries):
            try:
                logging.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{retries})")
                self.conn = pika.BlockingConnection(self.parameters)
                self.channel = self.conn.channel()
                self.channel.queue_declare(queue=QUEUE_NAME)
                logging.info("Successfully connected to RabbitMQ")
                break
            except pika.exceptions.AMQPConnectionError as e:
                logging.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(10)
                else:
                    logging.critical("All connection attempts failed")
                    raise e

    def publish(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=message.to_json())
            logging.info(f" [x] Sent {message}")
        except Exception as e:
            logging.error(f"Failed to publish message: {e}")
            raise e
        
    def publish_to(self, routing_key, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=routing_key, body=json.dumps(message))
            logging.info(f" [x] Sent {message}")
        except Exception as e:
            logging.error(f"Failed to publish message: {e}")
            raise e

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Connection to RabbitMQ closed")

if __name__ == "__main__":
    publisher = Publisher()
    message = {'key': 'value'}
    publisher.publish(message)
    publisher.close()
