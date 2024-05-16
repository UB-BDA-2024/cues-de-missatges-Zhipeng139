import pika
import logging
import time
from threading import Thread

class Subscriber:
    def __init__(self, config):
        self.queue_name = config['queue_name']
        print(f"Queue name: {self.queue_name}")
        self.credentials = pika.PlainCredentials(config['rabbitmq']['username'], config['rabbitmq']['password'])
        self.parameters = pika.ConnectionParameters(config['rabbitmq']['host'], config['rabbitmq']['port'], '/', self.credentials)
        self.conn = None
        self.channel = None
        self.connect()

    def connect(self):
        retries = 5
        for attempt in range(retries):
            try:
                logging.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{retries})")
                self.conn = pika.BlockingConnection(self.parameters)
                self.channel = self.conn.channel()
                self.channel.queue_declare(queue=self.queue_name)
                logging.info(f"Successfully connected to RabbitMQ, queue name: {self.queue_name}")
                break
            except pika.exceptions.AMQPConnectionError as e:
                logging.error(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(10)
                else:
                    logging.critical("All connection attempts failed")
                    raise e

    def consume(self):
        def callback(ch, method, properties, body):
            logging.info(f"Received message: {body}")

        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
            logging.info("Started consuming messages")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Error during consumption: {e}")
            raise e

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Connection to RabbitMQ closed")
