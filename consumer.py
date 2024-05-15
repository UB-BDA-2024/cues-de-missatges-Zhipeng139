import pika
import logging

logging.basicConfig(level=logging.INFO)

QUEUE_NAME = 'test'

class Consumer:
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

    def consume(self):
        def callback(ch, method, properties, body):
            logging.info(f"Received message: {body}")

        try:
            self.channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)
            logging.info("Started consuming messages")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Error during consumption: {e}")
            raise e

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Connection to RabbitMQ closed")

if __name__ == "__main__":
    consumer = Consumer()
    consumer.consume()
    consumer.close()
