from cassandra.cluster import Cluster
import pika
import logging
import time
logging.basicConfig(level=logging.INFO)
import json

QUEUE_NAME = 'casandra'

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts, protocol_version=4)
        self.session = self.cluster.connect()
        self.session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sensor
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        self.session.set_keyspace('sensor')
        self.create_table()

    def create_table(self):
        # Create a table for sensor data
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            sensor_id int,
            last_seen timestamp,
            type text,
            temperature float,
            velocity float,
            PRIMARY KEY (sensor_id, last_seen)
        )
        """)

        self.session.execute("""
        CREATE TABLE IF NOT EXISTS sensor_type (
            sensor_id int,
            type text,
            PRIMARY KEY (type, sensor_id)
        )
        """)

        self.session.execute("""
        CREATE TABLE IF NOT EXISTS sensor_battery_level (
            sensor_id int,
            battery_level float,
            PRIMARY KEY (sensor_id)
        )
        """)

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)

    def insert_data(self, sensor_id, last_seen, sensor_type, temperature=None, velocity=None):
        query = """
            INSERT INTO sensor_data (sensor_id, last_seen, type, temperature, velocity) 
            VALUES (%s, %s, %s, %s, %s)
        """
        self.session.execute(query, (sensor_id, last_seen,
                             sensor_type, temperature, velocity))

    def insert_sensor_type(self, sensor_id, sensor_type):
        query = """
            INSERT INTO sensor_type (sensor_id, type) 
            VALUES (%s, %s)
        """
        self.session.execute(query, (sensor_id, sensor_type))

    def insert_battery_level(self, sensor_id, battery_level):
        query = """
            INSERT INTO sensor_battery_level (sensor_id, battery_level) 
            VALUES (%s, %s)
        """
        self.session.execute(query, (sensor_id, battery_level))

    def update(self, sensor_id, battery_level=None, temperature=None, velocity=None):
        query = """
            UPDATE sensor_data
            SET temperature = %s, battery_level = %s, velocity = %s
            WHERE sensor_id = %s
        """
        self.session.execute(
            query, (temperature, battery_level, velocity, sensor_id))

    def get_temperature_values(self):
        # This query returns max, min, and avg temperature values for each sensor
        query = """
            SELECT sensor_id, MAX(temperature) AS max_temperature, MIN(temperature) AS min_temperature, AVG(temperature) AS avg_temperature, type
            FROM sensor_data
            WHERE type = 'Temperatura'
            GROUP BY sensor_id
            ALLOW FILTERING;
        """
        result = self.session.execute(query)
        # Convert the result set to a list of dictionaries for easier processing
        return [{
            "sensor_id": row.sensor_id,
            "max_temperature": row.max_temperature,
            "min_temperature": row.min_temperature,
            "avg_temperature": row.avg_temperature,
            "type": row.type
        } for row in result]

    def get_sensors_quantity_type(self):
        # This query returns the quantity of sensors grouped by type
        # The result set will contain two columns: type and quantity
        # The quantity column will contain the number of sensors of each type, possibly with duplicates if the same sensor is registered multiple times in the database count as 1 sensor

        query = """
            SELECT type, COUNT(type) AS quantity
            FROM sensor_type
            GROUP BY type
        """
        result = self.session.execute(query)
        # Convert the result set to a list of dictionaries for easier processing
        return [{
            "type": row.type,
            "quantity": row.quantity
        } for row in result]

    def get_sensor_low_battery(self):
        # This query returns the sensors with a battery level below 20%
        query = """
            SELECT sensor_id, battery_level
            FROM sensor_battery_level
            WHERE battery_level < 0.2
            ALLOW FILTERING;
        """
        result = self.session.execute(query)
        # Convert the result set to a list of dictionaries for easier processing
        return [{
            "sensor_id": row.sensor_id,
            "battery_level": round(row.battery_level, 2)
        } for row in result]
    
    def delete_sensor_data(self, sensor_id):
        query = f"""
            DELETE FROM sensor_data
            WHERE sensor_id = {sensor_id} 
        """
        self.session.execute(query)

class CassandreSubsciber:
    def __init__(self, cassandra_client: CassandraClient) -> None:
        self.credentials = pika.PlainCredentials('guest', 'guest')
        self.parameters = pika.ConnectionParameters('rabbitmq', 5672, '/', self.credentials)
        self.conn = None
        self.channel = None
        self.cassandra_client = cassandra_client
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
            message = json.loads(body)
            action = message.get('action')
            if action == "insert_data":
                self.cassandra_client.insert_data(
                    message.get('sensor_id'),
                    message.get('last_seen'),
                    message.get('type'),
                    message.get('temperature'),
                    message.get('velocity')
                )
            elif action == "insert_sensor_type":
                self.cassandra_client.insert_sensor_type(
                    message.get('sensor_id'),
                    message.get('type')
                )
            elif action == "insert_battery_level":
                self.cassandra_client.insert_battery_level(
                    message.get('sensor_id'),
                    message.get('battery_level')
                )
            elif action == "update":
                self.cassandra_client.update(
                    message.get('sensor_id'),
                    message.get('battery_level'),
                    message.get('temperature'),
                    message.get('velocity')
                )
            elif action == "delete_sensor_data":
                self.cassandra_client.delete_sensor_data(
                    message.get('sensor_id')
                )
            else:
                logging.error(f"Action {action} not recognized")

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