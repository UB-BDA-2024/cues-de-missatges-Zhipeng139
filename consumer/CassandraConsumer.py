import json
import os
import sys
import logging
from fastapi import Depends
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from shared.subscriber import Subscriber
from shared.cassandra_client import CassandraClient

class CassandraConsumer(Subscriber):
    def __init__(self, config):
        super().__init__(config)

    def consume(self):
        def callback(ch, method, properties, body):
            database = CassandraClient(hosts=["cassandra"])
            message = json.loads(body)
            action = message.get("action")
            data = message.get("data")
            logging.info(f"Cassandra: Received message of action: {action}")
            logging.info(f"Cassandra: Data: {data}")

            if action == "insert_sensor_type":
                database.insert_sensor_type(
                    sensor_id=data.get("sensor_id"),
                    sensor_type=data.get("sensor_type"),
                )
            elif action == "insert_data":
                database.insert_data(
                    sensor_id=data.get("sensor_id"),
                    last_seen=data.get("last_seen"),
                    sensor_type=data.get("sensor_type"),
                    temperature=data.get("temperature"),
                    velocity=data.get("velocity"),
                )
            elif action == "insert_battery_level":
                database.insert_battery_level(
                    sensor_id=data.get("sensor_id"),
                    battery_level=data.get("battery_level")
                )
            elif action == "delete":
                database.delete_sensor_data(
                    sensor_id=data.get("sensor_id"),
                )
            else:
                logging.error(f"Cassandra: Action {action} not supported")
            
            database.close()

            
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
            logging.info("Cassandra: Started consuming messages")
            self.channel.start_consuming()
        except Exception as e:
            logging.error(f"Error during consumption: {e}")
            raise e
        
    def close(self):
        super().close()    