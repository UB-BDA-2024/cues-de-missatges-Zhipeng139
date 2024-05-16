from cassandra.cluster import Cluster
import logging
class CassandraClient:
    def __init__(self, hosts):
        # Connect to the Cassandra cluster
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
