import psycopg2
import os
from datetime import datetime


class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor()
        
        
    def getCursor(self):
            return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        return self.conn.ping()
    
    def execute(self, query):
       return self.cursor.execute(query)
    
    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()

    def insert_data(self, sensor_id, velocity=None, temperature=None, humidity=None, battery_level=None, last_seen=None):
        """
        Inserts sensor data into the database.

        Args:
            sensor_id (str): The ID of the sensor.
            velocity (float, optional): The velocity data of the sensor.
            temperature (float, optional): The temperature data of the sensor.
            humidity (float, optional): The humidity data of the sensor.
            battery_level (float, optional): The battery level data of the sensor.
            last_seen (str, optional): The last seen timestamp in the format 'YYYY-MM-DDTHH:MM:SS.000Z'.

        Note:
            - If last_seen is not provided, the current timestamp will be used.
        """
        self.cursor.execute(
            "INSERT INTO sensor_data (sensor_id, temperature, humidity, battery_level, velocity, time) VALUES (%s, %s, %s, %s, %s, %s)",
            (sensor_id, temperature, humidity, battery_level, velocity, last_seen)
        )
        self.conn.commit()


    def get_data(self, sensor_id, from_date, to_date, bucket_size):
        """
        Retrieves sensor data for a specified time range and bucket size.

        Args:
            sensor_id (str): The ID of the sensor.
            from_date (str): The start date and time in the format 'YYYY-MM-DDTHH:MM:SS.000Z'.
            to_date (str): The end date and time in the format 'YYYY-MM-DDTHH:MM:SS.000Z'.
            bucket_size (str): The size of time buckets for aggregation. Can be 'hour', 'day', 'week', or 'month'.

        Returns:
            list: A list of tuples containing aggregated sensor data for each time bucket.
                Each tuple contains (bucket, temperature, humidity, battery_level, velocity).

        Note:
            - 'day' indicates that the aggregation is done by day.
        """
        bucket_size = f"1 {bucket_size}"
        self.cursor.execute(
            "SELECT time_bucket(%s, time) AS bucket, AVG(temperature) AS temperature, AVG(humidity) AS humidity, AVG(battery_level) AS battery_level, AVG(velocity) AS velocity FROM sensor_data WHERE sensor_id = %s AND time >= %s AND time <= %s GROUP BY bucket ORDER BY bucket ASC",
            (bucket_size, sensor_id, from_date, to_date)
        )
        return self.cursor.fetchall()
    
    def delete_sensor_data(self, sensor_id):
        """
        Deletes sensor data from the database.

        Args:
            sensor_id (str): The ID of the sensor.
        """
        self.cursor.execute("DELETE FROM sensor_data WHERE sensor_id = %s", (sensor_id,))
        self.conn.commit()