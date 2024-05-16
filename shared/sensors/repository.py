from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.cassandra_client import CassandraClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.timescale import Timescale
from shared.message import MessageStrcuture
from shared.publisher import Publisher
import json


class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, mongodb: MongoDBClient, sensor_id: int) -> Optional[models.Sensor]:
    db_sensor = db.query(models.Sensor).filter(
        models.Sensor.id == sensor_id).first()

    if db_sensor is None:
        raise HTTPException(
            status_code=404, detail="Sensor not found in SQL database")

    document = mongodb.get_data(sensor_id)

    if document is None:
        raise HTTPException(
            status_code=404, detail="Sensor not found in MongoDB")

    output = {
        "id": db_sensor.id,
        "name": db_sensor.name,
        "latitude": document['latitude'],
        "longitude": document['longitude'],
        "type": document['type'],
        "mac_address": document['mac_address'],
        "manufacturer": document['manufacturer'],
        "model": document['model'],
        "serie_number": document['serie_number'],
        "firmware_version": document['firmware_version'],
        "description": document['description'],
    }
    return output


def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()


def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()


def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb: MongoDBClient, elastic: ElasticsearchClient, publish: Publisher) -> models.Sensor:
    """
    Creates a new sensor record in both SQL and MongoDB databases.

    Parameters:
        db (Session): The SQLAlchemy session for SQL database operations.
        sensor (schemas.SensorCreate): The sensor object containing data to create the sensor.
        mongodb (MongoDBClient): The MongoDB client for NoSQL database operations.

    Returns:
        models.Sensor: The newly created sensor object from the SQL database.
    """
    # Create a new sensor record in the SQL database
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()  # Save the sensor record to the database
    # Refresh the instance from the database, to get the generated ID
    db.refresh(db_sensor)

    # Prepare the sensor document for MongoDB
    document = {
        "id": db_sensor.id,
        "longitude": sensor.longitude,
        "latitude": sensor.latitude,
        # Convert to string for JSON serialization
        "joined_at": db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"),
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "description": sensor.description,
    }

    # Insert the sensor document into MongoDB
    mongodb.insert_data(document)

    elastic_index_name = "sensors"

    if not elastic.index_exists(elastic_index_name):
        elastic.create_index(elastic_index_name)
        mapping = {
            "properties": {
                "id": {"type": "keyword"},
                "name": {"type": "keyword"},
                "type": {"type": "keyword"},
                "description": {"type": "text"},
            }
        }

        elastic.create_mapping(elastic_index_name, mapping)

    # cassandra.insert_sensor_type(db_sensor.id, sensor.type)
    message = MessageStrcuture(
        action_type="insert_sensor_type",
        data={
            "sensor_id": db_sensor.id,
            "sensor_type": sensor.type
        }
    )
    # Publish message to RabbitMQ
    publish.publish_to("cassandra", message)

    elastic_doc = {
        "id": db_sensor.id,
        "name": sensor.name,
        "type": sensor.type,
        "description": sensor.description,
    }

    elastic.index_document(elastic_index_name, elastic_doc)

    # Return the created sensor object from the SQL database
    output = {
        "id": db_sensor.id,
        "name": db_sensor.name,
        "latitude": document['latitude'],
        "longitude": document['longitude'],
        "type": document['type'],
        "mac_address": document['mac_address'],
        "manufacturer": document['manufacturer'],
        "model": document['model'],
        "serie_number": document['serie_number'],
        "firmware_version": document['firmware_version'],
        "description": document['description'],
    }

    return output


def record_data(db: Session, redis: RedisClient, mongo_db: MongoDBClient, sensor_id: int, ts_db: Timescale, data: schemas.SensorData, publisher: Publisher) -> schemas.Sensor:
    """
    Updates sensor data in SQL database, Redis, and MongoDB, then returns the updated sensor information.

    Parameters:
        db (Session): Database session for SQL operations.
        redis (RedisClient): Client for Redis operations.
        mongo_db (MongoDBClient): Client for MongoDB operations.
        sensor_id (int): The ID of the sensor to update.
        data (schemas.SensorData): The new data for the sensor.

    Returns:
        schemas.Sensor: The updated sensor information.

    Raises:
        HTTPException: If the sensor is not found in the SQL database or MongoDB.
    """
    # Retrieve the sensor from SQL database
    db_sensor = get_sensor(db, mongo_db, sensor_id)
    if db_sensor is None:
        raise HTTPException(
            status_code=404, detail="Sensor not found in SQL database")

    # Convert sensor data to JSON string for Redis
    # Using .dict() method if data is a Pydantic model
    dyn_data = json.dumps(data.dict())

    # Update sensor data in Redis
    #redis.set(str(sensor_id), dyn_data)  # Ensure the key is a string
    meesage = MessageStrcuture(
        action_type="set_data",
        data={
            "sensor_id": sensor_id,
            "data": dyn_data
        }
    )

    publisher.publish_to("redis", meesage)

    # Check if the sensor exists in MongoDB
    document = mongo_db.get_data(sensor_id)
    if document is None:
        raise HTTPException(
            status_code=404, detail="Sensor not found in MongoDB")

    # Assuming you might want to update the sensor data in MongoDB as well, but it's missing here.
    # mongo_db.update_data(sensor_id, data_dict) # Hypothetical method to update sensor data in MongoDB.

    # Deserialize dynamic data back into a Python dictionary for further processing
    try:
        data_dict = json.loads(dyn_data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Error parsing data: {e}")

    # Insert sensor data into TimescaleDB

    ts_db.insert_data(sensor_id,
                      velocity=data.velocity,
                      temperature=data.temperature,
                      humidity=data.humidity,
                      battery_level=data.battery_level,
                      last_seen=data.last_seen)

    """
    cassandra.insert_data(sensor_id,
                          last_seen=data.last_seen,
                          sensor_type=db_sensor['type'],
                          temperature=data.temperature,
                          velocity=data.velocity)

    cassandra.insert_battery_level(sensor_id, data.battery_level)
    """

    message = MessageStrcuture(
        action_type="insert_data",
        data={
            "sensor_id": sensor_id,
            "last_seen": data.last_seen,
            "sensor_type": db_sensor['type'],
            "temperature": data.temperature,
            "velocity": data.velocity
        }
    )

    publisher.publish_to("cassandra", message)

    message = MessageStrcuture(
        action_type="insert_battery_level",
        data={
            "sensor_id": sensor_id,
            "battery_level": data.battery_level
        }
    )
    
    publisher.publish_to("cassandra", message)

    return data_dict


def get_data(db: Session, redis: RedisClient, mongo_db: MongoDBClient, timescale: Timescale, sensor_id: int, from_date: str, to_date: str, bucket_size: str) -> schemas.Sensor:
    """
    Retrieves sensor data from SQL database, Redis, and MongoDB, and returns a consolidated sensor object.

    Parameters:
        db (Session): The SQLAlchemy session for SQL database operations.
        redis (RedisClient): The client for Redis operations.
        mongo_db (MongoDBClient): The client for MongoDB operations.
        sensor_id (int): The ID of the sensor to retrieve data for.

    Returns:
        schemas.Sensor: The consolidated sensor object with data from all sources.

    Raises:
        HTTPException: If the sensor is not found in the SQL database or MongoDB, or if there is an error parsing data from Redis.
    """
    # Retrieve the sensor from the SQL database
    db_sensor = get_sensor(db, mongo_db, sensor_id)
    if db_sensor is None:
        raise HTTPException(
            status_code=404, detail="Sensor not found in SQL database")

    # Get the sensor's dynamic data from Redis
    # Ensure sensor_id is a string for Redis keys
    dyn_data = redis.get(str(sensor_id))
    if dyn_data is None:
        # Assuming it's critical to have dynamic data; otherwise, adjust the logic as necessary
        raise HTTPException(
            status_code=404, detail="Sensor dynamic data not found in Redis")

    # Retrieve the sensor document from MongoDB
    document = mongo_db.get_data(sensor_id)
    if document is None:
        raise HTTPException(
            status_code=404, detail="Sensor not found in MongoDB")

    # Parse the dynamic data from Redis
    try:
        data_dict = json.loads(dyn_data)
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=400, detail=f"Error parsing dynamic data: {e}")

    timescale_data = timescale.get_data(
        sensor_id, from_date=from_date, to_date=to_date, bucket_size=bucket_size)
    return timescale_data


def delete_sensor(db: Session, mongo_db: MongoDBClient, redis: RedisClient, sensor_id: int):
    """
    Deletes a sensor from the SQL database, MongoDB, and Redis by its ID.

    Parameters:
        db (Session): The SQLAlchemy session for SQL database operations.
        mongo_db (MongoDBClient): The client for MongoDB operations.
        redis (RedisClient): The client for Redis operations.
        sensor_id (int): The ID of the sensor to be deleted.

    Returns:
        The sensor object from the SQL database that was deleted.

    Raises:
        HTTPException: If the sensor is not found in the SQL database.
    """
    # Attempt to retrieve the sensor by its ID from the SQL database
    db_sensor = db.query(models.Sensor).filter(
        models.Sensor.id == sensor_id).first()

    if db_sensor is None:
        # If no sensor is found, raise an HTTPException with a 404 status code
        raise HTTPException(status_code=404, detail="Sensor not found")

    # Delete the sensor from the SQL database
    db.delete(db_sensor)
    db.commit()

    # Delete the sensor data from MongoDB
    # Assuming mongo_db.delete_data is correctly implemented to handle deletion by sensor_id
    mongo_db.delete_data(sensor_id)

    # Delete the sensor data from Redis
    # The key used here should match how sensor data is stored/retrieved in Redis
    redis.delete(str(sensor_id))  # Ensure sensor_id is a string for Redis keys

    # Return the deleted sensor object from the SQL database
    return db_sensor


def get_sensors_near(db: Session, redis: RedisClient, mongodb: MongoDBClient, latitude: float, longitude: float, radius: float) -> list[schemas.Sensor]:
    """
    Retrieves sensors near a specified latitude and longitude within a given radius.

    Parameters:
        db (Session): The SQLAlchemy session for database operations.
        redis (RedisClient): The client for Redis operations.
        mongodb (MongoDBClient): The client for MongoDB operations.
        latitude (float): The latitude of the location.
        longitude (float): The longitude of the location.
        radius (float): The search radius in kilometers.

    Returns:
        list[schemas.Sensor]: A list of sensor schemas with details from SQL database, Redis, and MongoDB.

    Raises:
        HTTPException: If there's an issue parsing data from Redis for any sensor.
    """
    list_document = mongodb.get_near_sensors(latitude, longitude, radius)
    list_sensors = []

    for document in list_document:
        sensor_id = document['id']
        db_sensor = get_sensor(db, mongodb, sensor_id)

        # Skip sensors not found in the SQL database
        if db_sensor is None:
            continue

        # Convert sensor_id to string for Redis
        dyn_data = redis.get(str(sensor_id))

        # Handle missing or unparseable dynamic data
        if dyn_data is None:
            data_dict = {}
        else:
            try:
                data_dict = json.loads(dyn_data)
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=400, detail=f"Error parsing data for sensor {sensor_id}: {e}")
        # Construct the sensor object, using default values if dynamic data is missing
        list_sensors.append({
            "id": db_sensor["id"],
            "name": db_sensor["name"],
            "latitude": document.get("latitude", 0),
            "longitude": document.get("longitude", 0),
            "joined_at": document["joined_at"],
            "last_seen": data_dict.get("last_seen", ""),
            "type": document.get("type", ""),
            "mac_address": document.get("mac_address", ""),
            "battery_level": data_dict.get("battery_level", 0),
            "temperature": data_dict.get("temperature", 0),
            "humidity": data_dict.get("humidity", 0),
            "velocity": data_dict.get("velocity", 0)
        })

    return list_sensors


def search_sensors(db: Session, mongodb: MongoDBClient, elastic_search: ElasticsearchClient, query: str, size: int = 10, search_type: str = None) -> list[schemas.Sensor]:
    '''
    Search sensors by query in Elasticsearch.

    Parameters:
        - query: string to search
        - size (optional): number of results to return
        - search_type (optional): type of search to perform
        - db: database session
        - mongodb_client: mongodb client
    '''
    query_dict = eval(query)
    elasic_index_name = 'sensors'

    query_type = search_type if search_type else 'match'
    search_type = list(query_dict.keys())[0]
    value = query_dict[search_type]

    if query_type == 'similar':
        search_query = {
            "query": {
                "fuzzy": {
                    search_type: {
                        "value": value,
                        "fuzziness": "AUTO"
                    }
                }
            }
        }
    else:
        search_query = {
            "query": {
                query_type: {
                    search_type: value
                }
            }
        }

    results = elastic_search.search(
        index_name=elasic_index_name, query=search_query)

    sensors = []

    for hit in results['hits']['hits']:
        if len(sensors) == size:
            break
        sensor_data = hit['_source']
        id = sensor_data['id']
        db_sensor = get_sensor(db, mongodb, id)
        sensors.append(db_sensor)

    return sensors


def get_temperature_values(db: Session, cassandra: CassandraClient, mongodb: MongoDBClient):
    output = {
        "sensors": [],
    }

    temperature_values = cassandra.get_temperature_values()

    for row in temperature_values:
        sensor_id = row.get('sensor_id')
        db_sensor = get_sensor(db, mongodb, sensor_id)
        output["sensors"].append({
            "id": sensor_id,
            "name": db_sensor['name'],
            "latitude": db_sensor['latitude'],
            "longitude": db_sensor['longitude'],
            "type": db_sensor['type'],
            "mac_address": db_sensor['mac_address'],
            "manufacturer": db_sensor['manufacturer'],
            "model": db_sensor['model'],
            "serie_number": db_sensor['serie_number'],
            "firmware_version": db_sensor['firmware_version'],
            "description": db_sensor['description'],
            "values": [{
                "max_temperature": row.get('max_temperature'),
                "min_temperature": row.get('min_temperature'),
                "average_temperature": row.get('avg_temperature')
            }]
        })

    return output


def get_sensors_quantity(cassandra: CassandraClient):
    output = {
        "sensors": [],
    }

    sensors_quantity = cassandra.get_sensors_quantity_type()

    for row in sensors_quantity:
        output["sensors"].append(row)

    return output


def get_low_battery_sensors(db: Session, cassandra: CassandraClient, mongodb: MongoDBClient):
    output = {
        "sensors": [],
    }

    low_battery_sensors = cassandra.get_sensor_low_battery()

    for row in low_battery_sensors:
        sensor_id = row.get('sensor_id')
        db_sensor = get_sensor(db, mongodb, sensor_id)
        output["sensors"].append({
            "id": sensor_id,
            "name": db_sensor['name'],
            "latitude": db_sensor['latitude'],
            "longitude": db_sensor['longitude'],
            "type": db_sensor['type'],
            "mac_address": db_sensor['mac_address'],
            "manufacturer": db_sensor['manufacturer'],
            "model": db_sensor['model'],
            "serie_number": db_sensor['serie_number'],
            "firmware_version": db_sensor['firmware_version'],
            "description": db_sensor['description'],
            "battery_level": row.get('battery_level')
        })

    return output
