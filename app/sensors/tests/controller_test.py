from fastapi.testclient import TestClient
import pytest
from app.main import app
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.timescale import Timescale
from shared.cassandra_client import CassandraClient
import time

client = TestClient(app)


@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
     from shared.database import engine
     from shared.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)
     redis = RedisClient(host="redis")
     redis.clearAll()
     redis.close()
     mongo = MongoDBClient(host="mongodb")
     mongo.clearDb("sensors")
     mongo.close()
     ts = Timescale()
     ts.execute("CREATE TABLE IF NOT EXISTS sensor_data (time TIMESTAMPTZ NOT NULL, sensor_id INT NOT NULL, temperature DOUBLE PRECISION, humidity DOUBLE PRECISION, battery_level DOUBLE PRECISION, velocity DOUBLE PRECISION, PRIMARY KEY (time, sensor_id))")
     ts.execute("commit")
     ts.execute("DELETE FROM sensor_data")
     # TODO execute TS migrations
     ts.execute("commit")
     ts.close()
     es = ElasticsearchClient(host="elasticsearch")
     es.clearIndex("sensors")

     while True:
          try:
               cassandra = CassandraClient(["cassandra"])
               cassandra.get_session().execute("DROP KEYSPACE IF EXISTS sensor")
               cassandra.close()
               break
          except Exception as e:
               time.sleep(5)

     


#TODO ADD all your tests in test_*.py files: