
### **Kafka Producer Code (Avro)**
```python
from time import sleep
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import psycopg2
import json
import time
import avro.schema
import avro.io
import io
from confluent_kafka import Producer

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'ZGTIIO5OWVW3UYEB',
    'sasl.password': 'uiAMCnQRBi5JezzCiYhrY3alKvrs6fzLcVQtOXv0h0XBAj4WOlOhyDLOeYuQwkVP'
}
producer = Producer(KAFKA_CONFIG)

# PostgreSQL Configuration
PG_CONFIG = {
    'dbname': 'ecommerce',
    'user': 'postgres',
    'password': 'Nikhil@17',
    'host': 'localhost',
    'port': '5432'
}

# Load Avro Schema
schema_path = "product_avro_schema.json"
with open(schema_path, "r") as schema_file:
    schema_str = json.load(schema_file)
schema = avro.schema.Parse(json.dumps(schema_str))

# Fetch Data from PostgreSQL
def fetch_incremental_data():
    conn = psycopg2.connect(**PG_CONFIG)
    cursor = conn.cursor()
    query = "SELECT id, name, category, price, last_updated FROM product;"
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    records = [dict(zip(column_names, row)) for row in rows]
    cursor.close()
    conn.close()
    return records

# Avro Serialization
def serialize_avro(data):
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(data, encoder)
    return bytes_writer.getvalue()

# Send Data to Kafka
def produce_messages():
    while True:
        records = fetch_incremental_data()
        if records:
            for product_data in records:
                avro_value = serialize_avro(product_data)
                producer.produce(
                    topic="topic_0",
                    key=str(product_data["id"]),
                    value=avro_value
                )
                print(f"Sent to Kafka: {product_data}")
        producer.flush()
        time.sleep(5)

```

---
