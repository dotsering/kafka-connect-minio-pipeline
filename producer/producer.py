from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time

conf = {'bootstrap.servers': 'localhost:9092'}
schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
topic = "users"

value_schema_str = """
{
  "type": "record",
  "name": "User",
  "namespace": "com.dorjee.avro",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "first_name", "type": ["null", "string"], "default": null},
    {"name": "last_name", "type": ["null", "string"], "default": null},
    {"name": "email", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "raw_password_hash", "type": "string"},
    {"name": "internal_tracking_code", "type": "string"},
    {
      "name": "address",
      "type": {
        "type": "record",
        "name": "Address",
        "fields": [
          {"name": "street", "type": "string"},
          {"name": "city", "type": "string"},
          {"name": "zip_code", "type": "string"}
        ]
      }
    }
  ]
}
"""

avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)
string_serializer = StringSerializer('utf_8')
producer = Producer(conf)

print("Sending data to Kafka...")

# Generate 4 users
# Index 2 has empty name (should be FILTERED)
# All have password_hash (should be DROPPED)
for i in range(4):
    is_bad_record = (i == 2)

    user = {
        "user_id": f"id_{i}",
        "first_name": "" if is_bad_record else f"User{i}",
        "last_name": f"Doe{i}",
        "email": f"user{i}@example.com",
        "age": 20 + i,
        "raw_password_hash": "secret123",
        "internal_tracking_code": "TRACK_XYZ",
        "address": {"street": f"{i} St", "city": "City", "zip_code": "00000"}
    }

    print(f"Producing User {i}...")
    producer.produce(topic=topic,
                     key=string_serializer(user['user_id']),
                     value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE)))
    time.sleep(1)

producer.flush()
print("Done! Go check MinIO bucket 'user-profiles-dump'.")
