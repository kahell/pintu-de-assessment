from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# Configuration for the Schema Registry
schema_registry_conf = {'url': 'http://localhost:8085'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Define the JSON schema as a string
# Note that this is a JSON Schema, not an Avro schema
schema_str = """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order",
  "type": "object",
  "properties": {
    "order_id": {
      "type": "integer"
    },
    "symbol": {
      "type": "string"
    },
    "order_side": {
      "type": "string"
    },
    "size": {
      "type": "number"
    },
    "price": {
      "type": "number"
    },
    "status": {
      "type": "string"
    },
    "created_at": {
      "type": "integer"
    }
  },
  "required": ["order_id", "symbol", "order_side", "size", "price", "status", "created_at"]
}"""

# Create a Schema object with schema_type set to 'JSON'
schema = Schema(schema_str, schema_type='JSON')

# Register the schema under the subject 'technical_assessment-schema'
schema_id = schema_registry_client.register_schema(subject_name='technical_assessment-schema', schema=schema)

# Print the registered schema ID
print(f"Registered schema with ID: {schema_id}")
