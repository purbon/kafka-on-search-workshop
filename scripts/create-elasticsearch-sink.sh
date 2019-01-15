#!/bin/sh

curl -i -X POST -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://kafka-connect-cp:18083/connectors/ \
    -d '{
      "name": "elastic-sink",
      "config": {
          "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
          "tasks.max": "1",
          "topics": "asgard.demo.CUSTOMERS",
          "key.ignore": "true",
          "connection.url": "http://elasticsearch:9200",
          "type.name": "customers",
          "name": "elastic-sink",
          "schema.ignore": "true",
          "key.converter":"org.apache.kafka.connect.storage.StringConverter",
          "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "value.converter.schemas.enable": false
       }
    }'
