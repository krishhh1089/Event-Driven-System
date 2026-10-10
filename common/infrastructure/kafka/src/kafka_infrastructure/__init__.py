"""
Kafka Infrastructure
====================
Shared Kafka producer/consumer abstractions and Avro schema registry helpers.
"""
from __future__ import annotations

import json
import logging
import struct
from io import BytesIO
from typing import Any, Dict, Optional

import fastavro
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema Registry client factory
# ---------------------------------------------------------------------------

def build_schema_registry_client(url: str, auth: Optional[Dict] = None) -> SchemaRegistryClient:
    conf: Dict[str, Any] = {"url": url}
    if auth:
        conf.update(auth)
    return SchemaRegistryClient(conf)


# ---------------------------------------------------------------------------
# Generic Kafka producer wrapper
# ---------------------------------------------------------------------------

class KafkaProducerWrapper:
    """
    Wraps confluent-kafka Producer with synchronous delivery reports.

    Usage
    -----
    producer = KafkaProducerWrapper({"bootstrap.servers": "..."})
    producer.produce(topic="t", key="k", value=b"v")
    producer.flush()
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self._producer = Producer(config)

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        def _delivery_report(err, msg):
            if err:
                logger.error(
                    "Kafka delivery failed | topic=%s key=%s err=%s",
                    topic,
                    key,
                    err,
                )
                raise KafkaException(err)
            logger.debug(
                "Kafka delivery OK | topic=%s partition=%d offset=%d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

        self._producer.produce(
            topic=topic,
            value=value,
            key=key.encode() if key else None,
            headers=list(headers.items()) if headers else None,
            on_delivery=_delivery_report,
        )
        # Poll to trigger delivery callbacks synchronously.
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("%d messages not acknowledged within %ss", remaining, timeout)

    def close(self) -> None:
        self.flush()


# ---------------------------------------------------------------------------
# Avro serializer/deserializer helper
# ---------------------------------------------------------------------------

class AvroHelper:
    """
    Serialize / deserialize Avro messages compatible with Confluent Schema Registry
    wire format (magic byte + schema ID + binary payload).
    """
    _MAGIC_BYTE = 0

    def __init__(self, schema_registry_client: SchemaRegistryClient) -> None:
        self._client = schema_registry_client

    def serialize(self, schema_str: str, data: Dict, subject: str) -> bytes:
        """Register schema if needed, then serialize *data* to Avro bytes."""
        from confluent_kafka.schema_registry import Schema
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = self._client.register_schema(subject, schema)

        parsed = fastavro.parse_schema(json.loads(schema_str))
        buf = BytesIO()
        # Write Confluent wire format header
        buf.write(struct.pack(">bI", self._MAGIC_BYTE, schema_id))
        fastavro.schemaless_writer(buf, parsed, data)
        return buf.getvalue()

    def deserialize(self, data: bytes) -> Dict:
        """Deserialize Confluent wire-format Avro bytes."""
        if len(data) < 5:
            raise ValueError("Avro message too short")
        magic, schema_id = struct.unpack(">bI", data[:5])
        if magic != self._MAGIC_BYTE:
            raise ValueError(f"Unknown magic byte: {magic}")

        schema_meta = self._client.get_schema(schema_id)
        parsed = fastavro.parse_schema(json.loads(schema_meta.schema_str))
        buf = BytesIO(data[5:])
        return fastavro.schemaless_reader(buf, parsed)
