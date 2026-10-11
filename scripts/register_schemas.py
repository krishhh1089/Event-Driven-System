"""
Register Avro Schemas with Confluent Schema Registry
=====================================================
Reads all .avsc files from common/infrastructure/kafka/avro/
and registers them under their canonical subject names.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

SUBJECT_MAPPINGS = {
    "customer.avsc": "customer-value",
    "payment_request.avsc": "payment-request-value",
    "payment_response.avsc": "payment-response-value",
    "restaurant_approval_request.avsc": "restaurant-approval-request-value",
    "restaurant_approval_response.avsc": "restaurant-approval-response-value",
}


def register_all_schemas(url: str = SCHEMA_REGISTRY_URL) -> None:
    logger.info("Connecting to Schema Registry at %s ...", url)
    client = SchemaRegistryClient({"url": url})

    avro_dir = Path(__file__).resolve().parents[1] / "common" / "infrastructure" / "kafka" / "avro"
    if not avro_dir.is_dir():
        logger.error("Avro schemas directory not found at: %s", avro_dir)
        sys.exit(1)

    for schema_file, subject in SUBJECT_MAPPINGS.items():
        file_path = avro_dir / schema_file
        if not file_path.is_file():
            logger.warning("Schema file %s not found in %s, skipping", schema_file, avro_dir)
            continue

        schema_str = file_path.read_text(encoding="utf-8")
        schema = Schema(schema_str, schema_type="AVRO")
        try:
            schema_id = client.register_schema(subject, schema)
            logger.info("Registered '%s' -> Subject: '%s' (ID: %d)", schema_file, subject, schema_id)
        except Exception as exc:
            logger.error("Failed to register '%s' for subject '%s': %s", schema_file, subject, exc)


if __name__ == "__main__":
    register_all_schemas()
