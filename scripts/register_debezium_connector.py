"""
Register Debezium PostgreSQL Connector with Kafka Connect
==========================================================
Configures CDC ingestion from PostgreSQL outbox tables to Kafka.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import urllib.error
import urllib.request

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "food_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "food_pass")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "food_ordering")

CONNECTOR_NAME = "food-outbox-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        "database.hostname": POSTGRES_HOST,
        "database.port": POSTGRES_PORT,
        "database.user": POSTGRES_USER,
        "database.password": POSTGRES_PASSWORD,
        "database.dbname": POSTGRES_DB,
        "database.server.name": "food_server",
        "schema.include.list": "order,payment,restaurant,customer",
        "table.include.list": (
            "order.payment_outbox,"
            "order.restaurant_approval_outbox,"
            "payment.payment_outbox,"
            "restaurant.restaurant_approval_outbox,"
            "customer.customer_outbox"
        ),
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}


def register_connector(connect_url: str = KAFKA_CONNECT_URL) -> None:
    url = f"{connect_url.rstrip('/')}/connectors/{CONNECTOR_NAME}/config"
    data = json.dumps(CONNECTOR_CONFIG["config"]).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="PUT",
    )

    logger.info("Registering Debezium connector '%s' at %s ...", CONNECTOR_NAME, url)
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            logger.info("Successfully registered connector. Response status: %d", resp.status)
            logger.debug("Response body: %s", body)
    except urllib.error.HTTPError as err:
        logger.error("HTTP error registering connector: %d %s", err.code, err.reason)
        try:
            logger.error("Details: %s", err.read().decode("utf-8"))
        except Exception:
            pass
        sys.exit(1)
    except urllib.error.URLError as err:
        logger.error("Failed to reach Kafka Connect at %s: %s", connect_url, err.reason)
        sys.exit(1)


if __name__ == "__main__":
    register_connector()
