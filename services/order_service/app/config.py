"""
Order Service — Configuration
==============================
All configuration is loaded from environment variables.
Secrets must never be hardcoded.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class OrderServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Server
    service_name: str = "order-service"
    host: str = "0.0.0.0"
    port: int = 8181
    log_level: str = "INFO"

    # Database
    database_url: str = "postgresql+psycopg://food_user:food_pass@localhost:5432/food_ordering"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"

    # Consumer groups
    order_service_consumer_group: str = "order-service-group"

    # Topics
    topic_payment_request: str = "payment-request"
    topic_payment_response: str = "payment-response"
    topic_restaurant_approval_request: str = "restaurant-approval-request"
    topic_restaurant_approval_response: str = "restaurant-approval-response"
    topic_customer: str = "customer"

    # Outbox publisher
    outbox_poll_interval_seconds: float = 5.0
    outbox_max_retry_attempts: int = 5
    outbox_publisher_mode: str = "polling"  # polling | cdc

    @property
    def kafka_consumer_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.order_service_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # manual commit after DB commit
        }

    @property
    def kafka_producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
        }


@lru_cache
def get_settings() -> OrderServiceSettings:
    return OrderServiceSettings()
