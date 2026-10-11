"""Restaurant Service — Outbox infrastructure."""
from .publisher import RestaurantApprovalOutboxPublisher

__all__ = ["RestaurantApprovalOutboxPublisher"]
