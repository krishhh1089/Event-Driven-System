"""
Order Service — Pydantic Schemas (Request / Response)
======================================================
All prices are handled as strings at the API boundary to preserve precision.
Server-side, they are converted to Decimal for domain logic.
"""
from __future__ import annotations

from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class OrderAddressRequest(BaseModel):
    street: str = Field(..., min_length=1)
    postal_code: str = Field(..., min_length=1)
    city: str = Field(..., min_length=1)


class OrderItemRequest(BaseModel):
    product_id: UUID
    quantity: int = Field(..., ge=1)
    price: str = Field(..., description="Unit price as decimal string")
    sub_total: str = Field(..., description="quantity × price as decimal string")

    @model_validator(mode="after")
    def validate_subtotal(self) -> "OrderItemRequest":
        price_d = Decimal(self.price)
        subtotal_d = Decimal(self.sub_total)
        expected = price_d * self.quantity
        if abs(expected - subtotal_d) > Decimal("0.001"):
            raise ValueError(
                f"sub_total {subtotal_d} does not match price {price_d} × quantity {self.quantity} = {expected}"
            )
        return self


class CreateOrderRequest(BaseModel):
    customer_id: UUID
    restaurant_id: UUID
    address: OrderAddressRequest
    price: str = Field(..., description="Total order price as decimal string")
    items: List[OrderItemRequest] = Field(..., min_length=1)


class CreateOrderResponse(BaseModel):
    tracking_id: UUID
    order_status: str
    message: str = "Order submitted successfully"


class TrackOrderResponse(BaseModel):
    tracking_id: UUID
    order_status: str
    failure_messages: Optional[List[str]] = None


class ErrorResponse(BaseModel):
    code: str
    message: str
    details: Optional[List[str]] = None
