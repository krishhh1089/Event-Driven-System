"""
Seed Data Script
=================
Seeds initial restaurants, catalog products, and test customer accounts
into PostgreSQL so that the entire food ordering workflow can be exercised.
"""
from __future__ import annotations

import asyncio
import logging
import os
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://food_user:food_pass@localhost:5432/food_ordering",
)

# ---------------------------------------------------------------------------
# Seed Records
# ---------------------------------------------------------------------------

RESTAURANTS = [
    {
        "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb41"),
        "name": "Mario's Italian Bistro",
        "active": True,
        "products": [
            {
                "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb42"),
                "name": "Margherita Pizza",
                "price": Decimal("12.50"),
                "available": True,
            },
            {
                "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb43"),
                "name": "Pasta Carbonara",
                "price": Decimal("15.00"),
                "available": True,
            },
            {
                "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb44"),
                "name": "Tiramisu",
                "price": Decimal("6.00"),
                "available": True,
            },
        ],
    },
    {
        "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb51"),
        "name": "Tokyo Sushi Bar",
        "active": True,
        "products": [
            {
                "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb52"),
                "name": "Salmon Nigiri Set",
                "price": Decimal("18.00"),
                "available": True,
            },
            {
                "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb53"),
                "name": "Spicy Tuna Roll",
                "price": Decimal("11.00"),
                "available": True,
            },
            {
                "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb54"),
                "name": "Miso Soup",
                "price": Decimal("4.50"),
                "available": True,
            },
        ],
    },
]

CUSTOMER = {
    "id": UUID("d215b5f8-0249-4dc5-89a3-51fd148cfb11"),
    "username": "john_doe",
    "first_name": "John",
    "last_name": "Doe",
    "initial_credit": Decimal("100.00"),
}


async def seed() -> None:
    logger.info("Connecting to database at %s ...", DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            # 1. Seed Customer
            logger.info("Seeding customer '%s' ...", CUSTOMER["username"])
            await session.execute(
                text(
                    """
                    INSERT INTO customer.customers (id, username, first_name, last_name, created_at)
                    VALUES (:id, :username, :first_name, :last_name, NOW())
                    ON CONFLICT (id) DO NOTHING;
                    """
                ),
                {
                    "id": CUSTOMER["id"],
                    "username": CUSTOMER["username"],
                    "first_name": CUSTOMER["first_name"],
                    "last_name": CUSTOMER["last_name"],
                },
            )

            # 2. Seed Customer Credit Entry and History
            logger.info("Seeding credit balance for customer %s ($%s) ...", CUSTOMER["id"], CUSTOMER["initial_credit"])
            await session.execute(
                text(
                    """
                    INSERT INTO payment.credit_entries (id, customer_id, total_credit_amount, version)
                    VALUES (gen_random_uuid(), :customer_id, :amount, 0)
                    ON CONFLICT (customer_id) DO NOTHING;
                    """
                ),
                {"customer_id": CUSTOMER["id"], "amount": CUSTOMER["initial_credit"]},
            )

            await session.execute(
                text(
                    """
                    INSERT INTO payment.credit_history (id, customer_id, amount, transaction_type)
                    VALUES (gen_random_uuid(), :customer_id, :amount, 'CREDIT')
                    ON CONFLICT DO NOTHING;
                    """
                ),
                {"customer_id": CUSTOMER["id"], "amount": CUSTOMER["initial_credit"]},
            )

            # 3. Seed Restaurants and Products
            for r in RESTAURANTS:
                logger.info("Seeding restaurant '%s' (%s) ...", r["name"], r["id"])
                await session.execute(
                    text(
                        """
                        INSERT INTO restaurant.restaurants (id, name, active)
                        VALUES (:id, :name, :active)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, active = EXCLUDED.active;
                        """
                    ),
                    {"id": r["id"], "name": r["name"], "active": r["active"]},
                )

                for p in r["products"]:
                    logger.info("  -> Seeding product '%s' ($%s) ...", p["name"], p["price"])
                    await session.execute(
                        text(
                            """
                            INSERT INTO restaurant.products (id, name, price, available)
                            VALUES (:id, :name, :price, :available)
                            ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price, available = EXCLUDED.available;
                            """
                        ),
                        {"id": p["id"], "name": p["name"], "price": p["price"], "available": p["available"]},
                    )

                    await session.execute(
                        text(
                            """
                            INSERT INTO restaurant.restaurant_products (restaurant_id, product_id)
                            SELECT :rid, :pid
                            WHERE NOT EXISTS (
                                SELECT 1 FROM restaurant.restaurant_products
                                WHERE restaurant_id = :rid AND product_id = :pid
                            );
                            """
                        ),
                        {"rid": r["id"], "pid": p["id"]},
                    )

    await engine.dispose()
    logger.info("Seed data completed successfully!")


if __name__ == "__main__":
    asyncio.run(seed())
