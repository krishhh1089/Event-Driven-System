-- =============================================================================
-- PostgreSQL Initialization Script
-- =============================================================================
-- Creates logical schemas for each microservice.
-- In production, each service would connect to its own PostgreSQL instance.
-- Here they share one instance for local development convenience.
-- =============================================================================

-- Create schemas (quoting "order" because it is a reserved word in SQL)
CREATE SCHEMA IF NOT EXISTS "order";
CREATE SCHEMA IF NOT EXISTS payment;
CREATE SCHEMA IF NOT EXISTS restaurant;
CREATE SCHEMA IF NOT EXISTS customer;

-- Grant all privileges to the application user
GRANT ALL PRIVILEGES ON SCHEMA "order"     TO food_user;
GRANT ALL PRIVILEGES ON SCHEMA payment     TO food_user;
GRANT ALL PRIVILEGES ON SCHEMA restaurant  TO food_user;
GRANT ALL PRIVILEGES ON SCHEMA customer    TO food_user;

-- Set default search path so Alembic migrations work per-service
-- (each service sets its own search_path in its DB URL or connection config)
