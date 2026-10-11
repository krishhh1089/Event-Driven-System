FROM python:3.12-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy shared libraries first (leverage Docker layer cache)
COPY common/common_domain /app/common/common_domain
COPY common/common_application /app/common/common_application
COPY common/common_dataaccess /app/common/common_dataaccess
COPY common/infrastructure/saga /app/common/infrastructure/saga
COPY common/infrastructure/outbox /app/common/infrastructure/outbox
COPY common/infrastructure/kafka /app/common/infrastructure/kafka

# Install shared libraries
RUN pip install --no-cache-dir \
    /app/common/common_domain \
    /app/common/common_application \
    /app/common/common_dataaccess \
    /app/common/infrastructure/saga \
    /app/common/infrastructure/outbox \
    /app/common/infrastructure/kafka

# Copy service source
COPY services/restaurant_service /app/services/restaurant_service

# Install service dependencies
RUN pip install --no-cache-dir /app/services/restaurant_service

WORKDIR /app/services/restaurant_service

EXPOSE 8183

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
    CMD curl -f http://localhost:8183/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8183"]
