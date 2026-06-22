import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "tripforge")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "tripforge_pass")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "pricing")

# Existing public topology names that must remain stable.
PRICING_EXCHANGE = "pricing.exchange"
PRICING_ROUTING_KEY = "price.refresh"
PRICING_QUEUE = "pricing.refresh.q"
PRICING_DLQ = "pricing.refresh.dlq"

# Additional topology used to implement delayed bounded retries and DLQ routing.
PRICING_DLX = "pricing.dlx"
PRICING_DLQ_ROUTING_KEY = "pricing.refresh.dlq"
PRICING_RETRY_EXCHANGE = "pricing.retry.exchange"
PRICING_RETRY_QUEUE = "pricing.refresh.retry.q"
PRICING_RETRY_ROUTING_KEY = "price.refresh.retry"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "5000"))
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "5"))
