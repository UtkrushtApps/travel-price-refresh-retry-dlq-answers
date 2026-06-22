# Solution Steps

1. Keep the public API unchanged: the FastAPI handler should only create a job_id and correlation_id, publish a JSON message to RabbitMQ, and return 202 without calling any supplier code.

2. Extend configuration with retry and dead-letter topology names while preserving the required exchange pricing.exchange, routing key price.refresh, primary queue pricing.refresh.q, and DLQ pricing.refresh.dlq.

3. Declare a durable DLX and bind pricing.refresh.dlq to it. Declare pricing.refresh.q with x-dead-letter-exchange and x-dead-letter-routing-key so basic_nack(..., requeue=False) sends poison or exhausted messages to the DLQ.

4. Declare a durable retry exchange and retry queue. Configure the retry queue with x-message-ttl for the delay and x-dead-letter-exchange/x-dead-letter-routing-key pointing back to pricing.exchange/price.refresh so expired retry messages return to the primary queue.

5. Publish API jobs as persistent application/json messages and include correlation_id plus an x-retry-count header initialized to 0.

6. In the worker, set basic_qos(prefetch_count=PREFETCH_COUNT) before consuming and keep auto_ack=False so RabbitMQ does not flood the worker and messages remain unacked until processing completes.

7. Validate message structure and content_type before supplier processing. Invalid JSON, non-object bodies, missing required fields, or permanent job errors should be negatively acknowledged with requeue=False so they are routed to pricing.refresh.dlq.

8. On successful refresh_supplier_prices(job), acknowledge the message only after the function returns successfully.

9. On transient or recoverable failures, read x-retry-count. If it is below MAX_RETRIES, publish the same body to the retry exchange with x-retry-count incremented, preserving correlation_id and content_type=application/json, then ack the original message.

10. If x-retry-count is already at MAX_RETRIES, do not requeue. Nack the original message with requeue=False so RabbitMQ dead-letters it to pricing.refresh.dlq.

