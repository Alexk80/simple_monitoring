import json
import os
import time
from typing import Any, Iterable

import pika


def get_connection() -> pika.BlockingConnection:
    host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASSWORD", "guest")
    retry_delay = int(os.getenv("RABBITMQ_RETRY_DELAY_SECONDS", "5"))

    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )

    while True:
        try:
            print(f"[amqp] Connecting to RabbitMQ {host}:{port} ...")
            connection = pika.BlockingConnection(parameters)
            print("[amqp] Connected.")
            return connection
        except pika.exceptions.AMQPConnectionError as exc:
            print(f"[amqp] Connection error: {exc}. Retry in {retry_delay}s.")
            time.sleep(retry_delay)


def declare_queues(channel, queue_names: Iterable[str]) -> None:
    for queue_name in queue_names:
        channel.queue_declare(queue=queue_name, durable=True)


def publish_json(channel, queue_name: str, message: dict[str, Any]) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
    )


def decode_json(body: bytes) -> dict[str, Any]:
    return json.loads(body.decode("utf-8"))