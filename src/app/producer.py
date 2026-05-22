import json
import time
import uuid
from datetime import UTC, datetime

import pika

from app import settings


def event_time() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def pack_message(event_type: str, routing_key: str, payload: dict) -> tuple[str, dict]:
    data = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": event_time(),
        "producer": "booking-api",
        "trace_id": str(uuid.uuid4()),
        "payload": payload,
    }
    return routing_key, data


def booking_story() -> list[tuple[str, dict]]:
    return [
        pack_message(
            "UserRegistered",
            "user.registered",
            {
                "user_id": 501,
                "login": "event_user",
                "first_name": "Petr",
                "last_name": "Sokolov",
                "email": "event_user@example.com",
            },
        ),
        pack_message(
            "HotelCreated",
            "hotel.created",
            {
                "hotel_id": 301,
                "title": "Event Hotel",
                "city": "Moscow",
                "address": "Arbat 10",
                "rooms": 12,
            },
        ),
        pack_message(
            "BookingCreated",
            "booking.created",
            {
                "booking_id": 7001,
                "user_id": 501,
                "hotel_id": 301,
                "date_from": "2026-12-01",
                "date_to": "2026-12-04",
                "status": "active",
            },
        ),
        pack_message(
            "BookingCancelled",
            "booking.cancelled",
            {
                "booking_id": 7001,
                "user_id": 501,
                "reason": "user request",
                "status": "cancelled",
            },
        ),
    ]


def rabbit_connection() -> pika.BlockingConnection:
    params = pika.ConnectionParameters(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        heartbeat=30,
        blocked_connection_timeout=30,
    )

    last_error = None
    for _ in range(60):
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"RabbitMQ is not available: {last_error}")


def main() -> None:
    connection = rabbit_connection()
    channel = connection.channel()
    channel.exchange_declare(
        exchange=settings.EXCHANGE_NAME,
        exchange_type="topic",
        durable=True,
    )
    channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)
    channel.queue_bind(
        exchange=settings.EXCHANGE_NAME,
        queue=settings.QUEUE_NAME,
        routing_key="#",
    )
    channel.confirm_delivery()

    for routing_key, data in booking_story():
        channel.basic_publish(
            exchange=settings.EXCHANGE_NAME,
            routing_key=routing_key,
            body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=data["event_id"],
                type=data["event_type"],
            ),
            mandatory=True,
        )
        print(f"sent {data['event_type']} as {routing_key}")

    connection.close()


if __name__ == "__main__":
    main()
