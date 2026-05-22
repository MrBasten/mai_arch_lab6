import json
import time
from pathlib import Path

import pika

from app import settings


def blank_state() -> dict:
    return {
        "processed_event_ids": [],
        "users": {},
        "hotels": {},
        "bookings": {},
        "notifications": [],
    }


def read_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return blank_state()


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(text, encoding="utf-8")


def save_event_line(path: Path, event: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def change_state(state: dict, event: dict) -> bool:
    event_id = event["event_id"]
    if event_id in state["processed_event_ids"]:
        return False

    payload = event["payload"]
    event_type = event["event_type"]

    if event_type == "UserRegistered":
        state["users"][str(payload["user_id"])] = payload
    elif event_type == "HotelCreated":
        state["hotels"][str(payload["hotel_id"])] = payload
    elif event_type == "BookingCreated":
        state["bookings"][str(payload["booking_id"])] = payload
        state["notifications"].append({
            "kind": "booking_created",
            "booking_id": payload["booking_id"],
            "user_id": payload["user_id"],
        })
    elif event_type == "BookingCancelled":
        booking = state["bookings"].setdefault(
            str(payload["booking_id"]),
            {"booking_id": payload["booking_id"], "user_id": payload["user_id"]},
        )
        booking["status"] = "cancelled"
        booking["cancel_reason"] = payload.get("reason", "")
        state["notifications"].append({
            "kind": "booking_cancelled",
            "booking_id": payload["booking_id"],
            "user_id": payload["user_id"],
        })
    else:
        raise ValueError(f"Unknown event type: {event_type}")

    state["processed_event_ids"].append(event_id)
    return True


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


def make_channel(connection: pika.BlockingConnection) -> pika.channel.Channel:
    channel = connection.channel()
    channel.exchange_declare(exchange=settings.EXCHANGE_NAME, exchange_type="topic", durable=True)
    channel.queue_declare(queue=settings.QUEUE_NAME, durable=True)
    channel.queue_bind(exchange=settings.EXCHANGE_NAME, queue=settings.QUEUE_NAME, routing_key="#")
    channel.basic_qos(prefetch_count=10)
    return channel


def process_delivery(channel, method, body: bytes, state_file: Path, journal_file: Path) -> None:
    state = read_state(state_file)
    event = json.loads(body.decode("utf-8"))

    if change_state(state, event):
        save_state(state_file, state)
        save_event_line(journal_file, event)
        print(f"processed {event['event_type']}")

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> None:
    state_file = Path(settings.READ_MODEL_FILE)
    journal_file = Path(settings.EVENTS_LOG_FILE)
    connection = rabbit_connection()
    channel = make_channel(connection)

    print("waiting for events")
    while True:
        method, _, body = channel.basic_get(queue=settings.QUEUE_NAME, auto_ack=False)
        if method is None:
            time.sleep(0.5)
            continue

        try:
            process_delivery(channel, method, body, state_file, journal_file)
        except Exception as exc:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print(f"failed to process message: {exc}")


if __name__ == "__main__":
    main()
