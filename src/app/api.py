import json
import time
import uuid
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pika

from app import settings


class CommandState:
    def __init__(self) -> None:
        self.next_user_id = 501
        self.next_hotel_id = 301
        self.next_booking_id = 7001
        self.users: dict[int, dict] = {}
        self.hotels: dict[int, dict] = {}
        self.bookings: dict[int, dict] = {}


STATE = CommandState()
PUBLISHER = None


def event_time() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def read_model() -> dict:
    path = Path(settings.READ_MODEL_FILE)
    if not path.exists():
        return {
            "processed_event_ids": [],
            "users": {},
            "hotels": {},
            "bookings": {},
            "notifications": [],
        }
    return json.loads(path.read_text(encoding="utf-8"))


def contains_case_insensitive(value: str, pattern: str) -> bool:
    return pattern.lower() in value.lower()


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
    channel.confirm_delivery()
    return channel


class EventPublisher:
    def __init__(self) -> None:
        self.connection = rabbit_connection()
        self.channel = make_channel(self.connection)

    def publish(self, event_type: str, routing_key: str, payload: dict) -> dict:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_version": 1,
            "occurred_at": event_time(),
            "producer": "booking-api",
            "trace_id": str(uuid.uuid4()),
            "payload": payload,
        }
        self.channel.basic_publish(
            exchange=settings.EXCHANGE_NAME,
            routing_key=routing_key,
            body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=event["event_id"],
                type=event_type,
            ),
            mandatory=True,
        )
        return event


def publisher() -> EventPublisher:
    global PUBLISHER
    if PUBLISHER is None:
        PUBLISHER = EventPublisher()
    return PUBLISHER


class BookingApiHandler(BaseHTTPRequestHandler):
    server_version = "BookingEventApi/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/health":
            self.send_json(HTTPStatus.OK, {"status": "ok"})
            return
        if path == "/api/v1/users":
            self.find_users(query)
            return
        if path == "/api/v1/hotels":
            self.find_hotels(query)
            return
        if path.startswith("/api/v1/users/") and path.endswith("/bookings"):
            self.user_bookings(path)
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/v1/auth/register":
            self.register_user()
            return
        if path == "/api/v1/hotels":
            self.create_hotel()
            return
        if path == "/api/v1/bookings":
            self.create_booking()
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_DELETE(self) -> None:
        path = urlparse(self.path).path
        prefix = "/api/v1/bookings/"
        if path.startswith(prefix):
            self.cancel_booking(path.removeprefix(prefix))
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def send_json(self, status: HTTPStatus, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_json_array(self, status: HTTPStatus, body: list) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def register_user(self) -> None:
        body = self.read_json()
        login = body.get("login", "")
        first_name = body.get("first_name", "")
        last_name = body.get("last_name", "")
        if not login or not first_name or not last_name:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "login, first_name and last_name are required"})
            return

        user_id = STATE.next_user_id
        STATE.next_user_id += 1
        user = {
            "user_id": user_id,
            "login": login,
            "first_name": first_name,
            "last_name": last_name,
            "email": body.get("email", ""),
        }
        STATE.users[user_id] = user
        publisher().publish("UserRegistered", "user.registered", user)
        self.send_json(HTTPStatus.CREATED, {"id": user_id, **user})

    def find_users(self, query: dict[str, list[str]]) -> None:
        model = read_model()
        login = query.get("login", [""])[0]
        name = query.get("name", [""])[0]
        users = []
        for user in model["users"].values():
            if login and user.get("login") != login:
                continue
            full_name = f"{user.get('first_name', '')} {user.get('last_name', '')}"
            if name and not contains_case_insensitive(full_name, name):
                continue
            users.append(user)
        users.sort(key=lambda item: item["user_id"])
        self.send_json_array(HTTPStatus.OK, users)

    def create_hotel(self) -> None:
        body = self.read_json()
        title = body.get("title", "")
        city = body.get("city", "")
        address = body.get("address", "")
        rooms = int(body.get("rooms", 0))
        if not title or not city or not address or rooms <= 0:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "title, city, address and positive rooms are required"})
            return

        hotel_id = STATE.next_hotel_id
        STATE.next_hotel_id += 1
        hotel = {
            "hotel_id": hotel_id,
            "title": title,
            "city": city,
            "address": address,
            "rooms": rooms,
        }
        STATE.hotels[hotel_id] = hotel
        publisher().publish("HotelCreated", "hotel.created", hotel)
        self.send_json(HTTPStatus.CREATED, {"id": hotel_id, **hotel})

    def find_hotels(self, query: dict[str, list[str]]) -> None:
        model = read_model()
        city = query.get("city", [""])[0]
        hotels = []
        for hotel in model["hotels"].values():
            if city and hotel.get("city", "").lower() != city.lower():
                continue
            hotels.append(hotel)
        hotels.sort(key=lambda item: item["hotel_id"])
        self.send_json_array(HTTPStatus.OK, hotels)

    def create_booking(self) -> None:
        body = self.read_json()
        user_id = int(body.get("user_id", 0))
        hotel_id = int(body.get("hotel_id", 0))
        date_from = body.get("date_from", "")
        date_to = body.get("date_to", "")
        if user_id not in STATE.users or hotel_id not in STATE.hotels:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "user or hotel not found"})
            return
        if not date_from or not date_to or date_to <= date_from:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid booking dates"})
            return

        booking_id = STATE.next_booking_id
        STATE.next_booking_id += 1
        booking = {
            "booking_id": booking_id,
            "user_id": user_id,
            "hotel_id": hotel_id,
            "date_from": date_from,
            "date_to": date_to,
            "status": "active",
        }
        STATE.bookings[booking_id] = booking
        publisher().publish("BookingCreated", "booking.created", booking)
        self.send_json(HTTPStatus.CREATED, {"id": booking_id, **booking})

    def user_bookings(self, path: str) -> None:
        parts = path.strip("/").split("/")
        if len(parts) != 5:
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        try:
            user_id = int(parts[3])
        except ValueError:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid user id"})
            return

        model = read_model()
        bookings = [
            booking
            for booking in model["bookings"].values()
            if booking.get("user_id") == user_id
        ]
        bookings.sort(key=lambda item: item["date_from"], reverse=True)
        self.send_json_array(HTTPStatus.OK, bookings)

    def cancel_booking(self, booking_id_text: str) -> None:
        try:
            booking_id = int(booking_id_text)
        except ValueError:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid booking id"})
            return

        booking = STATE.bookings.get(booking_id)
        if booking is None:
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "booking not found"})
            return

        booking["status"] = "cancelled"
        event_payload = {
            "booking_id": booking_id,
            "user_id": booking["user_id"],
            "reason": "user request",
            "status": "cancelled",
        }
        publisher().publish("BookingCancelled", "booking.cancelled", event_payload)
        self.send_json(HTTPStatus.OK, {"id": booking_id, **booking})

    def log_message(self, fmt: str, *args) -> None:
        print("%s - %s" % (self.address_string(), fmt % args))


def main() -> None:
    publisher()
    server = ThreadingHTTPServer(("0.0.0.0", settings.API_PORT), BookingApiHandler)
    print(f"booking API listens on 0.0.0.0:{settings.API_PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
