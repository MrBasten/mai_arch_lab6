import json
import time
from urllib.error import URLError
from urllib.request import Request, urlopen

from app import settings


def call_api(method: str, path: str, payload: dict) -> dict | list:
    data = json.dumps(payload).encode("utf-8") if payload else None
    request = Request(
        settings.API_BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_api() -> None:
    last_error = None
    for _ in range(30):
        try:
            call_api("GET", "/health", {})
            return
        except (TimeoutError, URLError) as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Booking API is not available: {last_error}")


def wait_read_api(user_id: int, hotel_id: int, booking_id: int) -> None:
    last_error = "read model is empty"
    for _ in range(30):
        users = call_api("GET", "/api/v1/users?login=event_user", {})
        hotels = call_api("GET", "/api/v1/hotels?city=Moscow", {})
        bookings = call_api("GET", f"/api/v1/users/{user_id}/bookings", {})

        has_user = any(item.get("user_id") == user_id for item in users)
        has_hotel = any(item.get("hotel_id") == hotel_id for item in hotels)
        has_booking = any(
            item.get("booking_id") == booking_id and item.get("status") == "cancelled"
            for item in bookings
        )
        if has_user and has_hotel and has_booking:
            print("read API returned user, hotel and cancelled booking")
            return

        last_error = {
            "users": users,
            "hotels": hotels,
            "bookings": bookings,
        }
        time.sleep(1)
    raise RuntimeError(f"Read API was not synchronized: {last_error}")


def main() -> None:
    wait_api()

    user = call_api(
        "POST",
        "/api/v1/auth/register",
        {
            "login": "event_user",
            "first_name": "Petr",
            "last_name": "Sokolov",
            "email": "event_user@example.com",
        },
    )
    print(f"POST /api/v1/auth/register -> {user}")

    hotel = call_api(
        "POST",
        "/api/v1/hotels",
        {
            "title": "Event Hotel",
            "city": "Moscow",
            "address": "Arbat 10",
            "rooms": 12,
        },
    )
    print(f"POST /api/v1/hotels -> {hotel}")

    booking = call_api(
        "POST",
        "/api/v1/bookings",
        {
            "user_id": user["id"],
            "hotel_id": hotel["id"],
            "date_from": "2026-12-01",
            "date_to": "2026-12-04",
        },
    )
    print(f"POST /api/v1/bookings -> {booking}")

    cancelled = call_api("DELETE", f"/api/v1/bookings/{booking['id']}", {})
    print(f"DELETE /api/v1/bookings/{booking['id']} -> {cancelled}")
    wait_read_api(user["id"], hotel["id"], booking["id"])


if __name__ == "__main__":
    main()
