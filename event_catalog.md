# Каталог событий

Общие поля всех событий:

| Поле | Тип | Описание |
|---|---|---|
| `event_id` | `string uuid` | идентификатор события |
| `event_type` | `string` | название события |
| `event_version` | `integer` | версия схемы |
| `occurred_at` | `string datetime` | время создания события |
| `producer` | `string` | источник события |
| `trace_id` | `string uuid` | идентификатор цепочки запроса |
| `payload` | `object` | данные события |

Для всех событий используется доставка `at-least-once`.

## Связь событий с API

Все события публикуются не тестовым скриптом напрямую, а HTTP API после успешной обработки команды.

| API endpoint | Команда | Routing key | Событие |
|---|---|---|---|
| `POST /api/v1/auth/register` | `RegisterUser` | `user.registered` | `UserRegistered` |
| `POST /api/v1/hotels` | `CreateHotel` | `hotel.created` | `HotelCreated` |
| `POST /api/v1/bookings` | `CreateBooking` | `booking.created` | `BookingCreated` |
| `DELETE /api/v1/bookings/{id}` | `CancelBooking` | `booking.cancelled` | `BookingCancelled` |

Источник в поле `producer` для этих событий: `booking-api`.

## UserRegistered

Ключ маршрутизации: `user.registered`

Публикует: `Booking API` после команды `RegisterUser`.

Гарантия доставки: `at-least-once`.

Читают:

- построение представления для чтения;
- уведомления;
- аудит.

Данные события:

```json
{
  "user_id": 501,
  "login": "event_user",
  "first_name": "Petr",
  "last_name": "Sokolov",
  "email": "event_user@example.com"
}
```

Событие нужно, чтобы в представлении для чтения появился новый пользователь.

## HotelCreated

Ключ маршрутизации: `hotel.created`

Публикует: `Booking API` после команды `CreateHotel`.

Гарантия доставки: `at-least-once`.

Читают:

- построение представления для чтения;
- поиск и аналитика;
- аудит.

Данные события:

```json
{
  "hotel_id": 301,
  "title": "Event Hotel",
  "city": "Moscow",
  "address": "Arbat 10",
  "rooms": 12
}
```

Событие нужно, чтобы новый отель попал в каталог и поисковое представление.

## BookingCreated

Ключ маршрутизации: `booking.created`

Публикует: `Booking API` после команды `CreateBooking`.

Гарантия доставки: `at-least-once`.

Читают:

- построение представления для чтения;
- уведомления;
- аналитика;
- аудит.

Данные события:

```json
{
  "booking_id": 7001,
  "user_id": 501,
  "hotel_id": 301,
  "date_from": "2026-12-01",
  "date_to": "2026-12-04",
  "status": "active"
}
```

Событие нужно, чтобы бронь появилась в представлении для чтения и пользователь получил подтверждение.

## BookingCancelled

Ключ маршрутизации: `booking.cancelled`

Публикует: `Booking API` после команды `CancelBooking`.

Гарантия доставки: `at-least-once`.

Читают:

- построение представления для чтения;
- уведомления;
- аналитика;
- аудит.

Данные события:

```json
{
  "booking_id": 7001,
  "user_id": 501,
  "reason": "user request",
  "status": "cancelled"
}
```

Событие нужно, чтобы у брони изменился статус и пользователь получил уведомление об отмене.
