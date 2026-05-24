# Система бронирования отелей - событийная архитектура

В этой работе добавлена передача событий для бронирования отелей. HTTP API принимает команды, после успешного изменения write-модели публикует события в RabbitMQ, а consumer читает очередь и собирает отдельное представление для чтения.

Подробное описание находится в [`event_driven_design.md`](./event_driven_design.md), список сообщений - в [`event_catalog.md`](./event_catalog.md).

## Запуск

```bash
docker compose up -d --build rabbitmq api consumer
docker compose run --rm producer
```

API будет доступен на:

```text
http://localhost:8084
```

После отправки можно открыть получившиеся файлы:

```bash
cat data/read_model.json
cat data/events.jsonl
```

Веб-интерфейс RabbitMQ:

- `http://localhost:15674`
- `guest / guest`

Остановить контейнеры:

```bash
docker compose down
```

## Стек

- **Python 3.12**
- **HTTP API на стандартной библиотеке Python**
- **RabbitMQ 3.13 Management**
- **pika**
- **Docker Compose**

## Что сделано

- поднят RabbitMQ;
- добавлен `Booking API`, который принимает команды через HTTP;
- создан `exchange` с именем `booking.events`;
- сообщения попадают в очередь `booking.read_model`;
- события пользователя, отеля и бронирования публикуются из [`src/app/api.py`](./src/app/api.py) после обработки команд;
- [`src/app/producer.py`](./src/app/producer.py) теперь выступает клиентом API для проверки полного сценария и read-side запросов;
- обработка очереди находится в [`src/app/consumer.py`](./src/app/consumer.py);
- состояние для чтения сохраняется в `data/read_model.json`;
- полный журнал полученных событий пишется в `data/events.jsonl`;
- подтверждение сообщения отправляется после записи результата;
- повторная обработка отсекается по `event_id`.

## События

| Ключ маршрутизации | Событие | Источник | Для чего используется |
|---|---|---|---|
| `user.registered` | `UserRegistered` | Booking API / сервис пользователей | добавить пользователя в представление для чтения |
| `hotel.created` | `HotelCreated` | Booking API / сервис отелей | добавить новый отель |
| `booking.created` | `BookingCreated` | Booking API / сервис бронирований | сохранить активную бронь |
| `booking.cancelled` | `BookingCancelled` | Booking API / сервис бронирований | поменять статус брони на отмененный |

## CQRS

Командная часть меняет данные: регистрирует пользователя, создает отель, создает или отменяет бронь. После изменения публикуется событие.

В этой реализации API покрывает операции варианта. Команды меняют write-модель и публикуют события, запросы читают `data/read_model.json`, который обновляет consumer.

| Метод | Путь | Операция | CQRS | Событие |
|---|---|---|---|---|
| `POST` | `/api/v1/auth/register` | Создание пользователя | command | `UserRegistered` |
| `GET` | `/api/v1/users?login=...` | Поиск пользователя по логину | query | - |
| `GET` | `/api/v1/users?name=...` | Поиск пользователя по имени или фамилии | query | - |
| `POST` | `/api/v1/hotels` | Создание отеля | command | `HotelCreated` |
| `GET` | `/api/v1/hotels` | Получение списка отелей | query | - |
| `GET` | `/api/v1/hotels?city=...` | Поиск отелей по городу | query | - |
| `POST` | `/api/v1/bookings` | Создание бронирования | command | `BookingCreated` |
| `GET` | `/api/v1/users/{id}/bookings` | Получение бронирований пользователя | query | - |
| `DELETE` | `/api/v1/bookings/{id}` | Отмена бронирования | command | `BookingCancelled` |

Обработчик очереди команды не принимает. Он только читает уже произошедшие события и на их основе обновляет файл:

```text
data/read_model.json
```

## Доставка

Используется гарантия `at-least-once`:

- `exchange` и очередь объявлены как устойчивые;
- сообщения отправляются с `delivery_mode=2`;
- `ack` отправляется после записи представления для чтения;
- уже обработанные `event_id` не применяются второй раз.

Гарантии `exactly-once` здесь нет, поэтому повторные сообщения проверяются на стороне обработчика.

## Тест

```bash
bash tests/full_flow.sh
```

Скрипт поднимает RabbitMQ, API и consumer, отправляет HTTP-команды через сценарный клиент, проверяет read-side ответы API и затем проверяет `read_model.json` вместе с `events.jsonl`.

## Соответствие заданию

| Пункт задания | Где выполнено |
|---|---|
| Анализ событий и команд | [`event_driven_design.md`](./event_driven_design.md), разделы "Команды и события" и "Связь с API" |
| Производители, потребители, payload и поток событий | [`event_driven_design.md`](./event_driven_design.md), [`event_catalog.md`](./event_catalog.md) |
| RabbitMQ, формат сообщений и гарантии доставки | [`event_driven_design.md`](./event_driven_design.md), разделы "RabbitMQ", "Формат события", "Доставка" |
| CQRS, разделение command/query и синхронизация моделей | раздел "CQRS" в этом README и в [`event_driven_design.md`](./event_driven_design.md) |
| Producer, consumer и проверка взаимодействия | [`src/app/api.py`](./src/app/api.py), [`src/app/producer.py`](./src/app/producer.py), [`src/app/consumer.py`](./src/app/consumer.py), [`tests/full_flow.sh`](./tests/full_flow.sh) |
| Каталог событий | [`event_catalog.md`](./event_catalog.md) |

## Файлы

- `event_driven_design.md` - описание событийного взаимодействия;
- `event_catalog.md` - каталог событий;
- `src/app/api.py` - HTTP API, которое публикует события в RabbitMQ;
- `src/app/producer.py` - сценарный клиент, вызывающий API;
- `src/app/consumer.py` - чтение очереди и обновление файла для чтения;
- `docker-compose.yml` - контейнеры RabbitMQ и Python-скриптов;
- `tests/full_flow.sh` - проверка полного сценария.
