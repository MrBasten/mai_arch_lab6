import os


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))

EXCHANGE_NAME = "booking.events"
QUEUE_NAME = "booking.read_model"

READ_MODEL_FILE = os.getenv("READ_MODEL_FILE", "data/read_model.json")
EVENTS_LOG_FILE = os.getenv("EVENTS_LOG_FILE", "data/events.jsonl")

