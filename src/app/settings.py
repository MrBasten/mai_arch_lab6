import os


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
API_PORT = int(os.getenv("API_PORT", "8080"))
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080")

EXCHANGE_NAME = "booking.events"
QUEUE_NAME = "booking.read_model"

READ_MODEL_FILE = os.getenv("READ_MODEL_FILE", "data/read_model.json")
EVENTS_LOG_FILE = os.getenv("EVENTS_LOG_FILE", "data/events.jsonl")
