import csv
import os
from typing import Any

from common.amqp_utils import decode_json, declare_queues, get_connection

QUEUE_Y_TRUE = os.getenv("QUEUE_Y_TRUE", "y_true")
QUEUE_Y_PRED = os.getenv("QUEUE_Y_PRED", "y_pred")
LOG_FILE = os.getenv("METRIC_LOG_PATH", "/app/logs/metric_log.csv")

buffer: dict[str, dict[str, float | None]] = {}


def init_log_file() -> None:
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    if not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0:
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["id", "y_true", "y_pred", "absolute_error"])

        print(f"[metric] initialized log file: {LOG_FILE}")


def append_metric_row(
    message_id: str, y_true: float, y_pred: float, absolute_error: float
) -> None:
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([message_id, y_true, y_pred, absolute_error])


def process_message(field_name: str, message: dict[str, Any]) -> None:
    message_id = str(message["id"])
    value = float(message["body"])

    if message_id not in buffer:
        buffer[message_id] = {"y_true": None, "y_pred": None}

    buffer[message_id][field_name] = value

    y_true = buffer[message_id]["y_true"]
    y_pred = buffer[message_id]["y_pred"]

    if y_true is not None and y_pred is not None:
        absolute_error = abs(y_true - y_pred)
        append_metric_row(message_id, y_true, y_pred, absolute_error)

        print(
            f"[metric] id={message_id}, y_true={y_true}, y_pred={y_pred:.4f}, "
            f"absolute_error={absolute_error:.4f}"
        )

        del buffer[message_id]


def make_callback(field_name: str):
    def callback(ch, method, properties, body) -> None:
        try:
            message = decode_json(body)
            process_message(field_name, message)
        except Exception as exc:
            print(f"[metric] error while processing {field_name}: {exc}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    return callback


def main() -> None:
    init_log_file()

    connection = get_connection()
    channel = connection.channel()
    declare_queues(channel, [QUEUE_Y_TRUE, QUEUE_Y_PRED])
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=QUEUE_Y_TRUE,
        on_message_callback=make_callback("y_true"),
    )
    channel.basic_consume(
        queue=QUEUE_Y_PRED,
        on_message_callback=make_callback("y_pred"),
    )

    print("[metric] Waiting for y_true and y_pred messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()