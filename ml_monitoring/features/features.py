import os
import random
import time
from datetime import datetime

from common.amqp_utils import declare_queues, get_connection, publish_json

QUEUE_FEATURES = os.getenv("QUEUE_FEATURES", "features")
QUEUE_Y_TRUE = os.getenv("QUEUE_Y_TRUE", "y_true")
STREAM_DELAY_SECONDS = int(os.getenv("STREAM_DELAY_SECONDS", "10"))

def data_generator():
    while True:
        features = [
            random.uniform(0, 100),
            random.uniform(0, 50),
            random.uniform(0, 200)
        ]
        y_true = 2 * features[0] + 3 * features[1] + 0.5 * features[2] + random.uniform(-10, 10)
        yield features, y_true

def main() -> None:
    gen = data_generator()
    x, y = next(gen)
    message_id = f"{datetime.now().timestamp():.6f}"
    message_features = {"id": message_id, "body": x}
    message_y_true = {"id": message_id, "body": y}
    connection = get_connection()
    channel = connection.channel()
    declare_queues(channel, [QUEUE_FEATURES, QUEUE_Y_TRUE])

    print(f"[features] Start infinite stream. Delay={STREAM_DELAY_SECONDS}s")

    while True:
        publish_json(channel, QUEUE_FEATURES, message_features)
        publish_json(channel, QUEUE_Y_TRUE, message_y_true)

        print(
            f"[features] sent id={message_id}, row={x}, y_true={y}"
        )
        time.sleep(STREAM_DELAY_SECONDS)


if __name__ == "__main__":
    main()