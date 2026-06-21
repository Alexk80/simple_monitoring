import os
import random
import numpy as np
from common.amqp_utils import decode_json, declare_queues, get_connection, publish_json

QUEUE_FEATURES = os.getenv("QUEUE_FEATURES", "features")
QUEUE_Y_PRED = os.getenv("QUEUE_Y_PRED", "y_pred")

def main() -> None:

    connection = get_connection()
    channel = connection.channel()
    declare_queues(channel, [QUEUE_FEATURES, QUEUE_Y_PRED])
    channel.basic_qos(prefetch_count=1)

    print("[model] Waiting for feature messages...")

    def callback(ch, method, properties, body) -> None:
        try:
            message = decode_json(body)
            message_id = str(message["id"])
            features = np.asarray(message["body"], dtype=float)
            y_pred = 2 * features[0] + 3 * features[1] + 0.5 * features[2] + random.uniform(-10, 10)

            publish_json(
                ch,
                QUEUE_Y_PRED,
                {
                    "id": message_id,
                    "body": y_pred,
                },
            )

            print(f"[model] predicted id={message_id}, y_pred={y_pred:.4f}")
        except Exception as exc:
            print(f"[model] processing error: {exc}")
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_FEATURES, on_message_callback=callback)
    channel.start_consuming()


if __name__ == "__main__":
    main()