import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}

    def process_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        partial_top = fields[1]

        logging.info(f"Recibido Top Parcial del aggregator para {client_id[:8]}")

        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []
            self.eof_count_by_client[client_id] = 0

        for fruit, amount in partial_top:
            bisect.insort(self.fruit_top_by_client[client_id], fruit_item.FruitItem(fruit, amount))

        self.eof_count_by_client[client_id] += 1

        if self.eof_count_by_client[client_id] == AGGREGATION_AMOUNT:
            logging.info(f"Recibidos todos los tops parciales para {client_id[:8]}. Enviando Top Final.")

            fruit_chunk = list(self.fruit_top_by_client[client_id][-TOP_SIZE:])
            fruit_chunk.reverse()

            final_top = list(
                map(
                    lambda f_item: (f_item.fruit, f_item.amount),
                    fruit_chunk,
                )
            )

            self.output_queue.send(message_protocol.internal.serialize([client_id, final_top]))

            del self.fruit_top_by_client[client_id]
            del self.eof_count_by_client[client_id]

        ack()

    def handle_sigterm(self, signum, frame):
        logging.info("SIGTERM recibido")
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
