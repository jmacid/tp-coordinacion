import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data message for {client_id[:8]}: {fruit} - {amount}")

        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []

        client_fruit_top = self.fruit_top_by_client[client_id]

        for i in range(len(client_fruit_top)):
            if client_fruit_top[i].fruit == fruit:
                updated_item = client_fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                client_fruit_top.pop(i)
                bisect.insort(client_fruit_top, updated_item)
                return

        bisect.insort(client_fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF for {client_id[:8]}")

        if client_id not in self.eof_count_by_client:
            self.eof_count_by_client[client_id] = 0
        self.eof_count_by_client[client_id] += 1

        if self.eof_count_by_client[client_id] < SUM_AMOUNT:
            return

        logging.info(f"Received ALL EOF for {client_id[:8]}")
        if client_id not in self.fruit_top_by_client:
             self.fruit_top_by_client[client_id] = []

        fruit_chunk = list(self.fruit_top_by_client[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top]))
        del self.fruit_top_by_client[client_id]
        del self.eof_count_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
