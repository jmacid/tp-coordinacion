import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client_and_fruit = {}
        self.lock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"[_process_data]: Process data for client {client_id[:8]}: {fruit} - {amount}")
        with self.lock:
            if client_id not in self.amount_by_client_and_fruit:
                self.amount_by_client_and_fruit[client_id] = {}

            client_data = self.amount_by_client_and_fruit[client_id]
            client_data[fruit] = client_data.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Enviando resultados parciales para {client_id[:8]}")

        with self.lock:
            if client_id in self.amount_by_client_and_fruit:
                for final_fruit_item in self.amount_by_client_and_fruit[client_id].values():
                    hash_val = int(hashlib.md5(final_fruit_item.fruit.encode('utf-8')).hexdigest(), 16)
                    aggregator_index = hash_val % AGGREGATION_AMOUNT

                    self.data_output_exchanges[aggregator_index].send(
                        message_protocol.internal.serialize(
                            [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                        )
                    )
                del self.amount_by_client_and_fruit[client_id]

        logging.info(f"Enviando señal EOF a los aggregators para {client_id[:8]}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            logging.info(f"Retransmitiendo EOF a todos los SUMs para {fields[0][:8]}")
            self.control_exchange.send(message)
        ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._process_eof(fields[0])
        ack()

    def start(self):
        self.thread_pcm = threading.Thread(target=self.control_exchange.start_consuming, args=(self.process_control_message,))
        self.thread_pcm.start()

        self.input_queue.start_consuming(self.process_data_message)
        self.thread_pcm.join()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
