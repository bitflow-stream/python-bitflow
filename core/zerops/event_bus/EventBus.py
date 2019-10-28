import copy
import logging
import threading
import rabbitpy

import core.zerops.utils as utils
from core.zerops.event_bus.EventBusMessage import EventBusMessage


class EventBus:
    ENV_RABBITMQ_URI = "ZEROPS_RABBITMQ_URI"
    ENV_RABBITMQ_EXCHANGE_NAME = "ZEROPS_RABBITMQ_EXCHANGE_NAME"
    ENV_RABBITMQ_DEFAULT_HEADERS = "ZEROPS_RABBITMQ_SEND_HEADERS"

    def __init__(self, url=None, exchange_name=None, default_headers=None):
        if not url:
            url = utils.get_env(self.ENV_RABBITMQ_URI)
        self.url = url

        if not exchange_name:
            exchange_name = utils.get_env(self.ENV_RABBITMQ_EXCHANGE_NAME)
        self.exchange_name = exchange_name
        self.routing_key = exchange_name

        if not default_headers:
            try:
                default_headers = utils.get_env_var_map(self.ENV_RABBITMQ_DEFAULT_HEADERS)
            except IOError:
                logging.warning("Failed to get default event message headers from environment variable {}. "
                                "Not attaching any default headers.".format(self.ENV_RABBITMQ_DEFAULT_HEADERS))
                default_headers = {}
        self.default_headers = default_headers

        self.receive_callback = None

        logging.info("Connecting to event bus at {} with exchange name {}.".format(self.url, self.exchange_name))
        self.connection = rabbitpy.Connection(self.url)
        self.publish_channel = self.connection.channel()
        self.consume_channel = self.connection.channel()

        self.stop_threads = False
        self.consumer_threads = []

        self.__create_header_exchange(self.exchange_name)

    def __create_header_exchange(self, name):
        # Declare the exchange
        rabbitpy.HeadersExchange(self.publish_channel, self.exchange_name, durable=True).declare()
        logging.info("Created rabbitmq exchange with name: {}".format(name))

    def __create_and_bind_queue(self, filter_arg):
        # Declare the queue
        queue = rabbitpy.Queue(self.consume_channel, durable=True)
        queue.declare()
        # Bind the queue to the exchange
        queue.bind(self.exchange_name, self.routing_key, arguments=filter_arg)
        logging.info("Created Queue {} is bound to {} with filter {}.".format(queue, self.exchange_name, filter_arg))
        return queue

    def __fill_headers(self, headers):
        if self.default_headers:
            headers = copy.deepcopy(headers)
            headers = {**headers, **self.default_headers}
        return headers

    # Publishes an EventBusMessage with the given header on the EventBus.
    def publish_message(self, header, message):
        props = {"headers": header, "delivery_mode": 2, "priority": 1}
        payload = message.get_message()
        message = rabbitpy.Message(self.publish_channel, body_value=payload, properties=props)
        message.publish(self.exchange_name, self.routing_key)

    def get_consume_channel(self):
        return self.consume_channel

    # Receive messages on the EventBus which match the given filter and handle the messages
    # with the given receive_callback
    def receive_messages(self, filter_arg, callback):
        thread = threading.Thread(target=self.__run_receive_message,
                                  kwargs={"filter_arg": filter_arg, "callback": callback})
        self.consumer_threads.append(thread)
        thread.start()

    def __run_receive_message(self, filter_arg, callback):
        queue = self.__create_and_bind_queue(filter_arg)
        while not self.stop_threads:
            try:
                for message in queue.consume():
                    event_bus_message = EventBusMessage(message.body)
                    header = {k: v.decode() for k, v in message.properties["headers"].items()}
                    callback(header, event_bus_message)
                    if self.stop_threads:
                        break
            except Exception as e:
                logging.info("Closing...")
                break

    def close(self):
        if self.publish_channel.open:
            self.publish_channel.close()
        if self.consume_channel.open:
            self.consume_channel.close()
        if self.consumer_threads:
            self.stop_threads = True
            for thread in self.consumer_threads:
                thread.join()
