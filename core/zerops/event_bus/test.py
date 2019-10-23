import json

import rabbitpy
import threading

EXCHANGE = 'threading_example1'
QUEUE = 'threading_queue'
ROUTING_KEY = ''
MESSAGE_COUNT = 100


def consumer(connection):
    """Consume MESSAGE_COUNT messages on the connection and then exit.

    :param rabbitpy.Connection connection: The connection to consume on

    """
    received = 0
    with connection.channel() as channel:
        for message in rabbitpy.Queue(channel, QUEUE).consume():
            print(message.body)
            message.ack()
            received += 1
            if received == MESSAGE_COUNT:
                break


def publisher(connection):
    """Pubilsh up to MESSAGE_COUNT messages on connection
    on an individual thread.

    :param rabbitpy.Connection connection: The connection to publish on

    """
    with connection.channel() as channel:
        for index in range(0, MESSAGE_COUNT):
            message = rabbitpy.Message(channel, 'Message #%i' % index)
            message.publish(EXCHANGE, ROUTING_KEY)


# Connect to RabbitMQ
with rabbitpy.Connection("amqp://user:password@localhost:5672/zerops") as connection:

    # Open the channel, declare and bind the exchange and queue
    with connection.channel() as channel:

        # Declare the exchange
        exchange = rabbitpy.HeadersExchange(channel, EXCHANGE)
        exchange.declare()

        # Declare the queue
        queue = rabbitpy.Queue(channel)
        queue.declare()

        # Bind the queue to the exchange
        queue.bind(EXCHANGE, ROUTING_KEY)


    # Pass in the kwargs
    kwargs = {'connection': connection}

    # Start the consumer thread
    consumer_thread = threading.Thread(target=consumer, kwargs=kwargs)
    consumer_thread.start()

    # Start the pubisher thread
    #publisher_thread = threading.Thread(target=publisher, kwargs=kwargs)
    #publisher_thread.start()

    with connection.channel() as channel:
        for index in range(0, MESSAGE_COUNT):
            message = rabbitpy.Message(channel, 'Message #%i' % index)
            message.publish(EXCHANGE, ROUTING_KEY)

    # Join the consumer thread, waiting for it to consume all MESSAGE_COUNT messages
    consumer_thread.join()
