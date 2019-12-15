import threading
import time
import traceback

from zerops.event_bus.EventBus import EventBus
from zerops.event_bus.EventBusMessage import EventBusMessage
from zerops.serialize.JSONSerializer import JSONSerializer


def callback_any(header, message):
    date = message.get_date()
    try:
        payload = message.get_message(JSONSerializer(SimpleMessage))
        print("ANY: Message arrived at: {} with header: {} and payload: {}"
              .format(date.strftime("%m/%d/%Y, %H:%M:%S"), header, payload))
    except Exception as e:
        print("ANY: Error while receiving object: {}".format(e))
        traceback.print_exc()


def callback_all(header, message):
    date = message.get_date()
    try:
        payload = message.get_message(JSONSerializer(SimpleMessage))
        print("ALL: Message arrived at: {} with header: {} and payload: {}"
              .format(date.strftime("%m/%d/%Y, %H:%M:%S"), header, payload))
    except Exception as e:
        print("ALL: Error while receiving object: {}".format(e))
        traceback.print_exc()


class SimpleMessage:
    def __init__(self, message):
        self.message = message


# docker run -d --hostname rabbitmq --name rabbitmq-broker -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password -e RABBITMQ_DEFAULT_VHOST=zerops -p 5672:5672 -p 15672:15672 rabbitmq:3-management
event_bus = EventBus("amqp://user:password@localhost:5672/zerops", "testExchange")

# Receiving messages with any filter
filter_arg_any = {"x-match": "any", "type": "rca", "host": "wally133"}  # match any of the keys
event_bus.receive_messages(filter_arg_any, callback_any)

# Receiving messages with all filter
filter_arg_all = {"x-match": "all", "type": "rca", "host": "wally134"}  # match all of the keys
event_bus.receive_messages(filter_arg_all, callback_all)

# Create message which should arrive at respective callback functions
header_any = {"type": "rca", "host": "wally133"}
message_any = SimpleMessage("This should only arrive at callback function ANY (5 times)")
payload_any = EventBusMessage(message_any, JSONSerializer(SimpleMessage))

# Create message which should arrive at both callback functions
header_both = {"type": "rca", "host": "wally134"}
message_both = SimpleMessage("This should arrive at both consumers (10 times)")
payload_both = EventBusMessage(message_both, JSONSerializer(SimpleMessage))

time.sleep(5)

# Send messages
for i in range(5):
    event_bus.publish_message(header_any, payload_any)

print("HIer")

for i in range(5):
    event_bus.publish_message(header_both, payload_both)

print("HIer")

time.sleep(5)
event_bus.close()
