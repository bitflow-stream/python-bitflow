import time

from zerops.event_bus.EventBus import EventBus
from zerops.event_bus.EventBusMessage import EventBusMessage
from zerops.serialize.JSONSerializer import JSONSerializer


class SimpleMessage:
    def __init__(self, message):
        self.message = message


# docker run -d --hostname rabbitmq --name rabbitmq-broker -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password -e RABBITMQ_DEFAULT_VHOST=zerops -p 5672:5672 -p 15672:15672 rabbitmq:3-management
event_bus = EventBus("amqp://user:xxxxxxxx@localhost:5672/zerops-operator", "zerops-operator")

# Create message which should arrive at respective callback functions
header_any = {"type": "aa"}
message_any = SimpleMessage("This should only arrive at callback function ANY (5 times)")
payload_any = EventBusMessage(message_any, JSONSerializer(SimpleMessage))

time.sleep(5)

# Send messages
for i in range(5):
    event_bus.publish_message(header_any, payload_any)

time.sleep(5)
event_bus.close()
