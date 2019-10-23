import datetime


class EventBusMessage:

    def __init__(self, message, serializer=None, date=None):
        super().__init__()
        if not date:
            date = datetime.datetime.now()
        self.date = date

        if serializer:
            self.byte_data = serializer.serialize(message)
        else:
            self.byte_data = message

    # Get deserialized object of message payload if serializer is defined. Otherwise return raw binary message (as
    # byte array).
    def get_message(self, serializer=None):
        if serializer:
            return serializer.deserialize(self.byte_data)
        else:
            return self.byte_data

    # Get date of received message.
    def get_date(self):
        return self.date
