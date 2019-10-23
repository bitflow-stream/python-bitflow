import threading


class EventConsumer:
    def __init__(self, name="consumer"):
        super().__init__()
        self.name = name
        self.lock = threading.Lock()

    def callback(self, header, message):
        self.lock.acquire()
        self.consume(header, message)
        self.lock.release()

    def consume(self, header, message):
        raise NotImplementedError("Needs to be implemented.")