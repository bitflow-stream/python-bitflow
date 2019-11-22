import logging
import pickle


class PickleSerializer:

    def __init__(self, encode='utf8'):
        self.encode = encode

    def serialize(self, obj):
        bin_result = "".encode(self.encode)
        if obj:
            bin_result = pickle.dumps(obj)
            if not bin_result:
                logging.warning("Unable to serialize object {}.".format(obj))
                bin_result = "".encode(self.encode)
        return bin_result

    def deserialize(self, data):
        result = None
        if data:
            result = pickle.loads(data)
        return result
