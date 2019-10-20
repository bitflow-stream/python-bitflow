import json
import logging


class JSONSerializer:

    def __init__(self, cls):
        self.cls = cls

    def serialize(self, obj):
        json_str = "{}"
        if obj:
            if not isinstance(obj, self.cls):
                raise ValueError("Object {} of type {} is not of expected type {}.".format(obj, type(obj), self.cls))
            json_str = json.dumps(obj)
            if not json_str:
                json_str = "{}"
        return bytearray(json_str)

    def deserialize(self, data):
        result = None
        if data:
            json_str = data.decode()
            try:
                result = json.loads(json_str, cls=self.cls)
            except Exception as e:
                logging.warning("Unable to load class {} frm json string {}.".format(self.cls, json_str), exc_info=e)
        return result
