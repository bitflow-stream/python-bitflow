import json
import logging


class JSONSerializer:

    def __init__(self, cls=None):
        self.cls = cls

    def serialize(self, obj):
        json_str = "{}"
        if obj:
            if not self.cls:
                json_str = json.dumps(obj)
            else:
                if not isinstance(obj, self.cls):
                    raise ValueError("Object {} of type {} is not of expected type {}.".format(obj, type(obj), self.cls))
                json_str = json.dumps(obj.__dict__, ensure_ascii=True)
            if not json_str:
                logging.warning("Unable to serialize object {}.".format(obj))
                json_str = "{}"
        return json_str.encode(encoding='utf8')

    def deserialize(self, data):
        result = None
        if data:
            json_str = data.decode(encoding='utf8')
            json_dict = json.loads(json_str)
            if self.cls:
                try:
                    result = self.cls(**json_dict)
                except Exception as e:
                    logging.warning("Unable to load class {} from json string {}.".format(
                        self.cls, json_str), exc_info=e)
            else:
                result = json_dict
        return result
