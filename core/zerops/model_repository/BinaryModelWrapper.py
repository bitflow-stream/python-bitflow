import datetime


class BinaryModelWrapper:
    DATE_FORMAT = '%d.%m.%Y %H:%M:%S.%f'
    HEADER_SEPARATOR = ","

    MODEL_KEY = "model"
    TIME_KEY = "time"
    HEADER = "header"
    MODEL_KEY_BYTES = MODEL_KEY.encode(encoding='utf8')
    TIME_KEY_BYTES = TIME_KEY.encode(encoding='utf8')

    def __init__(self, serializer, model=None, binary_dict=None, meta_data=None):
        time = None
        self.serializer = serializer
        if binary_dict:
            model, time, meta_data = self.__decode_binary_dict(binary_dict)
        self.model = model
        self.time = datetime.datetime.now() if not time else time
        self.meta_data = meta_data

    def __decode_binary_dict(self, loaded_dict):
        keys = {key.decode(): key for key in loaded_dict.keys()}
        time = None
        if self.TIME_KEY in keys:
            time = self.__decode_time(loaded_dict[keys[self.TIME_KEY]])
            del loaded_dict[keys[self.TIME_KEY]]
            del keys[self.TIME_KEY]
        model = None
        if self.MODEL_KEY in keys:
            model = self.serializer.deserialize(loaded_dict[keys[self.MODEL_KEY]])
            del loaded_dict[keys[self.MODEL_KEY]]
            del keys[self.MODEL_KEY]

        meta_data = {k: loaded_dict[k_bin].decode(encoding='utf8') for k, k_bin in keys.items()}

        return model, time, meta_data

    def __encode_to_binary_dict(self):
        if self.meta_data:
            result = {k.encode(encoding='utf8'): v.encode(encoding='utf8') for k, v in self.meta_data.items()}
        else:
            result = {}
        result[self.TIME_KEY_BYTES] = self.__encode_time(self.time)
        result[self.MODEL_KEY_BYTES] = self.serializer.serialize(self.model)
        return result

    def __encode_time(self, time):
        str_time = time.strftime(self.DATE_FORMAT)
        return str_time.encode(encoding='utf8')

    def __decode_time(self, data):
        str_time = data.decode(encoding='utf8')
        return datetime.datetime.strptime(str_time, self.DATE_FORMAT)

    def get_str_date(self):
        return self.time.strftime(self.DATE_FORMAT)

    def get_meta_data(self):
        return self.meta_data

    def get_model(self):
        return self.model

    def get_byte_map(self):
        return self.__encode_to_binary_dict()

    def get_model_bytes(self):
        if self.model:
            return self.serializer.serialize(self.model)
