import logging

from zerops import utils
from zerops.model_repository.BinaryModelWrapper import BinaryModelWrapper
from zerops.model_repository.RedisSender import RedisSender


class BinaryModelRepository:
    ENV_REDIS_ENDPOINT = "ZEROPS_REDIS_ENDPOINT"
    ENV_REDIS_KEY_PREFIX = "ZEROPS_REDIS_KEY_PREFIX"
    LATEST_KEY_SUFFIX = "latest"
    KEY_SEPARATOR = ":"

    AUTO_INCREMENT_REVISION = False
    DEFAULT_REVISION = 0

    def __init__(self, serializer, redis_endpoint=None, key_prefix=None, step_name=""):
        if not redis_endpoint:
            redis_endpoint = utils.get_env(self.ENV_REDIS_ENDPOINT)
        self.sender = RedisSender(redis_endpoint)
        if not key_prefix:
            key_prefix = utils.get_env(self.ENV_REDIS_KEY_PREFIX)
        self.key_prefix = key_prefix
        self.step_name = step_name
        self.serializer = serializer

    # Stores the a new model with the given key. The revision is incremented.
    def store(self, key, model, meta_data=None):
        model_wrapper = BinaryModelWrapper(serializer=self.serializer, model=model, meta_data=meta_data)
        latest_key_bytes = self.__make_key_latest(key)
        if self.AUTO_INCREMENT_REVISION:
            logging.debug("Incrementing latest model key: {}.".format(latest_key_bytes.decode()))
            revision = self.sender.redis.incr(latest_key_bytes)
        else:
            revision = self.DEFAULT_REVISION
            self.sender.redis.set(latest_key_bytes, str(revision).encode(encoding='utf8'))

        new_key_bytes = self.__make_key(key, revision)
        # If this fails, the "latest" key will point to a non-existing revision object.
        # This is handled specially in load_latest.
        logging.debug("Storing byte model (size {} bytes) under key {} (time: {}, meta: {})".format(
            len(model_wrapper.get_model_bytes()), new_key_bytes.decode(encoding='utf8'), model_wrapper.get_str_date(),
            model_wrapper.get_meta_data()))
        encoded_model = model_wrapper.get_byte_map()
        self.sender.store_map(new_key_bytes, encoded_model)

        return revision

    # Load the model with the latest revision.
    def load_latest(self, key):
        if not self.exists(key):
            logging.warning("Latest model by key {} not found.".format(key))
            return None

        logging.debug("Loading latest model for key {}.".format(key))
        latest_revision = int(self.sender.redis.get(self.__make_key_latest(key)).decode(encoding='utf8'))
        while latest_revision >= 0:
            model = self.load_revision(key, latest_revision)
            if model:
                return model
            else:
                # If the given key does not exist, we assume that the revision was incremented,
                # but storing the model object failed. Decrement the version, until we find an existing object.
                latest_revision -= 1
        logging.warning("Key {} has {} stored as latest revision, but no stored model object was found."
                        .format(key, latest_revision))
        return None

    def load_all_latest_revisions(self, key_pattern):
        revision_map = self.get_all_latest_revisions(key_pattern)
        result = {}
        for k, v in revision_map.items():
            key = k.split(":")[3]
            model = self.load_revision(key, v)
            if model:
                result[k] = model
        return result

    # Load a model, given by its key and revision.
    def load_revision(self, key, revision):
        if not self.exists(key, revision):
            logging.warning("Model by key {} and revision {} not found.".format(key, revision))
            return None

        byte_key = self.__make_key(key, revision)
        binary_dict = self.sender.redis.hgetall(byte_key)
        model_wrapper = BinaryModelWrapper(binary_dict=binary_dict, serializer=self.serializer)

        logging.debug("Loaded byte model from key {} (time: {}, meta: {})" .format(
            byte_key.decode(encoding='utf8'), model_wrapper.get_str_date(), model_wrapper.get_meta_data()))
        return model_wrapper.get_model()

    def exists(self, key, revision=None):
        if not revision:
            latest_key = self.__make_key_latest(key)
            result = self.sender.redis.exists(latest_key)
        else:
            result = self.sender.redis.exists(self.__make_key(key, revision))
        return result

    def get_latest_revision(self, key):
        revision_number = self.sender.redis.get(self.__make_key_latest(key))
        return int(revision_number.decode(encoding='utf8'))

    def get_all_latest_revisions(self, key_pattern):
        keys_tmp = []
        # Compared to KEYS the SCAN command prevents very long waiting blocking times in redis
        cur, results = self.sender.redis.scan(0, self.__make_key_latest(key_pattern), 100)
        keys_tmp += results
        while cur != 0:
            cur, results = self.sender.redis.scan(cur, self.__make_key_latest(key_pattern), 100)
            keys_tmp += results
        unique_keys = set(keys_tmp)
        binary_revision_numbers = self.sender.redis.mget(list(unique_keys))
        revision_numbers = {}
        for bin_key, bin_rn in zip(unique_keys, binary_revision_numbers):
            revision_numbers[bin_key.decode(encoding='utf8')] = int(bin_rn.decode(encoding='utf8'))
        return revision_numbers

    def __make_key_latest(self, suffix):
        return self.__make_key(suffix, self.LATEST_KEY_SUFFIX)

    def __make_key(self, suffix, revision):
        self.__check_suffix(suffix)
        return self.__build_key([self.key_prefix, self.step_name, suffix, str(revision)]).encode(encoding='utf8')

    def __check_suffix(self, suffix):
        if self.KEY_SEPARATOR in suffix:
            raise ValueError("Failed to generate key. Suffix must not contain separator character {}, suffix: {}."
                             .format(self.KEY_SEPARATOR, suffix))

    def __build_key(self, parts):
        return self.KEY_SEPARATOR.join(parts)

    def close(self):
        self.sender.close()
