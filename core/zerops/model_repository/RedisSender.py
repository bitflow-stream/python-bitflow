import logging
import queue
import threading
from functools import reduce
import redis as redis

from zerops import utils


class RedisSender:
    ENV_REDIS_MAX_QUEUE_SIZE = "ZEROPS_REDIS_MAX_QUEUE_SIZE"
    DEFAULT_MAX_QUEUE_SIZE = 2 * 1024 * 1024  # Store up to 2GB of data to send to Redis in background

    class __RedisBorgWrapper:
        shared_state = {}

        def __init__(self):
            self.__dict__ = self.shared_state

        @staticmethod
        def redis_connect(endpoint):
            redis_pool = redis.BlockingConnectionPool.from_url(endpoint)  # Default timeout is 10 seconds
            return redis.StrictRedis(connection_pool=redis_pool)

    class __StoredMap:
        def __init__(self, key, value: dict):
            self.key = key
            self.value = value

        def size(self):
            # Sum up lengths of all values of value object
            return reduce(lambda x, y: x + y, map(lambda x: len(x), self.value.values()))

    def __init__(self, endpoint):
        self.queue = queue.Queue()
        self.queued_size = 0

        self.endpoint = endpoint
        self.redis = self.__RedisBorgWrapper().redis_connect(endpoint)
        self.redis.ping()
        self.max_queue_size = self.__getMaxQueueSize()
        self.send_thread = threading.Thread(target=self.send_values)
        self.stop_thread = False
        self.send_thread.start()

    def __getMaxQueueSize(self):
        result = self.DEFAULT_MAX_QUEUE_SIZE
        try:
            result = int(utils.get_env(self.ENV_REDIS_MAX_QUEUE_SIZE))
        except IOError:
            logging.info("Environment variable {} for maximum reddis queue size not defined. "
                         "Using default queue size of {}.".format(self.ENV_REDIS_MAX_QUEUE_SIZE, self.DEFAULT_MAX_QUEUE_SIZE))
        return result

    def store_map(self, key, value):
        stored = self.__StoredMap(key, value)
        stored_size = stored.size()
        self.queued_size += stored_size
        if self.queued_size > self.max_queue_size:
            # Cannot store this object, drop it.
            logging.warning("Redis queue (for {}) contains {} byte after adding {} byte object. Dropping object..."
                            .format(self.endpoint, self.queued_size, stored_size))
            return
        try:
            self.queue.put(stored)
        except Exception as e:
            logging.warning("Error while adding element {} to queue in order to be stored. Dropping object..."
                            .format(key), exc_info=e)

    def send_values(self):
        while not self.stop_thread:
            stored = None
            try:
                stored = self.queue.get(timeout=1)
            except queue.Empty:
                pass
            if stored:
                # Reduce stored size, regardless of success
                size = stored.size()
                self.queued_size -= size

                # Actually send the data
                logging.debug("Sending {} byte object to redis, key: {}".format(size, stored.key))
                try:
                    self.redis.hmset(stored.key, stored.value)
                except Exception as e:
                    logging.warning("Failed to store redis map with size {}, key: {}".format(size, stored.key), exc_info=e)
                self.queue.task_done()

    def close(self):
        self.stop_thread = True
        self.send_thread.join()
