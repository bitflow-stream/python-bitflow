import abc
import logging


class State(metaclass=abc.ABCMeta):

    def __init__(self, state_name):
        logging.info("Switching to state: {}.".format(state_name))

    @abc.abstractmethod
    def process_sample(self, sample, context):
        pass
