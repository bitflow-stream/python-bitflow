import logging
import sys
from bitflow.runner import ProcessingStep

class NoopStep(ProcessingStep):
    __description__ = "Step that silently forwards all received samples"
    step_name = "noop"

    def handle_sample(self, sample):
        self.output(sample)

class DebugStep(NoopStep):
    __description__ = "This step forwards all received samples, shows received parameters, and logs some statistics"
    step_name = "debug"

    def __init__(self, int: int = 42, float: float = 0.5, str: str = "str", bool: bool = True, list: list = [], dict: dict = {}):
        self.int = int
        self.bool = bool
        self.str = str
        self.list = list
        self.float = float
        self.dict = dict
        self.received_samples = 0
        super().__init__()

    def __str__(self):
        return "Noop step with parameters: BOOLEAN: {}, INT: {}, FLOAT: {}, STRING: {}, LIST: {}, DICT: {}"\
            .format(self.bool, self.int, self.float, self.str, self.list, self.dict)

    def initialize(self, context):
        logging.info("Initializing: {}".format(self))
        super().initialize(context)

    def handle_sample(self, sample):
        self.received_samples = self.received_samples + 1
        super().handle_sample(sample)
    
    def cleanup(self):
        logging.info("Cleaning up. Received samples: {}".format(self.received_samples))
        super().cleanup()

class DropStep(ProcessingStep):
    __description__ = "Silently drop all received samples"
    step_name = "drop"

class PrintStep(ProcessingStep):
    __description__ = "Prints all incoming samples before forwarding them"
    step_name = "print-samples"

    def handle_sample(self, sample):
        print(str(sample), file=sys.stderr)
        self.output(sample)

class EchoStep(NoopStep):
    __description__ = "Prints the given message to stderr, forwards all received samples unchanged."
    step_name = "echo"

    def __init__(self, msg:str):
        self.msg = msg
        print(self.msg, file=sys.stderr)
