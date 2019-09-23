import datetime
import logging
import multiprocessing
import queue
import threading
import typing
from multiprocessing import Value

from bitflow import helper
from bitflow.io.sources import Source
from bitflow.sample import Sample, Header

STRING_LIST_SEPARATOR = ","

BOOL_TRUE_STRINGS = ["true", "yes", "1", "ja", "y", "j"]
BOOL_FALSE_STRINGS = ["false", "no", "0", "nein", "n"]
ACCEPTED_BOOL_STRINGS = BOOL_TRUE_STRINGS + BOOL_FALSE_STRINGS

DEFAULT_QUEUE_MAXSIZE = 10000
DEFAULT_QUEUE_BLOCKING_TIMEOUT = 2
DEFAULT_MULTIPROCESSING_INPUT = True
DEFAULT_BATCH_MULTIPROCESSING_INPUT = False
DEFAULT_QUEUES_EMPTY_TIMEOUT = 0.1


def get_required_and_optional_args(step, required_step_args, optional_step_args):
    step_args = typing.get_type_hints(step.__init__)

    if step.__init__.__defaults__:
        optional_step_args_len = len(step.__init__.__defaults__)
    else:
        optional_step_args_len = 0

    for i in range(0, len(step_args) - optional_step_args_len):
        required_step_args[list(step_args.keys())[i]] = step_args[list(step_args.keys())[i]]

    for i in range(len(step_args) - optional_step_args_len, len(step_args)):
        optional_step_args[list(step_args.keys())[i]] = step_args[list(step_args.keys())[i]]

    return required_step_args, optional_step_args


def compare_and_parse_int(args_dict, script_key_name):
    try:
        args_dict[script_key_name] = int(args_dict[script_key_name])
        return True
    except:
        return False


def compare_and_parse_float(args_dict, script_key_name):
    try:
        args_dict[script_key_name] = float(args_dict[script_key_name])
        return True
    except:
        return False


def compare_and_parse_bool(args_dict, script_key_name):
    if not isinstance(args_dict[script_key_name], str):
        return False
    if args_dict[script_key_name].lower() not in ACCEPTED_BOOL_STRINGS:
        return False
    if args_dict[script_key_name].lower() in BOOL_TRUE_STRINGS:
        args_dict[script_key_name] = True
    elif args_dict[script_key_name].lower() in BOOL_FALSE_STRINGS:
        args_dict[script_key_name] = False
    return True


def compare_and_parse_str(args_dict, script_key_name):
    return True


def compare_and_parse_list(args_dict, script_key_name):
    if not isinstance(args_dict[script_key_name], list):
        return False
    return True


def compare_and_parse_dict(args_dict, script_key_name):
    if not isinstance(args_dict[script_key_name], dict):
        return False
    return True


def type_compare_and_parse(required_type, args_dict, script_key_name):
    rtn = True
    if required_type is int:
        rtn = compare_and_parse_int(args_dict, script_key_name)
    elif required_type is float:
        rtn = compare_and_parse_float(args_dict, script_key_name)
    elif required_type is bool:
        rtn = compare_and_parse_bool(args_dict, script_key_name)
    elif required_type is str:
        rtn = compare_and_parse_str(args_dict, script_key_name)
    elif required_type is list:
        rtn = compare_and_parse_list(args_dict, script_key_name)
    elif required_type is dict:
        rtn = compare_and_parse_dict(args_dict, script_key_name)
    return rtn


def compare_args(step, script_args):
    if len(script_args) == 0:
        return True
    # list not tested
    required_step_args = {}
    optional_step_args = {}
    get_required_and_optional_args(step=step,
                                   required_step_args=required_step_args,
                                   optional_step_args=optional_step_args)
    # if less than required arguments passed
    if len(script_args) < len(required_step_args):
        return False
    # compare required aruguments
    found_required_args = 0
    found_optional_args = 0
    for i in range(len(script_args)):
        script_key_name = list(script_args.keys())[i]
        if script_key_name in required_step_args.keys() and type_compare_and_parse(
                required_type=required_step_args[script_key_name], args_dict=script_args,
                script_key_name=script_key_name):
            found_required_args += 1
            logging.debug("Found required " + script_key_name + "...")
            continue
        if script_key_name in optional_step_args.keys() and type_compare_and_parse(
                required_type=optional_step_args[script_key_name], args_dict=script_args,
                script_key_name=script_key_name):
            found_optional_args += 1
            logging.debug("Found optional " + script_key_name + "...")
    # no optinal arguments passed
    if found_required_args == len(script_args):
        return True
    if (found_required_args + found_optional_args) == len(script_args):
        return True
    return False


def initialize_step(name, script_args):
    processing_steps = ProcessingStep.get_all_subclasses(ProcessingStep)
    for ps in processing_steps:
        if ps.__name__.lower() == name.lower() and compare_args(ps, script_args):
            logging.info("{} with args: {}  ok ...".format(name, script_args))
            try:
                ps_obj = ps(**script_args)
            except Exception as e:
                logging.error(str(e))
                ps_obj = None
            return ps_obj
    logging.info("{} with args: {}  failed ...".format(name, script_args))
    return None


def string_lst_to_lst(str_lst):
    values = str_lst.split(STRING_LIST_SEPARATOR)
    return [x.strip() for x in values]


SUBCLASSES_TO_IGNORE = ["AsyncProcessingStep", "Fork", "BatchProcessingStep", "PipelineTermination",
                        "BatchPipelineTermination"]


class ProcessingStep(metaclass=helper.CtrlMethodDecorator):
    """ Abstract ProcessingStep Class"""
    __description__ = "No description written yet."

    def __init__(self):
        self.__name__ = "ProcessingStep"
        self.lock = multiprocessing.Lock()
        self.next_step = None
        self.started = False

    @staticmethod
    def get_all_subclasses(cls):
        all_subclasses = []
        for subclass in cls.__subclasses__():
            if subclass.__name__ not in SUBCLASSES_TO_IGNORE:
                all_subclasses.append(subclass)
            all_subclasses.extend(ProcessingStep.get_all_subclasses(subclass))
        return all_subclasses

    def start(self):
        if self.next_step and not self.started:  # Execute once
            self.on_start()
            self.next_step.start()
            self.started = True

    def on_start(self):
        pass

    def set_next_step(self, next_step):
        self.next_step = next_step

    def write(self, sample):
        if sample and self.next_step:
            self.next_step.execute(sample)

    def execute(self, sample):
        pass

    def stop(self):
        self.on_close()
        if self.next_step:
            self.next_step.stop()

    def on_close(self):
        pass


class AsyncProcessingStep(ProcessingStep):

    def __init__(self):
        super().__init__()
        self.input_counter = Value('i', 0)
        self.sample_queue_in = queue.Queue(maxsize=DEFAULT_QUEUE_MAXSIZE)
        self.sample_queue_out = queue.Queue(maxsize=DEFAULT_QUEUE_MAXSIZE)
        self.threaded_step = None

    def on_start(self):
        self.input_counter.value += 1
        if self.threaded_step:
            self.threaded_step.start()
        else:
            raise Exception("%s: Internal asynchronous step undefined. This implementation of an asynchronous "
                            "step seems to be broken.", self.__name__)

    def execute(self, sample):
        self.sample_queue_in.put(sample)
        try:
            sample_out = self.sample_queue_out.get(block=False)
            self.write(sample_out)
        except queue.Empty:
            pass

    def clear_sample_queue_out(self):
        try:
            while True:  # Read until queue is empty
                sample_out = self.sample_queue_out.get(block=False)
                self.write(sample_out)
        except queue.Empty:
            pass

    def stop(self):
        self.on_close()
        super().stop()

    def on_close(self):
        self.input_counter.value -= 1
        self.sample_queue_in.join()
        self.threaded_step.join()
        self.clear_sample_queue_out()
        self.sample_queue_out.join()
        super().on_close()


class _AsyncProcessingStep(threading.Thread):

    def __init__(self, sample_queue_in, sample_queue_out, input_counter):
        threading.Thread.__init__(self)
        self.sample_queue_in = sample_queue_in
        self.sample_queue_out = sample_queue_out
        self.input_counter = input_counter

    def start(self):
        super().start()  # Trigger decorator

    def run(self):
        while self.input_counter.value > 0 or self.sample_queue_in.qsize() > 0:
            try:
                sample = self.sample_queue_in.get(block=False)
            except queue.Empty:
                continue
            sample_out = self.loop(sample)
            if sample_out:
                self.sample_queue_out.put(sample_out)
            self.sample_queue_in.task_done()
        self.on_close()

    def process_remaining_samples(self):
        while True:
            try:
                sample = self.sample_queue_in.get(block=False)
                sample_out = self.loop(sample)
                if sample_out:
                    self.sample_queue_out.put(sample_out)
                self.sample_queue_in.task_done()
            except queue.Empty:
                break

    def on_close(self):
        self.process_remaining_samples()

    def loop(self, sample):
        pass


class DebugGenerationStep(AsyncProcessingStep):
    """example generative processing step"""
    __name__ = "debug-generation-step"
    __description__ = "DEBUG. Generates random samples with different tages."

    def __init__(self):
        super().__init__()
        self.threaded_step = _DebugGenerationStep(self.sample_queue_in, self.sample_queue_out, self.input_counter)


class _DebugGenerationStep(_AsyncProcessingStep):

    def __init__(self, sample_queue_in, sample_queue_out, input_counter):
        super().__init__(sample_queue_in, sample_queue_out, input_counter)
        self.__name__ = "DebugGenerationStep_inner"
        self.is_running = True

    def loop(self, sample):
        self.sample_queue_out.put(sample)
        import time
        while self.is_running:
            time.sleep(1)
            import random
            v1 = random.random()
            metrics = [float(v1)]
            metric_names = ["random_value"]
            sample = Sample(header=Header(metric_names=metric_names), metrics=metrics)
            r_tag = random.randint(0, 1)
            if r_tag == 0:
                sample.add_tag("blub", "bla")
            else:
                sample.add_tag("blub", "blub")
                sample.add_tag("test", "rudolph")


class Noop(ProcessingStep):
    __description__ = "DEBUG. Noop."
    __name__ = "noop"

    def __init__(self, int: int = 42, float: float = 0.5, str: str = "str", bool: bool = True, list: list = [],
                 dict: dict = {}):
        self.int = int
        self.bool = bool
        self.str = str
        self.list = list
        self.float = float
        self.dict = dict
        super().__init__()

    def __str__(self):
        return "BOOLEAN: {}, INT: {}, FLOAT: {}, STRING: {}, LIST: {}, DICT: {}".format(self.bool, self.int, self.float,
                                                                                        self.str, self.list, self.dict)

    def execute(self, sample):
        self.write(sample)


class ModifyTimestamp(ProcessingStep):
    """ Modifies Timestamp of traversing samples
    start_time: in %Y-%m-%d %H:%M:%S.%f' like '2018-04-06 14:51:15.157232'
    interval: in seconds
    """
    __description__ = "Modifies Timestamp so that the first sample will get timestamp of start_time parameter"
    __name__ = "modify-timestamp"

    def __init__(self, interval: int, start_time: str = "now"):
        super().__init__()
        try:
            self.start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')
        except Exception as e:
            if self.start_time is not "now":
                logging.error("{}: no correct datetime str, using now ...:{}".format(self.__class__.__name__, e))
            self.start_time = datetime.datetime.now()

        try:
            self.interval = datetime.timedelta(seconds=interval)
        except Exception as mtf:
            logging.error("Could not parse interval value to float in " + str(self) + "!\n" + str(mtf))
            self.on_close()

    def execute(self, sample):
        self.start_time = self.start_time + self.interval
        sample.set_timestamp(self.start_time)

        self.write(sample)


class ListenForTags(ProcessingStep):
    """ Open Rest API to add or delete tags to samples

    port: port to listen on
    """
    __description__ = "Opens a rest-interface to add tags to traversing samples"
    __name__ = "listen-for-tags"

    def __init__(self, port: int = 7777):
        super().__init__()

        from bitflow.rest import RestServer

        self.lock_all_tags = threading.Lock()
        self.all_tags = {}
        self.rest_server = RestServer(self.lock_all_tags, self.all_tags, port)
        self.rest_server.start()

    def __str__(self):
        return "ListenForTagsProcessingStep"

    def execute(self, sample):
        logging.debug("tags: " + str(self.all_tags))
        self.lock_all_tags.acquire()
        if len(self.all_tags) != 0:
            for k, v in self.all_tags.items():
                sample.add_tag(k, v)
        self.lock_all_tags.release()
        self.write(sample)


class AddTag(ProcessingStep):
    """ Adds a give tag and value to the samples

    tag: tag name
    value: value string
    """
    __description__ = "Adds a given tags and values to the samples. Requires arguments as a dict"
    __name__ = "add-tag"

    def __init__(self, tags: dict):
        super().__init__()
        self.tags = tags

    def execute(self, sample):
        for k, v in self.tags.items():
            sample.add_tag(str(k), str(v))
        self.write(sample)
