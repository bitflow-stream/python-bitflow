import datetime
import logging
import multiprocessing
import queue
import random
import threading
import typing
import time
from multiprocessing import Value

from bitflow import helper
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

PARALLEL_MODE_THREAD = "thread"
PARALLEL_MODE_PROCESS = "process"
PARALLEL_MODES = [PARALLEL_MODE_THREAD, PARALLEL_MODE_PROCESS]


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
    # compare required arguments
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
    # no optional arguments passed
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
        self.next_step = None
        self.started = False
        self.sample_counter = 0

    @staticmethod
    def get_all_subclasses(cls):
        all_subclasses = []
        for subclass in cls.__subclasses__():
            if subclass.__name__ not in SUBCLASSES_TO_IGNORE:
                all_subclasses.append(subclass)
            all_subclasses.extend(ProcessingStep.get_all_subclasses(subclass))
        return all_subclasses

    def start(self):
        if not self.started:  # Execute once
            if self.next_step:
                self.next_step.start()
            self.on_start()
            self.started = True

    def on_start(self):
        pass

    def set_next_step(self, next_step):
        self.next_step = next_step

    def write(self, sample):
        if sample:
            self.sample_counter += 1
            if self.next_step:
                self.next_step.execute(sample)

    def execute(self, sample):
        pass

    def stop(self):
        self.on_close()
        if self.next_step:
            self.next_step.stop()

    def on_close(self):
        logging.info("%s: %d samples were processed", self.__name__, self.sample_counter)


class ParallelUtils:

    def __init__(self, sample_queue_to_child, sample_queue_to_parent):
        self.lock = multiprocessing.Lock()
        self.input_counter = Value('i', 0)
        self.sample_queue_to_child = sample_queue_to_child
        self.sample_queue_to_parent = sample_queue_to_parent

    def register_input(self):
        self.input_counter.value += 1

    def unregister_input(self):
        self.input_counter.value -= 1

    def write_to_child(self, sample):
        return self._write(self.sample_queue_to_child, sample)

    def write_to_parent(self, sample):
        return self._write(self.sample_queue_to_parent, sample)

    @staticmethod
    def _write(q, sample):
        if not sample:
            logging.warning("{}: Attempting to write None sample to queue. Dropping None sample.")
            return True
        success = False
        try:
            q.put(sample, block=False)
            success = True
        except queue.Full:
            pass
        except Exception as e:
            logging.warning("Unexpected exception while trying to put sample into queue."
                            " Dropping sample %s", sample.__str__(), exc_info=e)
        return success

    def read_from_parent(self):
        return self._read(self.sample_queue_to_child)

    def read_from_child(self):
        return self._read(self.sample_queue_to_parent)

    @staticmethod
    def _read(q):
        sample = None
        try:
            sample = q.get(block=False)
            q.task_done()
        except queue.Empty:
            pass
        except Exception as e:
            logging.warning("Unexpected exception while trying to read sample from queue.", exc_info=e)
        return sample

    def read_all_in(self):
        return self._read_all(self.sample_queue_to_child)

    def read_all_out(self):
        return self._read_all(self.sample_queue_to_parent)

    def _read_all(self, q):
        sample_list = []
        while q.qsize() > 0:
            sample = self._read(q)
            if sample:
                sample_list.append(sample)
        return sample_list


class AsyncProcessingStep(ProcessingStep):

    def __init__(self, maxsize, parallel_mode):
        if parallel_mode not in PARALLEL_MODES:
            raise ValueError("Unknown parallel mode {}. Supported modes are {}".format(parallel_mode,
                             ",".join(PARALLEL_MODES)))
        super().__init__()
        self.parallel_mode = parallel_mode
        if self.parallel_mode == PARALLEL_MODE_THREAD:
            self.parallel_utils = ParallelUtils(queue.Queue(), queue.Queue())
        elif self.parallel_mode == PARALLEL_MODE_PROCESS:
            self.parallel_utils = ParallelUtils(multiprocessing.JoinableQueue(maxsize),
                                                multiprocessing.JoinableQueue(maxsize))
        else:
            raise ValueError("Unknown parallel mode %s. Supported modes are $s", self.parallel_mode,
                             ",".join(PARALLEL_MODES))
        self.parallel_step = None

    @staticmethod
    def get_all_subclasses(cls):
        all_subclasses = []
        for subclass in cls.__subclasses__():
            if subclass.__name__ not in SUBCLASSES_TO_IGNORE:
                all_subclasses.append(subclass)
            all_subclasses.extend(ProcessingStep.get_all_subclasses(subclass))
        return all_subclasses

    def on_start(self):
        self.parallel_step = self.init_async_object(self.parallel_utils, self.parallel_mode)
        self.parallel_utils.register_input()
        self.parallel_step.start()

    def init_async_object(self, parallel_utils, parallel_mode):
        raise NotImplementedError("%s: Initialization of parallel object needs to be implemented.", self.__name__)

    def execute(self, sample):
        # Prevent deadlocks
        while not self.parallel_utils.write_to_child(sample):
            self.process_pending_samples()
        self.process_pending_samples()

    def receive_samples(self):
        while self.parallel_step.is_alive():
            sample_out = self.parallel_utils.read_from_child()
            super().write(sample_out)

    def process_pending_samples(self):
        for sample_out in self.parallel_utils.read_all_out():
            super().write(sample_out)

    def on_close(self):
        self.parallel_utils.unregister_input()
        self.receive_samples()  # Receive samples from parallel running step. Otherwise deadlock would occur.
        self.parallel_utils.sample_queue_to_child.join()
        self.parallel_step.join()
        # Clear pending sample from queue.
        # IMPORTANT: This call and receive_samples() are not redundant. BOTH are required.
        self.process_pending_samples()
        self.parallel_utils.sample_queue_to_parent.join()
        super().on_close()


class _ProcessingStepAsync:

    def __new__(cls, *args, **kwargs):
        if cls is _ProcessingStepAsync:  # Prevent class from being instantiated
            raise TypeError("%s class may not be instantiated", cls.__name__)
        return object.__new__(cls)

    def __init__(self, parallel_utils):
        self.parallel_utils = parallel_utils

    def run(self):
        self.on_start()
        while self.parallel_utils.input_counter.value > 0 or self.parallel_utils.sample_queue_to_child.qsize() > 0:
            samples = self.parallel_utils.read_all_in()
            for sample in samples:
                sample_out = self.loop(sample)
                if sample_out:
                    self.parallel_utils.write_to_parent(sample_out)
        self.on_close()

    def stop(self):
        self.on_close()

    def on_start(self):
        pass

    def on_close(self):
        pass

    def loop(self, sample):
        pass


class DebugGenerator(AsyncProcessingStep):
    """example generative processing step"""
    __name__ = "debug-generator"
    __description__ = "DEBUG. Generates random samples with different tags."

    def __init__(self, num_samples: int = 10, maxsize: int = DEFAULT_QUEUE_MAXSIZE,
                 parallel_mode: str = PARALLEL_MODE_THREAD):
        if not parallel_mode:
            raise ValueError("%s: Sequential mode is not supported. Define one of the following parallel modes: $s",
                             self.__name__, ",".join(PARALLEL_MODES))
        super().__init__(maxsize, parallel_mode)
        self.num_samples = num_samples

    def init_async_object(self, parallel_utils, parallel_mode):
        parallel_step = None
        if parallel_mode:
            if parallel_mode == PARALLEL_MODE_THREAD:
                parallel_step = _DebugGenerationStepThread(self.num_samples, parallel_utils)
            elif parallel_mode == PARALLEL_MODE_PROCESS:
                parallel_step = _DebugGenerationStepProcess(self.num_samples, parallel_utils)
            else:
                raise ValueError("Unknown parallel mode %s. Supported modes are $s", parallel_mode,
                                 ",".join(PARALLEL_MODES))
        return parallel_step


class _DebugGenerationStepAsync(_ProcessingStepAsync):

    def __init__(self, num_samples, parallel_utils):
        super().__init__(parallel_utils)
        self.__name__ = "DebugGenerationStep_Async"
        self.num_samples = num_samples
        self.sample_counter = 0

    def run(self):
        super().run()
        self.sample_counter += 1
        if self.sample_counter >= self.num_samples:
            self.stop()

    def loop(self, sample):
        time.sleep(1)
        v1 = random.random()
        metrics = [float(v1)]
        metric_names = ["random_value"]
        sample_out = Sample(header=Header(metric_names=metric_names), metrics=metrics)
        r_tag = random.randint(0, 1)
        if r_tag == 0:
            sample_out.add_tag("blub", "bla")
        else:
            sample_out.add_tag("blub", "blub")
            sample_out.add_tag("test", "rudolph")
        return sample_out


class _DebugGenerationStepThread(threading.Thread, _DebugGenerationStepAsync):

    def __init__(self, num_samples, parallel_utils):
        super().__init__()
        self.__name__ = "Pipeline_Thread"
        threading.Thread.__init__(self)
        _DebugGenerationStepAsync.__init__(self, num_samples, parallel_utils)

    def run(self):
        _DebugGenerationStepAsync.run(self)


class _DebugGenerationStepProcess(multiprocessing.Process, _DebugGenerationStepAsync):

    def __init__(self, num_samples, parallel_utils):
        self.__name__ = "Pipeline_Process"
        multiprocessing.Process.__init__(self)
        _DebugGenerationStepAsync.__init__(self, num_samples, parallel_utils)

    def run(self):
        _DebugGenerationStepAsync.run(self)


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
