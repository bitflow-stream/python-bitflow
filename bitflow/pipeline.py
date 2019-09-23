import multiprocessing
import queue as thread_queue
from threading import Thread

from bitflow.batchprocessingstep import *
from bitflow.processingstep import ProcessingStep


class PipelineTermination(ProcessingStep):

    def __init__(self, sample_queue):
        super().__init__()
        self.__name__ = "Pipeline_Termination"
        self.sample_queue = sample_queue

    def execute(self, s):
        if s:
            self.sample_queue.put(s)


class Pipeline(Thread, metaclass=helper.OnCloseDeco):

    def __init__(self, maxsize=DEFAULT_QUEUE_MAXSIZE, multiprocessing_input=DEFAULT_MULTIPROCESSING_INPUT):
        super().__init__()
        self.__name__ = "Pipeline"
        self.input_counter = multiprocessing.Value('i', 0)
        self.maxsize = maxsize
        self.sample_queue_in = self.create_queue(multiprocessing_input)
        self.sample_queue_out = self.create_queue(multiprocessing_input)
        self.pipeline_termination = PipelineTermination(self.sample_queue_out)
        self.processing_steps = []

    def create_queue(self, multiprocessing_input):
        if multiprocessing_input:
            sample_queue = multiprocessing.JoinableQueue(maxsize=self.maxsize)
        else:
            sample_queue = thread_queue.Queue(maxsize=self.maxsize)
        return sample_queue

    def run(self):
        self.start_processing_steps()
        while self.input_counter.value > 0 or not self.sample_queue_in.empty():
            try:
                sample = self.sample_queue_in.get(block=False)
            except thread_queue.Empty:
                continue
            self.execute(sample)
            self.sample_queue_in.task_done()
        self.on_close()

    def execute(self, s):
        if self.processing_steps:
            self.processing_steps[0].execute(s)

    def start_processing_steps(self):
        if self.processing_steps:
            self.processing_steps[0].start()

    def add_processing_step(self, processing_step):
        if processing_step:
            if self.processing_steps:
                self.processing_steps[-1].set_next_step(processing_step)
            processing_step.set_next_step(self.pipeline_termination)
            self.processing_steps.append(processing_step)

    def read_queue(self):
        try:
            while True:
                sample = self.sample_queue_in.get(block=False)
                if sample:
                    self.execute(sample)
                self.sample_queue_in.task_done()
        except thread_queue.Empty:
            pass

    def stop(self):
        self.input_counter.value = 0

    def on_close(self):
        self.read_queue()
        if self.processing_steps:
            self.processing_steps[0].stop()


class BatchPipelineTermination(ProcessingStep):

    def __init__(self, sample_queue):
        super().__init__()
        self.__name__ = "BatchPipelineTermination"
        self.sample_queue = sample_queue

    def execute(self, samples: list):
        if samples:
            for s in samples:
                if s:
                    self.sample_queue.put(s)


class BatchPipeline(Pipeline):

    def __init__(self, maxsize=DEFAULT_QUEUE_MAXSIZE, multiprocessing_input=DEFAULT_BATCH_MULTIPROCESSING_INPUT):
        super().__init__(maxsize, multiprocessing_input)
        self.__name__ = "BatchPipeline"
        self.pipeline_termination = BatchPipelineTermination(self.sample_queue_out)

    def run(self):
        self.start_processing_steps()
        while self.input_counter.value > 0 or not self.sample_queue_in.empty():
            try:
                samples = self.sample_queue_in.get(block=False)
            except thread_queue.Empty:
                continue
            self.execute(samples)
            self.sample_queue_in.task_done()
        self.on_close()

    def execute(self, samples: list):
        if self.processing_steps:
            self.processing_steps[0].execute(samples)

    def add_processing_step(self, processing_step):
        if processing_step and isinstance(processing_step, BatchProcessingStep):
            if self.processing_steps:
                self.processing_steps[-1].set_next_step(processing_step)
            processing_step.set_next_step(self.pipeline_termination)
            self.processing_steps.append(processing_step)
        else:
            raise Exception(
                "{}: {} not a batchprocessing step, bye ...".format(self.__name__, processing_step.__name__))

    def read_queue(self):
        try:
            while True:
                samples = self.sample_queue_in.get(block=False)
                if samples:
                    self.execute(samples)
        except thread_queue.Empty:
            pass
