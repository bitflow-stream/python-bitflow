import multiprocessing
import queue as thread_queue
from threading import Thread

from bitflow.batchprocessingstep import *
from bitflow.processingstep import ProcessingStep, _AsyncProcessingStep


class PipelineTermination(ProcessingStep):

    def __init__(self, sample_queue):
        super().__init__()
        self.__name__ = "Pipeline_Termination"
        self.sample_queue = sample_queue

    def execute(self, s):
        super().write(s)
        if s:
            self.sample_queue.put(s)


class Pipeline(SyncAsyncProcessingStep, metaclass=helper.CtrlMethodDecorator):

    def __init__(self, maxsize=DEFAULT_QUEUE_MAXSIZE,
                 parallel_mode=PARALLEL_MODE_THREAD):
        self.processing_steps = []
        if parallel_mode:
            if parallel_mode == PARALLEL_MODE_THREAD:
                self.parallel_step = _PipelineThreaded(self.processing_steps, maxsize)
            elif parallel_mode == PARALLEL_MODE_PROCESS:
                self.parallel_step = _PipelineProcess(self.processing_steps, maxsize)
        self.pipeline_termination = PipelineTermination(self.parallel_utils)
        super().__init__(self.parallel_step)

    def add_processing_step(self, processing_step):
        if processing_step:
            if self.processing_steps:
                self.processing_steps[-1].set_next_step(processing_step)
            processing_step.set_next_step(self.pipeline_termination)
            self.processing_steps.append(processing_step)
        else:
            raise ValueError("Trying to add a None processing step to pipeline.")


class _PipelineThreaded(threading.Thread, _AsyncProcessingStep):

    def __init__(self, processing_steps, maxsize=DEFAULT_QUEUE_MAXSIZE):
        self.__name__ = "Pipeline"
        self.parallel_utils = ParallelUtils(thread_queue.Queue(maxsize=maxsize), thread_queue.Queue(maxsize=maxsize))
        self.processing_steps =  processing_steps
        threading.Thread.__init__(self)
        _AsyncProcessingStep.__init__(self, self.parallel_utils)

    def on_start(self):
        if self.processing_steps:
            # Start call propagates through step hierarchy
            self.processing_steps[0].start()

    def loop(self, sample):
        if self.processing_steps:
            self.processing_steps[0].execute(sample)
        else:
            self.parallel_utils.write(sample)

    def on_close(self):
        if self.processing_steps:
            self.processing_steps[0].stop()


class _PipelineProcess(multiprocessing.Process, _AsyncProcessingStep):

    def __init__(self, processing_steps, maxsize=DEFAULT_QUEUE_MAXSIZE):
        self.__name__ = "Pipeline"
        self.parallel_utils = ParallelUtils(multiprocessing.JoinableQueue(maxsize=maxsize),
                                            multiprocessing.JoinableQueue(maxsize=maxsize))
        self.processing_steps = processing_steps
        multiprocessing.Process.__init__(self)
        _AsyncProcessingStep.__init__(self, self.parallel_utils)

    def on_start(self):
        if self.processing_steps:
            # Start call propagates through step hierarchy
            self.processing_steps[0].start()

    def loop(self, sample):
        if self.processing_steps:
            self.processing_steps[0].execute(sample)
        else:
            self.parallel_utils.write(sample)

    def on_close(self):
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

    def start(self):
        super().start()  # For Decorator

    def run(self):
        while self.input_counter.value > 0 or self.sample_queue_in.qsize() > 0:
            try:
                samples = self.sample_queue_in.get(block=False)
                self.execute(samples)
                self.sample_queue_in.task_done()
            except thread_queue.Empty:
                continue
        self.on_close()

    def execute(self, samples: list):
        if samples:
            self.sample_counter += 1
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
