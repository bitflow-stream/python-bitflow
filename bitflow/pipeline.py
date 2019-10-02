import multiprocessing
import queue as thread_queue
from threading import Thread

from bitflow.batchprocessingstep import *
from bitflow.processingstep import ProcessingStep, _ProcessingStepAsync


class PipelineTermination(ProcessingStep):

    def __init__(self, parallel_utils=None):
        super().__init__()
        self.__name__ = "PipelineTermination"
        self.parallel_utils = parallel_utils

    def execute(self, sample):
        if sample:
            if self.parallel_utils:
                self.parallel_utils.write_out(sample)
            elif self.next_step:
                self.next_step.execute(sample)


class Pipeline(SyncAsyncProcessingStep, metaclass=helper.CtrlMethodDecorator):

    def __init__(self, processing_steps=None, maxsize=DEFAULT_QUEUE_MAXSIZE, parallel_mode=None):
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "Pipeline"
        if processing_steps:
            self.processing_steps = processing_steps
        else:
            self.processing_steps = []
        self.pipeline_termination = PipelineTermination(self.parallel_utils)

    def init_async_object(self, parallel_utils):
        parallel_step = None
        if self.parallel_mode:
            if self.parallel_mode == PARALLEL_MODE_THREAD:
                parallel_step = _PipelineThread(self.processing_steps, parallel_utils)
            elif self.parallel_mode == PARALLEL_MODE_PROCESS:
                parallel_step = _PipelineProcess(self.processing_steps, parallel_utils)
            else:
                raise ValueError("Unknown parallel mode %s. Supported modes are $s", self.parallel_mode,
                                 ",".join(PARALLEL_MODES))
        return parallel_step

    def set_next_step(self, next_step):
        self.pipeline_termination.set_next_step(next_step)

    def add_processing_step(self, processing_step):
        if processing_step:
            if self.processing_steps:
                self.processing_steps[-1].set_next_step(processing_step)
            if self.pipeline_termination:
                processing_step.set_next_step(self.pipeline_termination)
            self.processing_steps.append(processing_step)
        else:
            raise ValueError("Trying to add a None processing step to pipeline.")

    def execute_sequential(self, sample):
        super().write(sample)


class _PipelineAsync(_ProcessingStepAsync):

    def __init__(self, processing_steps, parallel_utils):
        super().__init__(parallel_utils)
        self.__name__ = "Pipeline_Async"
        self.processing_steps = processing_steps

    def on_start(self):
        if self.processing_steps:
            # Start call propagates through step hierarchy
            self.processing_steps[0].start()

    def loop(self, sample):
        if self.processing_steps:
            self.processing_steps[0].execute(sample)
        else:
            self.parallel_utils.write_out(sample)

    def on_close(self):
        if self.processing_steps:
            self.processing_steps[0].stop()


class _PipelineThread(threading.Thread, _PipelineAsync):

    def __init__(self, processing_steps, parallel_utils):
        super().__init__()
        self.__name__ = "Pipeline_Thread"
        threading.Thread.__init__(self)
        _PipelineAsync.__init__(self, processing_steps, parallel_utils)

    def run(self):
        _PipelineAsync.run(self)


class _PipelineProcess(multiprocessing.Process, _PipelineAsync):

    def __init__(self, processing_steps, parallel_utils):
        self.__name__ = "Pipeline_Process"
        multiprocessing.Process.__init__(self)
        _PipelineAsync.__init__(self, processing_steps, parallel_utils)

    def run(self):
       _PipelineAsync.run(self)


class BatchPipelineTermination(ProcessingStep):

    def __init__(self, parallel_utils):
        super().__init__()
        self.__name__ = "BatchPipelineTermination"
        self.parallel_utils = parallel_utils

    def execute(self, sample_list: list):
        if sample_list:
            for sample in sample_list:
                if sample:
                    if self.parallel_utils:
                        self.parallel_utils.write(sample)
                    elif self.next_step:
                        self.next_step.execute(sample)


class BatchPipeline(SyncAsyncProcessingStep):

    def __init__(self, maxsize, parallel_mode, processing_steps=None):
        super().__init__()
        self.__name__ = "BatchPipeline"
        if processing_steps:
            self.processing_steps = processing_steps
        else:
            self.processing_steps = []
        if parallel_mode:
            if parallel_mode == PARALLEL_MODE_THREAD:
                self.parallel_step = _BatchPipelineThread(self.processing_steps, maxsize)
            elif parallel_mode == PARALLEL_MODE_PROCESS:
                self.parallel_step = _BatchPipelineProcess(self.processing_steps, maxsize)
            else:
                raise ValueError("Unknown parallel mode %s. Supported modes are $s", parallel_mode,
                                 ",".join(PARALLEL_MODES))
            self.parallel_utils = self.parallel_step.parallel_utils

        self.pipeline_termination = PipelineTermination(self.parallel_utils)

    def set_next_step(self, next_step):
        self.pipeline_termination.set_next_step(next_step)

    def add_processing_step(self, processing_step):
        if processing_step and isinstance(processing_step, BatchProcessingStep):
            if self.processing_steps:
                self.processing_steps[-1].set_next_step(processing_step)
            else:
                self.next_step = processing_step
            processing_step.set_next_step(self.pipeline_termination)
            self.processing_steps.append(processing_step)
        else:
            raise ValueError("%s: Added step %s not a batch processing step.", self.__name__, processing_step.__name__)

    def execute_sequential(self, sample_list):
        if isinstance(sample_list, list):
            self.next_step.execute(sample_list)
        else:
            logging.warning("%s: Can only handle sample lists. Dropping %s instance.", self.__name__, str(sample_list))


class _BatchPipelineAsync(_PipelineAsync):

    def __init__(self, processing_steps, maxsize):
        self.__name__ = "BatchPipeline_Async"
        self.parallel_utils = ParallelUtils(self.create_queue(maxsize), self.create_queue(maxsize))
        self.processing_steps = processing_steps
        _ProcessingStepAsync.__init__(self, self.parallel_utils)

    def create_queue(self, maxsize):
        raise NotImplementedError("Method needs to be implemented based on parallelization type (either thread or "
                                  "process).")

    def loop(self, sample_list: list):
        if self.processing_steps:
            self.processing_steps[0].execute(sample_list)
        else:
            self.parallel_utils.write(sample_list)


class _BatchPipelineThread(threading.Thread, _BatchPipelineAsync):

    def __init__(self, processing_steps, maxsize):
        self.__name__ = "BatchPipeline_Thread"
        threading.Thread.__init__(self)
        _PipelineAsync.__init__(self, processing_steps, maxsize)

    def create_queue(self, maxsize):
        return thread_queue.Queue(maxsize)


class _BatchPipelineProcess(multiprocessing.Process, _BatchPipelineAsync):

    def __init__(self, processing_steps, maxsize=DEFAULT_QUEUE_MAXSIZE):
        self.__name__ = "BatchPipeline_Process"
        multiprocessing.Process.__init__(self)
        _PipelineAsync.__init__(self, processing_steps, maxsize)

    def create_queue(self, maxsize):
        return multiprocessing.JoinableQueue(maxsize=maxsize)
