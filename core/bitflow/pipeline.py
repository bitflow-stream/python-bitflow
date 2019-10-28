import logging
import multiprocessing
import threading

from core.bitflow import helper
from core.bitflow.batchprocessingstep import BatchProcessingStep
from core.bitflow.processingstep import DEFAULT_QUEUE_MAXSIZE, PARALLEL_MODE_THREAD, PARALLEL_MODE_PROCESS, \
    _ProcessingStepAsync, ProcessingStep, AsyncProcessingStep


class PipelineTermination(ProcessingStep):

    def __init__(self, parallel_utils=None):
        super().__init__()
        self.__name__ = "PipelineTermination"
        self.parallel_utils = parallel_utils

    def execute(self, sample):
        if sample:
            if self.parallel_utils:
                self.parallel_utils.write_to_parent(sample)
            elif self.next_step:
                super().write(sample)


class PipelineSync(ProcessingStep, metaclass=helper.CtrlMethodDecorator):

    def __init__(self, processing_steps=None):
        super().__init__()
        self.__name__ = "Pipeline"
        self.pipeline_termination = PipelineTermination()
        self.processing_steps = []
        if processing_steps:
            for step in processing_steps:
                self.add_processing_step(step)

    def on_start(self):
        if self.processing_steps:
            self.processing_steps[0].start()
        else:
            self.pipeline_termination.start()

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
            raise ValueError("{}: Cannot add a None processing step to pipeline.".format(self.__name__))

    def execute(self, sample):
        if self.processing_steps:
            self.processing_steps[0].execute(sample)
        else:
            self.pipeline_termination.execute(sample)

    def on_close(self):
        if self.processing_steps:
            self.processing_steps[0].stop()
        else:
            self.pipeline_termination.stop()


class PipelineAsync(AsyncProcessingStep, metaclass=helper.CtrlMethodDecorator):

    def __init__(self, processing_steps=None, maxsize=DEFAULT_QUEUE_MAXSIZE, parallel_mode=PARALLEL_MODE_THREAD):
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "Pipeline"
        self.processing_steps = []
        if processing_steps:
            for step in processing_steps:
                self.add_processing_step(step)
        self.pipeline_termination = None

    def init_async_object(self, parallel_utils, parallel_mode):
        parallel_step = None
        if parallel_mode == PARALLEL_MODE_THREAD:
            parallel_step = _PipelineThread(self.processing_steps, parallel_utils)
        elif parallel_mode == PARALLEL_MODE_PROCESS:
            parallel_step = _PipelineProcess(self.processing_steps, parallel_utils)
        self.pipeline_termination = PipelineTermination(self.parallel_utils)
        if self.processing_steps:
            self.processing_steps[-1].set_next_step(self.pipeline_termination)
        return parallel_step

    def set_next_step(self, next_step):
        self.next_step = next_step

    def add_processing_step(self, processing_step):
        if processing_step:
            if self.processing_steps:
                self.processing_steps[-1].set_next_step(processing_step)
            self.processing_steps.append(processing_step)
        else:
            raise ValueError("Trying to add a None processing step to pipeline.")


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
            self.parallel_utils.write_to_parent(sample)

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

    def __init__(self, parallel_utils=None):
        super().__init__()
        self.__name__ = "BatchPipelineTermination"
        self.parallel_utils = parallel_utils

    def execute(self, sample_list: list):
        for sample in sample_list:
            if self.parallel_utils:
                self.parallel_utils.write_to_parent(sample)
            elif self.next_step:
                self.next_step.execute(sample)


class BatchPipelineSync(ProcessingStep, metaclass=helper.CtrlMethodDecorator):

    def __init__(self, processing_steps=None):
        super().__init__()
        self.__name__ = "BatchPipeline"
        self.pipeline_termination = BatchPipelineTermination()
        self.processing_steps = []
        if processing_steps:
            for step in processing_steps:
                self.add_processing_step(step)

    def on_start(self):
        if self.processing_steps:
            self.processing_steps[0].start()
        else:
            self.pipeline_termination.start()

    def set_next_step(self, next_step):
        self.pipeline_termination.set_next_step(next_step)

    def add_processing_step(self, processing_step):
        if processing_step:
            if isinstance(processing_step, BatchProcessingStep):
                if self.processing_steps:
                    self.processing_steps[-1].set_next_step(processing_step)
                if self.pipeline_termination:
                    processing_step.set_next_step(self.pipeline_termination)
                self.processing_steps.append(processing_step)
            else:
                raise ValueError("{}: Added step {} not a batch processing step.".format(
                    self.__name__, processing_step.__name__))
        else:
            raise ValueError("{}: Cannot add a None processing step to pipeline.".format(self.__name__))

    def execute(self, sample_list):
        if self.processing_steps:
            if isinstance(sample_list, list):
                self.processing_steps[0].execute(sample_list)
            else:
                logging.warning("%s: Can only handle sample lists. Dropping %s instance.",
                                self.__name__, str(sample_list))
        else:
            self.pipeline_termination.execute(sample_list)

    def on_close(self):
        if self.processing_steps:
            self.processing_steps[0].stop()
        else:
            self.pipeline_termination.stop()


class BatchPipelineAsync(AsyncProcessingStep, metaclass=helper.CtrlMethodDecorator):

    def __init__(self, processing_steps=None, maxsize=DEFAULT_QUEUE_MAXSIZE, parallel_mode=PARALLEL_MODE_THREAD):
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "BatchPipeline"
        self.processing_steps = []
        if processing_steps:
            for step in processing_steps:
                self.add_processing_step(step)
        self.pipeline_termination = None

    def init_async_object(self, parallel_utils, parallel_mode):
        parallel_step = None
        if parallel_mode == PARALLEL_MODE_THREAD:
            parallel_step = _BatchPipelineThread(self.processing_steps, parallel_utils)
        elif parallel_mode == PARALLEL_MODE_PROCESS:
            parallel_step = _BatchPipelineProcess(self.processing_steps, parallel_utils)
        self.pipeline_termination = BatchPipelineTermination(self.parallel_utils)
        if self.processing_steps:
            self.processing_steps[-1].set_next_step(self.pipeline_termination)
        return parallel_step

    def set_next_step(self, next_step):
        self.next_step = next_step

    def add_processing_step(self, processing_step):
        if processing_step:
            if isinstance(processing_step, BatchProcessingStep):
                if self.processing_steps:
                    self.processing_steps[-1].set_next_step(processing_step)
                self.processing_steps.append(processing_step)
            else:
                raise ValueError("{}: Added step {} not a batch processing step.".format(
                    self.__name__, processing_step.__name__))
        else:
            raise ValueError("{}: Cannot add a None processing step to pipeline.".format(self.__name__))


class _BatchPipelineAsync(_ProcessingStepAsync):

    def __init__(self, processing_steps, parallel_utils):
        super().__init__(parallel_utils)
        self.__name__ = "BatchPipeline_Async"
        self.processing_steps = processing_steps

    def on_start(self):
        if self.processing_steps:
            # Start call propagates through step hierarchy
            self.processing_steps[0].start()

    def loop(self, sample_list):
        if not isinstance(sample_list, list):
            logging.warning("%s: Can only handle sample lists. Dropping %s instance.", self.__name__, str(sample_list))
            return None
        if self.processing_steps:
            self.processing_steps[0].execute(sample_list)
        else:
            self.parallel_utils.write_to_parent(sample_list)

    def on_close(self):
        if self.processing_steps:
            self.processing_steps[0].stop()


class _BatchPipelineThread(threading.Thread, _PipelineAsync):

    def __init__(self, processing_steps, parallel_utils):
        super().__init__()
        self.__name__ = "BatchPipeline_Thread"
        threading.Thread.__init__(self)
        _PipelineAsync.__init__(self, processing_steps, parallel_utils)

    def run(self):
        _PipelineAsync.run(self)


class _BatchPipelineProcess(multiprocessing.Process, _PipelineAsync):

    def __init__(self, processing_steps, parallel_utils):
        self.__name__ = "BatchPipeline_Process"
        multiprocessing.Process.__init__(self)
        _PipelineAsync.__init__(self, processing_steps, parallel_utils)

    def run(self):
        _PipelineAsync.run(self)
