import queue

from bitflow.helper import NotSupportedError
from bitflow.pipeline import BatchPipeline
from bitflow.processingstep import *


class Batch(ProcessingStep):
    __description__ = "Batch instantiates a batch pipeline and pushes samples batch wise into this pipeline"

    def __init__(self, size: int, flush_tag: str = None, flush_header_change: bool = True,
                 maxsize: int = DEFAULT_QUEUE_MAXSIZE, parallel_mode: str = None):
        super().__init__()
        self.__name__ = "Batch"
        self.size = size
        if flush_tag:
            raise NotSupportedError("Flushing batch by changing tag is not supported currently ...")
        # self.flush_tag = flush_tag
        self.flush_header = flush_header_change
        self.batch_pipeline = BatchPipeline(maxsize, parallel_mode)
        self.batch = []
        self.header = None

    def set_next_step(self, next_step):
        self.batch_pipeline.set_next_step(next_step)

    def add_processing_step(self, processing_step):
        self.batch_pipeline.add_processing_step(processing_step)

    def on_start(self):
        self.batch_pipeline.set_next_step(self.next_step)
        self.next_step = None
        self.batch_pipeline.start()

    def on_close(self):
        self.batch_pipeline.stop()

    def execute(self, sample):
        if not self.header:
            self.header = sample.header
        elif sample.header.has_changed(self.header) and self.flush_header:
            self.flush()
            self.header = sample.header
        self.batch.append(sample)
        if len(self.batch) >= self.size:
            self.flush()

    def flush(self):
        if self.batch:
            self.write_batch(self.batch.copy())
            self.batch.clear()

    def write_batch(self, sample_list):
        self.batch_pipeline.write(sample_list)

    def write(self, sample):
        raise NotImplementedError("%s: Writing of a single sample not supported. Use write_batch() method instead.",
                                  self.__name__)
