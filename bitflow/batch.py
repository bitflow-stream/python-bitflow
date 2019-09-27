import queue

from bitflow.helper import NotSupportedError
from bitflow.pipeline import BatchPipeline
from bitflow.processingstep import *


class Batch(ProcessingStep):
    __description__ = "Batch instantiates a batch pipeline and pushes samples batch wise into this pipeline"

    def __init__(self, size: int, flush_tag: str = None, flush_header_change: bool = True):
        super().__init__()
        self.__name__ = "Batch"
        self.size = int(size)
        if flush_tag:
            raise NotSupportedError("Flushing batch by changing tag is not supported currently ...")
        # self.flush_tag = flush_tag
        self.flush_header = flush_header_change
        self.batch_pipeline = BatchPipeline()
        self.sample_queue_in = self.batch_pipeline.sample_queue_in
        self.sample_queue_out = self.batch_pipeline.sample_queue_out
        self.input_counter = self.batch_pipeline.input_counter
        self.batch = []
        self.header = None

    def on_start(self):
        self.input_counter.value += 1
        self.batch_pipeline.start()

    def add_processing_step(self, processing_step):
        self.batch_pipeline.add_processing_step(processing_step)

    def forward_samples(self):
        while True:
            try:
                sample = self.sample_queue_out.get(block=False)
                super().write(sample)
                self.sample_queue_out.task_done()
            except queue.Empty:
                return

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
        self.forward_samples()

    def write_batch(self, samples):
        self.sample_queue_in.put(samples)

    def write(self, sample):
        raise NotImplementedError("%s: Writing of a single sample not supported. Use write_batch() method instead.",
                                  self.__name__)

    def on_close(self):
        self.flush()
        # Terminate pipeline
        self.input_counter.value -= 1
        # Wait until everything that was written to the queue is processed
        self.sample_queue_in.join()
        # Wait until pipeline terminates
        self.batch_pipeline.join()
        # Forward all result samples to next step
        self.forward_samples()
        # Make sure everything was forwarded correctly
        self.sample_queue_out.join()
