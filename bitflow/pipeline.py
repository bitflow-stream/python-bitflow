import logging
import multiprocessing
import queue as thread_queue
import threading
import time

DEFAULT_QUEUE_MAXSIZE = 10000
DEFAULT_MULTIPROCESSING_INPUT = True
DEFAULT_BATCH_MULTIPROCESSING_INPUT = False
DEFAULT_QUEUES_EMPTY_TIMEOUT = 0.1


class Pipeline(threading.Thread):

    def __init__(self, maxsize=DEFAULT_QUEUE_MAXSIZE, multiprocessing_input=DEFAULT_MULTIPROCESSING_INPUT):
        self.__name__ = "Pipeline"
        self.queue = None
        self.maxsize = maxsize
        self.queue = self.create_queue(multiprocessing_input)
        self.subqueues = {}
        self.processing_steps = []
        self.running = True
        self.next_step = None
        self.first_step = None
        super().__init__()

    def create_queue(self, multiprocessing_input):
        if multiprocessing_input:
            queue = multiprocessing.Queue(maxsize=self.maxsize)
        else:
            queue = thread_queue.Queue(maxsize=self.maxsize)
        return queue

    # subqueue is a que that forwards samples to a later ps in pipeline
    def get_subqueue(self, pos):
        if pos not in self.subqueues:
            # might change some day, currently pipelines are always threads.
            self.subqueues[pos] = self.create_queue(multiprocessing_input=False)
        return self.subqueues[pos]

    def forward_to_first_ps(self):
        if self.queue.qsize() is 0:
            return False
        sample = self.queue.get()
        if self.first_step:
            self.first_step.execute(sample)
        if isinstance(self.queue, thread_queue.Queue):
            self.queue.task_done()
        return True

    # forwards samples to a processing step somewhere in the pipeline
    def forward_to_appending_ps(self):
        r = False
        if len(self.subqueues) == 0:
            return r
        for pos, q in self.subqueues.items():
            if q.qsize() is 0:
                continue
            sample = q.get()
            r = True
            if isinstance(q, thread_queue.Queue):
                q.task_done()
            self.processing_steps[pos].execute(sample)
        return r

    def run(self):
        self.prepare_processing_steps()
        while self.running:
            a = self.forward_to_first_ps()
            b = self.forward_to_appending_ps()
            # if all queues are emtpy wait, as we have several queues at this point
            # we can not wait on a certail queue
            if not a and not b:
                time.sleep(DEFAULT_QUEUES_EMPTY_TIMEOUT)
        self.on_close()

    def execute(self, s):
        self.queue.put(s)

    # forward sample to ps after the given one
    def execute_after(self, s, ps):
        pos = self.get_processing_step_position(ps)
        # do not forward after unknown ps or if there is no following ps
        if pos == -1 or len(self.get_processing_steps()) <= pos + 1:
            return
        q = self.get_subqueue(pos + 1)  # ps after the searched one
        q.put(s)

    def next_step(self, step):
        self.next_step = step

    def chain_steps(self):
        for i in range(len(self.processing_steps) - 1, -1, -1):
            if i == 0:
                self.first_step = self.processing_steps[i]
            else:
                self.processing_steps[i - 1].set_next_step(self.processing_steps[i])
        if self.next_step:
            self.processing_steps[len(self.processing_steps) - 1] = self.next_step

    def prepare_processing_steps(self):
        for processing_step in self.processing_steps:
            if isinstance(processing_step, threading.Thread) or isinstance(processing_step, Pipeline):
                processing_step.start()

    # processing_step ,fork or subpipeline
    def add_processing_step(self, processing_step):
        self.processing_steps.append(processing_step)
        self.chain_steps()

    def remove_processing_step(self, processing_step):
        try:
            self.processing_steps.remove(processing_step)
            self.chain_steps()
        except:
            logging.warning(processing_step.__name__ + " not found in pipeline")

    def get_processing_step_position(self, processing_step):
        for i in range(len(self.processing_steps)):
            if self.processing_steps[i] == processing_step:
                return i
        return -1

    def get_processing_steps(self):
        return self.processing_steps

    def close_processing_steps(self):
        for ps in self.processing_steps:
            ps.stop()
            if isinstance(ps, threading.Thread):
                ps.join()

    def stop(self):
        self.running = False

    def on_close(self):
        logging.info("{}: closing  ...".format(self.__name__))
        self.close_processing_steps()


class BatchPipeline(Pipeline):
    def __init__(self, maxsize=DEFAULT_QUEUE_MAXSIZE, multiprocessing_input=DEFAULT_BATCH_MULTIPROCESSING_INPUT):
        super().__init__(maxsize, multiprocessing_input)
        self.__name__ = "BatchPipeline"

    # handles a list of samples
    def forward_to_first_ps(self):
        if self.queue.qsize() is 0:
            return False
        samples = self.queue.get()
        if self.first_step:
            self.first_step.execute(samples)
        if isinstance(self.queue, thread_queue.Queue):
            self.queue.task_done()
        return True

    def add_processing_step(self, processing_step):
        import bitflow.batchprocessingstep
        if isinstance(processing_step, bitflow.batchprocessingstep.BatchProcessingStep):
            self.processing_steps.append(processing_step)
            self.chain_steps()
        else:
            raise Exception(
                "{}: {} not a batchprocessing step, bye ...".format(self.__name__, processing_step.__name__))
