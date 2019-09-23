from statistics import mean

from bitflow.processingstep import *


def initialize_batch_step(name, script_args):
    batch_steps = BatchProcessingStep.get_all_subclasses(BatchProcessingStep)

    for f in batch_steps:
        if f.__name__.lower() == name.lower() and compare_args(f, script_args):
            logging.info("{} with args: {}  ok ...".format(name, script_args))
            try:
                f_obj = f(**script_args)
            except Exception as e:
                logging.exception(e)
                f_obj = None
            return f_obj
    logging.info("{} with args: {}  failed ...".format(name, script_args))
    return None


class BatchProcessingStep(ProcessingStep):
    """ Abstract BatchProcessingStep Class"""
    __description__ = "No description written yet."

    def __init__(self):
        super().__init__()
        self.next_step = None

    @staticmethod
    def get_all_subclasses(cls):
        all_subclasses = []
        for subclass in cls.__subclasses__():
            if subclass.__name__ not in SUBCLASSES_TO_IGNORE:
                all_subclasses.append(subclass)
            all_subclasses.extend(ProcessingStep.get_all_subclasses(subclass))
        return all_subclasses

    def write_batch(self, samples: list):
        if samples and self.next_step:
            self.next_step.execute(samples)

    def write(self, sample):
        raise NotImplementedError("%s: Writing of a single sample not supported. Use write_batch() method instead.",
                                  self.__name__)

    def execute(self, samples: list):
        pass

    def stop(self):
        self.on_close()
        if self.next_step:
            self.next_step.stop()

    def on_close(self):
        pass

class AvgBatchProcessingStep(BatchProcessingStep):
    __description__ = "AVG all metrics"

    def __init__(self):
        super().__init__()
        self.__name__ = "AvgBatch"
        self.header = None

    # TODO This calculates the avg over all metrics of each sample which seems to be not useful. Better to calculate
    # TODO the avg for each metric over all samples
    def build_sample(self, metrics_lst, header, timestamp, tags):
        avg_metrics = [(mean(x) / len(x)) if len(x) > 0 else 0 for x in metrics_lst]
        return Sample(header=header, metrics=avg_metrics, timestamp=timestamp, tags=tags)

    def execute(self, samples: list):
        if samples:
            metrics = []
            if self.header is None:
                self.header = samples[0].header
            for sample in samples:
                metrics.append(sample.get_metrics())
            self.write_batch([self.build_sample(metrics, self.header, samples[0].timestamp, samples[0].tags)])
