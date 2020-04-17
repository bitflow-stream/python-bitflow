import logging


class SampleListChannel:
    def __init__(self, samples):
        self.input = samples
        self.output = []
        self.closed = False

    def read_sample(self):
        if len(self.input) == 0:
            return None
        return self.input.pop(0)

    def output_sample(self, sample):
        self.output.append(sample)

    def close(self):
        self.closed = True


def configure_logging():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.ERROR)
