import logging
from bitflow.runner import ProcessingStep

class MyCustomProcessingStep(ProcessingStep):
    """Example processing step"""
    step_name = "example-step"

    def __init__(self, intArg:int, strArg:str = "world"):
        self.intArg = intArg
        self.strArg = strArg
        self.num_samples = 0
        logging.info("Example processing step instantiated. Int arg: {}. Str arg: {}".format(self.intArg, self.strArg))

    def initialize(self, context):
        logging.info("Example processing step initializing")
        super().initialize(context)

    def handle_sample(self, sample):
        self.num_samples += 1

        # Modify the sample
        for i, value in enumerate(sample.metrics):
            sample.metrics[i] = value * self.intArg
        sample.set_tag("hello", self.strArg)

        # Only forward one out of two sample
        if self.num_samples % 2 == 0:
            self.output(sample)

    def cleanup(self):
        logging.info("Example processing step shutting down. Processed samples: {}.".format(self.num_samples))
