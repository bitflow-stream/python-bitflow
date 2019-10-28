#!/usr/bin/env python3

import logging
import time

from core.bitflow.io.sinksteps import TerminalOut
from core.bitflow.io.sources import FileSource
from core.bitflow.pipeline import Pipeline
from core.bitflow.processingstep import ProcessingStep


class Delay(ProcessingStep):
    __description__ = "Delys the pipeline by [delay] seconds."
    __name__ = "delay"

    def __init__(self, delay: int):
        super().__init__()
        self.delay = delay

    def execute(self, sample):
        time.sleep(self.delay)
        self.write(sample)



''' example python3-bitflow main'''


def main():
    # enable logging
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # file to read from
    input_filename = "testing/testing_file_in.csv"

    # define pipeline
    pipeline = Pipeline()

    # add processingsteps to pipeline
    pipeline.add_processing_step(Delay(delay=2))
    # add terminal output to pipeline
    pipeline.add_processing_step(TerminalOut())

    # define file source
    filesource = FileSource(path=input_filename,
                            pipeline=pipeline)
    # start file source
    filesource.start_and_wait()


if __name__ == '__main__':
    main()
