#!/usr/bin/env python3

import logging

from bitflow.io.marshaller import CSV_DATA_FORMAT
from bitflow.io.sinksteps import TerminalOut, FileSink
from bitflow.io.sources import FileSource
from bitflow.pipeline import Pipeline
from bitflow.processingstep import PARALLEL_MODE_PROCESS, ProcessingStep, PARALLEL_MODE_THREAD
from bitflow.steps.plotprocessingsteps import PlotLinePlot

''' example python3-bitflow main'''


class TestStep(ProcessingStep):

    def execute(self, sample):
        print(str(sample))


def main():
    # enable logging
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # file to read from
    input_filename = "testing/testing_file_in.csv"
    output_filename = "testing/o.csv"

    # define pipeline
    pipeline = Pipeline(maxsize=5, parallel_mode=PARALLEL_MODE_PROCESS)

    # add processing steps to pipeline
    #pipeline.add_processing_step(PlotLinePlot(metric_names="pkg_out_1000-1100"))
    # add terminal output to pipeline
    #pipeline.add_processing_step(FileSink(filename=output_filename, parallel_mode=PARALLEL_MODE_PROCESS))
    pipeline.add_processing_step(TerminalOut(data_format=CSV_DATA_FORMAT, parallel_mode=PARALLEL_MODE_PROCESS))

    # define file source
    filesource = FileSource(path=input_filename, pipeline=pipeline)
    # start file source
    filesource.start_and_wait()


if __name__ == '__main__':
    main()
