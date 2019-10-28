#!/usr/bin/env python3

import logging

from core.bitflow.io.sinksteps import TerminalOut, FileSink
from core.bitflow.io.sources import FileSource
from core.bitflow.pipeline import PipelineSync, PipelineAsync
from core.bitflow.processingstep import PARALLEL_MODE_PROCESS, ProcessingStep, PARALLEL_MODE_THREAD

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
    pipeline_sync = PipelineSync()

    # add processing steps to pipeline
    #pipeline.add_processing_step(PlotLinePlot(metric_names="pkg_out_1000-1100"))
    # add terminal output to pipeline
    pipeline_sync.add_processing_step(FileSink(filename=output_filename, parallel_mode=PARALLEL_MODE_PROCESS))
    #pipeline.add_processing_step(TerminalOut(data_format=CSV_DATA_FORMAT, parallel_mode=PARALLEL_MODE_PROCESS))

    # define file source
    filesource = FileSource(path=input_filename, pipeline=pipeline_sync)
    # start file source
    filesource.start_and_wait()

    # define pipeline
    pipeline_async = PipelineAsync(maxsize=5, parallel_mode=PARALLEL_MODE_PROCESS)

    # add processing steps to pipeline
    # pipeline.add_processing_step(PlotLinePlot(metric_names="pkg_out_1000-1100"))
    # add terminal output to pipeline
    pipeline_async.add_processing_step(FileSink(filename=output_filename, parallel_mode=PARALLEL_MODE_PROCESS))
    # pipeline.add_processing_step(TerminalOut(data_format=CSV_DATA_FORMAT, parallel_mode=PARALLEL_MODE_PROCESS))

    # define file source
    filesource = FileSource(path=input_filename, pipeline=pipeline_sync)
    # start file source
    filesource.start_and_wait()


if __name__ == '__main__':
    main()
