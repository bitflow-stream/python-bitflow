#!/usr/bin/env python3

import logging

from bitflow.io.sinksteps import TerminalOut
from bitflow.io.sources import FileSource
from bitflow.pipeline import Pipeline
from bitflow.steps.plotprocessingsteps import PlotLinePlot

''' example python3-bitflow main'''


def main():
    global pipeline
    # enable logging
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # file to read from
    input_filename = "testing/testing_file_in.csv"

    # define pipeline
    pipeline = Pipeline(maxsize=5)

    # add processing steps to pipeline
    pipeline.add_processing_step(PlotLinePlot(metric_names="pkg_out_1000-1100"))
    # add terminal output to pipeline
    pipeline.add_processing_step(TerminalOut())

    # start pipeline
    pipeline.start()

    # define file source
    filesource = FileSource(path=input_filename, pipeline=pipeline)
    # start file source
    filesource.start()

    import time
    time.sleep(4)
    filesource.stop()


if __name__ == '__main__':
    main()
