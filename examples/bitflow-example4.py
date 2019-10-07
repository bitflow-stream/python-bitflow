#!/usr/bin/env python3

import logging
import time

from bitflow import fork, processingstep
from bitflow.io import sinksteps
from bitflow.io.sinksteps import FileSink
from bitflow.io.sources import FileSource
from bitflow.io.marshaller import *
from bitflow.pipeline import PipelineSync, PipelineAsync
from bitflow.batchprocessingstep import AvgBatchProcessingStep
from bitflow.batch import Batch
from bitflow.processingstep import ProcessingStep, PARALLEL_MODE_THREAD


def main():

    input_filename = "testing/in_small.csv"
    out_filename = "testing/out_small.csv"

    pipeline = PipelineSync()

    file_source = FileSource(path=input_filename, pipeline=pipeline)
    tag_fork = fork.Fork_Tags(tag="filter", parallel_mode=None)
    tag_fork.add_processing_steps([processingstep.Noop()], ["*"])
    pipeline.add_processing_step(tag_fork)
    pipeline.add_processing_step(sinksteps.FileSink(filename=out_filename,
                                                    data_format=CSV_DATA_FORMAT))

    file_source.start_and_wait()


if __name__ == '__main__':
    main()
