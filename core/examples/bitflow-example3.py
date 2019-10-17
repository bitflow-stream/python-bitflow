#!/usr/bin/env python3

import logging
import time

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

    batch_size = 1
    pl = PipelineSync()
    file_source = FileSource(path=input_filename, pipeline=pl)
    batch_step = Batch(size=batch_size, parallel_mode=None)
    batch_step.add_processing_step(AvgBatchProcessingStep())
    pl.add_processing_step(batch_step)
    pl.add_processing_step(FileSink(filename=out_filename, data_format=CSV_DATA_FORMAT))

    file_source.start_and_wait()


if __name__ == '__main__':
    main()
