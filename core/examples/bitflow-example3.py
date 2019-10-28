#!/usr/bin/env python3
from core.bitflow.io.sinksteps import FileSink
from core.bitflow.io.sources import FileSource
from core.bitflow.io.marshaller import *
from core.bitflow.pipeline import PipelineSync
from core.bitflow.batchprocessingstep import AvgBatchProcessingStep
from core.bitflow.batch import Batch


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
