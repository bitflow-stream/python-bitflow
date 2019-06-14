import unittest
import logging
import os
import time
import math
from support import *
from context import script_parser, helper, fork, bitflow_pipeline, batch, processingstep, sinksteps, source, batchprocessingstep


class TestFork(unittest.TestCase):

    def test_fork_in_bf_script_simple(self):
         tp = script_parser.parse_script("debuggenerationstep() -> Fork_Tags(tag=blub)\
                                            {* -> addtag( tags={a=b} ) }")

    def test_fork_in_bf_script_intermediate(self):
         tp = script_parser.parse_script(
            "debuggenerationstep() -> Fork_Tags(tag=blub)\
                                        { bla -> addtag( tags={a=b} ) }\
                                    -> Noop()")
    def test_fork_in_bf_script_advanced(self):
         tp = script_parser.parse_script(
            "debuggenerationstep() -> Fork_Tags(tag=blub){\
                                        bla -> addtag(tags={a=b});\
                                        blub -> addtag(tags={a=c})}\
                                    -> Noop()")

    def test_pipeline_and_fork(self):
        tag_fork =fork.Fork_Tags(tag="blub")
        tag_fork.add_processing_steps([],["bla","blub"])
        pl = bitflow_pipeline.Pipeline()
        pl.add_processing_step(processingstep.DebugGenerationStep())
        pl.add_processing_step(tag_fork)
        pl.start()
        time.sleep(2)
        pl.stop()

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass

class TestBatchPipeline(unittest.TestCase):

    DEFAULT_SLEEPING_DURATION = 2

    def test_batch_pipeline_add_batch_step(self):
        batch_size = 20
        pl = bitflow_pipeline.Pipeline()
        file_source = source.FileSource(    filename=TESTING_IN_FILE_CSV,
                                            pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_pipeline = bitflow_pipeline.BatchPipeline(multiprocessing_input=False)
        batch_pipeline.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        batch_step.set_root_pipeline(pl)
        batch_step.set_batch_pipeline(batch_pipeline)
        pl.add_processing_step(batch_step)
        file_source.start()
        batch_pipeline.start()
        pl.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        batch_pipeline.stop()
        pl.stop()

    def test_batch_pipeline_number_of_samples_out_size_1(self):
        batch_size = 1
        pl = bitflow_pipeline.Pipeline()
        file_source = source.FileSource(   filename=TESTING_IN_FILE_CSV,
                                           pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_pipeline = bitflow_pipeline.BatchPipeline(multiprocessing_input=False)
        batch_pipeline.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        batch_step.set_root_pipeline(pl)
        batch_step.set_batch_pipeline(batch_pipeline)
        pl.add_processing_step(batch_step)
        pl.add_processing_step(sinksteps.FileSink(  filename=TESTING_OUT_FILE_CSV,
                                                    data_format=script_parser.CSV_DATA_FORMAT_IDENTIFIER))

        batch_pipeline.start()
        pl.start()
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        batch_pipeline.stop()
        pl.stop()

        a = file_len(TESTING_IN_FILE_CSV)
        b = file_len(TESTING_OUT_FILE_CSV)
        self.assertEqual(a,b)

    def test_batch_pipeline_number_of_samples_out_size_20(self):
        batch_size = 20
        pl = bitflow_pipeline.Pipeline()
        file_source = source.FileSource(filename=TESTING_IN_FILE_CSV,
                                        pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_pipeline = bitflow_pipeline.BatchPipeline(multiprocessing_input=False)
        batch_pipeline.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        batch_step.set_root_pipeline(pl)
        batch_step.set_batch_pipeline(batch_pipeline)
        pl.add_processing_step(batch_step)
        pl.add_processing_step(sinksteps.FileSink(  filename=TESTING_OUT_FILE_CSV,
                                                    data_format=script_parser.CSV_DATA_FORMAT_IDENTIFIER))

        batch_pipeline.start()
        pl.start()
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        batch_pipeline.stop()
        pl.stop()

        a = file_len(TESTING_IN_FILE_CSV)
        b = file_len(TESTING_OUT_FILE_CSV)
        self.assertEqual(math.floor((a - 1) / batch_size),b -1 ) # - header line

    def test_batch_pipeline_number_of_samples_out_size_37(self):
        batch_size = 37
        pl = bitflow_pipeline.Pipeline()
        file_source = source.FileSource(    filename=TESTING_IN_FILE_CSV,
                                            pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_pipeline = bitflow_pipeline.BatchPipeline(multiprocessing_input=False)
        batch_pipeline.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        batch_step.set_root_pipeline(pl)
        batch_step.set_batch_pipeline(batch_pipeline)
        pl.add_processing_step(batch_step)
        pl.add_processing_step(sinksteps.FileSink(  filename=TESTING_OUT_FILE_CSV,
                                                    data_format=script_parser.CSV_DATA_FORMAT_IDENTIFIER))

        file_source.start()
        batch_pipeline.start()
        pl.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        batch_pipeline.stop()
        pl.stop()

        a = file_len(TESTING_IN_FILE_CSV)
        b = file_len(TESTING_OUT_FILE_CSV)
        self.assertEqual(math.floor((a - 1) / batch_size),b -1 ) # - minus header line

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        remove_files(TEST_OUT_FILES)

class TestPipeline(unittest.TestCase):

    DEFAULT_SLEEPING_DURATION = 2

    def test_simple_empty_one_step_pipeline(self):
        pl = bitflow_pipeline.Pipeline()
        pl.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pl.stop()

    def test_generative_processing_step(self):
        pl = bitflow_pipeline.Pipeline()
        pl.add_processing_step(processingstep.DebugGenerationStep())
        pl.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pl.stop()

    def test_subpipeline(self):
        inner_pipeline = bitflow_pipeline.Pipeline()

        outer_pipeline = bitflow_pipeline.Pipeline()
        outer_pipeline.add_processing_step(processingstep.DebugGenerationStep())
        outer_pipeline.add_processing_step(inner_pipeline)
        outer_pipeline.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        outer_pipeline.stop()

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
