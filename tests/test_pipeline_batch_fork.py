import math
import time
import unittest

from bitflow import fork, batch, processingstep, batchprocessingstep
from bitflow import pipeline as pipe
from bitflow.io import sinksteps, sources
from bitflow.io.marshaller import *
from bitflow.script import script_parser
from tests.support import *

#TODO test all parallel and the sequential mode

class TestFork(unittest.TestCase):

    def test_fork_in_bf_script_simple(self):
        script_parser.parse_script("debuggenerationstep() -> Fork_Tags(tag=blub)\
                                            {* -> addtag( tags={a=b} ) }")

    def test_fork_in_bf_script_intermediate(self):
        script_parser.parse_script(
            "debuggenerationstep() -> Fork_Tags(tag=blub)\
                                        { bla -> addtag( tags={a=b} ) }\
                                    -> Noop()")

    def test_fork_in_bf_script_advanced(self):
        script_parser.parse_script(
            "debuggenerationstep() -> Fork_Tags(tag=blub){\
                                        bla -> addtag(tags={a=b});\
                                        blub -> addtag(tags={a=c})}\
                                    -> Noop()")

    def test_pipeline_and_fork(self):
        tag_fork = fork.Fork_Tags(tag="blub")
        tag_fork.add_processing_steps([], ["bla", "blub"])
        pl = pipe.Pipeline()
        pl.add_processing_step(processingstep.DebugGenerator())
        pl.add_processing_step(tag_fork)
        pl.start()
        pl.join()

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass


class TestBatchPipeline(unittest.TestCase):

    def test_batch_pipeline_add_batch_step(self):
        batch_size = 20
        pl = pipe.Pipeline()
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        pl.add_processing_step(batch_step)

        file_source.start_and_wait()

    def test_batch_pipeline_number_of_samples_out_size_1(self):
        remove_files(TEST_OUT_FILES)
        batch_size = 1
        pl = pipe.Pipeline()
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV_SMALL, pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        pl.add_processing_step(batch_step)
        pl.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))

        file_source.start_and_wait()

        a = read_file(TESTING_EXPECTED_BATCH_OUT_SMALL)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a, b)

    def test_batch_pipeline_number_of_samples_out_size_20(self):
        remove_files(TEST_OUT_FILES)
        batch_size = 20
        pl = pipe.Pipeline()
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        pl.add_processing_step(batch_step)
        pl.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))

        file_source.start_and_wait()

        a = file_len(TESTING_IN_FILE_CSV)
        b = file_len(TESTING_OUT_FILE_CSV)
        self.assertEqual(math.ceil((a - 1) / batch_size), b - 1)  # - header line

    def test_batch_pipeline_number_of_samples_out_size_37(self):
        remove_files(TEST_OUT_FILES)
        batch_size = 37
        pl = pipe.Pipeline()
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        pl.add_processing_step(batch_step)
        pl.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))

        file_source.start_and_wait()

        a = file_len(TESTING_IN_FILE_CSV)
        b = file_len(TESTING_OUT_FILE_CSV)
        self.assertEqual(math.ceil((a - 1) / batch_size), b - 1)  # - minus header line

    #
    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        time.sleep(2)
        remove_files(TEST_OUT_FILES)


class TestPipeline(unittest.TestCase):

    def test_simple_empty_one_step_pipeline(self):
        pl = pipe.Pipeline()
        pl.start()
        pl.join()

    def test_generative_processing_step(self):
        pl = pipe.Pipeline()
        pl.add_processing_step(processingstep.DebugGenerator())
        pl.start()
        pl.join()

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
