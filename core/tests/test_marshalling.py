import time
import unittest

from bitflow import pipeline as pipe
from bitflow.io import sinksteps, sources
from bitflow.io.marshaller import *
from tests.support import *


class TestMarshalling(unittest.TestCase):
    broken_files_no_output = \
        [
            TESTING_IN_FILE_CSV_EMPTY,
            TESTING_IN_FILE_BIN_EMPTY,
            TESTING_IN_FILE_CSV_ONLY_HEADER,
            TESTING_IN_FILE_BIN_ONLY_HEADER,
            TESTING_IN_FILE_CSV_BROKEN_1,
            TESTING_OUT_FILE_CSV
        ]

    broken_files_expected_output = \
        [
            (TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3, TESTING_IN_FILE_CSV_BROKEN_2),
            (TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3, TESTING_IN_FILE_BIN_BROKEN_2),
            (TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3, TESTING_IN_FILE_CSV_BROKEN_3),
            (TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3, TESTING_IN_FILE_BIN_BROKEN_3),
            (TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3, TESTING_IN_FILE_BIN_BROKEN_3),
        ]

    def test_broken_files_no_output(self):
        for broke_file in self.broken_files_no_output:
            with self.subTest(msg="Checking marshalling with broken file input. No output expected",
                              broke_file=broke_file):
                pipeline = pipe.PipelineSync()
                pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV,
                                                                data_format=CSV_DATA_FORMAT))
                file_source = sources.FileSource(pipeline=pipeline)
                file_source.add_path(broke_file)
                file_source.start_and_wait()
                with self.assertRaises(FileNotFoundError):
                    read_file(TESTING_OUT_FILE_CSV)

    def test_broke_files_expected_output(self):
        for broke_file in self.broken_files_no_output:
            with self.subTest(msg="Checking marshalling with broken file input.", broke_file=broke_file):
                pipeline = pipe.PipelineSync()
                pipeline.add_processing_step(
                    sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
                file_source = sources.FileSource(pipeline=pipeline)
                file_source.add_path(broke_file)
                file_source.start_and_wait()
                with self.assertRaises(FileNotFoundError):
                    read_file(TESTING_OUT_FILE_CSV)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
        remove_files(TEST_OUT_FILES)

    def tearDown(self):
        remove_files(TEST_OUT_FILES)
