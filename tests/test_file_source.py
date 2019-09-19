import time
import unittest

from bitflow import pipeline as pipe
from bitflow.io import sinksteps, sources
from bitflow.io.marshaller import *
from tests.support import *


class TestFileIO(unittest.TestCase):
    DEFAULT_SLEEPING_DURATION = 2

    def run_test(self, file_paths):
        if file_paths:
            pipeline = pipe.Pipeline()
            pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
            pipeline.start()
            file_source = sources.FileSource(pipeline=pipeline)
            for path in file_paths:
                file_source.add_path(path)
            file_source.start()
            time.sleep(self.DEFAULT_SLEEPING_DURATION)
            pipeline.stop()

    def test_read_empty_csv_file(self):
        self.run_test([TESTING_IN_FILE_CSV_EMPTY])
        with self.assertRaises(FileNotFoundError):
            read_file(TESTING_OUT_FILE_CSV)

    def test_read_empty_bin_file(self):
        self.run_test([TESTING_IN_FILE_BIN_EMPTY])
        with self.assertRaises(FileNotFoundError):
            read_file(TESTING_OUT_FILE_CSV)

    def test_read_only_header_csv_file(self):
        self.run_test([TESTING_IN_FILE_CSV_ONLY_HEADER])
        with self.assertRaises(FileNotFoundError):
            read_file(TESTING_OUT_FILE_CSV)

    def test_read_only_header_bin_file(self):
        self.run_test([TESTING_IN_FILE_BIN_ONLY_HEADER])
        with self.assertRaises(FileNotFoundError):
            read_file(TESTING_OUT_FILE_CSV)

    def test_read_broken_1_csv_file(self):
        self.run_test([TESTING_IN_FILE_CSV_BROKEN_1])
        with self.assertRaises(FileNotFoundError):
            read_file(TESTING_OUT_FILE_CSV)

    def test_read_broken_1_bin_file(self):
        self.run_test([TESTING_IN_FILE_BIN_BROKEN_1])
        with self.assertRaises(FileNotFoundError):
            read_file(TESTING_OUT_FILE_CSV)

    def test_read_broken_2_csv_file(self):
        self.run_test([TESTING_IN_FILE_CSV_BROKEN_2])

        a = read_file(TESTING_OUT_FILE_CSV)
        b = read_file(TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3)
        self.assertEqual(a, b)

    def test_read_broken_2_bin_file(self):
        self.run_test([TESTING_IN_FILE_BIN_BROKEN_2])

        a = read_file(TESTING_OUT_FILE_CSV)
        b = read_file(TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3)
        self.assertEqual(a, b)

    def test_read_broken_3_csv_file(self):
        self.run_test([TESTING_IN_FILE_CSV_BROKEN_3])

        a = read_file(TESTING_OUT_FILE_CSV)
        b = read_file(TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3)
        self.assertEqual(a, b)

    def test_read_broken_3_bin_file(self):
        self.run_test([TESTING_IN_FILE_BIN_BROKEN_3])

        a = read_file(TESTING_OUT_FILE_CSV)
        b = read_file(TESTING_EXPECTED_OUT_FILE_CSV_BROKEN_2_3)
        self.assertEqual(a, b)

    def test_read_multiple_bin_files(self):
        self.run_test([TESTING_IN_FILE_BIN_SMALL, TESTING_IN_FILE_BIN_SMALL])

        a = read_file(TESTING_OUT_FILE_CSV)
        b = read_file(TESTING_EXPECTED_OUT_FILE_CSV_DOUBLE)
        self.assertEqual(a, b)

    def test_read_multiple_csv_files(self):
        self.run_test([TESTING_IN_FILE_CSV_SMALL, TESTING_IN_FILE_CSV_SMALL])

        a = read_file(TESTING_OUT_FILE_CSV)
        b = read_file(TESTING_EXPECTED_OUT_FILE_CSV_DOUBLE)
        self.assertEqual(a, b)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        remove_files(TEST_OUT_FILES)
