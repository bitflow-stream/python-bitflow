import itertools
import socket
import unittest
from contextlib import closing as cl

from bitflow import pipeline as pipe
from bitflow.processingstep import PARALLEL_MODES
from bitflow.io import sinksteps, sources
from bitflow.io.marshaller import *
from tests.support import *


class TestFileIO(unittest.TestCase):
    parallel_modes = [None, *PARALLEL_MODES]
    input_files_and_expected = \
        [
            {"input": [TESTING_IN_FILE_BIN_SMALL],
             "expected": {CSV_DATA_FORMAT: TESTING_IN_FILE_CSV_SMALL,
                          BIN_DATA_FORMAT: TESTING_IN_FILE_BIN_SMALL}},
            {"input": [TESTING_IN_FILE_CSV_SMALL],
             "expected": {CSV_DATA_FORMAT: TESTING_IN_FILE_CSV_SMALL,
                          BIN_DATA_FORMAT: TESTING_IN_FILE_BIN_SMALL}},
            {"input": [TESTING_IN_FILE_BIN_SMALL, TESTING_IN_FILE_BIN_SMALL],
             "expected": {CSV_DATA_FORMAT: TESTING_EXPECTED_OUT_FILE_CSV_DOUBLE,
                          BIN_DATA_FORMAT: TESTING_EXPECTED_OUT_FILE_BIN_DOUBLE}},
            {"input": [TESTING_IN_FILE_CSV_SMALL, TESTING_IN_FILE_CSV_SMALL],
             "expected": {CSV_DATA_FORMAT: TESTING_EXPECTED_OUT_FILE_CSV_DOUBLE,
                          BIN_DATA_FORMAT: TESTING_EXPECTED_OUT_FILE_BIN_DOUBLE}}
        ]

    output_files = \
        [
            [(CSV_DATA_FORMAT, TESTING_OUT_FILE_CSV)],
            [(BIN_DATA_FORMAT, TESTING_OUT_FILE_BIN)],
            [(CSV_DATA_FORMAT, TESTING_OUT_FILE_CSV), (CSV_DATA_FORMAT, TESTING_OUT_FILE_CSV_2)],
            [(BIN_DATA_FORMAT, TESTING_OUT_FILE_BIN), (BIN_DATA_FORMAT, TESTING_OUT_FILE_BIN_2)],
            [(BIN_DATA_FORMAT, TESTING_OUT_FILE_BIN), (CSV_DATA_FORMAT, TESTING_OUT_FILE_CSV)]
        ]

    def test_file_io(self):
        for parallel_mode, input_file_list, output_file_list in itertools.product(
                self.parallel_modes, self.input_files_and_expected, self.output_files):
            remove_files(TEST_OUT_FILES)
            with self.subTest(msg="Checking file IO and if output result matches expectation.",
                              parallel_mode=parallel_mode, input_file_list=input_file_list,
                              output_file_list=output_file_list):
                if parallel_mode:
                    pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode)
                else:
                    pipeline = pipe.PipelineSync()
                file_source = sources.FileSource(pipeline=pipeline)
                for input_file in input_file_list["input"]:
                    file_source.add_path(input_file)
                for output_file in output_file_list:
                    pipeline.add_processing_step(sinksteps.FileSink(filename=output_file[1],
                                                                    data_format=output_file[0]))
                file_source.start_and_wait()

                if output_file_list:
                    for path_out in output_file_list:
                        a = read_file(input_file_list["expected"][path_out[0]])
                        b = read_file(path_out[1])
                        self.assertEqual(a, b)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        remove_files(TEST_OUT_FILES)


def find_free_port():
    with cl(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class TestNetworkIO(unittest.TestCase):
    host = "localhost"
    port = find_free_port()
    # After this amount of samples the network input should close. Should be the amount of samples that are read from
    # the respective files.
    sample_limit = 5

    def listen_source(self, pipeline):
        return sources.ListenSource(pipeline=pipeline, port=self.port, sample_limit=self.sample_limit)

    def download_source(self, pipeline):
        return sources.DownloadSource(host=self.host, port=self.port, pipeline=pipeline, sample_limit=self.sample_limit)

    parallel_modes = [None, *PARALLEL_MODES]
    input_files_and_expected = \
        [
            {"input": TESTING_IN_FILE_BIN_SMALL,
             "expected": {CSV_DATA_FORMAT: TESTING_IN_FILE_CSV_SMALL,
                          BIN_DATA_FORMAT: TESTING_IN_FILE_BIN_SMALL}},
            {"input": TESTING_IN_FILE_CSV_SMALL,
             "expected": {CSV_DATA_FORMAT: TESTING_IN_FILE_CSV_SMALL,
                          BIN_DATA_FORMAT: TESTING_IN_FILE_BIN_SMALL}},
        ]
    output_files = [(CSV_DATA_FORMAT, TESTING_OUT_FILE_CSV), (BIN_DATA_FORMAT, TESTING_OUT_FILE_BIN)]

    net_io_pairs = [(listen_source, sinksteps.TCPSink(host=host, port=port)),
                    (download_source, sinksteps.ListenSink(host=host, port=port))]

    def test_network_io(self):
        for parallel_mode, input_file, output_file, net_io_pair in itertools.product(
                self.parallel_modes, self.input_files_and_expected, self.output_files, self.net_io_pairs):
            remove_files(TEST_OUT_FILES)
            with self.subTest(msg="Checking network IO and if output result matches expectation.",
                              paralle_mode=parallel_mode, input_file=input_file, output_file=output_file,
                              net_io_pair=net_io_pair):
                if parallel_mode:
                    a_pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode)
                    b_pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode)
                else:
                    a_pipeline = pipe.PipelineSync()
                    b_pipeline = pipe.PipelineSync()

                a_pipeline.add_processing_step(sinksteps.FileSink(filename=output_file[1], data_format=output_file[0]))
                a_listen_source = net_io_pair[0](self, a_pipeline)
                a_listen_source.start()

                b_pipeline.add_processing_step(net_io_pair[1])
                b_file_source = sources.FileSource(path=input_file["input"], pipeline=b_pipeline,
                                                   sample_limit=self.sample_limit)
                b_file_source.start()

                b_file_source.wait()
                a_listen_source.wait()

                a = read_file(input_file["expected"][output_file[0]])
                b = read_file(output_file[1])
                self.assertEqual(a, b)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
        remove_files(TEST_OUT_FILES)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
