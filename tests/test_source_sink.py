import socket
import sys
import time
import unittest
from contextlib import closing as cl


from bitflow import pipeline as pipe
from bitflow.io import sinksteps, sources
from bitflow.script import script_parser
from bitflow.io.marshaller import *
from tests.support import *


class TestFileIO(unittest.TestCase):

    def test_csv_file_in_no_out(self):
        pipeline = pipe.Pipeline()
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pipeline)
        file_source.start_and_wait()

    def test_bin_file_in_no_out(self):
        pipeline = pipe.Pipeline()
        file_source = sources.FileSource(path=TESTING_IN_FILE_BIN, pipeline=pipeline)
        file_source.start_and_wait()

    def test_csv_file_in__csv_file_out(self):
        pipeline = pipe.Pipeline()
        pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pipeline)
        file_source.start_and_wait()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a, b)

    def test_csv_file_in__bin_file_out(self):
        pipeline = pipe.Pipeline()
        pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_BIN,
                                                        data_format=script_parser.BIN_DATA_FORMAT))
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pipeline)
        file_source.start_and_wait()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a, b)

    def test_bin_file_in__csv_file_out(self):
        pipeline = pipe.Pipeline()
        pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
        file_source = sources.FileSource(path=TESTING_IN_FILE_BIN, pipeline=pipeline)
        file_source.start_and_wait()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a, b)

    def test_bin_file_in__bin_file_out(self):
        pipeline = pipe.Pipeline()
        pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_BIN, data_format=BIN_DATA_FORMAT))
        file_source = sources.FileSource(path=TESTING_IN_FILE_BIN, pipeline=pipeline)
        file_source.start_and_wait()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a, b)

    def test_csv_file_in__multiple_csv_files_out(self):
        pipeline = pipe.Pipeline()
        pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))

        pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV_2, data_format=CSV_DATA_FORMAT))
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pipeline)
        file_source.start_and_wait()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV_2)
        self.assertEqual(a, b)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        remove_files(TEST_OUT_FILES)


# TODO: Naming is badly chosen here

def closing(s):
    if s:
        s.close()


def find_free_port():
    with cl(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class TestTcpIO(unittest.TestCase):
    DEFAULT_SLEEPING_DURATION = 15

    def test_csv_listen_in__csv_send_out(self):
        host = "localhost"
        port = find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = pipe.Pipeline()
        a_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
        a_listen_source = sources.ListenSource(pipeline=a_pipeline, port=port, sample_limit=1222)
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = pipe.Pipeline()
        b_pipeline.add_processing_step(sinksteps.TCPSink(host=host, port=port))
        b_file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=b_pipeline, sample_limit=1222)
        b_file_source.start()

        b_file_source.wait()
        a_listen_source.wait()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        print(len(a))
        print(len(b))
        self.assertEqual(a, b)

    def test_bin_listen_in__csv_send_out(self):
        host = "localhost"
        port = find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = pipe.Pipeline()
        a_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
        a_listen_source = sources.ListenSource(pipeline=a_pipeline, port=port, sample_limit=1222)
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = pipe.Pipeline()
        b_pipeline.add_processing_step(sinksteps.TCPSink(host=host, port=port))
        b_file_source = sources.FileSource(path=TESTING_IN_FILE_BIN, pipeline=b_pipeline, sample_limit=1222)
        b_file_source.start()

        b_file_source.wait()
        a_listen_source.wait()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a, b)

    # CHECK UNCLOSED SOCKET
    def test_csv_listen_in__bin_send_out(self):
        host = "localhost"
        port = find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = pipe.Pipeline()
        a_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_BIN, data_format=BIN_DATA_FORMAT))
        a_listen_source = sources.ListenSource(pipeline=a_pipeline, port=port, sample_limit=1222)
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = pipe.Pipeline()
        b_pipeline.add_processing_step(sinksteps.TCPSink(host=host, port=port))
        b_file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=b_pipeline, sample_limit=1222)
        b_file_source.start()

        b_file_source.wait()
        a_listen_source.wait()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a, b)

    # CHECK UNCLOSED SOCKET
    def test_bin_listen_in__bin_send_out(self):
        host = "localhost"
        port = find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = pipe.Pipeline()
        a_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_BIN, data_format=BIN_DATA_FORMAT))
        a_listen_source = sources.ListenSource(pipeline=a_pipeline, port=port, sample_limit=1222)
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = pipe.Pipeline()
        b_pipeline.add_processing_step(sinksteps.TCPSink(host=host, port=port))
        b_file_source = sources.FileSource(path=TESTING_IN_FILE_BIN, pipeline=b_pipeline, sample_limit=1222)
        b_file_source.start()

        b_file_source.wait()
        a_listen_source.wait()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a, b)

    def test_csv_download_in__csv_listen_out(self):
        host = "localhost"
        port = find_free_port()

        a_pipeline = pipe.Pipeline()
        a_file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=a_pipeline, sample_limit=1222)
        a_pipeline.add_processing_step(sinksteps.ListenSink(max_receivers=5, host=host, port=port))
        a_file_source.start()

        b_pipeline = pipe.Pipeline()
        b_download_source = sources.DownloadSource(host=host, port=port, pipeline=b_pipeline, sample_limit=1222)
        b_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
        b_download_source.start()

        a_file_source.wait()
        b_download_source.wait()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a, b)

    def test_bin_download_in__csv_listen_out(self):
        host = "localhost"
        port = find_free_port()

        a_pipeline = pipe.Pipeline()
        a_file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=a_pipeline, sample_limit=1222)
        a_pipeline.add_processing_step(sinksteps.ListenSink(max_receivers=5, host=host, port=port))
        a_file_source.start()
        b_pipeline = pipe.Pipeline()
        b_download_source = sources.DownloadSource(host=host, port=port, pipeline=b_pipeline, sample_limit=1222)
        b_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_BIN, data_format=BIN_DATA_FORMAT))
        b_download_source.start()

        a_file_source.wait()
        b_download_source.wait()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a, b)

    def test_csv_download_in__bin_listen_out(self):
        host = "localhost"
        port = find_free_port()

        a_pipeline = pipe.Pipeline()
        a_file_source = sources.FileSource(path=TESTING_IN_FILE_BIN, pipeline=a_pipeline, sample_limit=1222)
        a_pipeline.add_processing_step(sinksteps.ListenSink(max_receivers=5, host=host, port=port))
        a_file_source.start()
        b_pipeline = pipe.Pipeline()
        b_download_source = sources.DownloadSource(host=host, port=port, pipeline=b_pipeline, sample_limit=1222)
        b_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
        b_download_source.start()

        a_file_source.wait()
        b_download_source.stop()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a, b)

    def test_bin_download_in__csv_listen_out_2(self):
        host = "localhost"
        port = find_free_port()

        a_pipeline = pipe.Pipeline()
        a_file_source = sources.FileSource(path=TESTING_IN_FILE_BIN, pipeline=a_pipeline, sample_limit=1222)
        a_pipeline.add_processing_step(sinksteps.ListenSink(max_receivers=5, host=host, port=port))
        a_file_source.start()
        b_pipeline = pipe.Pipeline()
        b_download_source = sources.DownloadSource(host=host, port=port, pipeline=b_pipeline, sample_limit=1222)
        b_pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_BIN, data_format=BIN_DATA_FORMAT))
        b_download_source.start()

        a_file_source.wait()
        b_download_source.wait()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a, b)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
        remove_files(TEST_OUT_FILES)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
