#!/usr/bin/env python

import logging
import sys
import time
import unittest
import os
import filecmp
import math

from bitflow.script_parser import * 
from bitflow.sinksteps import *
from bitflow.processingstep import *
from bitflow.batchprocessingstep import *
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline, BatchPipeline
from bitflow.source import FileSource, ListenSource, DownloadSource
from bitflow.fork import *


LOGGING_LEVEL=logging.ERROR

TESTING_IN_FILE_CSV = "testing/in.csv"
TESTING_IN_FILE_BIN = "testing/in.bin"

OUT_FILE_DIR="/tmp"
PREFIX = "/python-bitflow-testing-"
TESTING_OUT_FILE_CSV = OUT_FILE_DIR  + PREFIX + "out.csv"
TESTING_OUT_FILE_CSV_2 = OUT_FILE_DIR + PREFIX + "out2.csv"
TESTING_OUT_FILE_BIN = OUT_FILE_DIR + PREFIX + "out.bin"
TESTING_OUT_FILE_BIN_2 = OUT_FILE_DIR + PREFIX + "out2.bin"

TEST_OUT_FILES = [  TESTING_OUT_FILE_CSV,
                    TESTING_OUT_FILE_CSV_2,
                    TESTING_OUT_FILE_BIN,
                    TESTING_OUT_FILE_BIN_2]

def remove_files(l):
    for f in l:
        remove_file(f)

def remove_file(f):
    if os.path.isfile(f):
        os.remove(f)
        logging.info("deleted file {} ...".format(f))

def remove_folder(d):
    if os.path.isdir(d):
        os.rmdir(d)

def create_folder(d):
    if not os.path.isdir(d):
        os.mkdir(d)

def read_file(fn):
    with open(fn,'rb') as f:
        s = f.read()
    return s

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

class PythonBitflow(unittest.TestCase):

    def test_capabilities(self):
        capabilities()



class TestFileIO(unittest.TestCase):

    DEFAULT_SLEEPING_DURATION = 2

    def test_csv_file_in_no_out(self):
        pipeline = Pipeline()
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_CSV,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

    def test_bin_file_in_no_out(self):
        pipeline = Pipeline()
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_BIN,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

    def test_csv_file_in__csv_file_out(self):
        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(  filename=TESTING_OUT_FILE_CSV,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_CSV,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a,b)
    
    def test_csv_file_in__bin_file_out(self):
        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(  filename=TESTING_OUT_FILE_BIN,
                                                data_format=BINARY_DATA_FORMAT_IDENTIFIER))
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_CSV,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a,b)

    def test_bin_file_in__csv_file_out(self):
        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(  filename=TESTING_OUT_FILE_CSV,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_BIN,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a,b)

    def test_bin_file_in__bin_file_out(self):
        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(  filename=TESTING_OUT_FILE_BIN,
                                                data_format=BINARY_DATA_FORMAT_IDENTIFIER))
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_BIN,
                                    pipeline=pipeline)

        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a,b)

    def test_csv_file_in__multiple_csv_files_out(self):
        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(  filename=TESTING_OUT_FILE_CSV,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))

        pipeline.add_processing_step(FileSink(  filename=TESTING_OUT_FILE_CSV_2,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))
        pipeline.start()

        file_source = FileSource(   filename=TESTING_IN_FILE_CSV,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV_2)
        self.assertEqual(a,b)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        remove_files(TEST_OUT_FILES)

def closing(s):
    if s:
        s.close()

from contextlib import closing
def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

class TestTcpIO(unittest.TestCase):

    DEFAULT_SLEEPING_DURATION = 2

    def test_csv_listen_in__csv_send_out(self):
        host="localhost"
        port=find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = Pipeline()
        a_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))
        a_listen_source = ListenSource( pipeline=a_pipeline,
                                        port=port)
        a_pipeline.start()
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = Pipeline()
        b_pipeline.add_processing_step(TCPSink(
                            host=host,
                            port=port))
        b_file_source = FileSource( filename=TESTING_IN_FILE_CSV,
                                    pipeline=b_pipeline)
        b_pipeline.start()
        b_file_source.start()

        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        b_file_source.stop()
        b_pipeline.stop()
        a_listen_source.stop()
        a_pipeline.stop()
        #time.sleep(1)

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a,b)

    def test_bin_listen_in__csv_send_out(self):
        host="localhost"
        port=find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = Pipeline()
        a_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))
        a_listen_source = ListenSource( pipeline=a_pipeline,
                                        port=port)
        a_pipeline.start()
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = Pipeline()
        b_pipeline.add_processing_step(TCPSink(
                            host=host,
                            port=port))
        b_file_source = FileSource( filename=TESTING_IN_FILE_BIN,
                                    pipeline=b_pipeline)
        b_pipeline.start()
        b_file_source.start()

        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        b_file_source.stop()
        b_pipeline.stop()
        a_listen_source.stop()
        a_pipeline.stop()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a,b)

    #CHECK UNCLOSED SOCKET
    def test_csv_listen_in__bin_send_out(self):
        host="localhost"
        port=find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = Pipeline()
        a_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_BIN,
                                                data_format=BINARY_DATA_FORMAT_IDENTIFIER))
        a_listen_source = ListenSource( pipeline=a_pipeline,
                                        port=port)
        a_pipeline.start()
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = Pipeline()
        b_pipeline.add_processing_step(TCPSink(
                            host=host,
                            port=port))
        b_file_source = FileSource( filename=TESTING_IN_FILE_CSV,
                                    pipeline=b_pipeline)
        b_pipeline.start()
        b_file_source.start()

        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        b_file_source.stop()
        b_pipeline.stop()
        a_listen_source.stop()
        a_pipeline.stop()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a,b)

    #CHECK UNCLOSED SOCKET
    def test_bin_listen_in__bin_send_out(self):
        host="localhost"
        port=find_free_port()

        # BUILD LISTEN TO FILE
        a_pipeline = Pipeline()
        a_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_BIN,
                                                data_format=BINARY_DATA_FORMAT_IDENTIFIER))
        a_listen_source = ListenSource( pipeline=a_pipeline,
                                        port=port)
        a_pipeline.start()
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_pipeline = Pipeline()
        b_pipeline.add_processing_step(TCPSink(
                            host=host,
                            port=port))
        b_file_source = FileSource( filename=TESTING_IN_FILE_BIN,
                                    pipeline=b_pipeline)
        b_pipeline.start()
        b_file_source.start()

        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        b_file_source.stop()
        b_pipeline.stop()
        a_listen_source.stop()
        a_pipeline.stop()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a,b)

    def test_csv_download_in__csv_listen_out(self):
        host="localhost"
        port=find_free_port()

        a_pipeline = Pipeline()
        a_file_source = FileSource( filename=TESTING_IN_FILE_CSV,
                                    pipeline=a_pipeline)
        a_pipeline.add_processing_step(ListenSink(  max_receivers=5,
                                                    host=host,
                                                    port=port))
        a_file_source.start()
        a_pipeline.start()
        b_pipeline = Pipeline()
        b_download_source = DownloadSource( host=host,
                                            port=port,
                                            pipeline=b_pipeline)
        b_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))
        b_pipeline.start()
        b_download_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        a_file_source.stop()
        a_pipeline.stop()
        b_download_source.stop()
        b_pipeline.stop()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a,b)

    def test_bin_download_in__csv_listen_out(self):
        host="localhost"
        port=find_free_port()

        a_pipeline = Pipeline()
        a_file_source = FileSource( filename=TESTING_IN_FILE_CSV,
                                    pipeline=a_pipeline)
        a_pipeline.add_processing_step(ListenSink(  max_receivers=5,
                                                    host=host,
                                                    port=port))
        a_file_source.start()
        a_pipeline.start()
        b_pipeline = Pipeline()
        b_download_source = DownloadSource( host=host,
                                            port=port,
                                            pipeline=b_pipeline)
        b_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_BIN,
                                                data_format=BINARY_DATA_FORMAT_IDENTIFIER))
        b_pipeline.start()
        b_download_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        a_file_source.stop()
        a_pipeline.stop()
        b_download_source.stop()
        b_pipeline.stop()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a,b)

    def test_csv_download_in__bin_listen_out(self):
        host="localhost"
        port=find_free_port()

        a_pipeline = Pipeline()
        a_file_source = FileSource( filename=TESTING_IN_FILE_BIN,
                                    pipeline=a_pipeline)
        a_pipeline.add_processing_step(ListenSink(  max_receivers=5,
                                                    host=host,
                                                    port=port))
        a_file_source.start()
        a_pipeline.start()
        b_pipeline = Pipeline()
        b_download_source = DownloadSource( host=host,
                                            port=port,
                                            pipeline=b_pipeline)
        b_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV,
                                                data_format=CSV_DATA_FORMAT_IDENTIFIER))
        b_pipeline.start()
        b_download_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        a_file_source.stop()
        a_pipeline.stop()
        b_download_source.stop()
        b_pipeline.stop()

        a = read_file(TESTING_IN_FILE_CSV)
        b = read_file(TESTING_OUT_FILE_CSV)
        self.assertEqual(a,b)

    def test_bin_download_in__csv_listen_out(self):
        host="localhost"
        port=find_free_port()

        a_pipeline = Pipeline()
        a_file_source = FileSource( filename=TESTING_IN_FILE_BIN,
                                    pipeline=a_pipeline)
        a_pipeline.add_processing_step(ListenSink(  max_receivers=5,
                                                    host=host,
                                                    port=port))
        a_file_source.start()
        a_pipeline.start()
        b_pipeline = Pipeline()
        b_download_source = DownloadSource( host=host,
                                            port=port,
                                            pipeline=b_pipeline)
        b_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_BIN,
                                                data_format=BINARY_DATA_FORMAT_IDENTIFIER))
        b_pipeline.start()
        b_download_source.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        a_file_source.stop()
        a_pipeline.stop()
        b_download_source.stop()
        b_pipeline.stop()

        a = read_file(TESTING_IN_FILE_BIN)
        b = read_file(TESTING_OUT_FILE_BIN)
        self.assertEqual(a,b)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
        remove_files(TEST_OUT_FILES)

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
