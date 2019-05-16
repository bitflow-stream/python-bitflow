#!/usr/bin/env python

import logging
import sys
import time
import unittest
import os
import filecmp
from bitflow.script_parser import *
from bitflow.sinksteps import *
from bitflow.processingstep import *
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
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

class PythonBitflow(unittest.TestCase):

    def test_capabilities(self):
        capabilities()

class WildcardCompare(unittest.TestCase):

    def test_string_to_string(self):
        string = "test"
        wildcard = "test"
        self.assertTrue(wildcard_compare(wildcard,string))

    def test_string_to_upper_string(self):
        string = "test"
        wildcard = "TesT"
        self.assertTrue(wildcard_compare(wildcard,string))

    def test_string_to_star_string(self):
        string = "test"
        wildcard = "t*t"
        self.assertTrue(wildcard_compare(wildcard,string))

    def test_upper_string_to_string(self):
        string = "TEST"
        wildcard = "test"
        self.assertTrue(wildcard_compare(wildcard,string))

    def test_numbers_to_star_string(self):
        string = "1111"
        wildcard = "1*1"
        self.assertTrue(wildcard_compare(wildcard,string))


class ExactCompare(unittest.TestCase):

    def test_string_to_string(self):
        string = "test"
        expression = "test"
        self.assertTrue(exact_compare(expression,string))

    def test_string_to_upper_string(self):
        string = "test"
        expression = "TEST"
        self.assertTrue(exact_compare(expression,string))
        
    def test_upper_string_to_string(self):
        string = "TEST"
        expression = "test"
        self.assertTrue(exact_compare(expression,string))
        
    def test_number_to_number(self):
        string = "34234"
        expression = "34234"
        self.assertTrue(exact_compare(expression,string))
        
    def test_number_string_to_number_string(self):
        string = "test23"
        expression = "test23"
        self.assertTrue(exact_compare(expression,string))
        
    def test_string_char_to_string_char(self):
        string = "?tes_23!"
        expression = "?tes_23!"
        self.assertTrue(exact_compare(expression,string))


class TestFork(unittest.TestCase):

    def test_pipeline_and_fork(self):
        fork = Fork_Tags(tag="blub")
        fork.add_processing_steps([],["bla","blub"])
        pipeline = Pipeline()
        pipeline.add_processing_step(DebugGenerationStep())
        pipeline.add_processing_step(fork)
        pipeline.start()
        time.sleep(2)
        pipeline.stop()

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass

class TestPipeline(unittest.TestCase):

    DEFAULT_SLEEPING_DURATION = 2

    def test_simple_empty_one_step_pipeline(self):
        pipeline = Pipeline()
        pipeline.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

    def test_generative_processing_step(self):
        pipeline = Pipeline()
        pipeline.add_processing_step(DebugGenerationStep())
        pipeline.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        pipeline.stop()

    def test_subpipeline(self):
        inner_pipeline = Pipeline()

        outer_pipeline = Pipeline()
        outer_pipeline.add_processing_step(DebugGenerationStep())
        outer_pipeline.add_processing_step(inner_pipeline)
        outer_pipeline.start()
        time.sleep(self.DEFAULT_SLEEPING_DURATION)
        outer_pipeline.stop()

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass


class TestBitflowScriptParser(unittest.TestCase):

    def test_get_file_data_format_BIN(self):
        data_format = get_file_data_format(None,"blacsv.bin")
        self.assertEqual(BINARY_DATA_FORMAT_IDENTIFIER, data_format)

    def test_get_file_data_format_CSV(self):
        data_format = get_file_data_format(None,"blabin.csv")
        self.assertEqual(CSV_DATA_FORMAT_IDENTIFIER, data_format)

    def test_get_file_data_format_PRE(self):
        data_format = get_file_data_format(CSV_DATA_FORMAT_IDENTIFIER,"blacsv.bin")
        self.assertEqual(CSV_DATA_FORMAT_IDENTIFIER, data_format)

    def test_parse_output_str__data_format__filename(self):
        output_str = "csv://test.csv"
        output_type, data_format, output_url = parse_output_str(output_str)
        self.assertEqual(output_type, None)
        self.assertEqual(data_format, CSV_DATA_FORMAT_IDENTIFIER)
        self.assertEqual(output_url, "test.csv")

    def test_parse_output_str__data_type__filename(self):
        output_str = "file://test.txt"
        output_type, data_format, output_url = parse_output_str(output_str)
        self.assertEqual(output_type, FILE_OUTPUT_TYPE)
        self.assertEqual(data_format, None)
        self.assertEqual(output_url, "test.txt")

    def test_parse_output_str__data_type__data_format__filename(self):
        output_str = "file+csv://test.csv"
        output_type, data_format, output_url = parse_output_str(output_str)
        self.assertEqual(output_type, FILE_OUTPUT_TYPE)
        self.assertEqual(data_format, CSV_DATA_FORMAT_IDENTIFIER)
        self.assertEqual(output_url, "test.csv")

    def test_parse_output_str__data_type__host_port_url(self):
        output_str = "tcp://test:5555"
        output_type, data_format, output_url = parse_output_str(output_str)
        self.assertEqual(output_type, TCP_SEND_OUTPUT_TYPE)
        self.assertEqual(data_format, None)
        self.assertEqual(output_url, "test:5555")

    def test_parse_output_str__data_type__data_format__host_port_url(self):
        output_str = "tcp+csv://test:5555"
        output_type, data_format, output_url = parse_output_str(output_str)
        self.assertEqual(output_type, TCP_SEND_OUTPUT_TYPE)
        self.assertEqual(data_format, CSV_DATA_FORMAT_IDENTIFIER)
        self.assertEqual(output_url, "test:5555")

    def test_parse_output_str__to_many_identifiers(self):
        output_str = "listen+bin+csv://:5555"
        with self.assertRaises(ParsingError):
            output_type, data_format, output_url = parse_output_str(output_str)

    def test_parse_output_str__unknown_data_format(self):
        output_str = "listen+bid://:5555"
        with self.assertRaises(ParsingError):
            output_type, data_format, output_url = parse_output_str(output_str)

    def test_parse_output_str__data_format__download(self):
        output_str = "tcp+bin://web.de:5555"
        output_type, data_format, output_url = parse_output_str(output_str)
        self.assertEqual(output_type, TCP_SEND_OUTPUT_TYPE)
        self.assertEqual(data_format,BINARY_DATA_FORMAT_IDENTIFIER)
        self.assertEqual(output_url, "web.de:5555")


    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass

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
