#!/usr/bin/env python

import logging
import sys
import time
import unittest
import os
from bitflow.script_parser import *
from bitflow.sinksteps import *
from bitflow.processingstep import *
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.source import FileSource, ListenSource, DownloadSource
from bitflow.fork import *

LOGGING_LEVEL=logging.ERROR

TESTING_IN_FILE_CSV = "testing/testing_file_in.csv"
TESTING_OUT_FILE_CSV = "testing/testing_file_out.csv"
TESTING_OUT_FILE_CSV_2 = "testing/testing_file_out2.csv"

TESTING_IN_FILE_BIN = "testing/testing_file_in.bin"

def remove_file(f):
    if os.path.isfile(f):
        os.remove(f)
        logging.info("deleted file {} ...".format(f))

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
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)


        fork = Fork_Tags(tag="blub")
        fork.add_processing_steps([],["bla","blub"])

        pipeline = Pipeline()
        pipeline.add_processing_step(DebugGenerationStep())
        pipeline.add_processing_step(fork)
        pipeline.start()

        time.sleep(5)
        pipeline.stop()

        time.sleep(2)
        self.assertTrue(True)


class TestPipeline(unittest.TestCase):

    def test_simple_empty_one_step_pipeline(self):
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        pipeline = Pipeline()
        #pipeline.add_processing_step(TerminalOut())
        pipeline.start()

        time.sleep(5)
        pipeline.stop()
        self.assertTrue(True)

    def test_generative_processing_step(self):
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        pipeline = Pipeline()
        pipeline.add_processing_step(DebugGenerationStep())
        #pipeline.add_processing_step(TerminalOut())
        pipeline.start()

        time.sleep(5)
        pipeline.stop()
        self.assertTrue(True)

    def test_subpipeline(self):
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        inner_pipeline = Pipeline()
       # inner_pipeline.add_processing_step(TerminalOut())

        outer_pipeline = Pipeline()
        outer_pipeline.add_processing_step(DebugGenerationStep())
        outer_pipeline.add_processing_step(inner_pipeline)
        outer_pipeline.start()

        time.sleep(5)
        outer_pipeline.stop()
        self.assertTrue(True)


class TestInputOutput(unittest.TestCase):

    def test_csv_file_in_no_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE_CSV
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        pipeline = Pipeline()
        pipeline.start()

        file_source = FileSource(   filename=TESTING_IN_FILE_CSV,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(2)
        pipeline.stop()

    def test_bin_file_in_no_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE_BIN
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        pipeline = Pipeline()
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_BIN,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(2)
        pipeline.stop()

    def test_csv_file_in__csv_file_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE_CSV, TESTING_OUT_FILE_CSV
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_CSV,
                                    pipeline=pipeline)
        file_source.start()
        time.sleep(5)
        pipeline.stop()

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE_CSV,TESTING_OUT_FILE_CSV))

    def test_bin_file_in__csv_file_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE_BIN, TESTING_IN_FILE_CSV, TESTING_OUT_FILE_CSV
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_BIN,
                                    pipeline=pipeline)
        
        file_source.start()
        time.sleep(5)
        pipeline.stop()

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE_CSV,TESTING_OUT_FILE_CSV))

    def test_csv_file_in__multiple_csv_files_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE_CSV, TESTING_OUT_FILE_CSV, TESTING_OUT_FILE_CSV
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV_2))
        pipeline.start()

        file_source = FileSource(   filename=TESTING_IN_FILE_CSV,
                                    pipeline=pipeline)

        file_source.start()
        time.sleep(5)
        pipeline.stop()

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_OUT_FILE_CSV_2,TESTING_OUT_FILE_CSV))

    def test_bin_file_in__multiple_csv_files_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE_BIN, TESTING_OUT_FILE_CSV, TESTING_OUT_FILE_CSV_2
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV_2))
        pipeline.start()
        file_source = FileSource(   filename=TESTING_IN_FILE_BIN,
                                    pipeline=pipeline)

        file_source.start()
        time.sleep(5)
        pipeline.stop()

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_OUT_FILE_CSV_2,TESTING_OUT_FILE_CSV))

    #CHECK UNCLOSED SOCKET
    def test_csv_listen_in__csv_send_out(self):
        global LOGGING_LEVEL
        global TESTING_OUT_FILE_CSV, TESTING_IN_FILE_CSV
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        host="localhost"
        port=5010

        # BUILD LISTEN TO FILE
        a_pipeline = Pipeline()
        a_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        a_listen_source = ListenSource( pipeline=a_pipeline,
                                        port=port)
        a_pipeline.start()
        a_listen_source.start()
        time.sleep(4)
        # BUILD FILE TO SEND
        b_pipeline = Pipeline()
        b_pipeline.add_processing_step(TCPSink(
                            host=host,
                            port=port))
        b_file_source = FileSource( filename=TESTING_IN_FILE_CSV,
                                    pipeline=b_pipeline)
        b_pipeline.start()
        b_file_source.start()

        time.sleep(5)
        b_file_source.stop()
        b_pipeline.stop()
        time.sleep(5)
        a_listen_source.stop()
        a_pipeline.stop()
        time.sleep(5)

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE_CSV,TESTING_OUT_FILE_CSV))

    def test_bin_listen_in__csv_send_out(self):
        global LOGGING_LEVEL
        global TESTING_OUT_FILE_CSV, TESTING_IN_FILE_CSV, TESTING_IN_FILE_BIN
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        host="localhost"
        port=5011

        # BUILD LISTEN TO FILE
        a_pipeline = Pipeline()
        a_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        a_listen_source = ListenSource(
                        pipeline=a_pipeline,
                        port=port)
        a_pipeline.start()
        a_listen_source.start()
        time.sleep(5)
        # BUILD FILE TO SEND
        b_pipeline = Pipeline()
        b_pipeline.add_processing_step(TCPSink(
                            host=host,
                            port=port))
        b_file_source = FileSource( filename=TESTING_IN_FILE_BIN,
                                    pipeline=b_pipeline)
        b_pipeline.start()
        b_file_source.start()

        time.sleep(5)
        b_file_source.stop()
        b_pipeline.stop()
        time.sleep(5)
        a_listen_source.stop()
        a_pipeline.stop()
        time.sleep(5)

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE_CSV,TESTING_OUT_FILE_CSV))

    def test_csv_download_in__csv_listen_out(self):
        global LOGGING_LEVEL
        global TESTING_OUT_FILE_CSV, TESTING_IN_FILE_CSV
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        host="localhost"
        port=5012

        a_pipeline = Pipeline()
        a_file_source = FileSource( filename=TESTING_IN_FILE_CSV,
                                    pipeline=a_pipeline)
        a_pipeline.add_processing_step(ListenSink(  max_receivers=5,
                                                    host=host,
                                                    port=port))
        a_file_source.start()
        a_pipeline.start()

        time.sleep(5)

        b_pipeline = Pipeline()
        b_download_source = DownloadSource( host=host,
                                            port=port,
                                            pipeline=b_pipeline)
        b_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        b_pipeline.start()
        b_download_source.start()

        time.sleep(5)
        a_file_source.stop()
        a_pipeline.stop()
        time.sleep(5)
        b_download_source.stop()
        b_pipeline.stop()
        time.sleep(5)

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE_CSV,TESTING_OUT_FILE_CSV))


    def test_bin_download_in__csv_listen_out(self):
        global LOGGING_LEVEL
        global TESTING_OUT_FILE_CSV, TESTING_IN_FILE_CSV, TESTING_IN_FILE_BIN
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE_CSV)
        remove_file(TESTING_OUT_FILE_CSV_2)

        host="localhost"
        port=5013

        a_pipeline = Pipeline()
        a_file_source = FileSource( filename=TESTING_IN_FILE_BIN,
                                    pipeline=a_pipeline)
        a_pipeline.add_processing_step(ListenSink(max_receivers=5,host=host,port=port))
        a_file_source.start()
        a_pipeline.start()

        time.sleep(5)

        b_pipeline = Pipeline()
        b_download_source = DownloadSource(host=host,port=port,pipeline=b_pipeline)
        b_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_CSV))
        b_pipeline.start()
        b_download_source.start()

        time.sleep(5)
        a_file_source.stop()
        a_pipeline.stop()
        time.sleep(5)
        b_download_source.stop()
        b_pipeline.stop()
        time.sleep(5)

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE_CSV,TESTING_OUT_FILE_CSV))

        def tearDown(self):
            time.sleep(3)  # sleep time in seconds

if __name__ == '__main__':
    unittest.main()
