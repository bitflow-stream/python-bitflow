#!/usr/bin/python3

import logging
import sys
import time
import unittest
import os
from bitflow.sinksteps import *
from bitflow.processingstep import *
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.source import FileSource, ListenSource, DownloadSource
from bitflow.fork import Tag_Fork

LOGGING_LEVEL=logging.DEBUG

TESTING_IN_FILE = "testing/testing_file_in.txt"
TESTING_OUT_FILE = "testing/testing_file_out.txt"
TESTING_OUT_FILE_2 = "testing/testing_file_out2.txt"


def remove_file(f):
    if os.path.isfile(f):
        os.remove(f)
        logging.info("deleted file {} ...".format(f))


class TestFork(unittest.TestCase):

    def test_pipeline_and_fork(self):
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        fork_pipeline = Pipeline()
        fork_pipeline.add_processing_step(TerminalOut())
        fork_pipeline.start()

        pipeline = Pipeline()
        pipeline.add_processing_step(DebugGenerationStep())
        pipeline.add_processing_step(Tag_Fork([fork_pipeline],"test"))
        #pipeline.add_processing_step(TerminalOut())
        pipeline.start()

        time.sleep(5)
        pipeline.stop()
        fork_pipeline.stop()

        time.sleep(2)
        self.assertTrue(True)


class TestPipeline(unittest.TestCase):

    def test_simple_empty_one_step_pipeline(self):
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        pipeline = Pipeline()
        pipeline.add_processing_step(TerminalOut())
        pipeline.start()

        time.sleep(5)
        pipeline.stop()
        self.assertTrue(True)


    def test_generative_processing_step(self):
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        pipeline = Pipeline()
        pipeline.add_processing_step(DebugGenerationStep())
        pipeline.add_processing_step(TerminalOut())
        pipeline.start()

        time.sleep(5)
        pipeline.stop()
        self.assertTrue(True)

    def test_subpipeline(self):
        global LOGGING_LEVEL
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        inner_pipeline = Pipeline()
        inner_pipeline.add_processing_step(TerminalOut())

        outer_pipeline = Pipeline()
        outer_pipeline.add_processing_step(DebugGenerationStep())
        outer_pipeline.add_processing_step(inner_pipeline)
        outer_pipeline.start()

        time.sleep(5)
        outer_pipeline.stop()
        self.assertTrue(True)


class TestInputOutput(unittest.TestCase):

    def test_file_in_no_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        pipeline = Pipeline()
        #pipeline.add_processing_step(TerminalOut())
        pipeline.start()

        file_source = FileSource(filename=TESTING_IN_FILE,pipeline=pipeline,marshaller=CsvMarshaller())    
        file_source.start()
        time.sleep(2)
        pipeline.stop()

    def test_file_in_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE, TESTING_OUT_FILE
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE)

        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE))

        pipeline.start()

        file_source = FileSource(filename=TESTING_IN_FILE,pipeline=pipeline,marshaller=CsvMarshaller())    
        
        file_source.start()
        time.sleep(2)
        pipeline.stop()

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE,TESTING_OUT_FILE))

    def test_file_in_multi_file_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE, TESTING_OUT_FILE, TESTING_OUT_FILE_2
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE)
        remove_file(TESTING_OUT_FILE_2)

        pipeline = Pipeline()
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE))
        pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE_2))
        pipeline.start()

        file_source = FileSource(filename=TESTING_IN_FILE,pipeline=pipeline,marshaller=CsvMarshaller())    

        file_source.start()
        time.sleep(2.0)
        pipeline.stop()
        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_OUT_FILE_2,TESTING_OUT_FILE))

    def test_listen_in_send_out(self):
        global LOGGING_LEVEL
        global TESTING_OUT_FILE, TESTING_IN_FILE
       
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
        remove_file(TESTING_OUT_FILE)
       
        host="localhost"
        port=5011

        # BUILD LISTEN TO FILE
        a_pipeline = Pipeline()
        a_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE))
        a_listen_source = ListenSource(
                        marshaller=CsvMarshaller(),
                        pipeline=a_pipeline,
                        port=port)
        a_pipeline.start()
        a_listen_source.start()

        time.sleep(2)
        # BUILD FILE TO SEND
        b_pipeline = Pipeline()
        b_pipeline.add_processing_step(TCPSink(
                            host=host,
                            port=port))
        b_file_source = FileSource(filename=TESTING_IN_FILE,pipeline=b_pipeline,marshaller=CsvMarshaller())    

        b_pipeline.start()
        b_file_source.start()

        time.sleep(5)
        b_file_source.stop()
        time.sleep(2)
        a_listen_source.stop()


        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE,TESTING_OUT_FILE))

    def test_download_in_listen_out(self):
        global LOGGING_LEVEL
        global TESTING_OUT_FILE, TESTING_IN_FILE
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
        remove_file(TESTING_OUT_FILE)
        host="localhost"
        port=5011

        a_pipeline = Pipeline()
        a_file_source = FileSource(filename=TESTING_IN_FILE,pipeline=a_pipeline,marshaller=CsvMarshaller())    
        a_pipeline.add_processing_step(ListenSink(max_receivers=5,host=host,port=port))
        a_file_source.start()
        a_pipeline.start()

        time.sleep(2)

        b_pipeline = Pipeline()
        b_download_source = DownloadSource(marshaller=CsvMarshaller(),host=host,port=port,pipeline=b_pipeline)
        b_pipeline.add_processing_step(FileSink(filename=TESTING_OUT_FILE))
        b_pipeline.start()
        b_download_source.start()

        time.sleep(5)
        a_file_source.stop()
        a_pipeline.stop()
        time.sleep(1)
        b_download_source.stop()
        b_pipeline.stop()
        time.sleep(3)

        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE,TESTING_OUT_FILE))

if __name__ == '__main__':
    unittest.main()
