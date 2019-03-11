#!/usr/bin/python3

import logging, sys, time, unittest, os

from bitflow.sink import FileSink, NoopSink, SendSink, StdSink, ListenSink
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.source import FileSource, ListenSource, DownloadSource

LOGGING_LEVEL=logging.DEBUG

TESTING_IN_FILE = "testing/testing_file_in.txt"
TESTING_OUT_FILE = "testing/testing_file_out.txt"
TESTING_OUT_FILE_2 = "testing/testing_file_out2.txt"


def remove_file(f):
    if os.path.isfile(f):
        os.remove(f)
        logging.info("deleted file {} ...".format(f))

class TestInputOutput(unittest.TestCase):

    def test_file_in_noop_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        noop_sink = NoopSink()
        noop_sink.start()

        pipeline = Pipeline()
        pipeline.set_sink([noop_sink])
        pipeline.start()

        file_source = FileSource(filename=TESTING_IN_FILE,pipeline=pipeline,marshaller=CsvMarshaller())    
        file_source.start()

    def test_file_in_multi_noop_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        noop_sink1 = NoopSink()
        noop_sink1.start()

        noop_sink2 = NoopSink()
        noop_sink2.start()

        pipeline = Pipeline()
        pipeline.set_sink([noop_sink1,noop_sink2])
        pipeline.start()

        file_source = FileSource(filename=TESTING_IN_FILE,pipeline=pipeline,marshaller=CsvMarshaller())    
        file_source.start()

    def test_file_in_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE, TESTING_OUT_FILE
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE)

        file_sink = FileSink(marshaller=CsvMarshaller(),filename=TESTING_OUT_FILE)
        file_sink.start()

        pipeline = Pipeline()
        pipeline.set_sink([file_sink])
        pipeline.start()

        file_source = FileSource(filename=TESTING_IN_FILE,pipeline=pipeline,marshaller=CsvMarshaller())    
        file_source.start()

        time.sleep(5.0)
        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE,TESTING_OUT_FILE))

    def test_file_in_multi_file_out(self):
        global LOGGING_LEVEL
        global TESTING_IN_FILE, TESTING_OUT_FILE, TESTING_OUT_FILE_2
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

        remove_file(TESTING_OUT_FILE)
        remove_file(TESTING_OUT_FILE_2)

        file_sink1 = FileSink(marshaller=CsvMarshaller(),filename=TESTING_OUT_FILE)
        file_sink2 = FileSink(marshaller=CsvMarshaller(),filename=TESTING_OUT_FILE_2)
        file_sink1.start()
        file_sink2.start()

        pipeline = Pipeline()
        pipeline.set_sink([file_sink1,file_sink2])
        pipeline.start()

        file_source = FileSource(filename=TESTING_IN_FILE,pipeline=pipeline,marshaller=CsvMarshaller())    
        file_source.start()

        time.sleep(2.0)
        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_OUT_FILE_2,TESTING_OUT_FILE))

    def test_listen_in_send_out(self):
        global LOGGING_LEVEL
        global TESTING_OUT_FILE, TESTING_IN_FILE
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
        remove_file(TESTING_OUT_FILE)
        host="localhost"
        port=5010

        # BUILD LISTEN TO FILE
        a_file_sink = FileSink(marshaller=CsvMarshaller(),filename=TESTING_OUT_FILE)
        a_pipeline = Pipeline()
        a_file_sink.start()
        a_pipeline.set_sink([a_file_sink])
        a_listen_source = ListenSource(marshaller=CsvMarshaller(),pipeline=a_pipeline,host=host,port=port)
        a_pipeline.start()
        a_listen_source.start()

        # BUILD FILE TO SEND
        b_send_sink = SendSink(marshaller=CsvMarshaller(),host=host,port=port)
        b_pipeline = Pipeline()
        b_pipeline.set_sink([b_send_sink])
        b_file_source = FileSource(filename=TESTING_IN_FILE,pipeline=b_pipeline,marshaller=CsvMarshaller())    

        b_send_sink.start()
        b_pipeline.start()
        b_file_source.start()

        time.sleep(2)

        a_listen_source.stop()
        time.sleep(2)

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

        a_listen_sink = ListenSink(marshaller=CsvMarshaller(),max_receivers=5,host=host,port=port)
        a_file_source.start()
        a_pipeline.set_sink([a_listen_sink])
        a_pipeline.start()
        a_listen_sink.start()

        time.sleep(2)

        b_pipeline = Pipeline()
        b_download_source = DownloadSource(marshaller=CsvMarshaller(),host=host,port=port,pipeline=b_pipeline)
        b_file_sink = FileSink(marshaller=CsvMarshaller(),filename=TESTING_OUT_FILE)
        b_file_sink.start()
        b_pipeline.set_sink([b_file_sink])
        b_pipeline.start()
        b_download_source.start()

        time.sleep(5)
        b_download_source.stop()
        time.sleep(1)
        import filecmp
        self.assertTrue(filecmp.cmp(TESTING_IN_FILE,TESTING_OUT_FILE))

if __name__ == '__main__':
    #test_listen_in_send_out()
    unittest.main()