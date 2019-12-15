#!/usr/bin/env python3

import logging
import signal
import sys

from bitflow.io.sinksteps import TerminalOut, FileSink
from bitflow.io.sources import DownloadSource
from bitflow.pipeline import PipelineSync
from bitflow.processingstep import ProcessingStep

CLOSING = False


# helper for strg+c
def strg_c_exit():
    global pipeline
    logging.warning("Closing provide-data script ...")
    download_source.stop()
    pipeline.stop()


# helper for strg+c
def sig_int_handler(signal, frame):
    global CLOSING
    if CLOSING:
        sys.exit(1)
    CLOSING = True
    strg_c_exit()


class SerialFilter_ProcessingStep(ProcessingStep):

    def __init__(self, accepted_serials=None):
        super().__init__()  # NEW
        if accepted_serials is None:
            accepted_serials = []
        self.__name__ = "SerialFilter"  # NEW
        self.accepted_serials = accepted_serials

    def execute(self, sample):
        if 'serial' in sample.tags.keys():
            if sample.tags["serial"] in self.accepted_serials:
                self.write(sample)  # NEW


''' example python3-bitflow main'''


def main():
    global pipeline, download_source
    # catch strg + c / kill event
    signal.signal(signal.SIGINT, sig_int_handler)

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # INSERT DOWNLOAD HOST
    download_host = "localhost"
    download_port = 5012
    out_file = "/tmp/test_filea.txt"
    serials = ["66"]

    sf_ps = SerialFilter_ProcessingStep(serials)
    # NEW
    fs = FileSink(filename=out_file)
    to = TerminalOut()

    pipeline = PipelineSync()
    pipeline.add_processing_step(sf_ps)
    pipeline.add_processing_step(fs)
    pipeline.add_processing_step(to)

    download_source = DownloadSource(host=download_host,
                                     port=download_port,
                                     pipeline=pipeline,
                                     buffer_size=2048)
    download_source.start_and_wait()


if __name__ == '__main__':
    main()
