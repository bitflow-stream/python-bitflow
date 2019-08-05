#!/usr/bin/env python3

import logging
import signal 
import sys
from bitflow.sinksteps import TerminalOut, FileSink, ListenSink
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.processingstep import ProcessingStep
from bitflow.source import DownloadSource, FileSource

CLOSING = False
#helper for strg+c
def strg_c_exit():
	global pipeline
	logging.warning("Closing provide-data script ...")
	pipeline.stop()

#helper for strg+c
def sig_int_handler(signal, frame):
	global CLOSING
	if CLOSING:
		sys.exit(1)
	CLOSING = True
	strg_c_exit()


''' example provide-data main'''
def main():
	# used to safe quit strg+c
	global pipeline
	# enable logging
	logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

	# catch strg + c / kill event
	signal.signal(signal.SIGINT, sig_int_handler)


	# port to provide data on
	listen_port=5012
	# file to provide via port
	in_file = "testing/testing_file_in.csv"

	# prepare ListenSink
	ls = ListenSink(host="localhost",
					port=listen_port)

	# prepare pipeline
	pipeline = Pipeline()
	# add processing step
	pipeline.add_processing_step(ls)

	# prepare file source
	file_source = FileSource(	filename=in_file,
								pipeline=pipeline)
	# start pipeline
	pipeline.start()
	# start file_source
	file_source.start()
	

if __name__ == '__main__':
	main()
