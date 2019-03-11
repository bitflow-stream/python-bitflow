#!/usr/bin/python3

import logging, sys
from bitflow.sinksteps import TerminalOut, FileSink
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.processingstep import ProcessingStep
from bitflow.source import DownloadSource


class 	 SerialFilter_ProcessingStep(ProcessingStep):

	def __init__(self,accepted_serials=[]):
		self.accepted_serials = accepted_serials

	def execute(self,sample):
		if sample.tags["serial"] in self.accepted_serials:
			return sample
		else:
			return None


''' example python3-bitflow main'''
def main():

	download_host="wally133.cit.tu-berlin.de"
	download_port=5025

	out_file = "/tmp/test_filea.txt"
	serials = ["66"]


	logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

	sf_ps = SerialFilter_ProcessingStep(serials)
	fs = FileSink(marshaller=CsvMarshaller(),filename=out_file)
	to = TerminalOut()

	pipeline = Pipeline()
	pipeline.add_processing_step(sf_ps)
	pipeline.add_processing_step(fs)
	#pipeline.add_processing_step(to)

	pipeline.start()

	download_source = DownloadSource(host=download_host,
									port=download_port,
									pipeline=pipeline,
									marshaller=CsvMarshaller(),
									buffer_size=1024)	
	download_source.start()

if __name__ == '__main__':
	main()
