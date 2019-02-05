#!/usr/bin/python3

import logging, sys
from bitflow.sink import StdSink
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.processingstep import SimpleLinePlotProcessingStep  
from bitflow.source import FileSource


''' example python3-bitflow main'''
def main():

	input_filename = "testing/testing_file_in.txt"

	std_out = StdSink(CsvMarshaller())
	std_out.start()
	
	pipeline = Pipeline()
	pipeline.add_processing_step(SimpleLinePlotProcessingStep("pkg_out_1000-1100"))
	pipeline.set_sink([std_out])
	pipeline.start()

	filesource = FileSource(input_filename,pipeline,CsvMarshaller())	
	filesource.start()

if __name__ == '__main__':
	main()
