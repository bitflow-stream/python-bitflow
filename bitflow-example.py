#!/usr/bin/env python3

import logging
import sys

from bitflow.sinksteps import TerminalOut
from bitflow.marshaller import CsvMarshaller
from bitflow.pipeline import Pipeline
from bitflow.steps.plotprocessingsteps import PlotLinePlot  
from bitflow.source import FileSource


''' example python3-bitflow main'''
def main():
	global pipeline
	# enable logging
	logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

	# file to read from
	input_filename = "testing/testing_file_in.txt"

	# define pipeline
	pipeline = Pipeline()

	# add processingsteps to pipeline
	pipeline.add_processing_step(PlotLinePlot(metric_names="pkg_out_1000-1100"))
	# add terminal output to pipeline
	pipeline.add_processing_step(TerminalOut())
	
	# start pipeline
	pipeline.add_processing_step(PlotLinePlot(metric_names="pkg_out_1000-1100"))
	pipeline.add_processing_step(TerminalOut())
	pipeline.start()

	# define file source
	filesource = FileSource(filename=input_filename,
							pipeline=pipeline,
							marshaller=CsvMarshaller())	
	# start file source
	filesource.start()

	import time
	time.sleep(4)
	filesource.stop()
	pipeline.stop()

if __name__ == '__main__':
	main()
