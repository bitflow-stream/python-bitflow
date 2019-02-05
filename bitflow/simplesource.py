import logging, threading
from bitflow.marshaller import *
from bitflow.sample import Header,Sample

class SimpleSource(threading.Thread):

	RECONNECT_TIMEOUT = 1

	def __init__(self,pipeline):
		self.pipeline = pipeline
		super().__init__()

	def __str__(self):
		return "SimpleSource"

	def into_pipeline(self,sample):
		self.pipeline.execute_sample(sample)

	def run(self):
		pass

	def on_close(self):
		logging.debug("closing " + self.__str__() +" ...")
		self.is_running=False
		self.pipeline.on_close()