import threading, logging,time
import multiprocessing
from bitflow.sinksteps import AsyncProcessingStep

class Pipeline(threading.Thread):

	marshaller = None
	
	def __init__(self,maxsize=10000):
		self.queue = multiprocessing.Queue(maxsize=maxsize)
		self.processing_steps = []
		self.running = True
		super(Pipeline, self).__init__(name=str(self))

	def __str__(self):
		return "Pipeline"

	def run(self):
		for processing_step in self.processing_steps:
			if isinstance(processing_step, AsyncProcessingStep):
				processing_step.start()
		while self.running:
			try:
				sample = self.queue.get(timeout=1)
			except:
				continue
			for processing_step in self.processing_steps:
				sample = processing_step.execute(sample)
				if sample is None:
					break
		self.on_close()

	def add_processing_step(self, processing_step):
		self.processing_steps.append(processing_step)

	def remove_processing_step(self, processing_step): 
		try:
			self.processing_steps.remove(processing_step)
		except:
			logging.warning(processing_step.__name__ + " not found in pipeline")

	def execute_sample(self,sample):
		self.queue.put(sample)

	def close_processing_steps(self):
		for ps in self.processing_steps:
			ps.stop()
			if isinstance(ps, AsyncProcessingStep):
				ps.join()

	def stop(self):
		self.running = False

	def on_close(self):
		logging.info	("closing "+self.__str__()+" ...")
		time.sleep(0.1)
		self.close_processing_steps()
