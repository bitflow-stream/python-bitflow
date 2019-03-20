import threading
import logging
import queue
import multiprocessing
from bitflow.processingstep import AsyncProcessingStep

class Pipeline(threading.Thread):

	marshaller = None
	
	def __init__(self,maxsize=10000,multiprocessing_input=True):
		self.__name__ = "Pipeline"
		if multiprocessing_input:
			self.queue = multiprocessing.Queue(maxsize=maxsize)
		else:
			self.queue = queue.Queue(maxsize=maxsize)

		self.processing_steps = []
		self.running = True
		self.next_step = None
		self.first_step = None
		super().__init__()

	def next_step(self,step):
		self.next_step = step

	def chain_steps(self):
		for i in range(len(self.processing_steps)-1,-1,-1):
			# chain steps
			if i == 0:
				self.first_step = self.processing_steps[i]
			else:
				self.processing_steps[i-1].set_next_step(self.processing_steps[i])
		if self.next_step:
			self.processing_steps[len(self.processing_steps)-1] = self.next_step
			#logging.debug(i,self.processing_steps[i-1],self.processing_steps[i-1].next_step)			
	
	def prepare_processing_steps(self):
		for processing_step in self.processing_steps:
			# start threads etc....
			if isinstance(processing_step, AsyncProcessingStep):
				processing_step.start()
			elif isinstance(processing_step, Pipeline):
				processing_step.start()

	def run(self):
		self.prepare_processing_steps()		
		while self.running:
			try:
				sample = self.queue.get(timeout=1)
			except:
				continue
			if self.first_step:
				self.first_step.execute(sample)
			if isinstance(self.queue, queue.Queue):
				self.queue.task_done()
		self.on_close()

	# processing_step ,fork or subpipeline
	def add_processing_step(self, processing_step):
		self.processing_steps.append(processing_step)
		self.chain_steps()

	def remove_processing_step(self, processing_step): 
		try:
			self.processing_steps.remove(processing_step)
			self.chain_steps()
		except:
			logging.warning(processing_step.__name__ + " not found in pipeline")

	def execute(self,sample):
		self.queue.put(sample)

	def close_processing_steps(self):
		for ps in self.processing_steps:
			ps.stop()
			if isinstance(ps, AsyncProcessingStep):
				ps.join()


	def stop(self):
		self.running = False

	def on_close(self):
		logging.info	("{}: closing  ...".format(self.__name__))
		self.close_processing_steps()
