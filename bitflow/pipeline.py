import threading, queue, logging,time

class Pipeline(threading.Thread):

	marshaller = None
	
	def __init__(self,sink=[]):
		self.que = queue.Queue()
		self.processing_steps = []
		self.sink = sink
		self.running = True
		super(Pipeline, self).__init__(name=str(self))

	def __str__(self):
		return "Pipeline"

	def set_sink(self,sink):
		self.sink = sink

	def run(self):

		while self.running:
			try:
				sample = self.que.get(timeout=1)
			except:
				continue
			for processing_step in self.processing_steps:
				sample = processing_step.execute(sample)
			for o in self.sink:
				o.send(sample)
			self.que.task_done()

	def add_processing_step(self, processing_step):
		self.processing_steps.append(processing_step)

	def remove_processing_step(self, processing_step): 
		try:
			self.processing_steps.remove(processing_step)
		except:
			logging.warning(processing_step.name + " not found in pipeline")

	def execute_sample(self,sample):
		self.que.put(sample)

	def close_processing_steps(self):
		for f in self.processing_steps:
			f.on_close()

	def on_close(self):
		logging.info	("closing "+self.__str__()+" ...")
		self.que.join()
		time.sleep(0.1)
		self.running = False
		for o in self.sink:
			o.on_close()
			o.join()
		self.close_processing_steps()
