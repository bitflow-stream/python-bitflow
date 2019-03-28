import copy
import logging
from bitflow.processingstep import *

def initialize_fork(name,script_args):
	fork_steps = Fork.subclasses

	for f in fork_steps:

		if f.__name__.lower() == name.lower():
			if compare_args(f,script_args):
				logging.info("{} with args: {}  ok ...".format(name,script_args))
				try:
					f_obj = f(**script_args)
				except Exception  as e:
					logging.error(str(e))
				return f_obj
	logging.info("{} with args: {}  failed ...".format(name,script_args))
	return None

class Fork(ProcessingStep):
	"""
	receives sample and executes fork_decision, if fork decision is true, 
	is copied and forwarded to each pipeline given via the constructor. After
	a sample is processed by a subpipeline it will be put returned by the execute function
	as a normal  processing step

	pipelines: a list of pipelines to add sample to
	"""
	subclasses = []

	def __init__(self):
		super().__init__()
		self.fork_pipelines = []

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
		cls.subclasses.append(cls)

	def fork_decision(self,sample):
		raise NotImplementedError

	def add_pipeline(self,pipeline,names=[]):
		self.fork_pipelines.append((pipeline,names))

	def remove_pipeline(self,i : int):
		self.fork_pipelines.remove(i)

	def get_pipelines(self):
		return self.fork_pipelines

	def execute(self,sample):
		if self.fork_decision(sample):
			for p in self.fork_pipelines:
				s = copy.copy(sample)
				p.execute(s)
		self.write(sample)

	def on_close(self):
		super().on_close()
		for p,names in self.fork_pipelines:
			p.stop()

# stop and on_close inherit from processingstep

class Fork_Tags(Fork):

	def __init__(self,tag : str):
		super().__init__()
		self.tag = tag

	def fork_decision(self,sample):
		if sample.get_tag(self.tag):
			return True
		return False
