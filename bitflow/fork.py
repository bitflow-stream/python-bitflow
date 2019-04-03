import copy
import logging
from bitflow.processingstep import *
from bitflow.pipeline import *

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

def wildcard_compare(wildcard_expression, string):
	import fnmatch
	return fnmatch.fnmatch(string.lower(),wildcard_expression.lower())

def exact_compare(expression,string):
	if expression.lower() == string.lower():
		return True
	return False

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
		self.running_pipelines = {}

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
		cls.subclasses.append(cls)

	# current workarround for bitflow script, parse names and processingsteps for new
	# subpipeline. generate new pipeline if required by incoming sample
	def add_processing_steps(self,processing_steps=[],names=[]):
		self.fork_pipelines.append((processing_steps,names))

	def remove_pipeline(self,i : int):
		self.fork_pipelines.remove(i)

	def get_pipelines(self):
		return self.fork_pipelines

	def execute(self,sample):
		raise NotImplementedError

	def on_close(self):
		super().on_close()
		for p in list(self.running_pipelines.keys()):
			self.running_pipelines[p].stop()

# stop and on_close inherit from processingstep

class Fork_Tags(Fork):

	supported_compare_methods = ["wildcard","exact"]

	def __init__(self,tag : str, mode : str = "wildcard"):
		super().__init__()
		self.__name__ = "Fork_Tags"
		if mode in self.supported_compare_methods:
			self.mode = mode
		else:
			raise NotSupportedError("{}: {} method not supported. Only support {}".format(
																					self.__name__,compare, 
																					self.supported_compare_methods))
		self.tag = tag

	def compare(self,tag_value,names,mode):
		if mode == "wildcard":
			l_cmp = [ name for name in names if wildcard_compare(name,tag_value) ]
		if mode == "exact":
			l_cmp = [ name for name in names if exact_compare(name,tag_value) ]
		if len(l_cmp) > 0:
			return True
		return False

	def execute(self,sample):
		if sample.get_tag(self.tag):
			tag_value = sample.get_tag(self.tag)
			for processing_steps_list,names in self.fork_pipelines:
				if not self.compare(tag_value,names,self.mode):
					continue

				if tag_value in self.running_pipelines:
					p = self.running_pipelines[tag_value]
				else:
					p = Pipeline(multiprocessing_input=False)
					for ps in processing_steps_list:
						p.add_processing_step(ps)
					p.start()
					self.running_pipelines[tag_value] = p

				s = copy.deepcopy(sample)
				p.execute(s)
	