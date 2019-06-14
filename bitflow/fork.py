import copy
import logging
from bitflow.processingstep import *
from bitflow.pipeline import *

def initialize_fork(name,script_args):
	fork_steps = Fork.subclasses

	for f in fork_steps:

		if f.__name__.lower() == name.lower() and compare_args(f,script_args):
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
	"""
	class _ForkPipelineTerminator(ProcessingStep):
		__name__ = "SubPipelineTerminator"
		__description__ =  "Terminates a subpipeline by forwarding samples to next ps in the outer pipeline. \
							Goal Seperate Threads, Processes for each Pipeline"

		def __init__(	self,
						fork_ps,
						outer_pipeline):
			self.op = outer_pipeline
			self.fork_ps = fork_ps

		def execute(self,sample):
			self.write(sample)

		def write(self,sample):
			self.op.execute_after(sample,self.fork_ps)

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

	def spawn_new_subpipeline(self,ps_list,name):
		p = Pipeline(multiprocessing_input=False)
		for ps in ps_list:
			p.add_processing_step(ps)
		p.start()
		if self.outer_pipeline:
			p.add_processing_step(	Fork._ForkPipelineTerminator(
										fork_ps=self,
										outer_pipeline=self.outer_pipeline))
		self.running_pipelines[name] = p
		return p

	def execute(self,sample):
		raise NotImplementedError

	def on_close(self):
		super().on_close()
		for p in list(self.running_pipelines.keys()):
			self.running_pipelines[p].stop()

class Fork_Tags(Fork):

	supported_compare_methods = ["wildcard","exact"]

	def __init__(self,tag : str, mode : str = "wildcard"):
		super().__init__()
		self.__name__ = "Fork_Tags"
		self.outer_pipeline = None
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

	def set_root_pipeline(self,outer_pipeline):
		self.outer_pipeline = outer_pipeline

	def execute(self,sample):
		if sample.get_tag(self.tag):
			tag_value = sample.get_tag(self.tag)
			for processing_steps_list,names in self.fork_pipelines:
				if not self.compare(tag_value,names,self.mode):
					# if tag value not known, igonre
					continue
				if tag_value in self.running_pipelines:
					p = self.running_pipelines[tag_value]
				else:
					p = self.spawn_new_subpipeline(processing_steps_list,tag_value)
				s = copy.deepcopy(sample)
				p.execute(s)
	