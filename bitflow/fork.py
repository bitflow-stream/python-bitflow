import copy

from bitflow.processingstep import ProcessingStep

class Fork(ProcessingStep):
	"""
	receives sample and executes fork_decision, if fork decision is true, 
	is copied and forwarded to each pipeline given via the constructor. After
	a sample is processed by a subpipeline it will be put returned by the execute function
	as a normal  processing step
	
	pipelines: a list of pipelines to add sample to
	"""
	def __init__(self, fork_pipelines):
		super().__init__()
		self.fork_pipelines=fork_pipelines

	def fork_decision(self,sample):
		raise NotImplementedError

	def execute(self,sample):
		if self.fork_decision(sample):
			for p in self.fork_pipelines:
				s = copy.copy(sample)
				p.execute(s)
		self.write(sample)

class Tag_Fork(Fork):

	def __init__(self,fork_pipelines,tag):
		self.tag = tag
		Fork.__init__(self,fork_pipelines=fork_pipelines)

	def fork_decision(self,sample):
		if sample.get_tag(self.tag):
			return True
		return False