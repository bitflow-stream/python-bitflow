import copy
import logging
from bitflow.processingstep import *
from bitflow.pipeline import *

def initialize_batch_step(name,script_args):
	batch_steps = BatchProcessingStep.subclasses

	for f in batch_steps:
		if f.__name__.lower() == name.lower() and compare_args(f,script_args):
			logging.info("{} with args: {}  ok ...".format(name,script_args))
			try:
				f_obj = f(**script_args)
			except Exception  as e:
				logging.error(str(e))
			return f_obj
	logging.info("{} with args: {}  failed ...".format(name,script_args))
	return None

class BatchProcessingStep():
	''' Abstract BatchProcessingStep Class'''
	subclasses = []
	__description__ = "No description written yet."

	def __init__(self):
		self.next_step = None

	def __init_subclass__(cls, **kwargs):
		super().__init_subclass__(**kwargs)
		cls.subclasses.append(cls)

	def set_next_step(self,next_step):
		self.next_step = next_step

	def write(self,samples : list):
		if samples and self.next_step:
			self.next_step.execute(samples)

	''' receives list of samples '''
	def execute(self,samples : list):
		raise NotImplementedError

	def stop(self):
		self.on_close()

	def on_close(self):
		logging.info("{}: closing ...".format(self.__name__))

class AvgBatchProcessingStep(BatchProcessingStep):

	__description__ = "AVG all metrics"
	__name__ = "AvgBatch"

	def __init__(self):
		super().__init__()
		self.header = None

	def build_sample(self, metrics_lst, header):
		sum_metrics = [0] * len(metrics_lst[0])
		for metrics in metrics_lst:
			for i in range(len(metrics)):
				sum_metrics[i] += metrics[i]
		for i in range(len(metrics)):
			sum_metrics[i] = sum_metrics[i] / len(metrics_lst)
		return Sample(header=header,metrics=sum_metrics)

	def execute(self,samples : list):
		header = None
		metrics = []
		if self.header is None:
			self.header = samples[0].header
		for sample in samples:
			metrics.append(sample.get_metrics())
		self.write([self.build_sample(metrics,self.header)])