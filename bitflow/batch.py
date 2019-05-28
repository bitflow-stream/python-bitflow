from bitflow.pipeline import Pipeline
from bitflow.processingstep import ProcessingStep
from bitflow.batchprocessingstep import BatchProcessingStep

class Batch(ProcessingStep):

	class _BatchPipelineTerminator(BatchProcessingStep):
		''' BatchPipelineTerminatorProcessingStep Class'''
		__name__ = "BatchPipelineTerminator"
		__description__ = "Translates Sample lists to samples. forwards them to next ps in root pipeline by attaching it to the root pipeline and the step number"

		def __init__(	self,
						batch_ps,
					 	root_pipeline):
			self.rp = root_pipeline
			self.batch_ps = batch_ps

		# must have be samples ... 
		# this step converts lists to single samples
		def execute(self,sample):
			for s in sample:
				self.write(s)

		def write(self,sample):
			self.rp.execute_after(sample,self.batch_ps)

	''' BatchProcessingStep Class'''
	__name__ = "Batch"
	__description__ = "Batch instantiates a batch pipeline and pushes samples batch wise into this pipeline"

	def __init__(	self,
				 	batch_size : int,
				 	flush_tag : str = None,
				 	flush_header_change : bool = True):

		super().__init__()
		self.batch_size = int(batch_size)
		self.flush_tag = flush_tag
		self.flush_header = flush_header_change
		self.batch_pipeline = None
		self.batch = []
		self.header = None

	def set_root_pipeline(self,pl):
		self.root_pipeline = pl

	def add_processing_step(self,processing_step):
		if self.batch_pipeline:
			self.batch_pipeline.add_processing_step(processing_step)
			return True
		else:
			warning("{}: Could not add processing step to batch pipeline, no pipeline set ...")
			return False

	def set_batch_pipeline(self,batch_pipeline):
		self.batch_pipeline = batch_pipeline

	def get_batch_pipeline(self):
		return self.batch_pipeline

	def start_batch_pipeline(self):
		# root_pipeline can be None if batch is pipeline_tail_element (bitflow_script),
		# in that case BatchPipelineTerminator is not necessary
		if self.root_pipeline:
			self.batch_pipeline.add_processing_step(
				Batch._BatchPipelineTerminator( batch_ps=self,
												root_pipeline=self.root_pipeline))
		self.batch_pipeline.start()

	def set_next_step(self,next_step):
		self.next_step = next_step

	def add_to_batch(self,sample):
		self.batch.append(sample)

	def execute(self,sample):
		if not self.header:
			self.header = sample.header
		elif sample.header.has_changed(self.header) and self.flush_header:
			self.write(self.batch)
			self.header = sample.header
		self.add_to_batch(sample)
		if len(self.batch) >= self.batch_size:
			self.write(self.batch)

	# sample in this case is a list of samples
	# abusing python, maybe there are better ways..
	def write(self,sample):
		if isinstance(sample,list) and self.next_step:
			self.batch_pipeline.execute(self.batch)
		self.batch = []

	def stop(self):
		self.on_close()

	def on_close(self):
# TODO handle buffered samples in closing event
#		if self.batch:
#			self.write(self.batch)
		super().on_close()
		self.batch_pipeline.stop()
