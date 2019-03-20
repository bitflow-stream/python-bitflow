from bitflow.processingstep import ProcessingStep

class SimpleTagger(ProcessingStep):
	def __init__(self, tagName, tagValue):
		self.__name__ = "SimpleTagger"
		self.tagName = tagName
		self.tagValue = tagValue
	
	def addTag(self, tagName, tagValue):
		sample.tags[tagName] = tagValue
	
	def execute(self, sample):
		# set sample tags here
		self.addTag(self.tagName, self.tagValue)
		# Give sample to next step with added tags
		return sample
	def on_close(self):
		logging.info("closing {} ...".format(self.__name__))

	