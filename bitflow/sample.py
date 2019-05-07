import logging
import numpy as np
import copy
import time

class Sample:

	def __init__(self,header,metrics,timestamp=None,tags=None):
		self.header = Header(header.header)
		self.metrics = metrics
		if not timestamp:
			self.timestamp = np.datetime64(time.time_ns(),'ns')
		else:
			if isinstance(timestamp,int):
				self.timestamp =  np.datetime64(timestamp,'ns')
			else:
				self.timestamp =  np.datetime64(timestamp)
		if tags:
			self.tags = tags
		else:
			self.tags = {}

	def extend(self,metric):
		self.metrics.append(metric)

	def get_metricsindex_by_name(self,metric_name):
		index = self.header.header.index(metric_name)
		return index

	def get_metricvalue_by_name(self,metric_name):
		index = self.header.header.index(metric_name)
		m = self.metrics[index]
		return m

	def get_timestamp(self):
		return self.timestamp

	def get_printable_timestamp(self):
		pts = str(self.timestamp).replace("T"," ")
		return pts

	def set_timestamp(self,timestamp : str):
		self.timestamp = np.datetime64(timestamp)

	def get_epoch_timestamp(self):
		epoch_timestamp = self.timestamp.astype('datetime64[ns]').astype('float')
		return epoch_timestamp

	def remove_metrics(self,index):
		self.header.header.remove(index)
		self.sample.metrics = self.sample.metrics[:index:]

	def get_tag(self,tag):
		if tag in self.tags:
			return self.tags[tag]
		else:
			return None

	def add_tag(self,tag_key,tag_value):
		self.tags[tag_key] = tag_value

	def header_changed(self,old_header):
		return header.has_changed(old_header)

	@staticmethod
	def new_empty_sample():
		pass

	def __str__(self):
		return str(self.metrics)

class Header:

	def __init__(self,header):
		self.header = list(header)
		self.has_tags = True

	def extend(self,metric_name):	
		self.header.append(metric_name)

	def num_fields(self):
		return len(self.header)

	def has_changed(self,new_header):
		if self.num_fields() != new_header.num_fields() :
			return True
		else:
			for i in range(0,len(self.header)):
				if self.header[i] != new_header.header[i]:
					return True
		return False
