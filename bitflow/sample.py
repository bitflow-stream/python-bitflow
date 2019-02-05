import logging,datetime,copy

class Sample:

	def __init__(self,header,metrics,timestamp):
		self.header = Header(header.header,header.has_tags)
		self.metrics = metrics
		self.timestamp = str(timestamp)
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
		dt = datetime.datetime.strptime(self.timestamp,'%Y-%m-%d %H:%M:%S.%f')
		return int(dt.strftime("%s"))

	def set_timestamp(self,timestamp):
		self.timestamp = str(timestamp)

	#def get_epoch_timestamp(self):
	#	epoch_timestamp = datetime.datetime(self.timestamp).timestamp()
	#	return epoch_timestamp

	def remove_metrics(self,index):
		self.header.header.remove(index)
		self.sample.metrics = self.sample.metrics[:index:]

	def get_tag(self,tag):
		return self.tags[tag]

	def add_tag(self,tag_key,tag_value):
		if self.header.has_tags == False:
			self.header.has_tags = True
		self.tags[tag_key] = tag_value

	def header_changed(self,old_header):
		return header.header_changed(old_header)

	@staticmethod
	def new_empty_sample():
		pass

	def __str__(self):
		return str(self.metrics)

class Header:

	HEADER_TIME = "time"
	HEADER_TAGS = "tags"

	def __init__(self,header,has_tags=False):
		self.has_tags = has_tags
		self.header = list(header)

	def extend(self,metric_name):	
		self.header.append(metric_name)

	def num_special_fields(self):
		if self.has_tags:
			return 2
		return 1

	def num_fields(self):
		return len(self.header)

	def get_special_fields(self):
		if self.has_tags:
			return [self.HEADER_TIME, self.HEADER_TAGS]
		else:
			return [self.HEADER_TIME ]

	def has_changed(self,new_header):
		if self.num_fields() != new_header.num_fields() :
			return True
		else:
			for i in range(0,len(self.header)):
				if self.header[i] != new_header.header[i]:
					return True
		if self.get_special_fields() != new_header.get_special_fields() :
			return True
		return False