import logging,datetime

from bitflow.sample import Sample,Header

CSV_HEADER_START_STRING = "time"
BIN_HEADER_START_STRING = "timB"

class Marshaller:

	def marshall_header(self, sink,header):
		raise NotImplementedError

	def marshall_sample(self, sink,sample):
		raise NotImplementedError

	def unmarshall_header(self,header):
		raise NotImplementedError

	def unmarshall_sample(self,header,metrics):
		raise NotImplementedError

class CsvMarshaller(Marshaller):

	SEPERATOR=","
	NEWLINE='\n'
	SPACE=" " 
	HEADER_START_STRING = "time"

	def __init__(self):
		pass

	def build_csv_string(self, h):
		s = ""
		string_count = len(list(h))
		list_position = 1
		for i in h:
			if(list_position == string_count):
				s += str(h[-1]) + self.NEWLINE	
			else:
				s += str(i) + self.SEPERATOR
				list_position += 1
		return s
	
	def build_csv_tags(self,tags):
		s = ""
		tags_count = len(tags.items())
		dict_position = 1
		for k,v in tags.items(): # todo
			if(dict_position == tags_count):
				s += str(k)+"="+str(v) + self.SEPERATOR
			else:
				s += str(k)+"="+str(v) + self.SPACE
				dict_position += 1
		return s

	def marshall_header(self, sink, header):
		special_f = header.get_special_fields()
		fields = special_f + header.header
		s = self.build_csv_string(fields)
		try:
			sink.write(s)
		except:
			logging.error("writing sink failed to sink %s", str(sink))

	def marshall_sample(self, sink, sample):
		if len(sample.metrics) > 0 or sample.header.has_tags:
			s = str(sample.timestamp) + self.SEPERATOR
		else:
			s = str(sample.timestamp)+"\n"
		
		if sample.header.has_tags:
			s += str(self.build_csv_tags(sample.tags))
		
		s += self.build_csv_string(sample.metrics)
		sink.write(s)

	##############
	# UNMARSHALL #
	##############

	def unmarshall_header(self,header_line):
		try:
			fields = header_line.split(',')
		except:
			pass #todo
		has_tags = True

		if len(fields) < 1 or fields[0] != Header.HEADER_TIME:
			raise Exception('no '+ Header.HEADER_TIME +" set")
			sys.exit(1)
		if len(fields) == 1:
			return Header([],False)

		if fields[1] == Header.HEADER_TAGS:

			has_tags = True
			h = Header(fields[2:],has_tags)
			return h

		return Header(fields[1:],has_tags)
		
	def parse_tags(self,tags_string):
		tags_dict = {}
		if tags_string == "":
			return tags_dict 
		try:
			tags_tuple = tags_string.split(' ')
			for tags in tags_tuple:
				if "=" in tags:
					key,value = tags.split('=')
					tags_dict[key] = value
				else:
					logging.warning("Unable to parse Tag: " + tags_string)
		except Exception as e:
			logging.error("Bad Tag parsing! \n " + e)
			return ""
		return tags_dict


	def unmarshall_sample(self,header,metrics_line):
		try:
			metrics = metrics_line.split(",")
		except Exception as metrics_split1:
			logging.warning("unable to ,split " + metrics_line + "in CsvMarshaller->unmarshall_sample. " + str(metrics_split1))
			return
		timestamp = metrics[0]
		if len(metrics)>1:
			if header.has_tags:
				tags_string = metrics[1]
				metrics = metrics[2:]
			else: 
				metrics = metrics[1:]
				tags_string = ""
			# clean last newline
			metrics[len(metrics)-1] = metrics[len(metrics)-1].strip()
			tags = self.parse_tags(tags_string)
			sample = Sample(header,metrics,timestamp)
			for k,v in tags.items():
				sample.add_tag(k,v)
		else:
			sample = Sample(header,[],timestamp)

		return sample

	def is_header(self,header_line):
		try:
			if header_line.split(',')[0] == "time":
				return True
			return False
		except:
			return False
