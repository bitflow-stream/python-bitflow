import logging
import struct
from bitflow.sample import Sample,Header


CSV_HEADER_START_STRING = "time"
BIN_HEADER_START_STRING = "timB"
CSV_HEADER_START_BYTES = CSV_HEADER_START_STRING.encode("UTF-8")
BIN_HEADER_START_BYTES = BIN_HEADER_START_STRING.encode("UTF-8")

class HeaderException(Exception):
	pass

def parse_tags(tags_string):
	tags_dict = {}
	if tags_string == "":
		return tags_dict
	tags_tuple = tags_string.split(" ")
	for tags in tags_tuple:
		if "=" in tags:
			key,value = tags.split('=')
			tags_dict[key] = value
		else:
			logging.warning("Unable to parse Tag: " + tags_string)
	return tags_dict

def build_header_string(column_names, seperator_string, end_of_header_string):
	s = ""
	string_count = len(column_names)
	list_position = 1
	for column_name in column_names:
		if(list_position == string_count):
			s += column_name + end_of_header_string
		else:
			s += column_name + seperator_string
			list_position += 1
	return s

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
	HEADER_START_STRING = CSV_HEADER_START_STRING
	HEADER_START_BYTES = CSV_HEADER_START_BYTES
	END_OF_HEADER_CHAR = b'\n'

	def __init__(self):
		pass

	def __get_metric_string__(self, metric):
		if metric.is_integer():
			return str(int(metric))
		return str(metric)

	def get_metrics_string(self, sample, seperator_string):
		s = ""
		list_position = 1
		for metric in sample.metrics:
			if(list_position == len(sample.metrics)):
				s += "{}".format(self.__get_metric_string__(metric))
			else:
				s += "{}{}".format(self.__get_metric_string__(metric), seperator_string)
				list_position += 1
		return s

	def get_timestamp_string(self, sample):
		pts = str(sample.get_timestamp()).replace("T"," ")
		pts = pts.rstrip('0')
		return pts

	def marshall_header(self, sink, header):
		special_f = [self.HEADER_START_STRING,"tags"]
		column_names = special_f + header.metric_names
		s = build_header_string(column_names=column_names,
								seperator_string=CsvMarshaller.SEPERATOR,
								end_of_header_string=CsvMarshaller.NEWLINE)
		sink.write(bytes(s,"UTF-8"))

	def marshall_sample(self, sink, sample):
		s = "{}{}{}{}{}{}".format(self.get_timestamp_string(sample=sample),
							CsvMarshaller.SEPERATOR,
							sample.get_tags_string(),
							CsvMarshaller.SEPERATOR,
							self.get_metrics_string(sample=sample,
													seperator_string=CsvMarshaller.SEPERATOR),
							CsvMarshaller.NEWLINE)
		sink.write(bytes(s,"UTF-8"))

	def unmarshall_header(self,header):
		header_line = header.decode("UTF-8").strip()
		fields = header_line.split(',')
		if len(fields) <= 1 or fields[0] != CsvMarshaller.HEADER_START_STRING:
			raise HeaderException("Header to small or wrong data format ...")
		return Header(fields[2:])

	def unmarshall_sample(self,header,metrics):
		metrics_line = metrics.decode("UTF-8").strip()
		values = metrics_line.split(self.SEPERATOR)
		try:
			metrics = [float(x) for x in values[2:len(values)]]
		except ValueError:
			logging.warning("Failed to marshall metrics, one metrics seems not to be a float value: {}".format(values))
			return
		timestamp = values[0]
		tags_string = values[1]
		tags = parse_tags(tags_string)
		sample = Sample(header=header,
						metrics=metrics,
						timestamp=timestamp,
						tags=tags)
		return sample

class BinMarshaller:

	NEWLINE='\n'
	NEWLINE_BYTE = b'\n'
	END_OF_HEADER_STRING = "\n\n"
	END_OF_HEADER_CHAR = b'\n\n'
	HEADER_START_STRING = BIN_HEADER_START_STRING
	HEADER_START_BYTES = BIN_HEADER_START_BYTES
	BEGIN_OF_SAMPLE_BYTE = b'X'

	TIMESTAMP_VALUE_BYTES_LEN = 8
	METICS_VALUE_BYTES_LEN = 8

	def __init__(self):
		self.__name__ = "BinaryMarshaller"

	def get_timestamp_bytes(self,sample):
		return struct.pack('>Q',sample.get_unix_timestamp())

	def __get_metric_bytes__(self,metric):
		return struct.pack('>d',metric)

	def get_metrics_bytes(self,sample):
		b = b''
		for metric in sample.metrics:
			b += self.__get_metric_bytes__(metric)
		return b

	def marshall_header(self, sink,header):
		special_f = [self.HEADER_START_STRING,"tags"]
		column_names = special_f + header.metric_names
		s = build_header_string(column_names=column_names,
								seperator_string=BinMarshaller.NEWLINE,
								end_of_header_string=BinMarshaller.END_OF_HEADER_STRING)
		sink.write(bytes(s,"UTF-8"))

	def marshall_sample(self, sink,sample):
		sample_bytes = BinMarshaller.BEGIN_OF_SAMPLE_BYTE \
					 + self.get_timestamp_bytes(sample=sample) \
					 + bytes(sample.get_tags_string(),"UTF-8") \
					 + BinMarshaller.NEWLINE_BYTE \
					 + self.get_metrics_bytes(sample=sample)
		sink.write(sample_bytes)

	def unmarshall_header(self,header):
		header_lines = header.decode("UTF-8").strip()
		fields = header_lines.split(BinMarshaller.NEWLINE)
		if len(fields) <= 1 or fields[0] != BinMarshaller.HEADER_START_STRING:
			raise HeaderException("Header to small or wrong data format ...")
		return Header(fields[2:])

	def unmarshall_sample(self,header,metrics):
		#offset 0 = BEGIN_OF_SAMPLE_BYTE
		offset = 1
		# handle timestamp
		timestamp_bytes = metrics[offset:offset + BinMarshaller.TIMESTAMP_VALUE_BYTES_LEN]
		timestamp = struct.unpack('>Q',timestamp_bytes)[0]
		offset += BinMarshaller.TIMESTAMP_VALUE_BYTES_LEN
		# handle tags
		# ignore timestamp bytes by searching for newline after tags
		end_of_tags_byte = metrics[offset:len(metrics)].find(BinMarshaller.NEWLINE_BYTE)
		end_of_tags_byte += offset
		try:
			tags_str = metrics[offset:end_of_tags_byte].decode("UTF-8")
		except UnicodeDecodeError as e:
			logging.warning("{}: Could not unmarshall sample correctly, dropping sample ...".format(self.__name__))
			return None

		tags_dict = parse_tags(tags_str)
		offset = end_of_tags_byte + 1
		# handle metrics
		metrics_lst = []
		for _ in range(header.num_fields()):
			metric = struct.unpack('>d',
							metrics[offset:offset + BinMarshaller.METICS_VALUE_BYTES_LEN])[0]
			metrics_lst.append(metric)
			offset += BinMarshaller.METICS_VALUE_BYTES_LEN
		sample = Sample(header=header,
						metrics=metrics_lst,
						timestamp=timestamp,
						tags=tags_dict)
		return sample
