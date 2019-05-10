import logging
import struct

from bitflow.sample import Sample,Header


CSV_HEADER_START_STRING = "time"
BIN_HEADER_START_STRING = "timB"
CSV_HEADER_START_BYTES = CSV_HEADER_START_STRING.encode("UTF-8")
BIN_HEADER_START_BYTES = BIN_HEADER_START_STRING.encode("UTF-8")

class HeaderException:
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
	END_OF_HEADER_CHAR_LEN = 1

	def __init__(self):
		pass

	def build_header_csv_string(self, header):
		s = ""
		string_count = len(header)
		list_position = 1
		for value in header:
			if(list_position == string_count):
				s += str(value) + CsvMarshaller.NEWLINE
			else:
				s += str(value) + CsvMarshaller.SEPERATOR
				list_position += 1
		return s
	
	def build_metrics_csv_string(self, metrics):
		s = ""
		string_count = len(metrics)
		list_position = 1
		for metric in metrics:
			if(list_position == string_count):
				if metric.is_integer():
					s += str(int(metric)) + CsvMarshaller.NEWLINE
				else:
					s += str(metric) + CsvMarshaller.NEWLINE
			else:
				if metric.is_integer():
					s += str(int(metric)) + CsvMarshaller.SEPERATOR
				else:
					s += str(metric) + CsvMarshaller.SEPERATOR
				list_position += 1
		return s

	def build_csv_tags(self,tags):
		s = ""
		tags_count = len(tags.items())
		dict_position = 1
		for k,v in tags.items():
			if(dict_position == tags_count):
				s += str(k)+"="+str(v) + CsvMarshaller.SEPERATOR
			else:
				s += str(k)+"="+str(v) + CsvMarshaller.SPACE
				dict_position += 1
		return s

	def marshall_header(self, sink, header):
		special_f = [self.HEADER_START_STRING,"tags"]
		fields = special_f + header.header
		s = self.build_header_csv_string(fields)
		sink.write(s)

	def marshall_sample(self, sink, sample):
		s = str(sample.get_printable_timestamp()) + CsvMarshaller.SEPERATOR
		if sample.tags:
			s += str(self.build_csv_tags(sample.tags))
		else:
			s += ","

		s += self.build_metrics_csv_string(sample.metrics)
		sink.write(s)

	def unmarshall_header(self,header):
		header_line = header.decode("UTF-8").strip()
		fields = header_line.split(',')
		if len(fields) <= 1 or fields[0] != CsvMarshaller.HEADER_START_STRING:
			raise HeaderException("Header to small or wrong data format ...")
		return Header(fields[2:])

	def unmarshall_sample(self,header,metrics):
		metrics_line = metrics.decode("UTF-8").strip()
		values = metrics_line.split(self.SEPERATOR)
		metrics = [float(x) for x in values[2:len(values)]]
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
	HEADER_START_STRING = BIN_HEADER_START_STRING
	HEADER_START_BYTES = BIN_HEADER_START_BYTES
	END_OF_HEADER_CHAR = b'\n\n'
	END_OF_HEADER_CHAR_LEN = 3
	BEGIN_OF_SAMPLE_BYTE = b'X'
	TIMESTAMP_VALUE_BYTES_LEN = 8
	METICS_VALUE_BYTES_LEN = 8

	def __init__(self):
		self.__name__ = "BinaryMarshaller"

	def marshall_header(self, sink,header):
		raise NotImplementedError

	def marshall_sample(self, sink,sample):
		raise NotImplementedError

	def unmarshall_header(self,header):
		header_lines = header.decode("UTF-8").strip()
		fields = header_lines.split(BinMarshaller.NEWLINE)
		if len(fields) <= 1 or fields[0] != BinMarshaller.HEADER_START_STRING:
			raise HeaderException("Header to small or wrong data format ...")
		return Header(fields[2:])

	def unmarshall_sample(self,header,metrics):
		offset = 0
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
