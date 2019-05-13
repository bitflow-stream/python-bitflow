import socket
import logging
import time
import select
import ctypes
import multiprocessing 
from bitflow.marshaller import *

def read_header(marshaller, s, buffer_size=2048):
	b = ""
	while '\n' not in b:
		b += s.recv(buffer_size).decode()
	header_and_rest = b.split("\n")
	header_str = header_and_rest[0]
	over_recv_b = header_and_rest[1]
	header = marshaller.unmarshall_header(header_str)
	return header, over_recv_b

class Source(multiprocessing.Process):

	def __init__(self, queue):
		self.queue = queue
		self.header = None
		super().__init__()

	def __str__(self):
		return "Source"

	def into_pipeline(self, marshaller, b_metrics, header):
		sample = marshaller.unmarshall_sample(header, b_metrics)
		if sample:
			self.queue.put(sample)

	def cut_bytes(self,cutting_pos,b,cut_len=0):
		begin = b[0:cutting_pos]
		rest = b[cutting_pos+cut_len:len(b)]
		return begin,rest

	def get_start_bytes(self,b):
		return b[0:4]

	def is_header_start(self,b,marshaller):
		if not self.marshaller:
			return False
		if self.get_start_bytes(b) == marshaller.HEADER_START_BYTES:
			return True
		return False

	# cuts at newline, end of line for csv end after tags for bin
	def get_newline_cutting_pos(self,b):
		newline = b'\n'
		btlen = BinMarshaller.TIMESTAMP_VALUE_BYTES_LEN
		if len(b) > btlen:
			cutting_pos = b[btlen:len(b)].find(newline)
			if cutting_pos > 0:
				return cutting_pos + btlen
		return -1

	def get_marshaller(self,b):
		marshaller = None
		header_start_bytes = self.get_start_bytes(b)
		if header_start_bytes == CSV_HEADER_START_BYTES:
			marshaller = CsvMarshaller()
		elif header_start_bytes == BIN_HEADER_START_BYTES:
			marshaller = BinMarshaller()
		return  marshaller

	def run(self):
		while self.running.value:
			self.loop()
		self.on_close()

	def stop(self):
		self.running.value = 0

	def on_close(self):
		logging.info("{}: closing ...".format(self.__name__))

class FileSource:

	def __init__(self, filename, pipeline):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = 2
		self.filesource = _FileSource(self.running,filename,pipeline.queue)

	def start(self):
		self.filesource.start()

	def stop(self):
		self.running.value = 0
		self.filesource.join()

class _FileSource(Source):

	def __init__(self,running,filename,queue, buffer_size=2048):
		self.running = running
		self.filename = filename
		self.f = None
		self.header = None
		self.marshaller = None
		self.b = b''
		self.buffer_size = buffer_size
		self.__name__ = "FileSource"
		super().__init__(queue)

	def __str__(self):
		return "FileSource"

	def open_file(self,filename):
		try:
			return open(filename, 'rb')
		except FileNotFoundError:
			logging.error("{}: could not open file {} ...".format(str(self), self.filename))
			self.stop()
			exit(1)

	def read_bytes(self,s,buffer_size):
		read_b = s.read(buffer_size)
		if not read_b:
			self.stop()
			return b''
		return read_b

	def loop(self):
		if not self.f:
			self.f = self.open_file(self.filename)
			self.b = self.read_bytes(s=self.f,buffer_size=self.buffer_size)
			return

		if not self.marshaller:
			self.marshaller = self.marshaller = self.get_marshaller(self.b)

		if self.is_header_start(self.b,self.marshaller):
			header_end_pos = self.b.find(self.marshaller.END_OF_HEADER_CHAR)
			if header_end_pos == -1:
				self.b += self.read_bytes(s=self.f,buffer_size=self.buffer_size)
				return
			b_header,self.b = self.cut_bytes(header_end_pos,self.b,len(self.marshaller.END_OF_HEADER_CHAR))
			try:
				self.header = self.marshaller.unmarshall_header(b_header)
			except HeaderException as e:
				self.header = None
				logging.warning("{}: {}".format(self.__name__,str(e)))
			return

		cutting_pos = self.get_newline_cutting_pos(self.b)
		if cutting_pos == -1:
			self.b += self.read_bytes(s=self.f,buffer_size=self.buffer_size)
			return

		if isinstance(self.marshaller,BinMarshaller):
			metric_bytes_len = self.header.num_fields() * BinMarshaller.METICS_VALUE_BYTES_LEN 
			if len(self.b) <= cutting_pos + metric_bytes_len:
				self.b += self.read_bytes(s=self.f,buffer_size=self.buffer_size)
				return
			else:
				cutting_pos += metric_bytes_len + 1 # add newline after tags byte

		b_metrics,self.b = self.cut_bytes(cutting_pos, self.b)
		self.into_pipeline(b_metrics=b_metrics, header=self.header, marshaller=self.marshaller)
		return

	def on_close(self):
		self.f.close()
		super().on_close()

# Pulls / Downloads incoming data on the specified host:port
class DownloadSource:

	def __init__(self, pipeline, host, port, buffer_size=2048):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = pipeline
		self.downloadsource = _DownloadSource(self.running, host, port, pipeline.queue, buffer_size)

	def start(self):
		self.downloadsource.start()
	
	def stop(self):
		self.running.value = 0
		self.downloadsource.join()

class _DownloadSource(Source):

	def __init__(self,running,host,port,queue,buffer_size=2048,timeout_after_failed_to_connect=1):
		self.running = running
		self.host = host
		self.port = port
		self.timeout_after_failed_to_connect = timeout_after_failed_to_connect
		self.s = None
		self.header = None
		self.b = b''
		self.marshaller = None
		self.buffer_size = buffer_size
		self.__name__ = "DownloadSource"
		super().__init__(queue)

	def __str__(self):
		return "DownloadSource"

	def connect(self):
		logging.info("{}: trying to connect to {}:{} ...".format(self.__name__,self.host,self.port))
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.settimeout(1)
		self.s.connect((self.host, self.port))
		logging.info("{}: connected to {}:{} ...".format(self.__name__,self.host,self.port))

	def is_connected(self):
		if self.s != None:
			return True
		else:
			return False

	def read_bytes(self,s,buffer_size):
		try:
			b = s.recv(buffer_size)
		except ConnectionResetError:
			logging.warning("{}: ConnectionResetError: connection was reset by peer, reconnecting ...".format(self.__name__))
			self.close_connection()
			return None
		except UnicodeError:
			logging.warning("{}: encoding error ...".format(self.__name__))
			self.close_connection()
			return None
		except socket.timeout:
			return
		if b == b'':
			logging.warning("{}: Connection closed by peer, trying to reconnect ...".format(self.__name__))
			self.close_connection()
			return None
		return b

	def loop(self):
		if not self.is_connected():
			try:
				self.connect()
			except socket.gaierror as gai:
				logging.warning("{}: Could not connect to {}:{} ...".format(self.__name__,self.host, self.port))
				time.sleep(self.timeout_after_failed_to_connect)
				self.s = None
				return
			except ConnectionRefusedError:
				logging.warning("{}: Connection refused from {}:{} ...".format(self.__name__,self.host, self.port))
				time.sleep(self.timeout_after_failed_to_connect)
				self.s = None
				return

			read_b = self.read_bytes(s=self.s,buffer_size=self.buffer_size)
			if read_b:
				self.b = read_b
			else:
				return

		if not self.marshaller:
			self.marshaller = self.get_marshaller(b=self.b)

		if self.is_header_start(self.b,self.marshaller):
			header_end_pos = self.b.find(self.marshaller.END_OF_HEADER_CHAR)
			if header_end_pos == -1:
				self.b += self.read_bytes(s=self.s,buffer_size=self.buffer_size)
				return
			b_header,self.b = self.cut_bytes(header_end_pos,self.b,len(self.marshaller.END_OF_HEADER_CHAR))
			try:
				self.header = self.marshaller.unmarshall_header(b_header)
			except HeaderException as e:
				self.header = None
				logging.warning("{}: {}".format(self.__name__,str(e)))
			return

		cutting_pos = self.get_newline_cutting_pos(self.b)
		if cutting_pos == -1:
			read_b = self.read_bytes(s=self.s,buffer_size=self.buffer_size)
			if not read_b:
				return
			self.b += read_b
			return

		if isinstance(self.marshaller,BinMarshaller):
			metric_bytes_len = self.header.num_fields() * BinMarshaller.METICS_VALUE_BYTES_LEN  
			if len(self.b) <= cutting_pos + metric_bytes_len:
				read_b = self.read_bytes(s=self.s,buffer_size=self.buffer_size)
				if not read_b:
					self.stop()
					return
				self.b += read_b
				return
			else:
				cutting_pos += metric_bytes_len + 1 # newline after tags byte

		b_metrics,self.b = self.cut_bytes(cutting_pos ,self.b)
		self.into_pipeline(b_metrics=b_metrics, header=self.header, marshaller=self.marshaller)
		return

	def close_connection(self):
		self.s.close()
		self.s = None
		time.sleep(0.1)

	def on_close(self):
		self.s.close()
		super().on_close()

# Listens for incoming data on the specified host:port
class ListenSource():

	def __init__(self,port,pipeline,max_number_of_peers=5,buffer_size=2048):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = pipeline
		self.listensource  = _ListenSource(running=self.running,
											queue=pipeline.queue,
											port=port,
											max_number_of_peers=max_number_of_peers,
											buffer_size=buffer_size)

	def start(self):
		self.listensource.start()

	def stop(self):
		self.running.value = 0
		self.listensource.join()

class _ListenSource(Source):

	def __init__(self,running,port,queue,max_number_of_peers,buffer_size):
		self.marshaller = None
		self.max_number_of_peers = max_number_of_peers
		self.buffer_size = buffer_size
		self.host = "0.0.0.0"
		self.port = port
		self.running = running
		self.server = None
		self.__name__ = "ListenSource"

		try:
			self.server = self.bind_port(self.host,self.port,self.max_number_of_peers)
		except socket.error as se:
			logging.error("{}: Could not bind socket ...".format(self.__name__)) # TODO exit properly here
			logging.error(str(se))
			exit(1)
		self.inputs = [ self.server ]
		self.connections = {}
		super().__init__(queue)

	def close_connection(self,s,connections,inputs):
		logging.info("{}: closing connection to peer {} ...".format(self.__name__,s))
		s.close()
		del connections[s]
		inputs.remove(s)

	def bind_port(self,host,port,max_number_of_peers):
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		server.bind((host, port))
		server.listen(max_number_of_peers)
		return server

	def read_bytes(self,s,buffer_size):
		b = s.recv(buffer_size)
		if b is "":
			return None
		return b

	def loop(self):
		readable, __, exceptional = select.select(
		self.inputs,[],self.inputs,1)
		for s in readable:
			if s is self.server:
				connection, client_address = s.accept()
				connection.setblocking(0)
				self.inputs.append(connection)
				self.connections[connection] = {}
				self.connections[connection]["b"] = b''
				self.connections[connection]["header"] = None
			else:
				read_b = self.read_bytes(s=s,buffer_size=self.buffer_size)

				if read_b:
					self.connections[s]["b"] += read_b
				else:
					self.close_connection(s,self.connections,self.inputs)
					return
				if not self.marshaller:
					self.marshaller = self.get_marshaller(b=self.connections[s]["b"])

				while self.running:
					if self.is_header_start(self.connections[s]["b"],self.marshaller):
						header_end_pos = self.connections[s]["b"].find(self.marshaller.END_OF_HEADER_CHAR)
						if header_end_pos == -1:
							break
						b_header,self.connections[s]["b"] = self.cut_bytes(header_end_pos,self.connections[s]["b"],len(self.marshaller.END_OF_HEADER_CHAR))
						try:
							self.connections[s]["header"] = self.marshaller.unmarshall_header(b_header)
						except HeaderException as e:
							self.header = None
							logging.warning("{}: {}".format(self.__name__,str(e)))
							break

					cutting_pos = self.get_newline_cutting_pos(self.connections[s]["b"])
					if cutting_pos == -1:
						break
					if isinstance(self.marshaller,BinMarshaller):
						metric_bytes_len = self.connections[s]["header"].num_fields() * BinMarshaller.METICS_VALUE_BYTES_LEN
						if len(self.connections[s]["b"]) <= cutting_pos + metric_bytes_len:
							break
						else:
							cutting_pos += metric_bytes_len + 1 # newline after tags byte

					b_metrics,self.connections[s]["b"] = self.cut_bytes(cutting_pos,self.connections[s]["b"])
					self.into_pipeline(b_metrics=b_metrics, header=self.connections[s]["header"], marshaller=self.marshaller)
			return

		for s in exceptional:
			self.close_connection(s,self.connections,self.inputs)

	def on_close(self):
		for i in self.inputs:
			i.close()
		self.server.close()
		self.server = None
		super().on_close()
