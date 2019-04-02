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

	def into_pipeline(self, marshaller, line, header):
		try:
			sample = marshaller.unmarshall_sample(header, line)
		except:
			logging.info("{}: marshalling of header {} \n and sample {} failed",self.__name__, header, line)
			return
		self.queue.put(sample)

	def run(self):
		while self.running.value:
			self.loop()
		self.on_close()

	def stop(self):
		self.running.value = 0

	def on_close(self):
		logging.info("{}: closing ...".format(self.__name__))

class FileSource:

	def __init__(self, filename, pipeline, marshaller):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = 2
		self.filesource = _FileSource(self.running,filename,pipeline.queue,marshaller)

	def start(self):
		self.filesource.start()
	
	def stop(self):
		self.running.value = 0
		self.filesource.join()

class _FileSource(Source):

	def __init__(self,running,filename,queue,marshaller):
		self.running = running
		self.filename = filename
		self.f = None
		self.header = None
		self.marshaller = marshaller
		self.__name__ = "FileSource"
		super().__init__(queue)

	def __str__(self):
		return "FileSource"

	def open_file(self):
		try:
			self.f = open(self.filename, 'r')
		except FileNotFoundError:
			logging.error("{}: could not open file {} ...".format(str(self), self.filename))
			self.stop()
			exit(1)

	def loop(self):
		if not self.f:
			self.open_file()
			header_line = self.read_line()
			if self.marshaller.is_header(header_line):
				self.header = self.marshaller.unmarshall_header(header_line)
			else:
				logging.error("{}: header line not found, closing file ...".format(self.__name__))
				self.stop()

		line = ""
		line = self.read_line()
		# EOF
		if line == "":
			self.stop()	
			return 
		self.into_pipeline(line=line, header=self.header, marshaller=self.marshaller)

	def read_line(self):
		line = self.f.readline().strip()
		return line

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
		self.b = ""
		self.cached_lines = []
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

	def loop(self):
		if not self.is_connected():
			try:
				self.connect()
			except socket.gaierror as gai:
				logging.warning("{}: Could not connect to {}:{} ...".format(self.__name__,self.host, self.port))
				self.s = None
				time.sleep(self.timeout_after_failed_to_connect)
				return
			except ConnectionRefusedError:
				logging.warning("{}: Connection refused from {}:{} ...".format(self.__name__,self.host, self.port))
				time.sleep(self.timeout_after_failed_to_connect)
				self.s = None
				return

		try:
			self.b += self.s.recv(self.buffer_size).decode()
		except ConnectionResetError:
			logging.warning("{}: ConnectionResetError: connection was reset by peer, reconnecting ...".format(self.__name__))
			self.close_connection()
		except UnicodeError:
			logging.warning("{}: encoding error ...".format(self.__name__))
			logging.warning(self.s.recv(self.buffer_size))
			self.close_connection()
		except socket.timeout:
			return

		lines = self.b.split("\n")
		last_element= lines[len(lines)-1]
		if not last_element.endswith("\n"):
			self.b=lines.pop()
		for line in lines:
			if not self.header: 
				if line.startswith(CSV_HEADER_START_STRING):
					self.marshaller = CsvMarshaller()
					self.header = self.marshaller.unmarshall_header(line)
					logging.info("{}: found header with {} values ...".format(self.__name__,len(self.header.header)))
					continue
				elif line.startswith(BIN_HEADER_START_STRING):
					raise Exception("Binary format not supported yet ...")
				else:
					raise Exception("Unsupported data format, or missing header")

			if line.startswith(self.marshaller.HEADER_START_STRING):
				self.header = self.marshaller.unmarshall_header(line)
				logging.info("{}: header changed, new header has {} values ...".format(self.__name__,len(self.header.header)))
				continue

			self.into_pipeline(line=line,header=self.header,marshaller=self.marshaller)

	def close_connection(self):
		self.s.close()
		self.s = None
		time.sleep(0.1)

	def on_close(self):
		self.s.close()
		super().on_close()

# Listens for incoming data on the specified host:port
class ListenSource():

	def __init__(self,port,marshaller,pipeline,buffer_size=2048):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = pipeline
		self.listensource  = _ListenSource(running=self.running,
											marshaller=marshaller,
											queue=pipeline.queue,
											port=port,
											buffer_size=buffer_size)

	def start(self):
		self.listensource.start()
	
	def stop(self):
		self.running.value = 0
		self.listensource.join()

class _ListenSource(Source):

	def __init__(self,running,port,marshaller,queue,buffer_size=2048):
		self.marshaller = marshaller
		self.buffer_size = buffer_size
		self.host = "0.0.0.0"
		self.port = port
		self.running = running
		self.server = None
		self.__name__ = "ListenSource"

		try:
			self.server = self.bind_port(self.host,self.port)
		except socket.error as se:
			logging.error("{}: Could not bind socket ...".format(self.__name__))
			logging.error(str(se))
			exit(1)
		self.inputs = [self.server]
		self.connections = {}
		super().__init__(queue)

	def close_connection(self,s,connections,inputs):
		logging.info("{}: closing connection to peer {} ...".format(self.__name__,s))
		s.close()
		del connections[s]
		inputs.remove(s)

	def bind_port(self,host,port):
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		server.bind((host, port))
		server.listen(1)
		return server

	def loop(self):
		readable, __, exceptional = select.select(
		self.inputs,[],self.inputs,1)
		for s in readable:
			if s is self.server:
				connection, client_address = s.accept()
				connection.setblocking(0)
				self.inputs.append(connection)
				self.connections[connection] = {}
				self.connections[connection]["remainng_bytes"] = ""
				self.connections[connection]["header"] = None
			else:
				if self.connections[s]["header"] is None:
					try:
						h,rb = read_header(	marshaller=self.marshaller,
											s=s,
											buffer_size=self.buffer_size)
						self.connections[s]["header"] = h
						self.connections[s]["remainng_bytes"] = rb
					except:
						continue
					continue
				data = s.recv(self.buffer_size).decode()
				if data is "":
					self.close_connection(s,self.connections,self.inputs)
					return
				data = self.connections[s]["remainng_bytes"] + data

				lines = data.split("\n")
				last_element= lines[len(lines)-1]
				if not last_element.endswith("\n"):
					self.connections[s]["remainng_bytes"]=lines.pop()
				for line in lines:
					self.into_pipeline(marshaller=self.marshaller,line=line,header=self.connections[s]["header"])

		for s in exceptional:
			self.close_connection(s,self.connections,self.inputs)

	def on_close(self):
		for i in self.inputs:
			i.close()
		self.server.close()
		super().on_close()
