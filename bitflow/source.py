import socket
import logging
import time
import select
import ctypes
import multiprocessing 
from bitflow.marshaller import CsvMarshaller

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

	def __init__(self, queue, marshaller):
		self.queue = queue
		self.marshaller = marshaller
		self.header = None
		super().__init__()

	def __str__(self):
		return "Source"

	def into_pipeline(self, line, header):
		try:
			sample = self.marshaller.unmarshall_sample(header, line)
		except:
			logging.info("marshalling of header %s \n and sample %s failed", header, line)
			return
		self.queue.put(sample)

	def run(self):
		while self.running.value:
			self.loop()
		self.on_close()

	def stop(self):
		self.running.value = 0

	def on_close(self):
		logging.info("closing %s ...",str(self))

class FileSource:

	def __init__(self, filename, pipeline, marshaller):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = pipeline
		self.filesource = _FileSource(self.running,filename,pipeline.queue,marshaller)

	def start(self):
		self.filesource.start()
	
	def stop(self):
		self.running.value = 0
		self.filesource.join()
		self.pipeline.stop()

class _FileSource(Source):

	def __init__(self,running,filename,queue,marshaller):
		self.running = running
		self.filename = filename
		self.f = None
		self.header = None
		super().__init__(queue, marshaller)

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
				logging.error("Header line not found, closing file ...")
				self.stop()

		line = ""
		line = self.read_line()
		# EOF
		if line == "":
			self.stop()	
			return 
		self.into_pipeline(line, self.header)

	def read_line(self):
		line = self.f.readline().strip()
		return line

	def on_close(self):
		self.f.close()
		super().on_close()

# Pulls / Downloads incoming data on the specified host:port
class DownloadSource:

	def __init__(self, marshaller, pipeline, host, port, buffer_size=2048):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = pipeline
		self.downloadsource = _DownloadSource(self.running, host, port, pipeline.queue, marshaller, buffer_size)

	def start(self):
		self.downloadsource.start()
	
	def stop(self):
		self.running.value = 0
		self.downloadsource.join()
		self.pipeline.stop()

class _DownloadSource(Source):

	TIMEOUT = 2

	def __init__(self,running,host,port,queue,marshaller,buffer_size=2048,timeout_after_failed_to_connect=0.5):
		self.running = running
		self.host = host
		self.port = port
		self.timeout_after_failed_to_connect = timeout_after_failed_to_connect
		self.s = None
		self.header = None
		self.b = ""
		self.cached_lines = []
		self.buffer_size = buffer_size

		super().__init__(queue, marshaller)

	def __str__(self):
		return "DownloadSource"

	def loop(self):
		if not self.is_connected():
			try:
				self.connect()
			except socket.gaierror as gai:
				logging.warning("Could not connect in DownloadSource to {}:{} ...".format(self.host, self.port))
				self.s.close()
				self.s = None
				time.sleep(self.timeout_after_failed_to_connect)
			except:
				logging.error("unknown socket error in DownloadSource...")
				self.stop()
		
			try:
				self.header, self.b = read_header(
					marshaller=self.marshaller,
					s = self.s,
					buffer_size=self.buffer_size)
			except:
				logging.error("Parsing header failed in DownloadSource ... ")
				self.stop()

		try:
			self.b += self.s.recv(self.buffer_size).decode()
		except ConnectionResetError:
			logging.warning("ConnectionResetError: connection was reset by peer, reconnecting ...")
			self.close_connection()
		except UnicodeError:
			print("Encoding Error in DownloadSource:")
			print(self.s.recv(self.buffer_size))
			

		lines = self.b.split("\n")
		last_element= lines[len(lines)-1]
		if not last_element.endswith("\n"):
			self.b=lines.pop()
		for line in lines:
			self.into_pipeline(line,self.header)	

	def close_connection(self):
		self.s.close()
		self.s = None
		time.sleep(0.1)

	def connect(self):
		logging.info("trying to connect to {}:{} ...".format(self.host,self.port))
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		self.s.connect((self.host, self.port))
		logging.info("connected to {}:{} ...".format(self.host,self.port))

	def is_connected(self):
		if self.s != None:
			return True
		else:
			return False

	def on_close(self):
		self.s.close()
		super().on_close()

# Listens for incoming data on the specified host:port
class ListenSource():

	def __init__(self,marshaller,pipeline,host=None,port=5010,buffer_size=2048):
		self.running = multiprocessing.Value(ctypes.c_int, 1)
		self.pipeline = pipeline
		self.listensource  = _ListenSource(self.running,marshaller,pipeline.queue,host,port,buffer_size)

	def start(self):
		self.listensource.start()
	
	def stop(self):
		self.running.value = 0
		self.listensource.join()
		self.pipeline.stop()

class _ListenSource(Source):

	def __init__(self,running,marshaller,queue,host=None,port=5010,buffer_size=2048):
		self.marshaller = marshaller
		self.buffer_size = buffer_size
		self.host = host
		self.port = port
		self.inputs = []
		self.running = running
		self.server = None
		try:
			self.server = self.bind_port(self.host,self.port)
		except socket.error as se:
			logging.error("Could not bind socket in ListenSource with {}:{}".format(self.host, self.port))
			logging.error(str(se))
			exit(1)
		self.inputs = [self.server]
		
		self.connections = {}
		super().__init__(queue,marshaller)

	def __str__(self):
		return "ListenSource"

	def bind_port(self,host,port):
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		server.setblocking(0)
		server.bind((host, port))
		server.listen(1)
		return server

	def loop(self):
		readable, __, exceptional = select.select(
		self.inputs,[],self.inputs,0)
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
						h,rb = read_header(marshaller=self.marshaller,s=s,buffer_size=self.buffer_size)
						self.connections[s]["header"] = h
						self.connections[s]["remainng_bytes"] = rb
					except:
						continue
					continue
				data = s.recv(self.buffer_size).decode()
				data = self.connections[s]["remainng_bytes"] + data

				lines = data.split("\n")
				last_element= lines[len(lines)-1]
				if not last_element.endswith("\n"):
					self.connections[s]["remainng_bytes"]=lines.pop()
				for line in lines:
					self.into_pipeline(line,self.connections[s]["header"])

		for s in exceptional:
			self.inputs.remove(s)
			s.close()
			del self.connections[s]

	def on_close(self):
		for i in self.inputs:
			i.close()
		self.server.close()
		super().on_close()
