import sys
import socket
import threading
import queue
import logging
import time
import select
from collections import deque
import datetime
#import graphitesend, datetime

from bitflow.processingstep import ProcessingStep, AsyncProcessingStep
from bitflow.marshaller import CsvMarshaller

CSV_FORMAT_IDENTIFIER = "csv"
BIN_FORMAT_IDENTIFIER = "bin"

def header_check(old_header, new_header):
		if old_header == None:
			return True
		if old_header.has_changed(new_header):
			return True
		return False

def get_marshaller(data_format):
	if data_format.lower() == CSV_FORMAT_IDENTIFIER:
		return CsvMarshaller()
	elif data_format.lower() == BIN_FORMAT_IDENTIFIER:
		raise NotImplementedError
	else:
		logging.error("Data format unknown ...")
		sys.exit(1)

###########################
# NETWORK TransportSink #
###########################
class TCPSink(AsyncProcessingStep):

	def __init__(self,
				host : str, 
				port : int, 
				data_format : str = CSV_FORMAT_IDENTIFIER,
				reconnect_timeout : int =2):
	
		super().__init__()
		self.marshaller = get_marshaller(data_format)
		self.__name__ = "TCPSink"
		self.s = None
		self.header = None
		self.wrapper = None
		self.is_running = True
		self.host = host
		self.port = port
		self.reconnect_timeout = reconnect_timeout
		self.que = queue.Queue()
		logging.info("{}: initialized ...".format(self.__name__))

	def connect(self):
		try:
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.s.connect((self.host, self.port))
			self.wrapper = SocketWrapper(self.s)
		except socket.error:
			logging.warning("{}: connection error in with {}:{} ...".format(self.__name__,self.host, self.port))
			sys.exit(1)

	def is_connected(self):
		connected = False
		if self.s:
			connected = True
		return connected

	def run(self):
		while self.is_running:
			self.loop()
		self.on_close()

	def loop(self):
		if not self.is_connected():
			try:
				self.connect()
			except socket.error:
				logging.warning("{}: could not connect to {}:{} ... ".format(self.__name__,self.host,self.port))
				time.sleep(self.reconnect_timeout)
				self.s = None
				return
		if self.que.qsize() is 0:
			try:
				self.que.get(timeout=1)
			except queue.Empty:
				return

		sample = self.que.get()

		try:		
			if header_check(self.header,sample.header):
				self.header = sample.header
				self.marshaller.marshall_header(self.wrapper, self.header)
			self.marshaller.marshall_sample(self.wrapper, sample)
		except socket.error:
			logging.error("{}: failed to send to peer {}:{}, closing connection ...".format(self.__name__,self.host,self.port))
			self.close_connection()
		self.que.task_done()

	def execute(self,sample):
		self.que.put(sample)
		self.write(sample)

	def close_connection(self):
		self.header = None
		if self.s:
			self.s.close()
		if self.wrapper:
			self.wrapper.socket.close()

	def stop(self):
		#self.que.join()
		self.is_running = False

	def on_close(self):
		self.close_connection()
		logging.info("closing {} ...".format(self.__name__))


class SocketWrapper:
	def __init__(self, socket):
		self.socket = socket
	def write(self, data):
		return self.socket.send(bytes(data, "utf-8"))
	def read(self, packet_size):
		return self.socket.recv(packet_size).decode()

####################
# ListenSocketSink #
####################

class ListenSink (AsyncProcessingStep):
	
	def __init__(self,
				host : str = "0.0.0.0",
				port : int = 5010,
				data_format : str = CSV_FORMAT_IDENTIFIER,
				sample_buffer_size : int =-1,
				max_receivers : int =5):

		super().__init__()
		self.__name__ = "ListenSink"
		self.marshaller = get_marshaller(data_format)

		self.host = host
		self.port = port
		self.max_receivers = max_receivers
		self.sample_queues = {}
		if sample_buffer_size is -1:
			self.sample_buffer = deque(maxlen=None)
		else:
			self.sample_buffer = deque(maxlen=sample_buffer_size)
		
		self.is_running = True
		try:
			self.server = self.bind_port(self.host,self.port,self.max_receivers)
		except socket.error as se:
			logging.error("{}: could not bind socket ...".format(self.__name__))
			logging.error(str(se))
			sys.exit(1)

		self.header = None
		self.inputs = [self.server]
		self.outputs = []



	def bind_port(self,host,port,max_receivers):
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		server.setblocking(1)
		server.bind((host, port))
		server.listen(max_receivers)
		logging.info("{}: binding socket on {}:{} ...".format(self.__name__,host,port))
		return server

	def close_connections(self,outputs,sample_queues):
		for s in outputs:
			s.close()
		outputs = []
		sample_queues = {}

	def close_connection(s,outputs,sample_queues):
		logging.info("{}: closing connection to peer {} ...".format(self.__name__,s))
		outputs.remove(s)
		s.close()
		del	sample_queues[s]

	def get_new_queue(self,sample_buffer):
		q = queue.Queue()
		for sample in sample_buffer:
			q.put(sample)
		return q

	def loop(self):
		empty = True
		for k,v in self.sample_queues.items():
				if not v["queue"].empty():
					empty = False

		readable, writable, exceptional = select.select(
		self.inputs, self.outputs, self.inputs,1)
		for s in readable:
			if s is self.server:
				connection, client_address = s.accept()
				connection.setblocking(0)
				self.outputs.append(connection)
				self.sample_queues[connection] = {}
				self.sample_queues[connection]["queue"] = self.get_new_queue(
												sample_buffer=self.sample_buffer)
				self.sample_queues[connection]["header"] = None
				logging.info("{}: new connection established with {} ...".format(self.__name__,client_address))

		for s in writable:
			sw = SocketWrapper(s)
			try:
				if empty:
					sample = self.sample_queues[s]["queue"].get(timeout=1)
					# TODO try to recv and check if "", connection test
				else:
					sample = self.sample_queues[s]["queue"].get_nowait()
			except queue.Empty:
				continue
			try:
				if 	self.sample_queues[s]["header"] is None:
					self.marshaller.marshall_header(sw,sample.header)
					self.sample_queues[s]["header"] = sample.header
				self.marshaller.marshall_sample(sw, sample)
			except socket.error:
				close_connection(s,self.outputs,self.sample_queues)
			self.sample_queues[s]["queue"].task_done()

		for s in exceptional:
			if s is self.server:
				logging.warning("{}: socket error! Reinitiating ...".format(self.__init__))
				self.close_connections(	outputs=self.outputs,
										sample_queues=self.sample_queues)
				self.header = None
				self.server.close()
				time.sleep(1)
				self.server = self.bind_port(self.host,self.port)
			elif s in self.outputs:
				close_connection(s,self.outputs,self.sample_queues)

	def execute(self,sample):
		if self.header is None:
			self.header = sample.header
		elif self.header.has_changed(sample.header):
			self.close_connections(	outputs=self.outputs,
									sample_queues=sample_queues)
			self.header = None

		for k,v in self.sample_queues.items():
			v["queue"].put(sample)
		self.sample_buffer.append(sample)
		self.write(sample)

	def run(self):
		while self.is_running:
			self.loop()
		self.on_close()		


	def stop(self):
		for k,v in self.sample_queues.items():
			v["queue"].join()
		self.is_running = False

	def on_close(self):
		logging.info("{}: closing ...".format(self.__name__))
		self.close_connections(self.outputs,self.sample_queues)
		self.server.close()


##########################
#  FILE TransportSink  #
##########################

class FileSink(AsyncProcessingStep):

	def __init__(self,
				filename : str,
				data_format : str = CSV_FORMAT_IDENTIFIER,
				reopen_timeout : int = 2):
		super().__init__()
		self.__name__ = "FileSink"
		self.marshaller = get_marshaller(data_format)
		self.que = queue.Queue()
		self.filename = filename
		self.f = None
		self.is_running = True
		self.header = None

	def check_file_exists(self,path):
		from pathlib import Path
		my_file = Path(path)
		if my_file.is_file():
			return True
		return False

	def open_file(self):
		suffix = ""
		i = 0
		while self.check_file_exists(path=self.filename+suffix):
			i+=1
			suffix="-{}".format(i)
		try:
			self.f = open(self.filename+suffix, 'w')
		except:
			logging.error("{}: could not open file,  {} ...".format(self.__name__,self.filename))

	def execute(self,sample):
		self.que.put(sample)
		self.write(sample)

	def run(self):
		while self.is_running:
			self.loop()
		self.on_close()

	def loop(self):
		if self.que.qsize() is 0:
			return
		sample = self.que.get()
		if header_check(old_header=self.header,new_header=sample.header):
			self.header = sample.header
			if self.f is not None:
				self.f.close()
			self.open_file()
			self.marshaller.marshall_header(sink=self.f, header=self.header)
		self.marshaller.marshall_sample(sink=self.f, sample=sample)
		self.f.flush()
		self.que.task_done()

	def stop(self):
		self.que.join()
		self.is_running = False

	def on_close(self):
		if self.f is not None:
			self.f.close()
			self.f = None
		logging.info("{}: closing ...".format(self.__name__))

############################
#  STDOUT TransportSink  #
############################

class TerminalOut(ProcessingStep):

	def __init__(self):
		super().__init__()
		self.__name__ = "TerminalOutput"
		self.header_printed = False

	def print_header(self,sample):
		h_str = ""
		for h in sample.header.header:
			h_str += "," + h
		print("time, tags " + h_str)

	def print_metrics(self,sample):
		s_str = ""
		for s in sample.metrics:
				s_str += "," + s
		t_str = ""
		if len(sample.tags.keys()) == 0:
			t_str = ","
		else:
			for k,v in sample.tags.items():
					t_str += ", {}={}".format(k,v)
				
		print("{} {} {}".format(sample.timestamp,t_str,s_str))

	def execute(self,sample):
		if self.header_printed is False:
			self.print_header(sample)
			self.header_printed = True
		self.print_metrics(sample)
		self.write(sample)


	def on_close(self):
		logging.info("{}: closing ...".format(self.__name__))

