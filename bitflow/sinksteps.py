import socket, threading, queue, logging, sys, time
from collections import deque
import datetime
#import graphitesend, datetime

from bitflow.processingstep import ProcessingStep

def header_check(old_header, new_header):
		if old_header == None:
			return True
		if old_header.has_changed(new_header):
			return True
		return False


class AsyncProcessingStep(ProcessingStep,threading.Thread):

	def __init__(self):
		ProcessingStep.__init__(self)
		threading.Thread.__init__(self)

###########################
# NETWORK TransportSink #
###########################
class TCPSink(AsyncProcessingStep):

	def __init__(self,marshaller,host,port,reconnect_timeout=2,que_size=500):
		super().__init__()

		self.__name__ = "TCPSink"
		self.s = None
		self.header = None
		self.is_running = True
		self.host = host
		self.port = port
		self.reconnect_timeout = reconnect_timeout
		self.que = queue.Queue(que_size)
		self.marshaller = marshaller
		logging.info("{}: initialized ...".format(self.__name__))

	def connect(self):
		try:
			self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.s.connect((self.host, self.port))
			self.wrapper = SocketWrapper(self.s)
		except self.s.error as error:
			print("Connection Error in TCPSink with {}:{}".format(self.host, self.port))
			print(os.strerror(error.errno))

	def is_connected(self):
		connected = False
		if self.s != None:
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
				return
		if self.que.qsize() is 0:
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
		return sample

	def close_connection(self):
		self.header = None
		self.s.close()
		self.wrapper.socket.close()

	def stop(self):
		self.que.join()
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

class ListenSink (ProcessingStep,threading.Thread):
	def __init__(self,marshaller,max_receivers=5,host=None,port=5010,que_size=5):
		super().__init__()
		logging.error("ListenSink: NOT USAble ATM")
		exit(1)
		self.marshaller = marshaller
		self.lock = threading.Lock()
		self.max_receivers = max_receivers
		self.ls = ListenSocket(self,host,port,max_receivers)
		self.que = deque()
		self.to_remove = []
		self.to_send_header = []

	def check_socket_open(self):
		connections = self.ls.number_of_connections() 
		if connections == 0 or self.ls == None:
			return False
		return True

	def open_socket(self):
		if self.ls == None:		
			self.ls = ListenSocket(self,host,port,self.max_receivers)
			self.ls.start()

	def run(self):
		while self.is_running:
			self.loop()
		self.on_close()		

	def exectute(self,sample):
		self.que.append(sample)
		return sample

	def loop(self):
		sample = self.que.popleft()
		if not self.check_socket_open():
			try:
				self.open_socket()
			except socket.error:
				logging.warning("{}: could not open socket on {}:{} ... ".format(self.__name__,self.host,self.port))
				sys.exit(1)
				return
		self.lock.acquire()
		for c in self.ls.connections: 
			try:
				if self.header_check(sample.header,c):
					self.marshaller.marshall_header(c,sample.header)
				self.marshaller.marshall_sample(c, sample)
			except socket.error:
				self.to_remove.append(c)
				logging.error("{}: Error in connection with peer {}...".format(self.__name__,c.addr))
				continue
		self.lock.release()
		self.remove_connections()
		return 

	def remove_connections(self):
		self.lock.acquire()
		for conn in self.ls.connections:
			if conn in self.to_remove:
				try:
					self.ls.connections.remove(conn)
				except:
					logging.debug("could not remove connection %s", str(c))
				self.to_remove.remove(conn)
		self.lock.release()	

	def on_close(self):
		logging.info("closing {} ...".format(self.__name__))

		self.running = False
		for c in list(self.ls.connections):
			self.to_remove.append(c)
		self.remove_connections()
		self.ls.on_close()
		self.ls = None
		super().on_close()

class ListenSocket(threading.Thread):
	TIMEOUT = 10
	def __init__(self,tcplistener,listen_host,listen_port,max_number_of_connections=10,listen_timeout=0.5):
		super().__init__()
		self.tcplistener = tcplistener
		self.max_number_of_connections = max_number_of_connections
		self.listen_timeout = listen_timeout
		self.s = None
		self.is_running = True
		if listen_host == None:
			self.listen_host = "0.0.0.0"
		else:
			self.listen_host = listen_host
		try:
			self.listen_port = int(listen_port)
		except Exception as e:
			logging.error("port parameter is somehow wrong, stopping listen for connection on port" + str(e))
			os._exit(1)
		tcplistener.lock.acquire()
		try:
			self.connections = []
		finally:
			tcplistener.lock.release()		

	def number_of_connections(self):		
		return len(self.connections)

	def get_connections(self):
		return self.connections

	def run(self):
		logging.info("Binding %s:%s" % (self.listen_host, self.listen_port))
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.s.bind((self.listen_host, self.listen_port))
		self.s.settimeout(self.listen_timeout)
		self.s.listen(self.max_number_of_connections)
		while self.is_running:
			try:
				c, addr = self.s.accept()
			except socket.timeout:
				continue
			self.tcplistener.lock.acquire()
			try:
				c = ListenSocketConnection(c,addr)
				self.connections.append(c)
			finally:
				self.tcplistener.lock.release()
			
	def on_close(self):
		for conn in self.connections:
			conn.on_close()
		self.is_running = False
		if self.s is not None:
			self.s.close()
			self.s = None

class ListenSocketConnection():

	header = None

	def __init__(self,socket,addr):
		self.socket = socket
		self.addr = addr

	def write(self, data):
		return self.socket.send(bytes(data, "utf-8"))

	def read(self, packet_size):
		return self.socket.recv(packet_size).decode()		

	def on_close(self):
		self.socket.close()

##########################
#  FILE TransportSink  #
##########################

class FileSink(ProcessingStep):

	def __init__(self,marshaller,filename,reopen_timeout=2):
		self.__name__ = "FileSink"
		super().__init__()
		self.filename = filename
		self.f = None
		self.marshaller = marshaller
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
		if header_check(old_header=self.header,new_header=sample.header):
			self.header = sample.header
			if self.f is not None:
				self.f.close()
			self.open_file()
			self.marshaller.marshall_header(sink=self.f, header=self.header)
		self.marshaller.marshall_sample(sink=self.f, sample=sample)
		self.f.flush()
		return sample

	def stop(self):
		self.is_running = False

	def on_close(self):
		if self.f is not None:
			self.f.close()
			self.f = None
		logging.info("closing {} ...".format(self.__name__))


############################
#  STDOUT TransportSink  #
############################

class TerminalOut(ProcessingStep):

	def __init__(self):
		self.__name__ = "TerminalOutput"
		self.header_printed = False
		super().__init__()

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
		for k,v in sample.tags.items():
				t_str += ", {}={}".format(k,v)
				
		print("{} {} {}".format(sample.timestamp,t_str,s_str))

	def execute(self,sample):
		if self.header_printed is False:
			self.print_header(sample)
			self.header_printed = True
		self.print_metrics(sample)
		return sample


	def on_close(self):
		logging.info("closing {} ...".format(self.__name__))


#####################
#  Graphitesend
#####################

class GraphiteOut (ProcessingStep):

	def __init__(self,marshaller,host,port,prefix=None,que_size=5):
		super().__init__()
		logging.error("GraphiteOut: NOT USAble ATM")
		exit(1)
		self.que.deque(maxsize=que_size)
		self.s = None
		if prefix is None:
			now = datetime.datetime.now()
			self.prefix = now
		else:
			self.prefix= prefix
		self.host = host
		self.port = port
		self.marshaller = marshaller
		self.is_running = True 
		self.header = None

	def connect(self):
		self.s = graphitesend.init(
        	graphite_server=self.host,
        	graphite_port=self.port,
        	prefix=self.prefix,
        	timeout_in_seconds=5,
        	system_name='python-bitflow'
        	)

	def run(self):
		while self.is_running:
			self.loop()
		self.on_close()

	def is_connected(self):
		if self.s != None:
			return True
		return False

	def close_connection(self):
		self.header = None
		if self.s is not None:
			self.s.close()
			self.s = None

	def loop(self):
		if not curr_sample:
			self.curr_sample = self.que.popleft()
		if not self.is_connected():
			try:
				self.connect()
			except socket.error:
				logging.warning("{}: could not connect to {}:{} ... ".format(self.__name__,self.host,self.port))
				time.sleep()
				task_done()
				return
			data = []
			timestamp = sample.get_timestamp()
			for feature in sample.header.header:
				data.append([feature,sample.get_metricvalue_by_name(feature),timestamp])
		try:		
			self.s.send_list(data)
		except socket.error:
			logging.error("{}: failed to send to peer {}:{}, closing connection ...".format(self.__name__,self.host,self.port))
			self.close_connection()
			return 
		self.que.task_done()
		self.curr_sample = None
		return 

	def exectute(self,sample):
		self.que.append(sample)
		return sample

	def stop(self):
		self.is_running = False

	def on_close(self):
		self.is_running = False
		self.close_connection()
		super().on_close()
