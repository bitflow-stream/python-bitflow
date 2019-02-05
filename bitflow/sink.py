import socket, threading, queue, logging, sys, time
import graphitesend, datetime


class Sink(threading.Thread):

	ERRORS_TILL_QUIT = 1
	RECONNECT_TIMEOUT = 1

	marshaller = None

	def __init__(self,que_size=200):
		self.que = queue.Queue(que_size)
		self.header = None
		self.is_running = True
		super().__init__()

	def __str__(self):
		return "SINK"

	def run(self):
		while self.is_running:
			if not self.is_connected():
				logging.warning("trying to connect or open sink destination in %s ...", str(self))
				self.connect()
				time.sleep(self.RECONNECT_TIMEOUT)
				continue
			ok,sample = self.do()
			if not ok:
				self.error_handling(sample)

	def send(self,sample):
		self.que.put(sample)

	def set_marshaller(self, marshaller):
		self.marshaller = marshaller

	def marshall(self, sample):
		return self.marshaller.marshall(sample)

	def do(self):
		try:
			sample = self.que.get(timeout=0.2)
		except:
			return True,""
		if self.header_check(sample.header):
			# TODO HANDLe WRITE HEADER ERRORS
			self.write_header(sample.header)
		ok = self.write_sample(sample)
		self.que.task_done()
		return ok,sample

	def header_check(self,header):
		ok = False
		if self.header == None or self.header.has_changed(header):
			ok = True
			self.header = header
		return ok

	def write_sample(self, sample):
		raise NotImplementedError

	def write_header(self,header):
		raise NotImplementedError

	def is_connected(self):
		raise NotImplementedError

	def connect(self):
		raise NotImplementedError
	
	def error_handling(self,sample):
		logging.warning("error writing sample in %s ...", self.__str__())

	def on_close(self):
		self.que.join()
		self.is_running=False
		logging.info("closing %s ...",str(self))

###########################
# NETWORK TransportSink #
###########################

class SendSink (Sink):

	def __init__(self,marshaller,host,port):
		super().__init__()
		self.s = None
		self.host = host
		self.port = port
		self.set_marshaller(marshaller)

	def __str__(self):
		return "SendSink"

	def connect(self):
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.connect((self.host, self.port))
		self.wrapper = SocketWrapper(self.s)

	def is_connected(self):
		connected = False
		if self.s != None:
			connected = True
		return connected

	def close_connection(self):
		self.header = None
		if self.s is not None:
			self.s.close()
			self.s = None
			self.wrapper = None

	def write_header(self, header):
		try:
			self.marshaller.marshall_header(self.wrapper, header)
		except Exception as failed_to_send1:
			logging.warning("failed to send Header, remote connection closed?")
			self.close_connection()
	
	def write_sample(self, sample):
		ok = True
		try:
			self.marshaller.marshall_sample(self.wrapper, sample)
		except Exception as failed_to_send1:
			logging.warning("failed to send Sample, remote connection closed?")
			self.close_connection()
			ok = False
		return ok

	def on_close(self):
		self.s.close()
		super().on_close()

class NoopSink (Sink):

	def __init__(self):
		super().__init__()

	def __str__(self):
		return "NoopSink"

	def connect(self):
		pass

	def is_connected(self):
		return True

	def close_connection(self):
		pass

	def write_sample(self, sample):
		return True

	def write_header(self, header):
		pass

	def error_handling(self,sample):
		super().error_handling()

	def on_close(self):
		self.close_connection()
		super().on_close()


class SocketWrapper:
	def __init__(self, socket):
		self.socket = socket
	def write(self, data):
		return self.socket.send(bytes(data, "utf-8"))
	def read(self, packet_size):
		return self.socket.recv(packet_size).decode()


class ListenSink (Sink):
	def __init__(self,marshaller,max_receivers=5,host=None,port=5010):
		super().__init__()
		self.set_marshaller(marshaller)
		self.lock = threading.Lock()
		self.ls = ListenSocket(self,host,port,max_receivers)
		self.ls.start()
		self.to_remove = []
		self.to_send_header = []

	def __str__(self):
		return "ListenSink"

	def is_connected(self):
		connected = True
		connections = self.ls.number_of_connections() 
		if connections == 0 or self.ls == None:
			connected = False
		return connected

	def connect(self):
		if self.ls == None:		
			self.ls = ListenSocket(self,host,port,max_receivers)
			self.ls.start()

	def do(self):
		self.lock.acquire()
		ok = super().do()
		self.lock.release()
		return ok

	def header_check(self,header):
		ok = False
		for c in self.ls.connections: 
			if c.header == None or c.header.has_changed(header):
				self.to_send_header.append(c)
				ok = True
		return ok

	def write_header(self, header):
		for c in self.to_send_header:
			self.marshaller.marshall_header(c,header)
			c.header = header
			self.to_send_header.remove(c)

	def write_sample(self, sample):
		ok = True
		for c in list(self.ls.connections):
			try:
				self.marshaller.marshall_sample(c, sample)
			except Exception as failed_to_listen:
				logging.warning("failed to send Sample, remote connection closed?")
				self.to_remove.append(c)
				ok = False
		return ok

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


	def error_handling(self,sample):
		self.remove_connections()
		logging.debug("number of connections %s" , str(len(self.ls.connections)))
		super().error_handling(sample)

	def on_close(self):
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
		#self.s.settimeout(self.listen_timeout)
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

class FileSink (Sink):

	def __init__(self,marshaller,filename):
		super().__init__()
		self.filename = filename
		self.f = None
		self.was_connected_before = False
		self.set_marshaller(marshaller)

	def __str__(self):
		return "FileSink"

	def is_connected(self):
		connected = True
		if self.f == None:
			connected = False
		return connected

	def connect(self):
		if self.was_connected_before:
			self.f = open(self.filename, 'a')
		else:
			self.f = open(self.filename, 'w+')
			self.was_connected_before = True


	def on_close(self):
		if self.f is not None:
			self.f.close()
			self.f = None
		super().on_close()

	def write_header(self, header):
		self.marshaller.marshall_header(self.f, header)

	def error_handling(self,sample):
		logging.warning("Cloud not write line properly ...")
		logging.warning("Sample: {}".format(sample))

	def write_sample(self, sample):
		ok = True
		try:
			self.marshaller.marshall_sample(self.f, sample)
			self.f.flush()
		except:
			ok = False
		return ok

############################
#  STDOUT TransportSink  #
############################

class StdSink (Sink):
	init = False

	def __init__(self,marshaller):
		super().__init__()
		self.set_marshaller(marshaller)

	def __str__(self):
		return "StdSink"

	def is_connected(self):
		return True

	def connect(self):
		pass

	def write_header(self, header):
		self.marshaller.marshall_header(self, header)

	def write_sample(self, sample):
		ok = True
		self.marshaller.marshall_sample(self, sample)
		return ok


	def error_handling(self,sample):
		super().error_handling()

	def write(self,data):
		sys.stdout.write(data)
		sys.stdout.flush()

	def on_close(self):
		super().on_close()

#####################
#  Graphitesend
#####################

class GraphiteSink (Sink):

	def __init__(self,marshaller,host,port,prefix=None):
		super().__init__()

		self.s = None
		if prefix is None:
			now = datetime.datetime.now()
			self.prefix = now
		else:
			self.prefix= prefix
		self.host = host
		self.port = port
		self.set_marshaller(marshaller)

	def __str__(self):
		return "GraphiteSink"

	def connect(self):
		self.s = graphitesend.init(
        	graphite_server=self.host,
        	graphite_port=self.port,
        	prefix=self.prefix,
        	timeout_in_seconds=5,
        	system_name='python-bitflow'
        	)

	def is_connected(self):
		connected = False
		if self.s != None:
			connected = True
		return connected

	def close_connection(self):
		self.header = None
		if self.s is not None:
			#self.s.close()
			self.s = None

	def write(self,data):
		self.s.send_list(data)

	def write_sample(self, sample):
		ok = True
		try:
			data = []
			timestamp = sample.get_timestamp()
			for feature in sample.header.header:
				data.append([feature,sample.get_metricvalue_by_name(feature),timestamp])
			self.write(data)
			#self.marshaller.marshall_sample(self, sample)
		except Exception as failed_to_send1:
			logging.warning("failed to send Sample, remote connection closed?")
			self.close_connection()
			ok = False
		return ok

	def write_header(self, header):
		self.marshaller.marshall_header(self,header)

	def error_handling(self,sample):
		super().error_handling()

	def on_close(self):
		self.close_connection()
		super().on_close()