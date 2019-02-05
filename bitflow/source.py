import datetime, socket, threading, queue, logging, os, time 
from bitflow.sample import Sample,Header
from bitflow.marshaller import CsvMarshaller
from bitflow.sink import ListenSocket

class NewSource(threading.Thread):
	marshaller = None

	def __init__(self,pipeline, marshaller):
		self.pipeline = pipeline
		self.marshaller = marshaller
		self.running = True
		super().__init__(name=str(self))

	def __str__(self):
		return "Source"

	def check_header(self,line):
		if self.marshaller.is_header(line):
			self.header = self.marshaller.unmarshall_header(line)
			return True
		return False

	def into_pipeline(self,line):
		try:
			sample = self.marshaller.unmarshall_sample(self.header,line)
		except:
			logging.info("marshalling of header %s \n and sample %s failed", self.header , line)
			return
		self.pipeline.execute_sample(sample)

	def run(self):
		while self.running:
			self.loop()
		self.on_close()

	def stop(self):
		self.running = False

	def on_close(self):
		logging.debug("closing %s ...",str(self))
		self.pipeline.on_close()

class Source(threading.Thread):
	''' old style source class (depecated Soon) '''
	RECONNECT_TIMEOUT = 1
	ERRORS_TILL_QUIT = 1
	marshaller = None

	def __init__(self,pipeline, marshaller):
		self.pipeline = pipeline
		self.marshaller = marshaller
		self.running = True
		super().__init__(name=str(self))

	def __str__(self):
		return "Source"

	def check_header(self,line):
		if self.marshaller.is_header(line):
			self.header = self.marshaller.unmarshall_header(line)
			return True
		return False

	def into_pipeline(self,line):
		sample = self.marshaller.unmarshall_sample(self.header,line)
		self.pipeline.execute_sample(sample)

	def run(self):
		while self.running:
			if not self.is_connected():
				logging.warning("trying to connect or open source: %s ...",str(self))
				self.connect()
				if not self.is_connected():
					time.sleep(self.RECONNECT_TIMEOUT)
				continue

			ok,line = self.read_line()
			if ok:
				if self.check_header(line):
					continue
				try:
					self.into_pipeline(line)
				except Exception as strangeSource:
					logging.info("marshalling of header %s \n and sample %s failed", self.header , line)
			else:
				self.error_handling(line)
		self.on_close()
		

	def read_line(self):
		"""reads Sample from source as text line.
		:returns success,line.
		"""
		raise NotImplementedError

	def error_handling(self,line):
		logging.warning("error reading sample in %s ...", self.__str__())

	def is_connected(self):
		raise NotImplementedError

	def connect(self):
		raise NotImplementedError

	
	def on_close(self):
		logging.debug("closing %s ...",str(self))
		self.running=False
		self.pipeline.on_close()


class GenerateMetricsSource(Source):
	''' used for seperatly implemented sources (deprecated soon) '''

	def into_pipeline(self,line):
		timestamp = datetime.datetime.now()
		sample = Sample(self.header,line,timestamp)
		self.pipeline.execute_sample(sample)

	def run(self):	
		while self.running:
			ok,line = self.read_line()
			if ok:
				try:
					self.into_pipeline(line)
				except Exception as strangeSource:
					logging.debug("marshalling of header %s \n and sample %s failed", self.header , line)
			else:
				self.error_handling(line)
		logging.info("closing %s ..." , str(self))	

class FileSource(NewSource):

	def __init__(self,filename,pipeline, marshaller):
		self.filename = filename
		self.f = None
		self.header = None
		super().__init__(pipeline,CsvMarshaller())

	def __str__(self):
		return "FileSource"

	def open_file(self):
		try:
			self.f = open(self.filename, 'r')
		except FileNotFoundError:
			logging.error("{}: could not open file {} ...".format(str(self),self.filename))
			self.running = False
			exit(1)

	def loop(self):
		if not self.f:
			self.open_file()
		line = ""
		line = self.read_line()
		# EOF
		if line == "":
			self.running = False
			return 
		if self.check_header(line):
			return
		self.into_pipeline(line)	

	def read_line(self):
		line = self.f.readline().strip()
		return line

	def on_close(self):
		self.f.close()
		time.sleep(0.1)
		super().on_close()

class DownloadSource(NewSource):

	TIMEOUT = 5
	PACKET_SIZE = 4096
	TIMEOUT_AFTER_FAILED_TO_CONNECCT = 1

	def __init__(self,host,port,pipeline, marshaller):
		self.host = host
		self.port = port
		self.s = None
		self.header = None

		self.cached_lines = []
		super().__init__(pipeline,CsvMarshaller())

	def __str__(self):
		return "DownloadSource"

	def loop(self):
		if not self.is_connected():
			self.connect()
		line=""
		try:
			line = self.read_line()
		except socket.timeout as se:
			logging.info("lost connection try to reconnect ...")
		if line == None:
			return
		if self.check_header(line):
			return
		self.into_pipeline(line)
	
	
	def connect(self):
		logging.info("trying to connect to {}:{} ...".format(self.host,self.port))
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.settimeout(self.TIMEOUT)
		try:
			self.s.connect((self.host, self.port))
		except:
			logging.info("Could not connect to {}:{} ...".format(self.host,self.port))
			self.s.close()
			self.s = None
			time.sleep(self.TIMEOUT_AFTER_FAILED_TO_CONNECCT)

	def is_connected(self):
		if self.s != None:
			return True
		else:
			return False

	def read_line(self):
		if len(self.cached_lines) > 0:
			line = self.cached_lines.pop(0)
			return line
		data = ""
		while True:
			data += self.s.recv(self.PACKET_SIZE).decode()
			if not data:
				return None
			if not data.endswith("\n"):
				continue
			lines = data.splitlines()
			break	
		self.cached_lines = lines        	
		line = self.cached_lines.pop(0)
		return line

	def on_close(self):
		self.s.close()
		super().on_close()

class ListenSource(NewSource):

	PACKET_SIZE = 1024

	def __init__(self,marshaller,pipeline,host=None,port=5010, socket_timeout=1):
		self.marshaller = marshaller
		self.host = host
		self.port = port
		self.lock = threading.Lock()
		self.connection = None
		
		self.ls = ListenSocket(self,self.host,self.port)
		self.ls.start()	
		self.cached_lines = []	
		self.header = None
		self.error = 0
		super().__init__(pipeline,CsvMarshaller())

	def __str__(self):
		return "ListenSource"

	def remove_connection(self,connections,c): 		
		self.lock.acquire() 		
		try: 			
			connections.remove(c) 		
		finally: 			
			self.lock.release()	

	def connect(self):
		pass

	def is_connected(self):
		connected = True
		connections = self.ls.number_of_connections() 
		if connections != 1 or self.ls == None:
			connected = False
		return connected


	def loop(self):
			if not self.is_connected():
				logging.debug("{}: waiting for peerz to connect ...".format(str(self)))
				return
			try:
				line = self.read_line()
			except socket.error: 
				logging.warning("{}: lost connection ...".format(str(self)))
			if line == "": # no data on socket
				return
			if self.check_header(line):
				return
			self.into_pipeline(line)


	def read_line(self):
		self.lock.acquire()
		self.connection = self.ls.get_connections()[0]
		self.lock.release()	
		if len(self.cached_lines) > 0:
			line = self.cached_lines.pop(0)
			return line
		
		data = ""
		while True:
			data += self.connection.read(self.PACKET_SIZE)
			if not data:
				return ""
			if not data.endswith("\n"):
				continue
			lines = data.splitlines()
			break		
		self.cached_lines = lines        	
		line = self.cached_lines.pop(0)
		return line

	def on_close(self):
		self.ls.on_close()
		super().on_close()
