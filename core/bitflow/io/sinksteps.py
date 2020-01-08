import queue as thread_queue
import select
import socket
import sys
from collections import deque

from bitflow.io.marshaller import *
from bitflow.processingstep import *
from bitflow.processingstep import _ProcessingStepAsync

NO_INPUT_TIMEOUT = 0.1
SOCKET_ERROR_TIMEOUT = 0.5


def header_check(old_header, new_header):
    if old_header is None:
        return True
    if old_header.has_changed(new_header):
        return True
    return False


###########################
# NETWORK TransportSink #
###########################

class SocketWrapper:
    def __init__(self, sock):
        self.socket = sock

    def write(self, sample):
        try:
            self.socket.sendall(sample)
        except socket.error as e:
            logging.warning("Could not transmit sample (len %s)", len(str(sample)), exc_info=e)
            raise e

    def read(self, packet_size):
        return self.socket.recv(packet_size)


class TCPSink(AsyncProcessingStep):

    def __init__(self, host: str, port: int, data_format: str = CSV_DATA_FORMAT, reconnect_timeout: int = 2,
                 maxsize: int = DEFAULT_QUEUE_MAXSIZE, parallel_mode: str = PARALLEL_MODE_THREAD):
        if not parallel_mode:
            raise ValueError("%s: Sequential mode is not supported. Define one of the following parallel modes: $s",
                             self.__name__, ",".join(PARALLEL_MODES))
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "TCPSink"
        self.host = host
        self.port = port
        self.reconnect_timeout = reconnect_timeout
        self.data_format = data_format

    def init_async_object(self, parallel_utils, parallel_mode):
        parallel_step = None
        if parallel_mode == PARALLEL_MODE_THREAD:
            parallel_step = _TCPSinkThread(self.host, self.port, self.data_format, self.reconnect_timeout,
                                           parallel_utils)
        elif parallel_mode == PARALLEL_MODE_PROCESS:
            parallel_step = _TCPSinkProcess(self.host, self.port, self.data_format, self.reconnect_timeout,
                                            parallel_utils)
        return parallel_step


class _TCPSinkAsync(_ProcessingStepAsync):
    def __init__(self, host, port, data_format, reconnect_timeout, parallel_utils):
        super().__init__(parallel_utils)
        self.__name__ = "TCPSink_Async"
        self.marshaller = get_marshaller_by_data_format(data_format)
        self.s = None
        self.header = None
        self.wrapper = None
        self.host = host
        self.port = port
        self.reconnect_timeout = reconnect_timeout
        self.counter = 0

    def connect(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((self.host, self.port))
        self.wrapper = SocketWrapper(self.s)

    def is_connected(self):
        connected = False
        if self.s:
            connected = True
        return connected

    def loop(self, sample):
        if not self.is_connected():
            try:
                self.connect()
            except socket.error:
                logging.warning("%s: could not connect to %s:%s ... ", self.__name__, self.host, self.port)
                time.sleep(self.reconnect_timeout)
                self.s = None

        if self.is_connected():
            try:
                if header_check(self.header, sample.header):
                    self.header = sample.header
                    self.marshaller.marshall_header(self.wrapper, self.header)
                self.marshaller.marshall_sample(self.wrapper, sample)
            except socket.error:
                logging.error("%s: failed to send to peer %s:%s, closing connection ...",
                              self.__name__, self.host, self.port)
                self.close_connection()
        return sample

    def close_connection(self):
        self.header = None
        if self.s:
            self.s.close()
            self.s = None
        if self.wrapper:
            self.wrapper.socket.close()
            self.wrapper = None

    def on_close(self):
        self.close_connection()
        super().on_close()


class _TCPSinkThread(threading.Thread, _TCPSinkAsync):

    def __init__(self, host: str, port: int, data_format: str, reconnect_timeout: int, maxsize: int):
        threading.Thread.__init__(self)
        _TCPSinkAsync.__init__(self, host, port, data_format, reconnect_timeout, maxsize)
        self.__name__ = "TCPSink_Thread"

    def run(self):
        _TCPSinkAsync.run(self)


class _TCPSinkProcess(multiprocessing.Process, _TCPSinkAsync):

    def __init__(self, host: str, port: int, data_format: str, reconnect_timeout: int, maxsize: int):
        multiprocessing.Process.__init__(self)
        _TCPSinkAsync.__init__(self, host, port, data_format, reconnect_timeout, maxsize)
        self.__name__ = "TCPSink_Process"

    def run(self):
        _TCPSinkAsync.run(self)


####################
# ListenSocketSink #
####################

class ListenSink(AsyncProcessingStep):

    def __init__(self, host: str = "0.0.0.0", port: int = 5010, data_format: str = CSV_DATA_FORMAT,
                 sample_buffer_size: int = -1, max_receivers: int = 10, retry_on_close: bool = False,
                 maxsize: int = DEFAULT_QUEUE_MAXSIZE, parallel_mode: str = PARALLEL_MODE_THREAD):
        if not parallel_mode:
            raise ValueError("%s: Sequential mode is not supported. Define one of the following parallel modes: $s",
                             self.__name__, ",".join(PARALLEL_MODES))
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "ListenSink"
        self.host = host
        self.retry_on_close = retry_on_close
        self.max_receivers = max_receivers
        self.sample_buffer_size = sample_buffer_size
        self.data_format = data_format
        self.port = port

    def init_async_object(self, parallel_utils, parallel_mode):
        parallel_step = None
        if parallel_mode == PARALLEL_MODE_THREAD:
            parallel_step = _ListenSinkThread(self.host, self.port, self.data_format, self.sample_buffer_size,
                                              self.max_receivers, self.retry_on_close, parallel_utils)
        elif parallel_mode == PARALLEL_MODE_PROCESS:
            parallel_step = _ListenSinkProcess(self.host, self.port, self.data_format, self.sample_buffer_size,
                                               self.max_receivers, self.retry_on_close, parallel_utils)
        return parallel_step


class _ListenSinkAsync(_ProcessingStepAsync):

    def __init__(self, host, port, data_format, sample_buffer_size, max_receivers, retry_on_close, parallel_utils):
        super().__init__(parallel_utils)
        self.__name__ = "ListenSink_Async"
        self.marshaller = get_marshaller_by_data_format(data_format)
        self.host = host
        self.port = port
        self.max_receivers = max_receivers
        self.sample_buffer_size = sample_buffer_size
        self.sample_buffer = None
        self.sample_queues = {}
        self.server = None
        self.inputs = []
        self.outputs = []
        self.lock = threading.Lock()
        self.retry_on_close = retry_on_close

    def on_start(self):
        if self.sample_buffer_size is -1:
            self.sample_buffer = deque(maxlen=None)
        else:
            self.sample_buffer = deque(maxlen=self.sample_buffer_size)
        try:
            self.server = self.bind_socket(self.host, self.port, self.max_receivers)
        except socket.error as se:
            logging.error("{}: could not bind socket ...".format(self.__name__))
            logging.error(str(se))
            sys.exit(1)
        self.inputs.append(self.server)

    def bind_socket(self, host, port, max_receivers):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(10)
        server.bind((host, port))
        server.listen(max_receivers)
        logging.info("{}: binding socket on {}:{} ...".format(self.__name__, host, port))
        return server

    def close_connections(self, outputs):
        # TODO log contained samples in queues to see whether there are dropped samples
        for s in outputs:
            s.close()
        self.outputs = []
        self.sample_queues = {}

    def close_connection(self, s):
        if s and s in self.outputs:
            logging.info("{}: closing connection to peer {} ...".format(self.__name__, s))
            self.outputs.remove(s)
            s.close()
            del self.sample_queues[s]

    def get_new_queue(self, sample_buffer):
        q = thread_queue.Queue()
        for sample in sample_buffer:
            q.put(sample)
        return q

    ''' accept new connection from peer '''

    def initialize_connection(self, s):
        connection, client_address = s.accept()
        connection.setblocking(0)
        self.outputs.append(connection)
        self.lock.acquire()
        self.sample_queues[connection] = {}
        self.sample_queues[connection]["queue"] = self.get_new_queue(sample_buffer=self.sample_buffer)
        self.sample_queues[connection]["header"] = None
        self.lock.release()
        logging.info("{}: new connection established with {} ...".format(self.__name__, client_address))

    ''' checks if there are samples to send to any of the connected peers '''

    def has_to_send(self):
        for k, v in self.sample_queues.items():
            if v["queue"].qsize() > 0:
                return True
        return False

    def offer_sample(self, sample, sock):
        if sample:
            try:
                # if no header send yet or header  has changed -> marshall header
                if self.sample_queues[sock.socket]["header"] is None or \
                        self.sample_queues[sock.socket]["header"].has_changed(sample.header):
                    self.marshaller.marshall_header(sock, sample.header)
                    self.sample_queues[sock.socket]["header"] = sample.header
                self.marshaller.marshall_sample(sock, sample)
            except socket.error as e:
                return sock.socket
        return None

    def rebind_socket(self, s):
        if s and s is self.server:
            logging.warning(
                "{}: Unexpected socket error occured. Trying to rebind socket ...".format(self.__name__))
            self.close_connections(outputs=self.outputs)
            self.server.close()
            time.sleep(SOCKET_ERROR_TIMEOUT)
            self.server = self.bind_socket(self.host, self.port, self.max_receivers)
        elif s and s in self.outputs:
            logging.warning("{}: Unexpected socket error occured ...")
            self.close_connection(s)

    def loop(self, sample):
        if sample:
            for k, v in self.sample_queues.items():
                v["queue"].put(sample)
            self.sample_buffer.append(sample)

            readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs, 1)
            for s in readable:
                if s is self.server:
                    self.initialize_connection(s)
            if not self.has_to_send():
                time.sleep(NO_INPUT_TIMEOUT)
                return sample

            for s in writable:
                try:
                    socket_sample = self.sample_queues[s]["queue"].get_nowait()
                except thread_queue.Empty:
                    continue
                e = self.offer_sample(socket_sample, SocketWrapper(s))
                if e:
                    exceptional.append(e)
                self.sample_queues[s]["queue"].task_done()

            for s in exceptional:
                if self.retry_on_close:
                    self.rebind_socket(s)
                else:
                    self.close_connection(s)

        return sample

    def clear_queues(self):
        readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs, 1)
        for s in writable:
            while True:
                try:
                    socket_sample = self.sample_queues[s]["queue"].get_nowait()
                except thread_queue.Empty:
                    self.close_connection(s)
                    break
                e = self.offer_sample(socket_sample, SocketWrapper(s))
                self.sample_queues[s]["queue"].task_done()
                if e:
                    self.close_connection(s)
                    break

    def on_close(self):
        self.clear_queues()
        for k, v in self.sample_queues.items():
            v["queue"].join()
        self.close_connections(self.outputs)
        self.server.close()
        super().on_close()


class _ListenSinkThread(threading.Thread, _ListenSinkAsync):

    def __init__(self, host: str, port: int, data_format: str, sample_buffer_size: int, max_receivers: int,
                 retry_on_close: bool, maxsize: int):
        threading.Thread.__init__(self)
        _ListenSinkAsync.__init__(self, host, port, data_format, sample_buffer_size, max_receivers,
                                  retry_on_close, maxsize)
        self.__name__ = "ListenSink_Thread"

    def run(self):
        _ListenSinkAsync.run(self)


class _ListenSinkProcess(multiprocessing.Process, _ListenSinkAsync):

    def __init__(self, host: str, port: int, data_format: str, sample_buffer_size: int, max_receivers: int,
                 retry_on_close: bool, maxsize: int):
        multiprocessing.Process.__init__(self)
        _ListenSinkAsync.__init__(self, host, port, data_format, sample_buffer_size, max_receivers,
                                  retry_on_close, maxsize)
        self.__name__ = "ListenSink_Process"

    def run(self):
        _ListenSinkAsync.run(self)


##########################
#  FILE TransportSink  #
##########################
def check_file_exists(path):
    from pathlib import Path
    my_file = Path(path)
    if my_file.is_file():
        return True
    return False


def get_filepath(filename):
    i = 0
    numbering = ""
    file_ending = ""
    last_dot_pos = filename.rfind(".")

    if last_dot_pos == -1:
        base_filename = filename
    else:
        base_filename = filename[0:last_dot_pos]
        file_ending = filename[last_dot_pos:len(filename)]

    while check_file_exists(path=base_filename + numbering + file_ending):
        i += 1
        numbering = "-{}".format(i)
    return base_filename + numbering + file_ending


class FileSink(AsyncProcessingStep):

    def __init__(self, filename: str, data_format: str = CSV_DATA_FORMAT, maxsize: int = DEFAULT_QUEUE_MAXSIZE,
                 parallel_mode: str = PARALLEL_MODE_THREAD):
        if not parallel_mode:
            raise ValueError("%s: Sequential mode is not supported. Define one of the following parallel modes: $s",
                             self.__name__, ",".join(PARALLEL_MODES))
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "FileSink"
        self.filename = filename
        self.data_format = data_format

    def init_async_object(self, parallel_utils, parallel_mode):
        parallel_step = None
        if parallel_mode == PARALLEL_MODE_THREAD:
            parallel_step = _FileSinkThread(self.filename, self.data_format, parallel_utils)
        elif parallel_mode == PARALLEL_MODE_PROCESS:
            parallel_step = _FileSinkProcess(self.filename, self.data_format, parallel_utils)
        return parallel_step


class _FileSinkAsync(_ProcessingStepAsync):

    def __init__(self, filename, data_format, parallel_utils):
        super().__init__(parallel_utils)
        self.__name__ = "FileSink_Async"
        self.marshaller = get_marshaller_by_data_format(data_format)
        self.filename = filename
        self.f = None
        self.header = None

    def open_file(self, filename):
        final_filename = get_filepath(filename)
        self.f = open(final_filename, 'bw')
        return final_filename

    def loop(self, sample):
        if sample:
            if header_check(old_header=self.header, new_header=sample.header):
                self.header = sample.header
                if self.f and isinstance(self.marshaller, CsvMarshaller):
                    self.f.close()
                    new_filename = self.open_file(self.filename)
                    logging.info("header changed, opening new file {} ...".format(new_filename))
                elif not self.f:
                    new_filename = self.open_file(self.filename)
                    logging.info("Opening new file {} ...".format(new_filename))
                self.marshaller.marshall_header(sink=self.f, header=self.header)
            self.marshaller.marshall_sample(sink=self.f, sample=sample)
            self.f.flush()
        return sample

    def on_close(self):
        if self.f is not None:
            self.f.close()
            self.f = None
        super().on_close()


class _FileSinkThread(threading.Thread, _FileSinkAsync):

    def __init__(self, filename: str, data_format: str, maxsize: int):
        threading.Thread.__init__(self)
        _FileSinkAsync.__init__(self, filename, data_format, maxsize)
        self.__name__ = "FileSink_Thread"

    def run(self):
        _FileSinkAsync.run(self)


class _FileSinkProcess(multiprocessing.Process, _FileSinkAsync):

    def __init__(self, filename: str, data_format: str, maxsize: int):
        multiprocessing.Process.__init__(self)
        _FileSinkAsync.__init__(self, filename, data_format, maxsize)
        self.__name__ = "FileSink_Process"

    def run(self):
        _FileSinkAsync.run(self)


############################
#  STDOUT TransportSink  #
############################

class TerminalOut(AsyncProcessingStep):

    def __init__(self, data_format: str = CSV_DATA_FORMAT, maxsize: int = DEFAULT_QUEUE_MAXSIZE,
                 parallel_mode: str = PARALLEL_MODE_THREAD):
        if not parallel_mode:
            raise ValueError("%s: Sequential mode is not supported. Define one of the following parallel modes: $s",
                             self.__name__, ",".join(PARALLEL_MODES))
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "TerminalOut"
        self.data_format = data_format
        self.maxsize = maxsize

    def execute_sequential(self, sample):
        pass

    def on_start(self):
        super().on_start()

    def init_async_object(self, parallel_utils, parallel_mode):
        parallel_step = None
        if self.parallel_mode == PARALLEL_MODE_THREAD:
            parallel_step = _TerminalOutThread(self.data_format, parallel_utils)
        elif self.parallel_mode == PARALLEL_MODE_PROCESS:
            parallel_step = _TerminalOutProcess(self.data_format, parallel_utils)
        return parallel_step

    def on_close(self):
        super().on_close()


class _TerminalOutAsync(_ProcessingStepAsync):
    class ConsoleWriter:
        def write(self, data):
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()

    def __init__(self, data_format: str, parallel_utils):
        super().__init__(parallel_utils)
        self.__name__ = "TerminalOut_Async"
        self.marshaller = get_marshaller_by_data_format(data_format)
        self.header = None
        self.console_writer = self.ConsoleWriter()

    def loop(self, sample):
        if header_check(self.header, sample.header):
            self.header = sample.header
            self.marshaller.marshall_header(sink=self.console_writer, header=self.header)
        self.marshaller.marshall_sample(sink=self.console_writer, sample=sample)
        return sample


class _TerminalOutThread(threading.Thread, _TerminalOutAsync):

    def __init__(self, data_format, parallel_utils):
        threading.Thread.__init__(self)
        _TerminalOutAsync.__init__(self, data_format, parallel_utils)
        self.__name__ = "TerminalOut_Thread"

    def run(self):
        _TerminalOutAsync.run(self)


class _TerminalOutProcess(multiprocessing.Process, _TerminalOutAsync):

    def __init__(self, data_format, parallel_utils):
        multiprocessing.Process.__init__(self)
        _TerminalOutAsync.__init__(self, data_format, parallel_utils)
        self.__name__ = "TerminalOut_Process"

    def run(self):
        _TerminalOutAsync.run(self)
