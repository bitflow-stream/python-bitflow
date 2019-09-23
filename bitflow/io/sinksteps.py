import queue
import select
import socket
import sys
import threading
import time
from collections import deque

from bitflow.io.marshaller import *
from bitflow.processingstep import ProcessingStep, AsyncProcessingStep, _AsyncProcessingStep


def header_check(old_header, new_header):
    if old_header is None:
        return True
    if old_header.has_changed(new_header):
        return True
    return False


###########################
# NETWORK TransportSink #
###########################
class TCPSink(AsyncProcessingStep):

    def __init__(self, host: str, port: int, data_format: str = CSV_DATA_FORMAT, reconnect_timeout: int = 2):
        super().__init__()
        self.__name__ = "TCPSink"
        self.threaded_step = _TCPSink(self.sample_queue_in, self.sample_queue_out, self.input_counter,
                                      host, port, data_format, reconnect_timeout)


class _TCPSink(_AsyncProcessingStep):

    def __init__(self, sample_queue_in, sample_queue_out, input_counter, host: str, port: int,
                 data_format: str = CSV_DATA_FORMAT, reconnect_timeout: int = 2):
        super().__init__(sample_queue_in, sample_queue_out, input_counter)
        self.marshaller = get_marshaller_by_data_format(data_format)
        self.__name__ = "TCPSink_inner"
        self.s = None
        self.header = None
        self.wrapper = None
        self.host = host
        self.port = port
        self.reconnect_timeout = reconnect_timeout
        logging.info("{}: initialized ...".format(self.__name__))

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
                logging.warning("{}: could not connect to {}:{} ... ".format(self.__name__, self.host, self.port))
                time.sleep(self.reconnect_timeout)
                self.s = None
                return None

        try:
            if header_check(self.header, sample.header):
                self.header = sample.header
                self.marshaller.marshall_header(self.wrapper, self.header)
            self.marshaller.marshall_sample(self.wrapper, sample)
        except socket.error:
            logging.error(
                "{}: failed to send to peer {}:{}, closing connection ...".format(self.__name__, self.host, self.port))
            self.close_connection()
        return None

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


class SocketWrapper:
    def __init__(self, sock):
        self.socket = sock

    def write(self, data):
        return self.socket.send(data)

    def read(self, packet_size):
        return self.socket.recv(packet_size)


####################
# ListenSocketSink #
####################
NO_INPUT_TIMEOUT = 0.1
SOCKET_ERROR_TIMEOUT = 0.5


class ListenSink(AsyncProcessingStep):

    def __init__(self, host: str = "0.0.0.0", port: int = 5010, data_format: str = CSV_DATA_FORMAT,
                 sample_buffer_size: int = -1, max_receivers: int = 5):
        super().__init__()
        self.__name__ = "ListenSink"
        self.threaded_step = _ListenSink(self.sample_queue_in, self.sample_queue_out, self.input_counter,
                                         host, port, data_format, sample_buffer_size, max_receivers)


class _ListenSink(_AsyncProcessingStep):

    def __init__(self, sample_queue_in, sample_queue_out, input_counter, host: str = "0.0.0.0", port: int = 5010,
                 data_format: str = CSV_DATA_FORMAT, sample_buffer_size: int = -1, max_receivers: int = 5):

        super().__init__(sample_queue_in, sample_queue_out, input_counter)
        self.__name__ = "ListenSink_inner"
        self.marshaller = get_marshaller_by_data_format(data_format)

        self.host = host
        self.port = port
        self.max_receivers = max_receivers
        self.sample_queues = {}
        if sample_buffer_size is -1:
            self.sample_buffer = deque(maxlen=None)
        else:
            self.sample_buffer = deque(maxlen=sample_buffer_size)

        try:
            self.server = self.bind_socket(self.host, self.port, self.max_receivers)
        except socket.error as se:
            logging.error("{}: could not bind socket ...".format(self.__name__))
            logging.error(str(se))
            sys.exit(1)
        self.inputs = [self.server]
        self.outputs = []
        self.lock = threading.Lock()

    def bind_socket(self, host, port, max_receivers):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setblocking(True)
        server.bind((host, port))
        server.listen(max_receivers)
        logging.info("{}: binding socket on {}:{} ...".format(self.__name__, host, port))
        return server

    def close_connections(self, outputs, sample_queues):
        # TODO log contained samples in queues to see whether there are dropped samples
        for s in outputs:
            s.close()
        self.outputs = []
        self.sample_queues = {}

    def close_connection(self, s, outputs, sample_queues):
        logging.info("{}: closing connection to peer {} ...".format(self.__name__, s))
        outputs.remove(s)
        s.close()
        del sample_queues[s]

    def get_new_queue(self, sample_buffer):
        q = queue.Queue()
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
        empty = True
        for k, v in self.sample_queues.items():
            if not v["queue"].empty():
                empty = False
        if empty:
            return False
        return True

    def loop(self, sample):
        self.lock.acquire()
        for k, v in self.sample_queues.items():
            v["queue"].put(sample)
        self.sample_buffer.append(sample)
        self.lock.release()

        readable, writable, exceptional = select.select(
            self.inputs, self.outputs, self.inputs, 1)
        for s in readable:
            if s is self.server:
                self.initialize_connection(s)
        if not self.has_to_send():
            time.sleep(NO_INPUT_TIMEOUT)
            return None

        for s in writable:
            sw = SocketWrapper(s)
            try:
                sample = self.sample_queues[s]["queue"].get_nowait()
            except queue.Empty:
                continue

            try:
                # if no header send yet or header  has changed -> marshall header
                if self.sample_queues[s]["header"] is None or self.sample_queues[s]["header"].has_changed(
                        sample.header):
                    self.marshaller.marshall_header(sw, sample.header)
                    self.sample_queues[s]["header"] = sample.header
                self.marshaller.marshall_sample(sw, sample)
            except socket.error:
                exceptional.append(s)
            self.sample_queues[s]["queue"].task_done()

        for s in exceptional:
            if s is self.server:
                logging.warning(
                    "{}: Unexpected socket error occured. Trying to rebind socket ...".format(self.__name__))
                self.close_connections(outputs=self.outputs,
                                       sample_queues=self.sample_queues)
                self.server.close()
                time.sleep(SOCKET_ERROR_TIMEOUT)
                self.server = self.bind_socket(self.host, self.port, self.max_receivers)
            elif s in self.outputs:
                logging.warning("{}: Unexpected socket error occured ...")
                self.close_connection(s, self.outputs, self.sample_queues)
        return None

    def on_close(self):
        for k, v in self.sample_queues.items():
            v["queue"].join()
        self.close_connections(self.outputs, self.sample_queues)
        self.server.close()
        super().on_close()


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

    def __init__(self, filename: str, data_format: str = CSV_DATA_FORMAT):
        super().__init__()
        self.__name__ = "FileSink"
        self.threaded_step = _FileSink(self.sample_queue_in, self.sample_queue_out,
                                       self.input_counter, filename, data_format)


class _FileSink(_AsyncProcessingStep):

    def __init__(self, sample_queue_in, sample_queue_out, input_counter, filename: str,
                 data_format: str = CSV_DATA_FORMAT):
        super().__init__(sample_queue_in, sample_queue_out, input_counter)
        self.__name__ = "FileSink_inner"
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
        return None

    def on_close(self):
        if self.f is not None:
            self.f.close()
            self.f = None
        super().on_close()


############################
#  STDOUT TransportSink  #
############################
class TerminalOut(ProcessingStep):
    class ConsoleWriter:
        def write(self, data):
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()

    def __init__(self, data_format: str = CSV_DATA_FORMAT):
        super().__init__()
        self.__name__ = "TerminalOutput"
        self.marshaller = get_marshaller_by_data_format(data_format)
        self.header = None
        self.console_writer = self.ConsoleWriter()

    def execute(self, sample):
        if header_check(self.header, sample.header):
            self.header = sample.header
            self.marshaller.marshall_header(sink=self.console_writer, header=self.header)
        self.marshaller.marshall_sample(sink=self.console_writer, sample=sample)
        self.write(sample)
