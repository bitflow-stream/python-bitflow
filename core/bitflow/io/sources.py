import multiprocessing
import queue
import os
import select
import socket
import time

from core.bitflow import helper
from core.bitflow.io.marshaller import *

NO_HEADER_LINE = 0
HEADER_UPDATED = 1
WAIT_TO_UPDATE = 2
NO_END_OF_HEADER_FOUND = -1


class SourceNotDefined(Exception):
    pass


class RunningAlready(Exception):
    pass


def read_header(marshaller, s, buffer_size=2048):
    b = ""
    while '\n' not in b:
        b += s.recv(buffer_size).decode()
    header_and_rest = b.split("\n")
    header_str = header_and_rest[0]
    over_recv_b = header_and_rest[1]
    header = marshaller.unmarshall_header(header_str)
    return header, over_recv_b


class Source(metaclass=helper.CtrlMethodDecorator):

    def __init__(self, pipeline, *args):
        self.__name__ = "Source"
        self.running = multiprocessing.Value('i', 0)
        self._source = self.init_parallel_source(self.running, pipeline, *args)
        self.started = False

    def init_parallel_source(self, running, pipeline, *args):
        raise NotImplementedError("Initialization of parallel source needs to be implemented.")

    def start_and_wait(self):
        self.start()
        self.wait()

    def start(self):
        if not self.started:  # Start only once
            self.running.value += 1
            if self._source:
                self._source.start()
            else:
                raise SourceNotDefined("Cannot start. No sample source defined.")
        self.started = True

    def wait(self):
        if self._source:
            self._source.join()

    def stop(self):
        self.on_close()
        self.wait()

    def on_close(self):
        self.running.value = 0


class _SourceProcess(multiprocessing.Process, metaclass=helper.CtrlMethodDecorator):
    BIN_NEWLINE = '\n'.encode('utf8')

    def __init__(self, running, pipeline, marshaller, sample_limit):
        super().__init__()
        self.__name__ = "Source_Process"
        self.pipeline = pipeline
        self.running = running
        self.marshaller = marshaller
        self.header = None
        self.sample_counter_in = 0
        self.sample_limit = sample_limit

    def into_pipeline(self, b_metrics, header):
        sample = self.marshaller.unmarshall_sample(header, b_metrics)
        if sample:
            self.pipeline.execute(sample)
            self.sample_counter_in += 1
        if 0 < self.sample_limit <= self.sample_counter_in:
            self.stop()

    def cut_bytes(self, cutting_pos, b, cut_len=0):
        begin = b[0:cutting_pos]
        rest = b[cutting_pos + cut_len:len(b)]
        return begin, rest

    def get_start_bytes(self, b):
        if b and len(b) >= 4:
            return b[0:4]
        else:
            return None

    def is_header_start(self, b):
        if not self.marshaller:
            return False
        if self.get_start_bytes(b) == self.marshaller.HEADER_START_BYTES:
            return True

        return False

    # cuts at newline, end of line for csv end after tags for bin
    def get_newline_cutting_pos(self, b):
        btlen = BinMarshaller.TIMESTAMP_VALUE_BYTES_LEN
        if len(b) > btlen:
            cutting_pos = b[btlen:len(b)].find(self.BIN_NEWLINE)
            if cutting_pos > 0:
                return cutting_pos + btlen
        return -1

    def get_marshaller(self, start_bytes):
        try:
            marshaller = get_marshaller_by_content_bytes(start_bytes)
        except UnsupportedDataFormat as e:
            logging.warning("The format inferred from '%s' of the current input is not supported.",
                            start_bytes.decode('utf-8'), exc_info=e)
            raise e
        return marshaller

    # TODO: if header change is forbidden for current data format -> exit
    def update_header(self, b, header):
        if self.is_header_start(b):
            header_end_pos = b.find(self.marshaller.END_OF_HEADER_BYTES)
            if header_end_pos == -1:
                return b, header, NO_END_OF_HEADER_FOUND
            b_header, b = self.cut_bytes(header_end_pos, b, len(self.marshaller.END_OF_HEADER_BYTES))
            header = self.marshaller.unmarshall_header(b_header)
            return b, header, HEADER_UPDATED
        return b, header, NO_HEADER_LINE

    def on_start(self):
        pass

    def run(self):
        self.on_start()
        if self.pipeline:
            self.pipeline.start()
        while self.running.value:
            self.loop()
        self.on_close()

    def stop(self):
        self.running.value = 0  # Signal stop to self (break out from run method)

    def on_close(self):
        self.pipeline.stop()

    # Abstract
    def loop(self):
        pass


class EmptySource(Source):

    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.__name__ = "EmptySource"

    def init_parallel_source(self, running, pipeline, *args):
        return _EmptySourceProcess(self.running, pipeline)


class _EmptySourceProcess(_SourceProcess):

    def __init__(self, running, pipeline):
        super().__init__(running, pipeline, None, -1)
        self.__name__ = "EmptySource_Process"

    def loop(self):
        pass

    def on_close(self):
        super().on_close()


class FileSource(Source):

    def __init__(self, pipeline, path=None, buffer_size=2048, sample_limit=-1):
        super().__init__(pipeline, path, buffer_size, sample_limit)
        self.__name__ = "FileSource"

    def init_parallel_source(self, running, pipeline, *args):
        return _FileSourceProcess(running, pipeline, *args)

    def add_path(self, path):
        if not self._source.is_alive():  # Can only append paths before started. Not possible during runtime of process
            self._source.add_path(path)
        else:
            raise RunningAlready("%s: Already started. Cannot add paths at runtime.")


class _FileSourceProcess(_SourceProcess):

    def __init__(self, running, pipeline, path, buffer_size, sample_limit):
        super().__init__(running, pipeline, None, sample_limit)
        self.__name__ = "FileSource_Process"
        self.files = self._handle_path(path)
        self.file_iter = iter(self.files)
        self.f = None
        self.header = None
        self.b = b''
        self.buffer_size = buffer_size

    def add_path(self, path):
        self.files += self._handle_path(path)

    @staticmethod
    def _handle_path(path):
        files = []
        if path:
            if not os.path.isabs(path):
                abs_path = os.path.abspath(path)
            else:
                abs_path = path
            if os.path.isdir(abs_path):  # Expand directory. Recursive to comply with java bitflow file input
                files += [f for r, d, f in os.walk(path)]
            elif os.path.isfile(path):  # Only add if file exists
                files += [path]
        return files

    def open_file(self, path):
        try:
            f = open(path, 'rb')
        except IOError:
            logging.error("{}: could not open file {} ...".format(str(self), path))
            f = None
        return f

    def read_bytes(self, s, buffer_size):
        read_b = s.read(buffer_size)
        if not read_b:
            self.close_current_file()
        return read_b

    def get_next_file_path(self):
        try:
            file = next(self.file_iter)
        except StopIteration:
            logging.info("Finished reading all files.")
            file = None
        return file

    def _initial_read(self, file_path):
        self.f = self.open_file(file_path)
        if self.f:
            self.b = self.read_bytes(s=self.f, buffer_size=self.buffer_size)

    def close_current_file(self):
        self.f.close()
        self.f = None
        self.b = None

    def loop(self):
        if not self.f:
            file_path = self.get_next_file_path()
            if file_path is None:
                self.stop()
                return
            self._initial_read(file_path)
            return

        if not self.marshaller:
            start_bytes = self.get_start_bytes(self.b)
            if start_bytes:
                try:
                    self.marshaller = self.get_marshaller(start_bytes)
                except UnsupportedDataFormat:  # File is not in bitflow-supported format. Skip and move to next file
                    self.close_current_file()
                    return
            else:
                return

        try:
            self.b, self.header, header_update_status = self.update_header(b=self.b, header=self.header)
        except HeaderException as e:
            logging.warning("%s: Invalid header.", self.__name__, exc_info=e)
            return
        if header_update_status == NO_END_OF_HEADER_FOUND:
            self.b += self.read_bytes(s=self.f, buffer_size=self.buffer_size)
            return

        cutting_pos = self.get_newline_cutting_pos(self.b)
        if cutting_pos == -1:
            self.b += self.read_bytes(s=self.f, buffer_size=self.buffer_size)
            return

        if isinstance(self.marshaller, BinMarshaller):
            metric_bytes_len = self.header.num_fields() * BinMarshaller.METICS_VALUE_BYTES_LEN
            if len(self.b) <= cutting_pos + metric_bytes_len:
                self.b += self.read_bytes(s=self.f, buffer_size=self.buffer_size)
                return
            else:
                cutting_pos += metric_bytes_len + 1  # add newline after tags byte

        b_metrics, self.b = self.cut_bytes(cutting_pos, self.b, len(self.marshaller.END_OF_SAMPLE_BYTES))
        self.into_pipeline(b_metrics=b_metrics, header=self.header)
        return

    def on_close(self):
        if self.f:
            self.f.close()
        super().on_close()


# Pulls / Downloads incoming data on the specified host:port
class DownloadSource(Source):

    def __init__(self, pipeline, host, port, buffer_size=2048, timeout_after_failed_to_connect=1, sample_limit=-1):
        super().__init__(pipeline, host, port, buffer_size, timeout_after_failed_to_connect, sample_limit)
        self.__name__ = "DownloadSource"

    def init_parallel_source(self, running, pipeline, *args):
        return _DownloadSourceProcess(running, pipeline, *args)

    def __str__(self):
        return "DownloadSource"


class _DownloadSourceProcess(_SourceProcess):

    def __init__(self, running, pipeline, host, port, buffer_size, timeout_after_failed_to_connect, sample_limit):
        super().__init__(running, pipeline, None, sample_limit)
        self.__name__ = "DownloadSource_Process"
        self.host = host
        self.port = port
        self.timeout_after_failed_to_connect = timeout_after_failed_to_connect
        self.s = None
        self.header = None
        self.b = b''
        self.buffer_size = buffer_size

    def connect(self):
        logging.info("{}: trying to connect to {}:{} ...".format(self.__name__, self.host, self.port))
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.settimeout(1)
        self.s.connect((self.host, self.port))
        logging.info("{}: connected to {}:{} ...".format(self.__name__, self.host, self.port))

    def is_connected(self):
        if self.s is not None:
            return True
        else:
            return False

    def read_bytes(self, s, buffer_size):
        try:
            b = s.recv(buffer_size)
        except ConnectionResetError:
            logging.warning(
                "{}: ConnectionResetError: connection was reset by peer, reconnecting ...".format(self.__name__))
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
                logging.warning("{}: Could not connect to {}:{} ...:{}".format(
                    self.__name__, self.host, self.port, gai))
                time.sleep(self.timeout_after_failed_to_connect)
                self.s = None
                return
            except ConnectionRefusedError:
                logging.warning("{}: Connection refused from {}:{} ...".format(self.__name__, self.host, self.port))
                time.sleep(self.timeout_after_failed_to_connect)
                self.s = None
                return

            read_b = self.read_bytes(s=self.s, buffer_size=self.buffer_size)
            if read_b:
                self.b = read_b
            else:
                return

        if not self.marshaller:
            start_bytes = self.get_start_bytes(self.b)
            if start_bytes:
                try:
                    self.marshaller = self.get_marshaller(start_bytes)
                except UnsupportedDataFormat:  # Stream is not in bitflow-supported format. Skip and wait for next input
                    return
            else:
                return

        try:
            self.b, self.header, header_update_status = self.update_header(b=self.b, header=self.header)
        except HeaderException as e:
            logging.warning("{}: {}".format(self.__name__, str(e)))
            return
        if header_update_status == NO_END_OF_HEADER_FOUND:
            self.b += self.read_bytes(s=self.s, buffer_size=self.buffer_size)
            return

        cutting_pos = self.get_newline_cutting_pos(self.b)
        if cutting_pos == -1:
            read_b = self.read_bytes(s=self.s, buffer_size=self.buffer_size)
            if not read_b:
                return
            self.b += read_b
            return

        if isinstance(self.marshaller, BinMarshaller):
            metric_bytes_len = self.header.num_fields() * BinMarshaller.METICS_VALUE_BYTES_LEN
            if len(self.b) <= cutting_pos + metric_bytes_len:
                read_b = self.read_bytes(s=self.s, buffer_size=self.buffer_size)
                if not read_b:
                    self.stop()
                    return
                self.b += read_b
                return
            else:
                cutting_pos += metric_bytes_len + 1  # newline after tags byte

        b_metrics, self.b = self.cut_bytes(cutting_pos, self.b, len(self.marshaller.END_OF_SAMPLE_BYTES))
        self.into_pipeline(b_metrics=b_metrics, header=self.header)
        return

    def close_connection(self):
        if self.s:
            self.s.close()
            self.s = None
        time.sleep(0.1)

    def on_close(self):
        self.close_connection()
        super().on_close()


# Listens for incoming data on the specified host:port
class ListenSource(Source):

    def __init__(self, pipeline, port, max_number_of_peers=5, buffer_size=2048, sample_limit=-1):
        super().__init__(pipeline, port, max_number_of_peers, buffer_size, sample_limit)
        self.__name__ = "ListenSource"

    def init_parallel_source(self, running, pipeline, *args):
        return _ListenSourceProcess(running, pipeline, *args)


class _ListenSourceProcess(_SourceProcess):

    def __init__(self, running, pipeline, port, max_number_of_peers, buffer_size, sample_limit):
        super().__init__(running, pipeline, None, sample_limit)
        self.__name__ = "ListenSource_Process"
        self.max_number_of_peers = max_number_of_peers
        self.buffer_size = buffer_size
        self.host = "0.0.0.0"
        self.port = port
        self.server = None
        self.b = b''
        self.inputs = []
        self.connections = {}

    def on_start(self):
        self.init_server()

    def init_server(self):
        try:
            self.server = self.bind_port(self.host, self.port, self.max_number_of_peers)
        except socket.error as se:
            logging.error("{}: Could not bind socket ...".format(self.__name__))  # TODO exit properly here
            logging.error(str(se))
            exit(1)
        self.inputs.append(self.server)

    def close_connection(self, s, connections, inputs):
        logging.info("{}: closing connection to peer {} ...".format(self.__name__, s))
        s.close()
        del connections[s]
        inputs.remove(s)

    @staticmethod
    def bind_port(host, port, max_number_of_peers):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(max_number_of_peers)
        return server

    @staticmethod
    def read_bytes(s, buffer_size):
        b = s.recv(buffer_size)
        if b is "":
            return None
        return b

    def loop(self):
        readable, __, exceptional = select.select(self.inputs, [], self.inputs, 1)
        for s in readable:
            if s is self.server:
                connection, client_address = s.accept()
                connection.setblocking(0)
                self.inputs.append(connection)
                self.connections[connection] = {}
                self.connections[connection]["b"] = b''
                self.connections[connection]["header"] = None
            else:
                read_b = self.read_bytes(s=s, buffer_size=self.buffer_size)

                if read_b:
                    self.connections[s]["b"] += read_b
                else:
                    self.close_connection(s, self.connections, self.inputs)
                    return
                if not self.marshaller:
                    start_bytes = self.get_start_bytes(self.connections[s]["b"])
                    if start_bytes:
                        try:
                            self.marshaller = self.get_marshaller(start_bytes)
                        except UnsupportedDataFormat:
                            # Stream is not in bitflow-supported format. Skip and wait for next input
                            continue
                    else:
                        continue

                while self.running:
                    try:
                        self.connections[s]["b"], self.connections[s]["header"], header_update_status = \
                            self.update_header(b=self.connections[s]["b"], header=self.connections[s]["header"])
                    except HeaderException as e:
                        logging.warning("{}: {}".format(self.__name__, str(e)))
                        break
                    if header_update_status == NO_END_OF_HEADER_FOUND:
                        self.b += self.read_bytes(s=s, buffer_size=self.buffer_size)
                        break

                    cutting_pos = self.get_newline_cutting_pos(self.connections[s]["b"])
                    if cutting_pos == -1:
                        break
                    if isinstance(self.marshaller, BinMarshaller):
                        metric_bytes_len = self.connections[s]["header"].num_fields() * \
                                           BinMarshaller.METICS_VALUE_BYTES_LEN
                        if len(self.connections[s]["b"]) <= cutting_pos + metric_bytes_len:
                            break
                        else:
                            cutting_pos += metric_bytes_len + 1  # newline after tags byte

                    b_metrics, self.connections[s]["b"] = self.cut_bytes(cutting_pos, self.connections[s]["b"],
                                                                         len(self.marshaller.END_OF_SAMPLE_BYTES))
                    self.into_pipeline(b_metrics=b_metrics, header=self.connections[s]["header"])
            return

        for s in exceptional:
            self.close_connection(s, self.connections, self.inputs)

    def on_close(self):
        for c in self.connections:
            c.close()
        for i in self.inputs:
            i.close()
        self.server.close()
        self.server = None
        super().on_close()
