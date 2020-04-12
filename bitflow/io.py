import sys

from bitflow.marshaller import BinaryMarshaller, BitflowProtocolError
from bitflow.sample import Sample, Header


# TODO necessary to close std in/out streams?
# TODO correct/efficient use of buffered IO?

class SampleChannel():

    def __init__(self, input_stream=None, output_stream=None):
        if input_stream is None:
            input_stream = sys.stdin.buffer
        if output_stream is None:
            output_stream = sys.stdout.buffer
        self.marshaller = BinaryMarshaller()
        self.out_header = None
        self.in_header = None
        self.writer = self.FlushingWriter(output_stream)
        self.reader = input_stream

    def close(self):
        # We do not explicitely close the std in/out streams
        pass

    # ===============================
    # Writing samples to standard out
    # ===============================

    def output_sample(self, sample):
        if self.out_header_changed(sample.header):
            self.out_header = sample.header
            self.marshaller.write_header(stream=self.writer, header=self.out_header)
        self.marshaller.write_sample(stream=self.writer, sample=sample)

    def out_header_changed(self, new_header):
        if self.out_header is None:
            return True
        return self.out_header.has_changed(new_header)

    class FlushingWriter:
        def __init__(self, stream):
            self.stream = stream

        def write(self, data):
            self.stream.write(data)
            self.stream.flush()

    # ================================
    # Reading samples from standard in
    # ================================

    def read_sample(self):
        while True:
            sampleOrHeader = self.marshaller.read(self.reader, self.in_header)
            if sampleOrHeader is None:
                return None  # Possible EOF
            if isinstance(sampleOrHeader, Sample):
                return sampleOrHeader
            elif isinstance(sampleOrHeader, Header):
                # Wait for the next received sample
                self.in_header = sampleOrHeader
            else:
                raise BitflowProtocolError("wrong unmarshalled object", "Header or Sample", sampleOrHeader)
