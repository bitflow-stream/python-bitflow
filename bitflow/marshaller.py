import datetime
import struct

from bitflow.sample import Sample, Header


class BitflowProtocolError(Exception):
    def __init__(self, description, expected=None, received=None):
        msg = "Bitflow binary protocol error: {}.".format(description)
        if expected is not None:
            msg += " Expected: {} (type {}).".format(expected, type(expected))
        if received is not None:
            msg += " Received: {} (type {}).".format(received, type(received))
        super().__init__(msg)


TIMESTAMP_NUM_BYTES = 8
METRIC_NUM_BYTES = 8
HEADER_START = "timB"
TAGS_FIELD = "tags"
SAMPLE_MARKER_BYTE = b'X'
SEPARATOR_BYTE = b'\n'

TAGS_SEPARATOR = " "
TAGS_EQ = "="


class BinaryMarshaller:

    # ===============
    # General helpers
    # ===============

    def pack_long(self, value):
        return struct.pack('>Q', value)

    def pack_double(self, value):
        return struct.pack('>d', value)

    def unpack_long(self, data):
        return struct.unpack('>Q', data)[0]

    def unpack_double(self, data):
        return struct.unpack('>d', data)[0]

    def pack_string(self, string):
        return bytes(string, "UTF-8")

    def unpack_string(self, data):
        return data.decode("UTF-8")

    # =======================================
    # Reading and parsing samples and headers
    # =======================================

    # Read either a Header or a Sample from the stream.
    def read(self, stream, previousHeader):
        try:
            return self._read(stream, previousHeader)
        except (struct.error, UnicodeDecodeError) as e:
            raise BitflowProtocolError("failed to parse data: {}".format(str(e)))

    def _read(self, stream, previousHeader):
        if previousHeader is None:
            return self.read_header(stream)

        start = stream.peek(len(SAMPLE_MARKER_BYTE))
        if len(start) == 0:
            return None  # Possible EOF
        elif len(start) >= len(SAMPLE_MARKER_BYTE) and start[:len(SAMPLE_MARKER_BYTE)] == SAMPLE_MARKER_BYTE:
            return self.read_sample(stream, previousHeader)
        else:
            return self.read_header(stream)

    def read_header(self, stream):
        timeField = self.read_line(stream)  # Header fields are terminated by newline characters
        if timeField == "":
            return None  # Possible EOF
        if timeField != HEADER_START:
            raise BitflowProtocolError("unexpected line", HEADER_START, timeField)

        tagsField = self.read_line(stream)
        if tagsField != TAGS_FIELD:
            raise BitflowProtocolError("unexpected line", TAGS_FIELD, tagsField)

        fields = []
        while True:
            fieldName = self.read_line(stream)
            if len(fieldName) == 0:
                break  # Empty line terminates the header
            fields.append(fieldName)

        return Header(fields)

    def read_sample(self, stream, header):
        stream.read(len(SAMPLE_MARKER_BYTE))  # Result ignored, was already peeked

        num_fields = header.num_fields()
        timeBytes = stream.read(TIMESTAMP_NUM_BYTES)
        tagBytes = self.read_line(stream)  # New line terminates the tags
        valueBytes = stream.read(num_fields * METRIC_NUM_BYTES)

        timestamp = self.unpack_utc_nanos_timestamp(self.unpack_long(timeBytes))
        tags = self.parse_tags(tagBytes)

        metrics = []
        for index in range(num_fields):
            offset = index * METRIC_NUM_BYTES
            metricBytes = valueBytes[offset: offset + METRIC_NUM_BYTES]
            metric = self.unpack_double(metricBytes)
            metrics.append(metric)
        return Sample(header=header, metrics=metrics, timestamp=timestamp, tags=tags)

    def parse_tags(self, tags_string):
        tags_dict = {}
        if tags_string == "":
            return tags_dict
        tags = tags_string.split(TAGS_SEPARATOR)
        for tag_string in tags:
            if TAGS_EQ in tag_string:
                key, value = tag_string.split(TAGS_EQ)
                tags_dict[key] = value
            else:
                raise BitflowProtocolError("illegal tag string", "key=value pair", tag_string)
        return tags_dict

    def read_line(self, stream):
        return self.unpack_string(stream.readline())[:-1]

    # ==========================================
    # Formatting and sending samples and headers
    # ==========================================

    def write_sample(self, stream, sample):
        stream.write(SAMPLE_MARKER_BYTE)
        stream.write(self.pack_long(self.pack_utc_nanos_timestamp(sample)))
        stream.write(self.pack_string(self.format_tags(sample)))
        stream.write(SEPARATOR_BYTE)
        for val in sample.metrics:
            stream.write(self.pack_double(val))

    def write_header(self, stream, header):
        for field in [HEADER_START, TAGS_FIELD] + header.metric_names:
            stream.write(self.pack_string(field))
            stream.write(SEPARATOR_BYTE)
        stream.write(SEPARATOR_BYTE)

    def format_tags(self, sample):
        s = ""
        pairs = ["{}={}".format(key, value) for key, value in sample.get_tags().items()]
        pairs.sort()
        return " ".join(pairs)

    # ==================
    # Timestamp handling
    # ==================
    # Note: Bitflow timestamps are represented in UTC, both in binary marshalled format, and internally.
    # Printing the timestamps as-is might result in a time that deviates from the local time.
    # Especially, UTC timetamps differ from what is printed by the Go-based bitflow-pipeline tool, which converts to local time.

    epoch = datetime.datetime.utcfromtimestamp(0)

    def unpack_utc_nanos_timestamp(self, timestamp):
        seconds = timestamp // 1000000000
        micros = (timestamp // 1000) % 1000000
        return datetime.datetime.utcfromtimestamp(seconds) + datetime.timedelta(microseconds=micros)

    def pack_utc_nanos_timestamp(self, sample):
        time = sample.get_timestamp()
        delta = time - self.epoch
        return int(delta.total_seconds() * 1000000000)  # Nanoseconds, rounded to microseconds
