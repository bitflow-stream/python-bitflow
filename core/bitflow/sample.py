import time

import numpy as np


class Sample:

    def __init__(self, header, metrics, timestamp=None, tags=None):
        self.header = Header(header.metric_names)
        self.metrics = metrics
        if not timestamp:
            self.timestamp = np.datetime64(time.time_ns(), 'ns')
        else:
            if isinstance(timestamp, int):
                self.timestamp = np.datetime64(timestamp, 'ns')
            else:
                self.timestamp = np.datetime64(timestamp)
        if tags:
            self.tags = tags
        else:
            self.tags = {}

    def __str__(self):
        return "{}:{}, {}, {}, {}".format(
            "bitflow.sample",
            str(self.header),
            self.get_timestamp_string(),
            self.get_tags(),
            self.metrics)

    # METRICS
    def get_metrics(self):
        return self.metrics

    def extend(self, metric):
        self.metrics.append(metric)

    def get_metricsindex_by_name(self, metric_name):
        index = self.header.metric_names.index(metric_name)
        return index

    def get_metricvalue_by_name(self, metric_name):
        index = self.header.metric_names.index(metric_name)
        m = self.metrics[index]
        return m

    def remove_metrics(self, index):
        self.header.metric_names.remove(index)
        self.metrics = self.metrics[:index:]

    # TIMESTAMP
    def get_timestamp(self):
        return self.timestamp

    def get_timestamp_string(self):
        pts = str(self.timestamp).replace("T", " ")
        pts = pts.rstrip('0')
        return pts

    def set_timestamp(self, timestamp: str):
        self.timestamp = np.datetime64(timestamp)

    def get_unix_timestamp(self):
        return self.timestamp.astype('datetime64[ns]').astype('int')

    def get_epoch_timestamp(self):
        return self.timestamp.astype('datetime64[ns]').astype('float')

    # TAGS
    def get_tag(self, tag):
        if tag in self.tags:
            return self.tags[tag]
        else:
            return None

    def get_tags(self):
        return self.tags

    def get_tags_string(self):
        s = ""
        tags_count = len(self.tags.items())
        dict_position = 1
        for key, value in self.tags.items():
            if dict_position == tags_count:
                s += "{}={}".format(key, value)
            else:
                s += "{}={} ".format(key, value)
                dict_position += 1
        return s

    def add_tag(self, tag_key, tag_value):
        self.tags[tag_key] = tag_value

    def has_tag(self, key):
        if self.tags is None or len(self.tags) == 0:
            return False
        else:
            return key in self.tags

    # HEADER
    def header_changed(self, value):
        if isinstance(value, Header):
            return self.header.has_changed(value)
        elif isinstance(value, list):
            return self.header.has_changed(Header(value))
        else:
            raise ValueError("Cannot perform comparison of header with type %s".format(type(value)))

    @staticmethod
    def new_empty_sample():
        pass


class Header:

    def __init__(self, metric_names: list):
        self.metric_names = metric_names

    def __str__(self):
        return str(self.metric_names)

    def extend(self, metric_name):
        self.metric_names.append(metric_name)

    def num_fields(self):
        return len(self.metric_names)

    def has_changed(self, header):
        if self.num_fields() != header.num_fields():
            return True
        else:
            for i in range(0, len(self.metric_names)):
                if self.metric_names[i] != header.metric_names[i]:
                    return True
        return False
