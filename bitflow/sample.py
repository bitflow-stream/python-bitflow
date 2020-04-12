import datetime


class Sample:
    time_format = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(self, header, metrics, timestamp=None, tags=None):
        self.header = header
        self.metrics = metrics
        self.set_timestamp(timestamp)
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
        self.timestamp.strftime(self.time_format)

    def set_timestamp(self, timestamp: str):
        if not timestamp:
            self.timestamp = datetime.datetime.now()
        elif isinstance(timestamp, int):
            # Given in nanoseconds
            seconds = timestamp // 1000000000
            micros = (timestamp // 1000) % 1000000
            self.timestamp = datetime.datetime.fromtimestamp(seconds) + datetime.timedelta(microseconds=micros)
        elif isinstance(timestamp, datetime.datetime):
            self.timestamp = timestamp
        else:
            self.timestamp = datetime.datetime.strptime(timestamp, self.time_format)

    # TAGS
    def get_tag(self, tag):
        if tag in self.tags:
            return self.tags[tag]
        else:
            return None

    def get_tags(self):
        return self.tags

    def set_tag(self, tag_key, tag_value):
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
