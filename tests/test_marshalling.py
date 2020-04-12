import unittest
import os
import io
from bitflow.marshaller import BitflowProtocolError
from bitflow.io import SampleChannel
from bitflow.sample import Sample, Header
from tests.helpers import configure_logging

dir_path = os.path.dirname(os.path.realpath(__file__))

expected_header = Header(metric_names = ["last_timestamp_in_pcap", "opened_connections", "closed_connections", "stalled_connections", "ongoing_connections", "avg_duration", "bytes_in", "pkg_in_0-100", "pkg_in_100-200", "pkg_in_200-300", "pkg_in_300-400", "pkg_in_400-500", "pkg_in_500-600", "pkg_in_600-700", "pkg_in_700-800", "pkg_in_800-900", "pkg_in_900-1000", "pkg_in_1000-1100", "pkg_in_1100-1200", "pkg_in_1200-1300", "pkg_in_1300-1400", "pkg_in_1400-1500", "pkg_in_1500-1600", "bytes_out", "pkg_out_0-100", "pkg_out_100-200", "pkg_out_200-300", "pkg_out_300-400", "pkg_out_400-500", "pkg_out_500-600", "pkg_out_600-700", "pkg_out_700-800", "pkg_out_800-900", "pkg_out_900-1000", "pkg_out_1000-1100", "pkg_out_1100-1200", "pkg_out_1200-1300", "pkg_out_1300-1400", "pkg_out_1400-1500", "pkg_out_1500-1600", "avg_bytes_in", "avg_bytes_out", "ttl_changes_in", "ttl_changes_out"])

expected_tags = {"filter": "port_1935"}

# TODO test changing header in stream

class TestMarshalling(unittest.TestCase):

    def setUp(self):
        configure_logging()

    def read_file(self, filename):
        with open(filename, "rb") as data:
            return data.read()

    def stream_file(self, filename):
        return io.BytesIO(self.read_file(filename))

    def assert_equal_sample(self, sample1, sample2):
        # Allow small difference in timestamps due to rounding error
        delta = abs(sample1.get_timestamp() - sample2.get_timestamp())
        self.assertLessEqual(delta.microseconds, 1)

        self.assertDictEqual(sample1.get_tags(), sample2.get_tags())
        self.assertListEqual(sample1.header.metric_names, sample2.header.metric_names)
        self.assertListEqual(sample1.metrics, sample2.metrics)

    def unmarshall(self, input_file, num_samples, expected_error=None, expected_samples={}):
        input_file = dir_path + "/test_data/" + input_file
        input = io.BufferedReader(self.stream_file(input_file))
        output = io.BufferedWriter(io.BytesIO())
        channel = SampleChannel(input_stream=input, output_stream=output)
        
        # Use helper object to update the num_samples variable from the read_loop() method even when it raises an error
        class Helper():
            def __init__(helper):
                helper.samples = []

            def read_test(helper):
                if expected_error is None:
                    helper.read_loop()
                else:
                    with self.assertRaises(BitflowProtocolError):
                        helper.read_loop()

            def read_loop(helper):
                while True:
                    sample = channel.read_sample()
                    if sample is None:
                        break
                    helper.samples.append(sample)

        # Read all samples and check correctness
        helper = Helper()
        helper.read_test()
        self.assertEqual(len(helper.samples), num_samples)
        self.assertEqual(len(output.raw.getbuffer()), 0) # Should not have written anything

        for index, sample in expected_samples.items():
            self.assert_equal_sample(sample, helper.samples[index])

        return channel, output, helper.samples

    def marshall(self, channel, output, samples):
        # Marshall the previously unmarshalled samples, then unmarshall them again and check that the same samples are produced
        for sample in samples:
            channel.output_sample(sample)

        input2 = io.BufferedReader(io.BytesIO(output.raw.getvalue()))
        output2 = io.BufferedWriter(io.BytesIO())
        channel2 = SampleChannel(input_stream=input2, output_stream=output2)
        samples2 = []
        while True:
            sample = channel2.read_sample()
            if sample is None:
                break
            samples2.append(sample)
        self.assertEqual(len(output2.raw.getbuffer()), 0) # Should not have written anything
        
        self.assertEqual(len(samples), len(samples2))
        for index, sample in enumerate(samples):
            self.assert_equal_sample(sample, samples2[index])

    def test_empty_input(self):
        self.unmarshall("empty.bin", 0)

    def test_only_header(self):
        self.unmarshall("only_header.bin", 0)

    def test_broken1(self):
        self.unmarshall("broken1.bin", 0, expected_error=BitflowProtocolError)

    def test_broken2(self):
        metrics = [1.519143432357129e+09,2,0,0,2,0.022063016891479492,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,169,5,9,2,0,2,2,3,4,1,1,4,112,30,15,0,0,1683.395966228893,0,215]

        channel, output, samples = self.unmarshall("broken2.bin", 1, expected_error=BitflowProtocolError, expected_samples={
            0: Sample(expected_header, metrics, timestamp="2018-03-01 14:52:23.777949", tags=expected_tags)
        })
        self.marshall(channel, output, samples)

    def test_broken3(self):
        self.unmarshall("broken3.bin", 0, expected_error=BitflowProtocolError)

    def test_data_small(self):
        metrics1 = [1.519143432357129e+09,2,0,0,2,0.022063016891479492,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,169,5,9,2,0,2,2,3,4,1,1,4,112,30,15,0,0,1683.395966228893,0,215]
        metrics2 = [1.519143434392478e+09,0,0,0,2,0.09862303733825684,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,199,2,11,4,5,0,4,5,3,3,0,4,139,42,11,0,0,1768.4285591045357,0,245]

        channel, output, samples = self.unmarshall("in_small.bin", 5, expected_samples={
            0: Sample(expected_header, metrics1, timestamp="2018-03-01 14:52:23.777949", tags=expected_tags),
            4: Sample(expected_header, metrics2, timestamp="2018-03-01 14:52:23.854518", tags=expected_tags)
        })
        self.marshall(channel, output, samples)

    def test_data(self):
        metrics1 = [1.519143432357129e+09,2,0,0,2,0.022063016891479492,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,169,5,9,2,0,2,2,3,4,1,1,4,112,30,15,0,0,1683.395966228893,0,215]
        metrics2 = [1.519143686177767e+09,0,0,0,2,10.393117666244507,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,262,2,11,1,2,1,5,3,2,0,1,6,214,32,14,0,0,1783.842885901682,0,311]
        metrics3 = [1.519144745075841e+09,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 ]

        channel, output, samples = self.unmarshall("in.bin", 1222, expected_samples={
            0: Sample(expected_header, metrics1, timestamp="2018-03-01 14:52:23.777949", tags=expected_tags),
            500: Sample(expected_header, metrics2, timestamp="2018-03-01 14:52:34.149012", tags=expected_tags),
            1221: Sample(expected_header, metrics3, timestamp="2018-03-01 14:52:46.657", tags=expected_tags)
        })
        self.marshall(channel, output, samples)
    
    def test_data_mini(self):
        metrics = [33.33333334950213,2.600195949404688]
        channel, output, samples = self.unmarshall("in-mini.bin", 1, expected_samples={
            0: Sample(Header(metric_names=["cpu", "cpu-jiffies"]), metrics, timestamp="2020-04-11 09:49:52.828602", tags={})
        })
        self.marshall(channel, output, samples)

if __name__ == '__main__':
    unittest.main()
