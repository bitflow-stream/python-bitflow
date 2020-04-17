import unittest

from bitflow.runner import BitflowRunner, ProcessingStep
from bitflow.sample import Sample
from tests.helpers import configure_logging, SampleListChannel


class TestRunner(unittest.TestCase):
    class MockStep(ProcessingStep):
        def __init__(self, test):
            super().__init__()
            self.test = test
            self.current_stage = None
            self.all_stages = set()
            self.stage(None, "construct")

        def stage(self, previous, current):
            self.test.assertEqual(self.current_stage, previous)
            self.all_stages.add(current)
            self.current_stage = current

        def initialize(self, context):
            self.stage("construct", "running")
            super().initialize(context)

        def handle_sample(self, sample):
            self.stage("running", "running")
            self.output(sample)

        def cleanup(self):
            self.stage("running", "shutdown")

    def setUp(self):
        configure_logging()

    def perform_test(self, samples):
        step = self.MockStep(self)
        channel = SampleListChannel(list(samples))
        runner = BitflowRunner()
        runner.run(step, channel)

        self.assertEqual(step.current_stage, "shutdown")
        self.assertSetEqual(step.all_stages, {"construct", "running", "shutdown"})
        self.assertTrue(channel.closed)
        self.assertListEqual(samples, channel.output)

    def test_runner_0(self):
        self.perform_test([Sample(None, [])])

    def test_runner_1(self):
        self.perform_test([Sample(None, [])])

    def test_runner_2(self):
        self.perform_test([Sample(None, []), Sample(None, [])])

    def test_runner_5(self):
        self.perform_test([Sample(None, []), Sample(None, []), Sample(None, []), Sample(None, []), Sample(None, [])])

    def test_runner_many(self):
        self.perform_test([Sample(None, []) for _ in range(10000)])


if __name__ == '__main__':
    unittest.main()
