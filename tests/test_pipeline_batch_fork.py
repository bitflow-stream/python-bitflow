import itertools
import math
import unittest

from bitflow import fork, batch, processingstep, batchprocessingstep
from bitflow import pipeline as pipe
from bitflow.io import sources, sinksteps
from bitflow.io.marshaller import CSV_DATA_FORMAT
from bitflow.processingstep import PARALLEL_MODES
from bitflow.script import script_parser
from tests.support import *


# TODO test all parallel and the sequential mode
# TODO pipeline without source but with generative step --> does not work since pipelines execute methods are
#  never called. Idea: Empty input which calls method all the time. But how to terminate?


class TestBitflowCoreFunctions(unittest.TestCase):
    parallel_modes_pipeline = [None, *PARALLEL_MODES]
    parallel_modes_fork = [None, *PARALLEL_MODES]
    parallel_modes_batch = [None, *PARALLEL_MODES]

    pipeline_steps = [None, [processingstep.DebugGenerator(10)]]

    batch_sizes = range(1, 5, 1)

    fork_scripts = [
        "debuggenerator() -> Fork_Tags(tag=blub){ * -> addtag( tags={a=b} ) }",
        "debuggenerator() -> Fork_Tags(tag=blub){ bla -> addtag( tags={a=b} ) } -> Noop()",
        "debuggenerator() -> Fork_Tags(tag=blub){ bla -> addtag(tags={a=b});"
        "                                         blub -> addtag(tags={a=c}) } -> Noop()"
    ]

    def test_pipeline(self):
        for parallel_mode_pipeline, pipeline_steps in itertools.product(self.parallel_modes_pipeline,
                                                                        self.pipeline_steps):
            with self.subTest(msg="Pipeline base functionality.", parallel_mode_pipeline=parallel_mode_pipeline,
                              pipeline_steps=pipeline_steps):
                if parallel_mode_pipeline:
                    pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode_pipeline)
                else:
                    pipeline = pipe.PipelineSync()
                if pipeline_steps:
                    for step in pipeline_steps:
                        pipeline.add_processing_step(step)
                pipeline.start()
                pipeline.stop()

    def test_fork_script(self):
        for parallel_mode_pipeline, fork_script in itertools.product(self.parallel_modes_pipeline, self.fork_scripts):
            with self.subTest(msg="Fork bf script base functionality.", parallel_mode_pipeline=parallel_mode_pipeline,
                              fork_script=fork_script):
                _, heads = script_parser.parse_script(fork_script, parallel_mode_pipeline)
                for head in heads:
                    head.start()
                    head.stop()

    # TODO should be extended
    def test_fork(self):
        for parallel_mode_pipeline, parallel_mode_fork in itertools.product(
                self.parallel_modes_pipeline, self.parallel_modes_fork):
            with self.subTest(msg="Fork base functionality.", parallel_mode_pipeline=parallel_mode_pipeline,
                              parallel_mode_fork=parallel_mode_fork):
                if parallel_mode_pipeline:
                    pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode_pipeline)
                else:
                    pipeline = pipe.PipelineSync()
                tag_fork = fork.Fork_Tags(tag="blub", parallel_mode=parallel_mode_fork)
                tag_fork.add_processing_steps([], ["bla", "blub"])
                pipeline.add_processing_step(processingstep.DebugGenerator())
                pipeline.add_processing_step(tag_fork)
                pipeline.start()
                pipeline.stop()

    def test_fork_output_content(self):
        for parallel_mode_pipeline, parallel_mode_fork in itertools.product(
                self.parallel_modes_pipeline, self.parallel_modes_fork):
            remove_files(TEST_OUT_FILES)
            with self.subTest(msg="Batch output content comparison.", parallel_mode_pipeline=parallel_mode_pipeline,
                              parallel_mode_fork=parallel_mode_fork):
                if parallel_mode_pipeline:
                    pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode_pipeline)
                else:
                    pipeline = pipe.PipelineSync()
                file_source = sources.FileSource(path=TESTING_IN_FILE_CSV_SMALL, pipeline=pipeline)
                tag_fork = fork.Fork_Tags(tag="filter", parallel_mode=parallel_mode_fork)
                tag_fork.add_processing_steps([processingstep.Noop()], ["*"])
                pipeline.add_processing_step(tag_fork)
                pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV,
                                                                data_format=CSV_DATA_FORMAT))

                file_source.start_and_wait()

                a = read_file(TESTING_IN_FILE_CSV_SMALL)
                b = read_file(TESTING_OUT_FILE_CSV)
                self.assertEqual(a, b)

    def test_batch(self):
        batch_size = 20
        pl = pipe.PipelineSync()
        file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pl)
        batch_step = batch.Batch(size=batch_size)
        batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
        pl.add_processing_step(batch_step)

        file_source.start_and_wait()

    def test_batch_output_content(self):
        for parallel_mode_pipeline, parallel_mode_batch in itertools.product(
                self.parallel_modes_pipeline, self.parallel_modes_batch):
            remove_files(TEST_OUT_FILES)
            with self.subTest(msg="Batch output content comparison.", parallel_mode_pipeline=parallel_mode_pipeline,
                              parallel_mode_batch=parallel_mode_batch):
                if parallel_mode_pipeline:
                    pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode_pipeline)
                else:
                    pipeline = pipe.PipelineSync()
                file_source = sources.FileSource(path=TESTING_IN_FILE_CSV_SMALL, pipeline=pipeline)
                batch_step = batch.Batch(size=1)
                batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
                pipeline.add_processing_step(batch_step)
                pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV,
                                                                data_format=CSV_DATA_FORMAT))

                file_source.start_and_wait()

                a = read_file(TESTING_EXPECTED_BATCH_OUT_SMALL)
                b = read_file(TESTING_OUT_FILE_CSV)
                self.assertEqual(a, b)

    def test_batch_output_num_samples(self):
        for parallel_mode_pipeline, parallel_mode_batch, batch_size in itertools.product(
                self.parallel_modes_pipeline, self.parallel_modes_batch, self.batch_sizes):
            remove_files(TEST_OUT_FILES)
            with self.subTest(msg="Batch output sample number comparison.",
                              parallel_mode_pipeline=parallel_mode_pipeline,
                              parallel_mode_batch=parallel_mode_batch,
                              batch_size=batch_size):
                if parallel_mode_pipeline:
                    pipeline = pipe.PipelineAsync(parallel_mode=parallel_mode_pipeline)
                else:
                    pipeline = pipe.PipelineSync()
                file_source = sources.FileSource(path=TESTING_IN_FILE_CSV_SMALL, pipeline=pipeline)
                batch_step = batch.Batch(size=batch_size)
                batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
                pipeline.add_processing_step(batch_step)
                pipeline.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV,
                                                                data_format=CSV_DATA_FORMAT))

                file_source.start_and_wait()

                # - 1 for header line
                num_samples_expected = math.ceil((file_len(TESTING_IN_FILE_CSV_SMALL) - 1) / batch_size)
                num_samples = file_len(TESTING_OUT_FILE_CSV) - 1
                self.assertEqual(num_samples_expected, num_samples)

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)

    def tearDown(self):
        pass


# class TestBatchPipeline(unittest.TestCase):
#
#     def test_batch_pipeline_add_batch_step(self):
#         batch_size = 20
#         pl = pipe.Pipeline()
#         file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pl)
#         batch_step = batch.Batch(size=batch_size)
#         batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
#         pl.add_processing_step(batch_step)
#
#         file_source.start_and_wait()
#
#     def test_batch_pipeline_number_of_samples_out_size_1(self):
#         remove_files(TEST_OUT_FILES)
#         batch_size = 1
#         pl = pipe.Pipeline()
#         file_source = sources.FileSource(path=TESTING_IN_FILE_CSV_SMALL, pipeline=pl)
#         batch_step = batch.Batch(size=batch_size)
#         batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
#         pl.add_processing_step(batch_step)
#         pl.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
#
#         file_source.start_and_wait()
#
#         a = read_file(TESTING_EXPECTED_BATCH_OUT_SMALL)
#         b = read_file(TESTING_OUT_FILE_CSV)
#         self.assertEqual(a, b)
#
#     def test_batch_pipeline_number_of_samples_out_size_20(self):
#         remove_files(TEST_OUT_FILES)
#         batch_size = 20
#         pl = pipe.Pipeline()
#         file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pl)
#         batch_step = batch.Batch(size=batch_size)
#         batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
#         pl.add_processing_step(batch_step)
#         pl.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
#
#         file_source.start_and_wait()
#
#         a = file_len(TESTING_IN_FILE_CSV)
#         b = file_len(TESTING_OUT_FILE_CSV)
#         self.assertEqual(math.ceil((a - 1) / batch_size), b - 1)  # - header line
#
#     def test_batch_pipeline_number_of_samples_out_size_37(self):
#         remove_files(TEST_OUT_FILES)
#         batch_size = 37
#         pl = pipe.Pipeline()
#         file_source = sources.FileSource(path=TESTING_IN_FILE_CSV, pipeline=pl)
#         batch_step = batch.Batch(size=batch_size)
#         batch_step.add_processing_step(batchprocessingstep.AvgBatchProcessingStep())
#         pl.add_processing_step(batch_step)
#         pl.add_processing_step(sinksteps.FileSink(filename=TESTING_OUT_FILE_CSV, data_format=CSV_DATA_FORMAT))
#
#         file_source.start_and_wait()
#
#         a = file_len(TESTING_IN_FILE_CSV)
#         b = file_len(TESTING_OUT_FILE_CSV)
#         self.assertEqual(math.ceil((a - 1) / batch_size), b - 1)  # - minus header line
#
#     #
#     def setUp(self):
#         logging.basicConfig(format='%(asctime)s %(message)s', level=LOGGING_LEVEL)
#
#     def tearDown(self):
#         time.sleep(2)
#         remove_files(TEST_OUT_FILES)


if __name__ == '__main__':
    unittest.main()
