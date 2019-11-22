import re

import core
from core.bitflow.io.sources import DownloadSource
from core.bitflow.pipeline import PipelineSync
from core.bitflow.processingstep import ProcessingStep
from core.zerops.anomaly_classification.classify_anomaly import ClassifyAnomaly


class PrintStep(ProcessingStep):

    def __init__(self):
        super().__init__()

    def execute(self, sample):
        print(str(sample))


host = "0.0.0.0"
port = 5678

pipeline = PipelineSync()
source = DownloadSource(pipeline, host, port)


classify = ClassifyAnomaly("RNN_GRU_S2S", min_k=4, enable_model_storage=True, enable_message_exchange=True,
                           num_consecutive_predictions=30, max_num_predictions=200)

pipeline.add_processing_step(classify)

source.start_and_wait()
