import logging

from core.bitflow.processingstep import ProcessingStep
from core.bitflow.steps.trainingdataset import TrainingDataSet, SequenceTooSmall
from core.zerops.TagLib import *


# Not forward, forward train and forward eval must be called

class WrapPytroch(ProcessingStep):
    MODEL_TYPE_RNN_GRU = "RNN_GRU"
    MODEL_TYPES = [MODEL_TYPE_RNN_GRU]

    def __init__(self, model_type, c, n, learning_key=DEFAULT_LEARNING_TAG, enable_model_storage=False,
                 enable_message_exchange=False, normal_sequence_length = 120):
        # Number of classes
        # Number of learning instances per class
        # Model type
        super().__init__()
        self.c = c
        self.n = n
        # When anomaly detection reports false-positive this decides the number of subsequent samples to collect for
        # creating sequence with label "normal"
        self.normal_sequence_length = normal_sequence_length
        self.model_type = model_type
        self.model = None
        self.learning_key = learning_key
        self.training_dataset = TrainingDataSet()

        # ############ Model Storage #############
        if enable_model_storage:
            self.modelRepo = None  # TODO needs to be implemented (reddis)
        else:
            self.modelRepo = None

        # ############ Event Bus #############
        self.eventHeader = {"type": "rca"}  # TODO move to event header implementation (of rabbitMQ)
        if enable_message_exchange:
            self.eventbus = None  # TODO implement event bus wrapper
            self.eventbus.receiveMessages(self.eventHeader, self)  # TODO Needs to be implemented as thread listening for messages
        else:
            self.eventbus = None

        # ############ Internal State Parameters #############
        self.overall_sample_counter = 0
        self.per_anomaly_sample_counter = 0
        self.result_sent = False
        self.trained = False
        self.normal_data_collection = False

    # Change: After anomaly was triggered (even if only once), the analysis should be finished.
    # Due to the envisioned ability to classify normal states (detect and prevent FPs)
    def execute(self, sample):
        if self.overall_sample_counter == 0 and self.model_type is not None:
            self.model = self.modelRepo.load()

        if self.trained:
            sample = self.process_sample_trained(sample)
        else:
            self.process_sample_untrained(sample)

        if sample.hasTag(self.learning_key) and not self.result_sent and sample.hasTag(BINARY_PREDICTION_TAG) and \
                sample.getTag(BINARY_PREDICTION_TAG) == ANOMALY:
            self.write_status_sample()

        self.overall_sample_counter += 1
        super().write(sample)

    # Problem: Model is trained with optima on anomalies, but should learn misclassifications from
    # real anomaly detection. "Online" exchange is not possible due to analysis steps dependency in zerops.
    # How to solve...?
    def process_sample_untrained(self, sample):
        # Anomaly detection reported this anomaly
        if (sample.hasTag(BINARY_PREDICTION_TAG) and sample.getTag(BINARY_PREDICTION_TAG) == ANOMALY) \
                or self.normal_data_collection:
            self.per_anomaly_sample_counter += 1
            modelKey = "test"  # TODO implement model key generation (see Java)
            if self.learning_key and sample.hasTag(self.learning_key):
                # This is a real anomaly
                label = sample.getTag(self.learning_key)
            else:
                # This is a false-positive
                label = NORMAL
                self.normal_data_collection = True

            try:
                self.training_dataset.update_sequence(label, sample.get_metrics())
            except SequenceTooSmall as e:
                logging.warning("Switch of label to {} was too early. Sequence labeled as {} was too small "
                                "to be used as learning data.".format(label, label), exc_info=e)
                self.per_anomaly_sample_counter = 0

            self.per_anomaly_sample_counter += 1

            if self.normal_data_collection and self.per_anomaly_sample_counter >= self.normal_sequence_length:
                self.normal_data_collection = False
                self.training_dataset.submit_current()
                self.per_anomaly_sample_counter = 0
        else:
            self.per_anomaly_sample_counter = 0
            self.training_dataset.submit_current()

    def process_sample_trained(self, sample):
        return sample

    def write_status_sample(self, is_result=False):
        pass

