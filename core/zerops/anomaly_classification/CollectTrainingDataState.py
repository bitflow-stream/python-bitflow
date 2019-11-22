import datetime
import logging

from core.zerops.anomaly_classification.State import State
from core.zerops.TagLib import *
from core.zerops.anomaly_classification.TrainModelState import TrainModelState
from core.zerops.anomaly_classification.trainingdataset import TrainingDataSet, SequenceTooSmall, \
    MaxSequenceLengthReached


# The step initially awaits training data. Both parameters (min_k and time_to_wait_for_data) define the waiting
# behaviour.
# min_k: The amount of examples per class must be reached.
# time_to_wait_for_data: It is possible to configure a certain waiting time, before training is started.
# If waiting time is set, this time will always be waited (even if number of instances per class is already
# reached)
# If number of instances per class is set, the step will wait for the required amount of instances, even if
# the time is reached.
# At least one of the parameter must be set.
class CollectTrainingDataState(State):

    # k is always referred to as the number of examples per class
    def __init__(self, time_to_collect_data, min_k, learning_key, learn_normal_states, normal_sequence_length,
                 min_sequence_length, max_sequence_length):
        super().__init__("Collect Training Data")

        self.time_to_collect_data = time_to_collect_data
        self.min_k = min_k
        self.learning_key = learning_key
        # When anomaly detection reports false-positive this decides the number of subsequent samples to collect for
        # creating sequence with label "normal"
        self.learn_normal_states = learn_normal_states
        self.normal_sequence_length = normal_sequence_length

        self.training_dataset = TrainingDataSet(min_sequence_length, max_sequence_length)
        self.initial_time = None

        # State at runtime
        self.per_anomaly_sample_counter = 0
        self.collecting_normal_data = False

    # Problem: Model is trained with optima on anomalies, but should learn misclassifications from
    # real anomaly detection. "Online" exchange is not possible due to analysis steps dependency in zerops.
    # How to solve...?
    def process_sample(self, sample, context):
        # Anomaly detection reported this anomaly
        if (sample.has_tag(BINARY_PREDICTION_TAG) and sample.get_tag(BINARY_PREDICTION_TAG) == ANOMALY) \
                or self.collecting_normal_data:
            label = None
            if self.learning_key and sample.has_tag(self.learning_key):
                # This is a real anomaly
                label = sample.getTag(self.learning_key)
                self.collecting_normal_data = False
            elif self.learn_normal_states:
                # This is a false-positive
                label = NORMAL
                self.collecting_normal_data = True
            if label:
                try:
                    self.training_dataset.update_sequence(label, sample.get_metrics())
                except SequenceTooSmall as e:
                    logging.warning("Switching label to {} was too early. Sequence labeled as {} was too small "
                                    "to be used as learning data.".format(label, label), exc_info=e)
                except MaxSequenceLengthReached as e:
                    logging.info("Current sequence labeled as {} reached its maximum length."
                                 .format(label), exc_info=e)
                    if self.collecting_normal_data:
                        self.collecting_normal_data = False
                # We collected enough normal samples for this sequence
                if self.collecting_normal_data and self.per_anomaly_sample_counter >= self.normal_sequence_length:
                    self.training_dataset.submit_current()
                    self.collecting_normal_data = False
        else:
            if self.training_dataset.training_data_pending():
                try:
                    self.training_dataset.submit_current()
                except SequenceTooSmall as e:
                    logging.warning("Pending sequence labeled as {} was too short to be used as learning data."
                                    .format(self.training_dataset.current_label), exc_info=e)
            self.per_anomaly_sample_counter = 0

        if self.is_finished_training_data_collection():
            context.setState(TrainModelState, self.training_dataset)

    def is_finished_training_data_collection(self):
        number_examples_reached = self.min_k == -1 or self.training_dataset.get_min_number_of_examples() >= self.min_k
        time_passed = datetime.datetime.now() - self.initial_time
        time_reached = self.time_to_collect_data is None or time_passed > self.time_to_collect_data
        return number_examples_reached and time_reached
