import copy
import datetime
import logging
import operator
import random
import threading
import uuid

import torch
import torch.nn.functional as F

from bitflow.sample import Sample

from core.bitflow.processingstep import ProcessingStep
from core.bitflow.sample import Header
from core.zerops.anomaly_classification.models import GRUS2S
from core.zerops.anomaly_classification.trainingdataset import TrainingDataSet, SequenceTooSmall, MaxSequenceLengthReached
from core.zerops.event_bus.EventBusMessage import EventBusMessage
from core.zerops.event_bus.EventHeaders import *
from core.zerops.serialize import JSONSerializer
from core.zerops.TagLib import *


# Not forward, forward train and forward eval must be called

# Header must not change

class ClassifyAnomaly(ProcessingStep):
    MODEL_TYPE_RNN_GRU_S2S = "RNN_GRU_S2S"
    MODEL_TYPES = [MODEL_TYPE_RNN_GRU_S2S]

    CONVERGE_CHECKER_CONSECUTIVE = "consecutive"
    CONVERGE_CHECKERS = [CONVERGE_CHECKER_CONSECUTIVE]

    # k is always referred to as the number of examples per class
    def __init__(self, model_type, model_parameters, num_epochs=100, learning_rate=1e-3, min_k=-1,
                 time_to_wait_for_data=None, learning_key=DEFAULT_LEARNING_TAG, enable_model_storage=False,
                 enable_message_exchange=False, learn_normal_states=False, normal_sequence_length=120,
                 min_sequence_length=50, max_anomaly_sequence_length=768, num_consecutive_predictions=10,
                 max_num_predictions=50):
        if time_to_wait_for_data is None and min_k == -1:
            raise ValueError("At least one of the following parameters must be set: wait_for_train_data and "
                             "min_num_per_class.")
        if model_type in self.MODEL_TYPES:
            raise ValueError("Model type {} is not supported. Supported model types are: {}"
                             .format(model_type, ",".join(self.MODEL_TYPES)))
        super().__init__()
        # The step initially awaits training data. Both parameters (min_k and time_to_wait_for_data) define the waiting
        # behaviour.
        # min_k: The amount of examples per class must be reached.
        # time_to_wait_for_data: It is possible to configure a certain waiting time, before training is started.
        # If waiting time is set, this time will always be waited (even if number of instances per class is already
        # reached)
        # If number of instances per class is set, the step will wait for the required amount of instances, even if
        # the time is reached.
        # At least one of the parameter must be set.
        self.min_k = min_k
        self.time_to_wait_for_data = time_to_wait_for_data
        # When anomaly detection reports false-positive this decides the number of subsequent samples to collect for
        # creating sequence with label "normal"
        self.normal_sequence_length = normal_sequence_length
        self.model_type = model_type
        self.model_parameters = model_parameters
        self.model = None
        self.learning_key = learning_key
        self.learn_normal_states = learn_normal_states
        self.training_dataset = TrainingDataSet(min_sequence_length, max_anomaly_sequence_length)

        # ############ Classification Result Decision Making #############
        self.num_consecutive_predictions = num_consecutive_predictions
        self.max_num_predictions = max_num_predictions
        self.current_prediction_summary = None
        self.last_sent_message = None
        self.pending_results = {}

        # ############ Model Storage #############
        if enable_model_storage:
            self.modelRepo = None  # TODO needs to be implemented (reddis)
        else:
            self.modelRepo = None

        # ############ Event Bus #############
        self.message_serializer = JSONSerializer(ClassificationResultMessage)
        self.eventbus = None
        self.eventHeader = {"type": "rca"}  # TODO move to event header implementation (of rabbitMQ)
        if enable_message_exchange:
            self.eventbus = None  # TODO implement event bus wrapper
            self.eventbus.receiveMessages(self.eventHeader,
                                          self)  # TODO Needs to be implemented as thread listening for messages
        else:
            self.eventbus = None

        # ############ Internal State Parameters #############
        self.collecting_normal_data = False
        self.predicting = False
        self.result_sent = False
        self.initial_time = None

        # ############ Parameters Related to Training #############
        self.num_epochs = num_epochs
        self.learning_rate = learning_rate
        self.train_thread = threading.Thread(target=self._execute_model_training)
        self.lock = threading.Lock()

    # Change: After anomaly was triggered (even if only once), the analysis should be finished.
    # Due to the envisioned ability to classify normal states (detect and prevent FPs)
    def execute(self, sample):
        if not self.initial_time:
            self.model = self.modelRepo.load()  # TODO implement
            self.initial_time = datetime.datetime.now()

        # If currently running training mode and no model was previously trained: Forward samples until training is done
        if self.train_thread.is_alive() and self.model is None:
            super().write(sample)
            return

        if self.model:  # We have a trained model
            sample_out = self.process_sample_trained(sample)
            super().write(sample_out)
        else:
            # Train model if enough training data was collected
            if not self.is_finished_training_data_collection():
                self.train_model()
            # Collect training data
            else:
                self.process_sample_untrained(sample)

        super().write(sample)

    def is_finished_training_data_collection(self):
        number_examples_reached = self.min_k == -1 or self.training_dataset.get_min_number_of_examples() >= self.min_k
        time_passed = datetime.datetime.now() - self.initial_time
        time_reached = self.time_to_wait_for_data is None or time_passed > self.time_to_wait_for_data
        return number_examples_reached and time_reached

    def train_model(self):
        self.train_thread.start()

    # Problem: Model is trained with optima on anomalies, but should learn misclassifications from
    # real anomaly detection. "Online" exchange is not possible due to analysis steps dependency in zerops.
    # How to solve...?
    def process_sample_untrained(self, sample):
        # Anomaly detection reported this anomaly
        if (sample.hasTag(BINARY_PREDICTION_TAG) and sample.getTag(BINARY_PREDICTION_TAG) == ANOMALY) \
                or self.collecting_normal_data:
            label = None
            if self.learning_key and sample.hasTag(self.learning_key):
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
                self.training_dataset.submit_current()

    def process_sample_trained(self, sample):
        sample_result = None
        reset = False
        # Anomaly detection reported this anomaly
        anomaly_reported = sample.hasTag(BINARY_PREDICTION_TAG) and sample.getTag(BINARY_PREDICTION_TAG) == ANOMALY
        if anomaly_reported:
            if not self.predicting:
                self.predicting = True
                reset = True  # Reset whenever a _new_ anomaly situation is encountered
        # Still in prediction mode but result was already send
        # Since the anomaly detection stopped to report the anomaly, we assume it to be resolved at this point
        elif not anomaly_reported and self.predicting and self.result_sent:
            self.predicting = False
            self.result_sent = False

        if self.predicting:
            sample_result, predicted_anomaly, confidence = self._do_prediction(sample, reset)
            if predicted_anomaly and \
                    (not self.result_sent or
                     (self.result_sent and self.prediction_changed(predicted_anomaly))):
                # Method returns true on success, false otherwise
                self.result_sent = self._send_message(sample, predicted_anomaly, confidence)

        return sample_result

    def _do_prediction(self, sample, reset):
        if reset:
            self.current_prediction_summary = PredictionSummary(self.num_consecutive_predictions,
                                                                self.max_num_predictions)
        output = self.model.forward_eval(torch.tensor([[sample.get_metrics()]], dtype=torch.float64), reset)
        metrics = output.tolist()[0][0]
        header = Header([self.training_dataset.id2label[i] for i, _ in enumerate(metrics)])
        result_sample = Sample(header, metrics, sample.timestamp, sample.tags)
        result_sample.add_tag(RESULT, RESULT_CLASSIFICATION)

        self.current_prediction_summary.add_prediction(metrics)
        prediction, confidence = self.current_prediction_summary.check_converged()
        prediction_label = self.training_dataset.id2label[prediction]

        return result_sample, prediction_label, confidence

    def _execute_model_training(self):
        model = self._get_model_instance(self.model_type, self.model_parameters).cpu()
        model.double()
        criterion = torch.nn.CrossEntropyLoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=self.learning_rate, weight_decay=1e-5)

        X_train_normalized, y_train = self.training_dataset.get_training_data()
        for epoch in range(self.num_epochs):
            indices = [*(range(len(X_train_normalized)))]
            random.shuffle(indices)
            mean_epoch_loss = 0
            for i, index in enumerate(indices):
                X = X_train_normalized[index].unsqueeze(0)
                y = y_train[index]
                # ===================forward=====================
                output = model(X)
                output_2D = output.view(-1, output.size(2))
                loss = criterion(output_2D, y)
                # ===================backward====================
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                mean_epoch_loss += loss.data.item()
            # ===================log========================
            logging.info("epoch [{}/{}], mean loss during this epoch: {:.4f}"
                         .format(epoch + 1, self.num_epochs, (mean_epoch_loss / len(indices))))

        self.lock.acquire()
        self.model = model
        self.per_anomaly_sample_counter = 0
        self.lock.release()

    def _get_model_instance(self, model_type, model_parameters):
        model_parameters["input_dimensions"] = self.training_dataset.get_num_features()
        model_parameters["num_classes"] = self.training_dataset.get_num_classes()
        if model_type == self.MODEL_TYPE_RNN_GRU_S2S:
            return GRUS2S(**model_parameters)

    def _send_message(self, predicted_anomaly, confidence, sample=None, source_msg=None):
        if self.eventbus:
            if not predicted_anomaly:
                predicted_anomaly = RESULT_UNKNOWN
            if source_msg:
                header, msg = self._get_header_and_msg_from_msg(predicted_anomaly, confidence, source_msg)
            else:
                id = self._generatePendingModelKey()
                header, msg = self._get_header_and_msg_from_sample(id, predicted_anomaly, confidence, sample)
                self.pending_results[id] = (1, msg, self.current_prediction_summary)

            self.eventbus.publishMessage(header, EventBusMessage().publish_message(msg, self.message_serializer))

    @staticmethod
    def _get_header_and_msg_from_sample(id, predicted_anomaly, confidence, sample):
        msg = ClassificationResultMessage(id, predicted_anomaly, confidence)
        header = {TYPE_KEY: TYPE_ANOMALY_ANALYSIS}
        if sample.hasTag(COMPONENT):
            msg.component = sample.get_tag(COMPONENT)
            header[COMPONENT_KEY] = sample.get_tag(COMPONENT)
        else:
            header[COMPONENT_KEY] = COMPONENT_UNKNOWN
        if sample.hasTag(NODE):
            msg.node = sample.get_tag(NODE)
        if sample.hasTag(VM):
            msg.vm = sample.get_tag(VM)
        if sample.hasTag(SERVICE):
            msg.service = sample.get_tag(SERVICE)
        return header, msg

    @staticmethod
    def _get_header_and_msg_from_msg(predicted_anomaly, confidence, source_msg):
        msg = copy.deepcopy(source_msg)
        msg.feedback = False
        msg.label = predicted_anomaly
        msg.confidence = confidence
        header = {TYPE_KEY: TYPE_ANOMALY_ANALYSIS}
        if source_msg.component:
            header[COMPONENT_KEY] = source_msg.component
        else:
            header[COMPONENT_KEY] = COMPONENT_UNKNOWN
        return header, msg

    def prediction_changed(self, predicted_anomaly):
        if not self.last_sent_message:
            return True
        last_reported_label = self.last_sent_message.label
        return predicted_anomaly != last_reported_label

    def _generatePendingModelKey(self):
        while True:
            id = uuid.UUID()
            if id not in self.pending_results.keys():
                break
        return id


class PredictionSummary:

    def __init__(self, num_consecutives, max_num_of_predictions):
        self.num_consecutives = num_consecutives
        self.max_num_of_predictions = max_num_of_predictions
        # Counts the number of consecutive predictions. Will be reset if prediction class changes.
        self.current_consecutive_class = {"label": -1, "consecutive_counter": 0, "confidence_sum": 0}
        self.num_predictions = 0
        self.predictions = list()
        self.scores = []
        self.base_parameter = 0.95

    def add_prediction(self, prediction):
        self.num_predictions += 1
        index, confidence = max(enumerate(prediction), key=operator.itemgetter(1))
        if self.current_consecutive_class["label"] == index:
            self.current_consecutive_class["consecutive_counter"] += 1
            self.current_consecutive_class["confidence_sum"] += confidence
        else:
            self._reset_current_consecutive_class()
        self.predictions.append(prediction)

    def _reset_current_consecutive_class(self):
        self.current_consecutive_class["label"] = -1
        self.current_consecutive_class["consecutive_counter"] = 0
        self.current_consecutive_class["confidence_sum"] = 0

    def check_converged(self):
        if self.num_predictions >= self.max_num_of_predictions:
            return self._check_long_term_converge()
        else:
            return self._check_short_term_converge()

    def _check_short_term_converge(self):
        if not self.current_consecutive_class:
            return None
        if self.current_consecutive_class["consecutive_counter"] >= self.num_consecutives:
            avg_confidence = self.current_consecutive_class["confidence_sum"] / \
                             self.current_consecutive_class["consecutive_counter"]
            return self.current_consecutive_class["label"], avg_confidence

    def _check_long_term_converge(self):
        if self.num_predictions < self.max_num_of_predictions:
            return None, None
        T = len(self.predictions)
        for t, prediction in enumerate(self.predictions):
            index, _ = max(enumerate(prediction), key=operator.itemgetter(1))
            self.scores[index] *= self.base_parameter ** (T - (t + 1))
        self.scores = F.softmax(torch.tensor(self.scores, dtype=torch.float64), dim=0).tolist()
        top_index, top_confidence = max(enumerate(self.scores), key=operator.itemgetter(1))
        return top_index, top_confidence

    def get_n_best_prediction(self, n, skip_indices=None):
        if n > len(self.scores):
            raise ValueError("n cannot be greater than the number of predicted classes. Maximal value for n can be {} "
                             "but n is actually {}".format(len(self.scores), n))
        if not skip_indices:
            skip_indices = []
        n -= 1
        max_index, max_score = -1, -1
        for i, score in enumerate(self.scores):
            if i not in skip_indices and score > max_score:
                max_index, max_score = i, score
        if n == 0:
            return max_index, max_score
        else:
            skip_indices.append(max_index)
            return self.get_n_best_prediction(n, skip_indices)

    def handleEventBusMessage(self):  # TODO implement
        pass


class ClassificationResultMessage:

    def __init__(self, id, label, confidence, feedback=False):
        self.id = id
        self.label = label
        self.confidence = confidence
        self.feedback = feedback
        self.component = ""
        self.node = ""
        self.vm = ""
        self.service = ""

    def __str__(self):
        return "{}: label={} confidence={} id={} node={} vm={} service={} isFeedback={}" \
            .format(type(self), self.label, self.confidence, self.id, self.node, self.vm,
                    self.service, self.feedback)
