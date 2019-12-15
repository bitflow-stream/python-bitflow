import operator
import uuid
from random import randint

import torch
import torch.nn.functional as F

from zerops.anomaly_classification.State import State
from zerops.TagLib import *


class PredictionState(State):

    def __init__(self, num_consecutive_predictions, max_num_predictions, model, id2label):
        super().__init__("Prediction")
        self.model = model
        self.model.eval()
        self.model.double()
        self.id2label = id2label

        self.num_consecutive_predictions = num_consecutive_predictions
        self.max_num_predictions = max_num_predictions

        self.current_prediction_summary = None

        self.last_label = ""
        self.predicting = False
        self.result_sent = False
        self.pending_results = {}

    def process_sample(self, sample, context):
        sample_result = None
        reset = False
        # Anomaly detection reported this anomaly
        anomaly_reported = sample.has_tag(BINARY_PREDICTION_TAG) and sample.get_tag(BINARY_PREDICTION_TAG) == ANOMALY
        if anomaly_reported:
            if not self.predicting:
                self.predicting = True
                reset = True  # Reset whenever a _new_ anomaly situation is encountered
        # Still in prediction mode but result was already sent
        # Since the anomaly detection stopped to report the anomaly, we assume it to be resolved at this point
        elif not anomaly_reported and self.predicting and self.result_sent:
            self.predicting = False
            self.result_sent = False

        if self.predicting:
            metrics, labels, predicted_anomaly, confidence = self._do_prediction(sample, reset)
            pred_changed = self.prediction_changed(predicted_anomaly)
            if predicted_anomaly and (not self.result_sent or (self.result_sent and pred_changed)):
                # Method returns true on success, false otherwise
                self.result_sent = self._send_message(context, self.current_prediction_summary,
                                                      predicted_anomaly, confidence, sample)
            if metrics and labels:
                context.process_prediction_results(metrics, labels)

    def _do_prediction(self, sample, reset):
        if reset:
            self.current_prediction_summary = PredictionSummary(self.num_consecutive_predictions,
                                                                self.max_num_predictions)
        output = self.model.forward_eval(torch.tensor([[sample.get_metrics()]], dtype=torch.float64), reset)
        metrics = output.tolist()[0][0]
        labels = [self.id2label[i] for i, _ in enumerate(metrics)]
        self.current_prediction_summary.add_prediction(metrics)
        prediction, confidence = self.current_prediction_summary.check_converged()
        prediction_label = None
        if prediction:
            prediction_label = self.id2label[prediction]
            self.last_label = prediction_label

        return metrics, labels, prediction_label, confidence

    def prediction_changed(self, predicted_anomaly):
        if not self.last_label:
            return True
        return predicted_anomaly != self.last_label

    def _send_message(self, context, prediction_summary, predicted_anomaly, confidence, sample):
        if not predicted_anomaly:
            predicted_anomaly = RESULT_UNKNOWN
        result_id = self._generate_uuid()
        self.pending_results[result_id] = (1, prediction_summary)
        return context.send_message(result_id, predicted_anomaly, confidence, sample=sample)

    def _generate_uuid(self):
        while True:
            msg_id = randint(1, 1000000)
            if msg_id not in self.pending_results.keys():
                break
        return msg_id

    def process_feedback(self, msg_id, feedback):
        pass


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
            self._reset_current_consecutive_class(index)
        self.predictions.append(prediction)

    def _reset_current_consecutive_class(self, class_index):
        self.current_consecutive_class["label"] = class_index
        self.current_consecutive_class["consecutive_counter"] = 0
        self.current_consecutive_class["confidence_sum"] = 0

    def check_converged(self):
        if self.num_predictions >= self.max_num_of_predictions:
            return self._check_long_term_converge()
        else:
            return self._check_short_term_converge()

    def _check_short_term_converge(self):
        if not self.current_consecutive_class:
            return None, None
        current_consecutive_counter = self.current_consecutive_class["consecutive_counter"]
        nc = self.num_consecutives
        if current_consecutive_counter >= nc:
            cs = self.current_consecutive_class["confidence_sum"]
            avg_confidence = cs / current_consecutive_counter
            return self.current_consecutive_class["label"], avg_confidence
        return None, -1

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
