import logging

import torch as torch


class SequenceTooSmall(Exception):
    pass


class MaxSequenceLengthReached(Exception):
    pass


# Train with inbalanced dataset
#   Sub-sample
#   Weight loss accordingly

class TrainingDataSet:

    def __init__(self, min_sequence_length, max_sequence_length):
        super().__init__()
        self.min_sequence_length = min_sequence_length
        self.max_sequence_length = max_sequence_length

        # ############ For Convenience #############
        self.id2label = {}
        self.label2id = {}
        self.class_counter = {}

        # ############ Actual Dataset #############
        self.ids = []
        self.data = []

        # ############ Metadata #############
        self.num_features = -1

        # ############ Related to normalization #############
        self.min_max_tensors = {"min": None, "max": None}

        # ############ Internal State Parameters #############
        self.current_metrics = None  # Tensor of size (S, N) --> S is sequence length, N is number of features
        self.current_label = None

    def update_sequence(self, label, metrics):
        if self.num_features == -1:  # Number of features within each step of the sequence
            self.num_features = len(metrics)
        if self.num_features != len(metrics):  # Number of features must not change. Otherwise we get a problem.
            logging.warning("Number of features per timestamp in sequence with label {} changed from {} to {}. "
                            "Changed feature vector will be dropped.".format(label, self.num_features, len(metrics)))
            return
        if self.current_label != label:
            logging.info("Switched from label {} to {}.".format(self.current_label, label))
            if self.current_label:
                self.submit_current()
            self.current_label = label
            self.current_metrics = torch.tensor([metrics])
        elif self.current_metrics.size(0) < self.max_sequence_length:
            self.current_metrics = torch.cat((self.current_metrics, torch.tensor([metrics])), 0)
            if self.current_metrics.size(0) >= self.max_sequence_length:
                self.submit_current()
                raise MaxSequenceLengthReached("Current sequence labeled as {} reached its maximum allowed length "
                                               "of {}. It was submitted to learning data."
                                               .format(self.current_label, self.current_metrics.size(0)))

    def training_data_pending(self):
        return self.current_label is not None

    def submit_current(self):
        if self.current_label and self.current_metrics is not None:
            logging.info("Submitting sequence of size {} labeled as {} to training dataset."
                         .format(self.current_metrics.size(0), self.current_label))
            self._submit(self.current_label, self.current_metrics)
        else:
            logging.info("No current data to submit.")

    def _submit(self, label, data):
        # Minimum required sequence length was not reached
        if data.size(0) < self.min_sequence_length:
            raise SequenceTooSmall("Sequence with label {} is too small and will not be used for training. Expected "
                                   "minimum length is {} but current sequence has length {}."
                                   .format(label, self.min_sequence_length, data.size(0)))
        else:
            # Label already known
            if label not in self.label2id.keys():
                i = self._get_next_label_id()
                self.id2label[i] = label
                self.label2id[label] = i
            else:
                i = self.label2id[label]
            self.ids.append(i)
            self.data.append(data)
            self.class_counter[label] += 1
        self.current_metrics = None
        self.current_label = None

    def _get_next_label_id(self):
        return max(self.ids) + 1 if self.ids else 0

    def get_min_number_of_examples(self):
        if self.class_counter:
            return min(self.class_counter)
        else:
            return 0

    def get_num_features(self):
        return self.num_features

    def get_num_classes(self):
        return len(self.class_counter.keys())

    def get_training_data(self):
        return None, None
