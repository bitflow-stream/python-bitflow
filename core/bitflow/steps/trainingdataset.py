import logging

import torch as torch


class SequenceTooSmall(Exception):
    pass


# Train with inbalanced dataset
#   Sub-sample
#   Weight loss accordingly

class TrainingDataSet:

    def __init__(self, min_sequence_length=50):
        super().__init__()
        self.min_sequence_length = min_sequence_length

        self.id2label = {}
        self.label2id = {}

        # ############ Actual dataset #############
        self.ids = []
        self.data = []

        # ############ Internal State Parameters #############
        self.current_metrics = None  # Tensor of size (S, N) --> S is sequence length, N is number of features
        self.current_label = None

    def update_sequence(self, label, metrics):
        if self.current_label != label:
            logging.info("Switched from label {} to {}.".format(self.current_label, label))
            if self.current_label:
                self.submit_current()
            self.current_label = label
            self.current_metrics = torch.tensor([metrics])

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
            else:
                i = self.label2id[label]
            self.ids.append(i)
            self.data.append(data)
        self.current_metrics = None
        self.current_label = None

    def _get_next_label_id(self):
        return max(self.ids) + 1
