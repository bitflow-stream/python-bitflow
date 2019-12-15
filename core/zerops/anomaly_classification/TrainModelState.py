import logging
import random
import threading

import torch

from zerops.anomaly_classification.PredictionState import PredictionState
from zerops.anomaly_classification.State import State
from zerops.anomaly_classification.models.rnn_gru_s2s import GRUS2S


class TrainModelState(State):
    MODEL_TYPE_RNN_GRU_S2S = "RNN_GRU_S2S"
    MODEL_TYPES = [MODEL_TYPE_RNN_GRU_S2S]

    def __init__(self, model_type, model_parameters, num_epochs, learning_rate, training_dataset):
        super().__init__("Train Model")

        self.model_type = model_type
        self.model_parameters = model_parameters
        self.num_epochs = num_epochs
        self.learning_rate = learning_rate
        self.training_dataset = training_dataset

        self.train_thread = threading.Thread(target=self._execute_model_training)
        self.lock = threading.Lock()
        self.model = None

    def process_sample(self, sample, context):
        if not self.model and not self.train_thread.is_alive():
            self.train_model()
        if self.model:
            self.train_thread.join()
            context.store_model(self.model)
            context.set_state(PredictionState, self.model, self.training_dataset.id2label)

    def train_model(self):
        self.train_thread.start()

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
        self.lock.release()

    def _get_model_instance(self, model_type, model_parameters):
        model_parameters["input_dimensions"] = self.training_dataset.get_num_features()
        model_parameters["num_classes"] = self.training_dataset.get_num_classes()
        if model_type == self.MODEL_TYPE_RNN_GRU_S2S:
            return GRUS2S(**model_parameters)
