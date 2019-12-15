import logging
import os
import time

import torch

from zerops.anomaly_classification.classify_anomaly import ModelWrapper
from zerops.anomaly_classification.models.rnn_gru_s2s import GRUS2S
from zerops.model_repository.BinaryModelRepository import BinaryModelRepository
from zerops.serialize.PickleSerializer import PickleSerializer

directory = "/home/alex/data/huaweidata/test/models"

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def get_pytorch_models(directory):
    result = {}
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".pth"):
                group_name = os.path.splitext(file)[0]
                result[group_name] = os.path.join(root, file)
    return result


step_name = "ClassifyAnomaly"
serializer_pickle = PickleSerializer()
repo = BinaryModelRepository(serializer_pickle, step_name=step_name)

pytorch_model_paths = get_pytorch_models(directory)

for group_name, path in pytorch_model_paths.items():
    state = torch.load(path)
    model = GRUS2S(**state["model_parameters"])
    mw = ModelWrapper(model, state["id2label"], state["header"])
    repo.store(group_name, mw)
time.sleep(5)
repo.close()
