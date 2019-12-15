import copy
import logging
import re
import typing

from bitflow.sample import Sample

from bitflow.processingstep import *
from bitflow.sample import Header
from zerops.TagLib import *
from zerops.anomaly_classification.CollectTrainingDataState import CollectTrainingDataState
from zerops.anomaly_classification.PredictionState import PredictionState
from zerops.anomaly_classification.TrainModelState import TrainModelState
from zerops.event_bus.EventBus import EventBus
from zerops.event_bus.EventBusMessage import EventBusMessage
from zerops.event_bus.EventHeaders import *
from zerops.model_repository.BinaryModelRepository import BinaryModelRepository
from zerops.serialize.JSONSerializer import JSONSerializer
from zerops.serialize.PickleSerializer import PickleSerializer


class ClassifyAnomaly(ProcessingStep):
    DEFAULT_KEY_PATTERN = "(-[0-9]*$|-[0-9]*.ims4$)"

    # k is always referred to as the number of examples per class
    def __init__(self, model_type: str, model_parameters: dict = None, num_epochs: int = 100,
                 learning_rate: float = 1e-3, min_k: int = -1, time_to_wait_for_data: int = None,
                 learning_key: str = DEFAULT_LEARNING_TAG, enable_model_storage: bool = False,
                 model_storage_tag: str = "group", key_pattern: str = None, enable_message_exchange: bool = False,
                 learn_normal_states: bool = False, normal_sequence_length: int = 120, min_sequence_length: int = 50,
                 max_anomaly_sequence_length: int = 768, num_consecutive_predictions: int = 10,
                 max_num_predictions: int = 50):
        if time_to_wait_for_data is None and min_k == -1:
            raise ValueError("At least one of the following parameters must be set: wait_for_train_data and "
                             "min_num_per_class.")
        if model_type not in TrainModelState.MODEL_TYPES:
            raise ValueError("Model type {} is not supported. Supported model types are: {}"
                             .format(model_type, ",".join(TrainModelState.MODEL_TYPES)))
        super().__init__()

        # ############ State which control the behavior of the step #############
        self.state = None

        # ############ Collect Training Data State Parameters #############
        self.time_to_wait_for_data = time_to_wait_for_data
        self.min_k = min_k
        self.learn_normal_states = learn_normal_states
        self.normal_sequence_length = normal_sequence_length
        self.learning_key = learning_key
        self.min_sequence_length = min_sequence_length
        self.max_anomaly_sequence_length = max_anomaly_sequence_length

        # ############ Train Model State Parameters #############
        self.model_type = model_type
        self.model_parameters = {} if model_parameters is None else model_parameters
        self.num_epochs = num_epochs
        self.learning_rate = learning_rate
        self.key_pattern = key_pattern if key_pattern else self.DEFAULT_KEY_PATTERN
        self.model_storage_tag = model_storage_tag

        # ############ Prediction State Parameters #############
        self.num_consecutive_predictions = num_consecutive_predictions
        self.max_num_predictions = max_num_predictions

        # ############ Model Storage #############
        if enable_model_storage:
            try:
                self.model_repo = BinaryModelRepository(PickleSerializer(), step_name="ClassifyAnomaly")
            except Exception as e:
                logging.warning("Error while trying to connect to model repository. Wont be able to store/load models",
                                exc_info=e)
                self.model_repo = None
        else:
            self.model_repo = None
        self.key = None

        # ############ Event Bus #############
        self.enable_message_exchange = enable_message_exchange
        if enable_message_exchange:
            try:
                self.message_serializer = JSONSerializer(ClassificationResultMessage)
                self.eventHeader = {MATCH_KEY: MATCH_ANY}
                self.eventHeader = {TYPE_KEY: TYPE_RCA}
                self.eventHeader = {TYPE_KEY: TYPE_ANOMALY_ANALYSIS_FEEDBACK}
                self.eventbus = EventBus()
            except Exception as e:
                logging.warning("Error occurred while trying to connect to rabbitmq. No message transfer will be used.",
                                exc_info=e)
                self.eventbus = None
            # if self.eventbus:
            #     try:
            #         self.eventbus.receive_messages(self.eventHeader, self.receive_message)
            #     except Exception as e:
            #         logging.warning("Error while configuring receive message channel. Unable to receive messages.",
            #                         exc_info=e)
        else:
            self.eventbus = None

        # ############ Runtime state #############
        self.current_sample = None
        self.header = None

    # Change: After anomaly was triggered (even if only once), the analysis should be finished.
    # Due to the envisioned ability to classify normal states (detect and prevent FPs)
    def execute(self, sample):
        if not self.header:
            self.header = sample.header
        elif sample.header_changed(self.header):
            logging.warning("Header changed from %s to %s. Cannot perform classification with current model.",
                            self.header.metric_names.join(","), sample.header.metric_names.join(","))
            super().write(sample)
            return

        self.current_sample = sample
        if self.state is None:  # Initial state switch
            logging.info("Received first sample. Switching from initial state to some sample processing state...")
            key = self._generate_key(sample)
            model = None
            if self.model_repo:
                logging.info("Trying to load model for key {} ...".format(key))
                model = self.model_repo.load_latest(key)
                if self.header.has_changed(Header(model.header)):
                    logging.warning("Header from loaded model and header of current sample stream are not the same.\n"
                                    "Header from sample stream: %s\n"
                                    "Header from loaded model: %s\n"
                                    "Cannot perform classification with loaded model.",
                                    self.header.metric_names.join(","), model.header.join(","))
                    model = None
            if model:
                logging.info("Successfully loaded model for key {}.".format(key))
                self.state = PredictionState(self.num_consecutive_predictions, self.max_num_predictions,
                                             model.model, model.id2label)
            else:
                logging.info("No model for key {} available.".format(key))
                self.state = CollectTrainingDataState(self.time_to_wait_for_data, self.min_k, self.learning_key,
                                                      self.learn_normal_states, self.normal_sequence_length,
                                                      self.min_sequence_length, self.max_anomaly_sequence_length)
        self.state.process_sample(sample, self)
        super().write(sample)

    def set_state(self, state_class, **kwargs):
        if state_class == CollectTrainingDataState:
            self.state = CollectTrainingDataState(self.time_to_wait_for_data, self.min_k, self.learning_key,
                                                  self.learn_normal_states, self.normal_sequence_length,
                                                  self.min_sequence_length, self.max_anomaly_sequence_length)
        elif state_class == TrainModelState:
            self.state = TrainModelState(self.model_type, self.model_parameters, self.num_epochs,
                                         self.learning_rate, **kwargs)
        elif state_class == PredictionState:
            self.state = PredictionState(self.num_consecutive_predictions, self.max_num_predictions, **kwargs)
        else:
            logging.warning("Unknown state {}. Cannot switch state. Staying in state {}."
                            .format(state_class, self.state))

    def _generate_key(self, sample):
        result = "unknown_type"
        if sample.has_tag("group"):
            result = sample.get_tag("group")
        elif sample.has_tag(VM):
            result = re.sub(self.key_pattern, '', sample.get_tag(VM))
        elif sample.has_tag(NODE):
            result = sample.get_tag(NODE)
        return result

    def process_prediction_results(self, metrics, labels):
        labels = ["similarity-{}".format(label) for label in labels]
        header = Header(labels)
        result_sample = Sample(header, metrics, self.current_sample.timestamp, self.current_sample.tags)
        result_sample.add_tag(RESULT, RESULT_CLASSIFICATION)
        super().write(result_sample)

    def store_model(self, model, id2label):
        logging.info("Storing model under key {}...".format(self.key))
        try:
            self.model_repo.store(self.key, ModelWrapper(model, id2label, self.header))
        except Exception as e:
            logging.warning("Failed to store model under key {}.".format(self.key), exc_info=e)

    def send_message(self, result_id, predicted_anomaly, confidence, sample=None, source_msg=None):
        if self.eventbus:
            if not predicted_anomaly:
                predicted_anomaly = RESULT_UNKNOWN
            if source_msg:
                header, msg = self._get_header_and_msg_from_msg(predicted_anomaly, confidence, source_msg)
            else:
                header, msg = self._get_header_and_msg_from_sample(result_id, predicted_anomaly, confidence, sample)

            # TODO some fucked up shit but it wont work with the self.eventbus for whatever reasons
            if self.enable_message_exchange:
                event_bus = EventBus()
                if event_bus:
                    payload = EventBusMessage(msg, self.message_serializer)
                    event_bus.publish_message(header, payload)

            return True
        return True

    @staticmethod
    def _get_header_and_msg_from_sample(msg_id, predicted_anomaly, confidence, sample):
        msg = ClassificationResultMessage(msg_id, predicted_anomaly, confidence)
        header = {TYPE_KEY: TYPE_ANOMALY_ANALYSIS}
        if sample.has_tag(COMPONENT):
            msg.component = sample.get_tag(COMPONENT)
            # header[COMPONENT_KEY] = sample.get_tag(COMPONENT)
        # else:
        #    header[COMPONENT_KEY] = COMPONENT_UNKNOWN
        if sample.has_tag(NODE):
            msg.node = sample.get_tag(NODE)
        if sample.has_tag(VM):
            msg.vm = sample.get_tag(VM)
        if sample.has_tag(SERVICE):
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

    def receive_message(self, header, message):
        msg_type = header[TYPE_KEY] if TYPE_KEY in header else None
        if msg_type == TYPE_RCA:
            self.process_rca()
        elif msg_type == TYPE_ANOMALY_ANALYSIS_FEEDBACK and isinstance(self.state, PredictionState):
            self.process_feedback()
            msg_id = message.msg_id
            predicted_anomaly, confidence = self.state.process_feedback(msg_id, message.feedback)
            if predicted_anomaly:
                self.send_message(msg_id, predicted_anomaly, confidence, message)

    def process_rca(self):
        pass

    def process_feedback(self):
        pass


class ModelWrapper:

    def __init__(self, model, id2label, header):
        self.model = model
        self.id2label = id2label
        self.header = header


class ClassificationResultMessage:

    def __init__(self, id, label, confidence, feedback=0):
        self.id = id
        self.label = label
        self.confidence = confidence
        self.component = ""
        self.node = ""
        self.vm = ""
        self.service = ""
        self.feedback = feedback

    def __str__(self):
        feedback = "UNKNOWN_FEEDBACK_TYPE"
        if feedback in ANOMALY_ANALYSIS_FEEDBACK_TYPES:
            feedback = ANOMALY_ANALYSIS_FEEDBACK_TYPES[self.feedback]
        return "{}: label={} confidence={} id={} node={} vm={} service={} feedback={}" \
            .format(type(self), self.label, self.confidence, self.id, self.node, self.vm,
                    self.service, feedback)


class SimpleMessage:
    def __init__(self, message):
        self.message = message
