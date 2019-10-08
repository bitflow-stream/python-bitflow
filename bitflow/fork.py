from bitflow.helper import NotSupportedWarning
from bitflow.pipeline import *
from bitflow.processingstep import *


def initialize_fork(name, script_args):
    fork_steps = Fork.get_all_subclasses(Fork)

    for f in fork_steps:
        if f.__name__.lower() == name.lower() and compare_args(f, script_args):
            logging.info("{} with args: {}  ok ...".format(name, script_args))
            try:
                f_obj = f(**script_args)
            except Exception as e:
                logging.exception(e)
                f_obj = None
            return f_obj
    logging.info("{} with args: {}  failed ...".format(name, script_args))
    return None


def wildcard_compare(wildcard_expression, string):
    import fnmatch
    return fnmatch.fnmatch(string.lower(), wildcard_expression.lower())


def exact_compare(expression, string):
    if expression.lower() == string.lower():
        return True
    return False


class Fork(ProcessingStep, metaclass=helper.CtrlMethodDecorator):
    """
    receives sample and executes fork_decision, if fork decision is true,
    is copied and forwarded to each pipeline given via the constructor. After
    a sample is processed by a subpipeline it will be put returned by the execute function
    as a normal  processing step
    """

    def __init__(self, maxsize=DEFAULT_QUEUE_MAXSIZE, parallel_mode=None):
        super().__init__()
        self.maxsize = maxsize
        self.parallel_mode = parallel_mode
        self.pipeline_steps = []
        self.running_pipelines = {}
        self.merging_step = NoMerging()

    @staticmethod
    def get_all_subclasses(cls):
        all_subclasses = []
        for subclass in cls.__subclasses__():
            if subclass.__name__ not in SUBCLASSES_TO_IGNORE:
                all_subclasses.append(subclass)
            all_subclasses.extend(ProcessingStep.get_all_subclasses(subclass))
        return all_subclasses

    def set_next_step(self, next_step):
        self.merging_step.set_next_step(next_step)  # Mind the merging step in-between

    def add_processing_steps(self, processing_steps=None, names=None):
        if names is None:
            names = []
        if processing_steps is None:
            processing_steps = []
        self.pipeline_steps.append((processing_steps, names))

    def spawn_new_subpipeline(self, processing_steps, name):
        if self.parallel_mode:
            p = PipelineAsync(processing_steps=processing_steps, maxsize=self.maxsize, parallel_mode=self.parallel_mode)
        else:
            p = PipelineSync(processing_steps=processing_steps)
        p.set_next_step(self.merging_step)
        self.running_pipelines[name] = p
        self.running_pipelines[name].start()

    def on_close(self):
        for key in self.running_pipelines:
            self.running_pipelines[key].stop()


class Fork_Tags(Fork):
    supported_compare_methods = ["wildcard", "exact"]

    def __init__(self, tag: str, mode: str = "wildcard", maxsize: int = DEFAULT_QUEUE_MAXSIZE,
                 parallel_mode: str = None):
        super().__init__(maxsize, parallel_mode)
        self.__name__ = "Fork_Tags"
        if mode in self.supported_compare_methods:
            self.mode = mode
        else:
            raise NotSupportedWarning("%s: %s method not supported. Supported methods: %s.", self.__name__, self.mode,
                                      str(self.supported_compare_methods))
        self.tag = tag

    def compare(self, tag_value, names, mode):
        l_cmp = 0
        if mode == "wildcard":
            l_cmp = [name for name in names if wildcard_compare(name, tag_value)]
        if mode == "exact":
            l_cmp = [name for name in names if exact_compare(name, tag_value)]
        if len(l_cmp) > 0:
            return True
        return False

    def execute(self, sample):
        if sample.get_tag(self.tag):
            tag_value = sample.get_tag(self.tag)
            for steps, names in self.pipeline_steps:
                if not self.compare(tag_value, names, self.mode):
                    # if tag value not known, ignore
                    continue
                if tag_value not in self.running_pipelines:
                    self.spawn_new_subpipeline(steps, tag_value)
                self.running_pipelines[tag_value].execute(sample)


# TODO Can be extended for other merging steps that somehow combine samples coming from several sources.
class NoMerging(ProcessingStep):

    def __init__(self):
        super().__init__()

    def execute(self, sample):
        super().write(sample)
