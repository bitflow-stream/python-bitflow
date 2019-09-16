import copy

from bitflow.helper import NotSupportedWarning
from bitflow.pipeline import *
from bitflow.processingstep import *


def initialize_fork(name, script_args):
    fork_steps = Fork.subclasses

    for f in fork_steps:
        print(name.lower())
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


class Fork(ProcessingStep):
    """
    receives sample and executes fork_decision, if fork decision is true,
    is copied and forwarded to each pipeline given via the constructor. After
    a sample is processed by a subpipeline it will be put returned by the execute function
    as a normal  processing step
    """

    class _ForkPipelineTerminator(ProcessingStep):
        __name__ = "SubPipelineTerminator"
        __description__ = "Terminates a subpipeline by forwarding samples to next ps in the outer pipeline. " \
                          "Goal Seperate Threads, Processes for each Pipeline"

        def __init__(self, fork_ps, outer_pipeline):
            super().__init__()
            self.op = outer_pipeline
            self.fork_ps = fork_ps

        def execute(self, sample):
            self.write(sample)

        def write(self, sample):
            self.op.execute_after(sample, self.fork_ps)

    subclasses = []

    def __init__(self, outer_pipeline=None):
        super().__init__()
        self.fork_pipelines = []
        self.running_pipelines = {}
        self.outer_pipeline = outer_pipeline

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses.append(cls)

    # current workaround for bitflow script, parse names and processing steps for new
    # subpipeline. generate new pipeline if required by incoming sample
    def add_processing_steps(self, processing_steps=None, names=None):
        if names is None:
            names = []
        if processing_steps is None:
            processing_steps = []
        self.fork_pipelines.append((processing_steps, names))

    def remove_pipeline(self, i: int):
        self.fork_pipelines.remove(i)

    def get_pipelines(self):
        return self.fork_pipelines

    def spawn_new_subpipeline(self, ps_list, name):
        p = Pipeline(multiprocessing_input=False)
        for ps in ps_list:
            p.add_processing_step(ps)
        p.start()
        if self.outer_pipeline:
            p.add_processing_step(self._ForkPipelineTerminator(
                fork_ps=self,
                outer_pipeline=self.outer_pipeline))
        self.running_pipelines[name] = p
        return p

    def execute(self, sample):
        raise NotImplementedError

    def on_close(self):
        super().on_close()
        for p in list(self.running_pipelines.keys()):
            self.running_pipelines[p].stop()


class Fork_Tags(Fork):
    supported_compare_methods = ["wildcard", "exact"]

    def __init__(self, tag: str, mode: str = "wildcard"):
        super().__init__(outer_pipeline=None)
        self.__name__ = "Fork_Tags"
        if mode in self.supported_compare_methods:
            self.mode = mode
        else:
            raise NotSupportedWarning("{}: {} method not supported. Only support {}".format(
                self.__name__, self.mode,
                self.supported_compare_methods))
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

    def set_root_pipeline(self, outer_pipeline):
        self.outer_pipeline = outer_pipeline

    def execute(self, sample):
        if sample.get_tag(self.tag):
            tag_value = sample.get_tag(self.tag)
            for processing_steps_list, names in self.fork_pipelines:
                if not self.compare(tag_value, names, self.mode):
                    # if tag value not known, ignore
                    continue
                if tag_value in self.running_pipelines:
                    p = self.running_pipelines[tag_value]
                else:
                    p = self.spawn_new_subpipeline(processing_steps_list, tag_value)
                s = copy.deepcopy(sample)
                p.execute(s)
